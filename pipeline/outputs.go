/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# The Initial Developer of the Original Code is the Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2012
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
#   Rob Miller (rmiller@mozilla.com)
#
# ***** END LICENSE BLOCK *****/
package pipeline

import (
	"encoding/json"
	"fmt"
	"github.com/crankycoder/g2s"
	"github.com/rafrombrc/go-notify"
	. "heka/message"
	"log"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Output interface {
	Plugin
	Deliver(pipelinePack *PipelinePack)
}

type LogOutput struct {
}

func (self *LogOutput) Init(config interface{}) error {
	return nil
}

func (self *LogOutput) Deliver(pipelinePack *PipelinePack) {
	log.Printf("%+v\n", *(pipelinePack.Message))
}

type CounterOutput struct {
}

type CounterGlobal struct {
	count chan uint
	once  sync.Once
}

var counterGlobal CounterGlobal

func InitCountChan() {
	counterGlobal.count = make(chan uint, 30000)
	go counterLoop()
}

func (self *CounterOutput) Init(config interface{}) error {
	counterGlobal.once.Do(InitCountChan)
	return nil
}

func (self *CounterOutput) Deliver(pipelinePack *PipelinePack) {
	counterGlobal.count <- 1
}

func counterLoop() {
	tick := time.NewTicker(time.Duration(time.Second))
	aggregate := time.NewTicker(time.Duration(10 * time.Second))
	lastTime := time.Now()
	lastCount := uint(0)
	count := uint(0)
	zeroes := int8(0)
	var (
		msgsSent, inc uint
		elapsedTime   time.Duration
		now           time.Time
		rate          float64
		rates         []float64
	)
	for {
		// Here for performance reasons
		runtime.Gosched()
		select {
		case <-aggregate.C:
			amount := len(rates)
			if amount < 1 {
				continue
			}
			sort.Float64s(rates)
			min := rates[0]
			max := rates[amount-1]
			mean := min
			sum := float64(0)
			for _, val := range rates {
				sum += val
			}
			mean = sum / float64(amount)
			log.Printf("AGG Sum. Min: %0.2f   Max: %0.2f     Mean: %0.2f",
				min, max, mean)
			rates = rates[:0]
		case <-tick.C:
			now = time.Now()
			msgsSent = count - lastCount
			lastCount = count
			elapsedTime = now.Sub(lastTime)
			lastTime = now
			rate = float64(msgsSent) / elapsedTime.Seconds()
			if msgsSent == 0 {
				if msgsSent == 0 || zeroes == 3 {
					continue
				}
				zeroes++
			} else {
				zeroes = 0
			}
			log.Printf("Got %d messages. %0.2f msg/sec\n", count, rate)
			rates = append(rates, rate)
		case inc = <-counterGlobal.count:
			count += inc
		}
	}
}

// FileWriters actually do the work of writing out to the filesystem.
type FileWriter struct {
	DataChan    chan []byte
	RecycleChan chan []byte
	path        string
	file        *os.File
}

// Create the data channel and the open the file for writing
func NewFileWriter(path string, perm os.FileMode) (*FileWriter, error) {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, perm)
	if err != nil {
		return nil, err
	}
	dataChan := make(chan []byte, 2*PoolSize)
	recycleChan := make(chan []byte, 2*PoolSize)

	// Stuff the recycle channel with 2 * poolsize
	for i := 0; i < 2*PoolSize; i++ {
		recycleChan <- make([]byte, 0, 2000)
	}

	self := &FileWriter{dataChan, recycleChan, path, file}
	go self.writeLoop()
	return self, nil
}

// Wait for messages to come through the data channel and write them out to
// the file
func (self *FileWriter) writeLoop() {
	stopChan := make(chan interface{})
	notify.Start(STOP, stopChan)
	var outputBytes []byte
writeloop:
	for {
		// Yielding before a channel select improves scheduler performance
		runtime.Gosched()
		select {
		case outputBytes = <-self.DataChan:
			n, err := self.file.Write(outputBytes)
			if err != nil {
				log.Printf("Error writing to %s: %s", self.path, err.Error())
			} else if n != len(outputBytes) {
				log.Printf("Truncated output for %s", self.path)
			}
			outputBytes = outputBytes[:cap(outputBytes)]
			self.RecycleChan <- outputBytes
		case <-time.After(time.Second):
			self.file.Sync()
		case <-stopChan:
			self.file.Close()
			break writeloop
		}
	}
}

var (
	FileWriters = make(map[string]*FileWriter)

	FILEFORMATS = map[string]bool{
		"json": true,
		"text": true,
	}

	TSFORMAT = "[2006/Jan/02:15:04:05 -0700] "
)

const NEWLINE byte = 10

// FileOutput formats the output and then hands it off to the dataChan so the
// FileWriter can do its thing.
type FileOutput struct {
	dataChan    chan []byte
	recycleChan chan []byte
	outputBytes []byte
	path        string
	format      string
	prefix_ts   bool
}

type StatsdOutputConfig struct {
	Url string
}

type FileOutputConfig struct {
	Path      string
	Format    string
	Prefix_ts bool
	Perm      os.FileMode
}

func (self *FileOutput) ConfigStruct() interface{} {
	return &FileOutputConfig{Format: "text", Perm: 0666}
}

// Initialize a FileWriter, but only once.
func (self *FileOutput) Init(config interface{}) error {
	conf := config.(*FileOutputConfig)
	_, ok := FILEFORMATS[conf.Format]
	if !ok {
		return fmt.Errorf("Unsupported FileOutput format: %s", conf.Format)
	}
	// Using a map to guarantee there's only one FileWriter is only safe b/c
	// the PipelinePacks (and therefore the FileOutputs) are initialized in
	// series. If this ever changes such that outputs might be created in
	// different threads then this will require a lock to make sure we don't
	// end up w/ two FileWriters for the same file.
	writer, ok := FileWriters[conf.Path]
	if !ok {
		var err error
		writer, err = NewFileWriter(conf.Path, conf.Perm)
		if err != nil {
			return fmt.Errorf("Error creating FileWriter: %s\n", err.Error())
		}
		FileWriters[conf.Path] = writer
	}
	self.dataChan = writer.DataChan
	self.recycleChan = writer.RecycleChan
	self.path = conf.Path
	self.format = conf.Format
	self.prefix_ts = conf.Prefix_ts

	return nil
}

func (self *FileOutput) Deliver(pack *PipelinePack) {
	self.outputBytes = <-self.recycleChan
	if self.prefix_ts {
		ts := time.Now().Format(TSFORMAT)
		self.outputBytes = append(self.outputBytes, ts...)
	}

	switch self.format {
	case "json":
		jsonMessage, err := json.Marshal(pack.Message)
		if err != nil {
			log.Printf("Error converting message to JSON for %s", self.path)
			return
		}
		self.outputBytes = append(self.outputBytes, jsonMessage...)
	case "text":
		self.outputBytes = append(self.outputBytes, pack.Message.Payload...)
	}
	self.outputBytes = append(self.outputBytes, NEWLINE)
	self.dataChan <- self.outputBytes
}

// Interface that all statsd clients must implement.
type StatsdClient interface {
	IncrementSampledCounter(bucket string, n int, srate float32)
	SendSampledTiming(bucket string, ms int, srate float32)
}

type StatsdOutput struct {
	statsdClient StatsdClient

	/* The variables below are used when decoding the ns, key, value
	 * and rate from the pipelinepack
	 */
	msg *Message
	ns  string

	key    string
	key_ok bool

	tmp_value int64
	value_ok  error
	value     int

	rate     float32
	tmp_rate float64
	rate_ok  bool
}

func NewStatsdClient(url string) StatsdClient {
	sd, err := g2s.NewStatsd(url, 0)
	if err != nil {
		log.Printf("Error!! No statsd client was created! %v", err)
		return nil
	}
	return sd
}

func (self *StatsdOutput) ConfigStruct() interface{} {
	return &StatsdOutputConfig{Url: "localhost:5555"}
}

func (self *StatsdOutput) Init(config interface{}) error {
	conf := config.(*StatsdOutputConfig)
	statsd_url := conf.Url
	self.statsdClient = NewStatsdClient(statsd_url)
	return nil
}

func (self *StatsdOutput) Deliver(pipelinePack *PipelinePack) {

	self.msg = pipelinePack.Message

	// we need the ns for the full key
	self.ns = self.msg.Logger

	self.key, self.key_ok = self.msg.Fields["name"].(string)
	if self.key_ok == false {
		log.Printf("Error parsing key for statsd from msg.Fields[\"name\"]")
		return
	}

	if strings.TrimSpace(self.ns) != "" {
		s := []string{self.ns, self.key}
		self.key = strings.Join(s, ".")
	}

	self.tmp_value, self.value_ok = strconv.ParseInt(self.msg.Payload, 10, 32)
	if self.value_ok != nil {
		log.Printf("Error parsing value for statsd")
		return
	}
	// Downcast this
	self.value = int(self.tmp_value)

	self.tmp_rate, self.rate_ok = self.msg.Fields["rate"].(float64)
	if self.rate_ok == false {
		log.Printf("Error parsing key for statsd from msg.Fields[\"rate\"]")
		return
	}

	self.rate = float32(self.tmp_rate)

	switch self.msg.Type {
	case "counter":
		self.statsdClient.IncrementSampledCounter(self.key, self.value, self.rate)
	case "timer":
		self.statsdClient.SendSampledTiming(self.key, self.value, self.rate)
	default:
		log.Printf("Warning: Unexpected event passed into StatsdOutput.\nEvent => %+v\n", *(pipelinePack.Message))
	}
}
