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
	"errors"
	. "github.com/mozilla-services/heka/message"
	"github.com/rafrombrc/go-notify"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Control channel event types used by go-notify
const (
	RELOAD = "reload"
	STOP   = "stop"
)

var PoolSize int

type Plugin interface {
	Init(config interface{}) error
}

type PluginGlobal interface {
	// Called when an event occurs, either RELOAD or STOP
	Event(eventType string)
}

type PluginWithGlobal interface {
	Init(global PluginGlobal, config interface{}) error
	InitOnce(config interface{}) (global PluginGlobal, err error)
}

type PipelinePack struct {
	MsgBytes    []byte
	Message     *Message
	Config      *PipelineConfig
	Decoder     string
	Decoders    map[string]Decoder
	Filters     map[string]Filter
	OutputChans map[string]chan *PipelinePack
	Decoded     bool
	Blocked     bool
	FilterChain string
	ChainCount  int
	OutputNames map[string]bool
}

func NewPipelinePack(config *PipelineConfig) *PipelinePack {
	msgBytes := make([]byte, 3+MAX_HEADER_SIZE+MAX_MESSAGE_SIZE)
	message := &Message{}
	outputNames := make(map[string]bool)
	filters := make(map[string]Filter)
	decoders := make(map[string]Decoder)
	outputChans := make(map[string]chan *PipelinePack)

	pack := &PipelinePack{
		MsgBytes:    msgBytes,
		Message:     message,
		Config:      config,
		Decoder:     config.DefaultDecoder,
		Decoders:    decoders,
		Decoded:     false,
		Blocked:     false,
		Filters:     filters,
		FilterChain: config.DefaultFilterChain,
		OutputChans: outputChans,
		OutputNames: outputNames,
	}
	pack.InitDecoders(config)
	pack.InitFilters(config)
	pack.InitOutputs(config)
	return pack
}

func (self *PipelinePack) InitDecoders(config *PipelineConfig) {

	var plugin interface{}
	var err error

	for name, wrapper := range config.Decoders {
		plugin, err = wrapper.CreateWithError()
		if err != nil {
			log.Printf("Error initializing [%s] : [%s]", name, err.Error())
		} else {
			self.Decoders[name] = plugin.(Decoder)
		}
	}
}

func (self *PipelinePack) InitFilters(config *PipelineConfig) {

	var plugin interface{}
	var err error

	for name, wrapper := range config.Filters {
		plugin, err = wrapper.CreateWithError()
		if err != nil {
			log.Printf("Error initializing [%s] : [%s]", name, err.Error())
		} else {
			self.Filters[name] = plugin.(Filter)
		}
	}
}

func (self *PipelinePack) InitOutputs(config *PipelineConfig) {
	for name, outRunner := range config.OutputRunners {
		self.OutputChans[name] = outRunner.Chan
	}
}

func (self *PipelinePack) Zero() {
	self.MsgBytes = self.MsgBytes[:cap(self.MsgBytes)]
	self.Decoder = self.Config.DefaultDecoder
	self.Decoded = false
	self.Blocked = false
	self.FilterChain = self.Config.DefaultFilterChain
	for outputName, _ := range self.OutputNames {
		delete(self.OutputNames, outputName)
	}
}

func filterProcessor(pipelinePack *PipelinePack) {
	pipelinePack.OutputNames = map[string]bool{}
	config := pipelinePack.Config
	filterChainName, ok := config.Lookup.LocateChain(pipelinePack.Message)
	if ok {
		pipelinePack.FilterChain = filterChainName
	} else {
		filterChainName = pipelinePack.FilterChain
	}
	filterChain, ok := config.FilterChains[filterChainName]
	if !ok {
		log.Printf("Filter chain doesn't exist: %s", filterChainName)
		return
	}
	for _, outputName := range filterChain.Outputs {
		pipelinePack.OutputNames[outputName] = true
	}
	for _, filterName := range filterChain.Filters {
		filter := pipelinePack.Filters[filterName]
		safe_filter(filterName, filter, pipelinePack)
		if pipelinePack.Blocked {
			return
		}
	}
}

func safe_filter(filterName string, filter Filter, pipelinePack *PipelinePack) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Error executing filter [%s] event: %s.  Blocking the pack.", filterName, r)
			pipelinePack.Blocked = true
		}
	}()
	filter.FilterMsg(pipelinePack)
}

func safe_plugin_global_event(wrapper *PluginWrapper, eventType string) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Error sending [%s] event: %s", eventType, r)
		}
	}()
	wrapper.global.Event(eventType)
}

func BroadcastEvent(config *PipelineConfig, eventType string) {
	err := notify.Post(eventType, nil)
	if err != nil {
		log.Printf("Error sending %s event:", err.Error())
	}

	var wrapper *PluginWrapper
	for _, wrapper = range config.Filters {
		if wrapper.global != nil {
			safe_plugin_global_event(wrapper, eventType)
		}
	}
	for _, wrapper = range config.Outputs {
		if wrapper.global != nil {
			safe_plugin_global_event(wrapper, eventType)
		}
	}
}

func safe_decode(decoder Decoder, pack *PipelinePack) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("Error running decoder on pack.")
		}
	}()
	err = decoder.Decode(pack)
	return
}

// TODO vng: do we need this still?
func safe_deliver(output Output, pack *PipelinePack) {
	defer func() {
		if r := recover(); r != nil {
			log.Println("Output delivery failed: %s", r)
		}
	}()
	output.Deliver(pack)
}

// Main pipeline function
func pipeline(pack *PipelinePack) (recycle bool) {

	// Decode message if necessary
	if !pack.Decoded {
		decoderName := pack.Decoder
		decoder, ok := pack.Decoders[decoderName]
		if !ok {
			log.Printf("Decoder doesn't exist: %s\n", decoderName)
			recycle = true
			return
		}
		// TODO: inline the safe_decoder call here
		if err := decoder.Decode(pack); err != nil {
			log.Printf("Error decoding message (%s): %s", decoderName,
				err)
			recycle = true
			return
		} else {
			pack.Decoded = true
		}
	}

	// Run message through the appropriate filters
	filterProcessor(pack)
	if pack.Blocked {
		recycle = true
		return
	}

	i := 0
	// Deliver message to appropriate outputs
	for outputName, use := range pack.OutputNames {
		if !use {
			continue
		}
		outChan, ok := pack.OutputChans[outputName]
		if !ok {
			log.Printf("Output doesn't exist: %s\n", outputName)
			continue
		}
		outChan <- pack
		i++
	}
	if i == 0 {
		recycle = true
	}
	return
}

func Run(config *PipelineConfig) {
	log.Println("Starting hekad...")

	// Used for passing around populated and recycled PipelinePack objects
	dataChan := make(chan *PipelinePack, config.PoolSize+1)
	recycleChan := make(chan *PipelinePack, config.PoolSize+1)

	var wg sync.WaitGroup
	var outRunner *OutputRunner

	for name, wrapper := range config.Outputs {
		plugin, err := wrapper.CreateWithError()
		if err != nil {
			log.Panicf("Error while creating output: [%s]: %s", plugin, err.Error())
		}
		output := plugin.(Output)
		outRunner = NewOutputRunner(name, output)
		config.OutputRunners[name] = outRunner
		outRunner.Start(recycleChan, &wg)
		wg.Add(1)
		log.Printf("Output started: %s\n", name)
	}

	// Initialize all of the PipelinePacks that we'll need
	for i := 0; i < config.PoolSize; i++ {
		recycleChan <- NewPipelinePack(config)
	}

	var inRunner *InputRunner
	timeout := time.Duration(time.Second / 2)
	inputRunners := make(map[string]*InputRunner)

	for name, wrapper := range config.Inputs {
		plugin, err := wrapper.CreateWithError()
		if err != nil {
			log.Panicf("Error while creating input: [%s]: %s", plugin, err.Error())
		}
		input, ok := plugin.(Input)
		if !ok {
			log.Panicf("Expected an Input plugin.  Got: [%s]", plugin)
		}

		inRunner = &InputRunner{name, input, &timeout}
		inputRunners[name] = inRunner
		inRunner.Start(dataChan, recycleChan, &wg)
		wg.Add(1)
		log.Printf("Input started: %s\n", name)
	}

	// wait for sigint
	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGHUP)
	var pack *PipelinePack

sigListener:
	for {
		select {
		case pack = <-dataChan:
			recycle := pipeline(pack)
			if recycle {
				pack.Zero()
				recycleChan <- pack
			}
		case sig := <-sigChan:
			switch sig {
			case syscall.SIGHUP:
				log.Println("Reload initiated.")
				BroadcastEvent(config, RELOAD)
			case syscall.SIGINT:
				log.Println("Shutdown initiated.")
				BroadcastEvent(config, STOP)
				break sigListener
			}
		}
	}

	wg.Wait()
	log.Println("Shutdown complete.")
}
