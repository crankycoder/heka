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
	"fmt"
	"github.com/rafrombrc/go-notify"
	"log"
	"runtime"
	"time"
)

type DataRecycler interface {
	// This must create exactly one instance of the `outData` data object type
	// expected by the `Write` method. Will be called multiple times to create
	// a pool of reusable objects.
	MakeOutData() (outData interface{})

	// Will be handed a used output object which should be reset to a zero
	// state for in preparation for reuse. This method will be in use by
	// multiple goroutines simultaneously, it should modify the passed
	// `outData` object **only**.
	ZeroOutData(outData interface{})

	// Extracts relevant information from the provided `PipelinePack`
	// (probably from the `Message` attribute) and uses it to populate the
	// provided output object. This method will be in use by multiple
	// goroutines simultaneously, it should modify the passed `outData` object
	// **only**. `timeout` will be nil unless the Runner plugin is being used
	// as an Input.
	PrepOutData(pack *PipelinePack, outData interface{}, timeout *time.Duration) error
}

// Interface for output objects that need to share a global resource (such as
// a file handle or network connection) to actually emit the output data.
type Writer interface {
	PluginGlobal
	DataRecycler

	// Setup method, called exactly once
	Init(config interface{}) error

	// Receives a populated output object, handles the actual work of writing
	// data out to an external destination.
	Write(outData interface{}) error
}

type BatchWriter interface {
	PluginGlobal
	DataRecycler

	// Setup method, called exactly once, returns a channel that ticks when the
	// Commit should be called
	Init(config interface{}) (<-chan time.Time, error)

	// Receives a populated output object, handles batching it as needed before
	// its to be committed
	Batch(outData interface{}) error

	// Called when a tick occurs to commit a batch
	Commit() error
}

// Plugin that drives a Writer or BatchWriter, instantiated many times
type Runner struct {
	Writer      Writer
	BatchWriter BatchWriter
	outData     interface{}
	global      *RunnerGlobal
}

// Global instance used by every runner
type RunnerGlobal struct {
	Recycler    DataRecycler
	Events      PluginGlobal
	dataChan    chan interface{}
	recycleChan chan interface{}
	ticker      <-chan time.Time
}

func (self *RunnerGlobal) Event(eventType string) {
	if self.Events != nil {
		self.Events.Event(eventType)
	}
}

func RunnerMaker(writer interface{}) interface{} {
	runner := new(Runner)
	if batch, ok := writer.(BatchWriter); ok {
		runner.BatchWriter = batch
	} else {
		runner.Writer = writer.(Writer)
	}
	return runner
}

func safe_batch_init(batchwriter BatchWriter, config interface{}) (ticker <-chan time.Time,
	err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("WriteRunner panic during Init: %s", r)
			return
		}
	}()

	ticker, err = batchwriter.Init(config)
	return
}

func safe_writer_init(writer Writer, config interface{}) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("Writer panic during Init: %s", r)
			return
		}
	}()
	err = writer.Init(config)
	return
}

func (self *Runner) InitOnce(config interface{}) (global PluginGlobal, err error) {
	conf := config.(*PluginConfig)
	g := new(RunnerGlobal)
	self.global = g
	var confLoaded interface{}

	// Determine how to initialize and if we hold a ticker
	if self.BatchWriter != nil {
		g.Recycler = self.BatchWriter.(DataRecycler)
		confLoaded, err = LoadConfigStruct(conf, self.BatchWriter)

		if g.ticker, err = safe_batch_init(self.BatchWriter, confLoaded); err != nil {
			return g, errors.New("WriteRunner initialization error: " + err.Error())
		}
	} else {
		g.Recycler = self.Writer.(DataRecycler)
		confLoaded, err = LoadConfigStruct(conf, self.Writer)
		if err = safe_writer_init(self.Writer, confLoaded); err != nil {
			return g, errors.New("WriteRunner initialization error: " + err.Error())
		}
	}
	if err != nil {
		return g, errors.New("WriteRunner config parsing error: " + err.Error())
	}

	g.dataChan = make(chan interface{}, 2*PoolSize)
	g.recycleChan = make(chan interface{}, 2*PoolSize)

	err = preallocate_outdata(PoolSize, g)
	if err != nil {
		return g, err
	}

	if self.BatchWriter != nil {
		go self.batch_runner()
		return g, nil
	}
	go self.runner()
	return g, nil
}

func preallocate_outdata(pool_size int, global *RunnerGlobal) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("Error while preallocating outData for: [%s]", global.Recycler)
		}
	}()
	for i := 0; i < 2*pool_size; i++ {
		global.recycleChan <- global.Recycler.MakeOutData()
	}
	return
}
func (self *Runner) Init(global PluginGlobal, config interface{}) error {
	self.global = global.(*RunnerGlobal)
	return nil
}

func safe_batchwriter_commit(batchwriter BatchWriter) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("Error while calling commit on batchwriter: %s", r)
			return
		}
	}()
	batchwriter.Commit()
	return
}

func (self *Runner) batch_runner() {
	stopChan := make(chan interface{})
	notify.Start(STOP, stopChan)
	var outData interface{}
	var err error
	for {
		// Yield before channel select can improve scheduler performance
		runtime.Gosched()
		select {
		case <-self.global.ticker:
			if err = safe_batchwriter_commit(self.BatchWriter); err != nil {
				log.Println("BatchWriter commit error: ", err)
			}
		case outData = <-self.global.dataChan:
			if err = self.BatchWriter.Batch(outData); err != nil {
				log.Println("OutputWriter error: ", err)
			}
			self.RecycleOutData(outData)
		case <-stopChan:
			return
		}
	}
}

func safe_writer(writer Writer, outData interface{}) error {
	defer func() {
		if r := recover(); r != nil {
			log.Println("Writer error : %s", r)
		}
	}()
	return writer.Write(outData)
}

func (self *Runner) runner() {
	stopChan := make(chan interface{})
	notify.Start(STOP, stopChan)
	var outData interface{}
	var err error
	for {
		// Yield before channel select can improve scheduler performance
		runtime.Gosched()
		select {
		case outData = <-self.global.dataChan:

			if err = safe_writer(self.Writer, outData); err != nil {

				log.Println("OutputWriter error: ", err)
			}
			self.RecycleOutData(outData)
		case <-stopChan:
			return
		}
	}
}

func (self *Runner) RecycleOutData(outData interface{}) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Not replacing recycle channel. ZeroOutData failed for plugin [%s]", self)
		}
	}()
	self.global.Recycler.ZeroOutData(outData)
	self.global.recycleChan <- outData
}

func safe_prepoutdata(recycler DataRecycler, pack *PipelinePack, outData interface{}, timeout *time.Duration) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("PrepOutData panic detected: %s", r)
			return
		}
	}()
	return recycler.PrepOutData(pack, outData, timeout)
}

func (self *Runner) Deliver(pack *PipelinePack) {
	self.outData = <-self.global.recycleChan

	err := safe_prepoutdata(self.global.Recycler, pack, self.outData, nil)

	if err != nil {
		log.Printf("PipelinePack skipping data channel. PrepOutData error in plugin [%s]: %s\n", self, err.Error())
		self.RecycleOutData(self.outData)
		return
	}
	self.global.dataChan <- self.outData
}

func (self *Runner) FilterMsg(pipelinePack *PipelinePack) {
	self.outData = <-self.global.recycleChan

	err := safe_prepoutdata(self.global.Recycler, pipelinePack, self.outData, nil)

	if err != nil {
		log.Printf("PrepOutData error: %s", err.Error())
		self.RecycleOutData(self.outData)
		return
	}
	self.global.dataChan <- self.outData
}

func (self *Runner) Read(pipelinePack *PipelinePack, timeout *time.Duration) (err error) {
	self.outData = <-self.global.recycleChan

	err = safe_prepoutdata(self.global.Recycler, pipelinePack, self.outData, timeout)
	if err != nil {
		self.RecycleOutData(self.outData)
		return
	}
	self.global.dataChan <- self.outData
	return
}
