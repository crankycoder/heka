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
	"log"
	"runtime"
	"time"
)

type Output interface {
	Plugin
	Deliver(pipelinePack *PipelinePack)
}

type LogOutput struct {
}

func (self *LogOutput) Init(config *PluginConfig) error {
	return nil
}

func (self *LogOutput) Deliver(pipelinePack *PipelinePack) {
	log.Printf("%+v\n", *(pipelinePack.Message))
}

type CounterOutput struct {
	count uint
}

func NewCounterOutput() *CounterOutput {
	self := CounterOutput{0}
	ticker := time.NewTicker(time.Duration(time.Second))
	go self.timerLoop(ticker)
	return &self
}

func (self *CounterOutput) Init(config *PluginConfig) error {
	return nil
}

func (self *CounterOutput) Deliver(pipelinePack *PipelinePack) {
	self.count++
	runtime.Gosched()
}

func (self *CounterOutput) timerLoop(ticker *time.Ticker) {
	lastTime := time.Now()
	lastCount := self.count
	zeroes := int8(0)
	var (
		msgsSent, newCount uint
		elapsedTime        time.Duration
		now                time.Time
		rate               float64
	)
	for {
		_ = <-ticker.C
		newCount = self.count
		now = time.Now()
		msgsSent = newCount - lastCount
		lastCount = newCount
		elapsedTime = now.Sub(lastTime)
		lastTime = now
		rate = float64(msgsSent) / elapsedTime.Seconds()
		if msgsSent == 0 {
			if newCount == 0 || zeroes == 3 {
				continue
			}
			zeroes++
		} else {
			zeroes = 0
		}
		log.Printf("Got %d messages. %0.2f msg/sec\n", newCount, rate)
	}
}
