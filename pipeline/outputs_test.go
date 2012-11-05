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
#   Victor Ng (vng@mozilla.com)
#
# ***** END LICENSE BLOCK *****/
package pipeline

import (
	"code.google.com/p/gomock/gomock"
	gs "github.com/orfjackal/gospec/src/gospec"
	mocks "heka/pipeline/mocks"
)

func getIncrPipelinePack() *PipelinePack {
	pipelinePack := getTestPipelinePack()

	fields := make(map[string]interface{})
	pipelinePack.Message.Fields = fields

	// Force the message to be a statsd increment message
	pipelinePack.Message.Logger = "thenamespace"
	pipelinePack.Message.Fields["name"] = "myname"
	pipelinePack.Message.Fields["rate"] = "30"
	pipelinePack.Message.Fields["type"] = "counter"
	pipelinePack.Message.Payload = "-1"
	return pipelinePack
}

func OutputsSpec(c gs.Context) {
	c.Specify("A StatsdOutput", func() {

		t := &SimpleT{}
		ctrl := gomock.NewController(t)

		// Setup of the pipelinePack in here
		pipelinePack := getIncrPipelinePack()

		mockClient := mocks.NewMockStatsdClient(ctrl)
		mockClient.EXPECT().IncrementSampledCounter("myname", -1, float32(30))
		statsdOutput := NewStatsdOutput(mockClient)
		statsdOutput.Deliver(pipelinePack)
	})
}