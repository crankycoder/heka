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
#   Victor Ng (vng@mozilla.com)
#
# ***** END LICENSE BLOCK *****/
package pipeline

import (
	ts "github.com/mozilla-services/heka/testsupport"
	gs "github.com/rafrombrc/gospec/src/gospec"
)

func BadPluginsSpec(c gs.Context) {
	c.Specify("Plugin interface tests", func() {
		c.Specify("buggy Init", func() {
			// This is only invoked in config.go:PluginWrapper.CreateWithError
			wrapper := new(PluginWrapper)
			wrapper.name = "DemoPlugin"
			wrapper.configCreator = func() interface{} { return nil }
			wrapper.pluginCreator = func() interface{} {
				return ts.NewBuggyPlugin(map[string]bool{"Init": true})
			}
			_, err := wrapper.CreateWithError()
			c.Expect(err.Error(), gs.Equals, "Error while initializing plugin: [*testsupport.BuggyPlugin][Init Failed]")
		})
	})

	c.Specify("PluginWithGlobal interfaces", func() {
		c.Specify("buggy Init", func() {
		})

		c.Specify("buggy InitOnce", func() {
		})
	})

	c.Specify("PluginGlobal interfaces", func() {
		c.Specify("buggy Event", func() {
		})
	})

	c.Specify("HasConfigStruct interfaces", func() {
		c.Specify("buggy ConfigStruct()", func() {
		})
	})

	c.Specify("Input interfaces", func() {
		c.Specify("buggy Read()", func() {
		})
	})

	c.Specify("Decoder interfaces", func() {
		c.Specify("buggy Decode()", func() {
		})
	})

	c.Specify("Filter interfaces", func() {
		c.Specify("buggy FilterMsg()", func() {
		})
	})

	c.Specify("Output interfaces", func() {
		c.Specify("buggy Deliver()", func() {
		})
	})

	c.Specify("Writer interfaces", func() {
		c.Specify("buggy Init()", func() {
		})
		c.Specify("buggy Write()", func() {
		})
	})

	c.Specify("BatchWriter interfaces", func() {
		c.Specify("buggy Init()", func() {
		})
		c.Specify("buggy Batch()", func() {
		})
		c.Specify("buggy Commit()", func() {
		})
	})

	c.Specify("DataRecycler interfaces", func() {
		c.Specify("buggy MakeOutData()", func() {
		})
		c.Specify("buggy ZeroOutData()", func() {
		})
		c.Specify("buggy PrepOutData()", func() {
		})
	})

}
