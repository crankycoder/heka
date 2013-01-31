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
				return &BuggyPlugin{map[string]bool{"Init": true}}
			}
			_, err := wrapper.CreateWithError()
			c.Expect(err.Error(), gs.Equals, "Error while initializing plugin: [*pipeline.BuggyPlugin][Init Failed]")
		})
	})

	c.Specify("PluginWithGlobal interfaces", func() {
		c.Specify("buggy Init", func() {
			// This is only invoked in config.go:PluginWrapper.CreateWithError
			wrapper := new(PluginWrapper)
			wrapper.name = "DemoPlugin"
			wrapper.configCreator = func() interface{} { return nil }
			wrapper.pluginCreator = func() interface{} {
				return &BuggyPluginWithGlobal{map[string]bool{"Init": true}}
			}
			wrapper.global = new(MockGlobal)

			plugin, err := wrapper.CreateWithError()
			_, is_pluginwithglobal := plugin.(PluginWithGlobal)

			c.Expect(is_pluginwithglobal, gs.Equals, true)
			c.Expect(err.Error(), gs.Equals, "Error while initializing plugin: [*pipeline.BuggyPluginWithGlobal][Init Failed]")

		})

		c.Specify("buggy InitOnce", func() {
			// This is only invoked in config.go:PluginWrapper.CreateWithError

			// This is only invoked in config.go:PluginWrapper.CreateWithError
			plugin := &BuggyPluginWithGlobal{map[string]bool{"InitOnce": true}}
			wrapper := new(PluginWrapper)
			wrapper.name = "DemoPlugin"
			wrapper.configCreator = func() interface{} { return nil }
			wrapper.pluginCreator = func() interface{} {
				return plugin
			}
			wrapper.global = new(MockGlobal)

			_, err := safe_initonce(plugin, wrapper)
			c.Expect(err.Error(), gs.Equals, "Error while calling InitOnce : [*pipeline.BuggyPluginWithGlobal][InitOnce Failed]")

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

/************/
// An implementation of a buggy Plugin interface
type BuggyPlugin struct {
	buggy map[string]bool
}

type BuggyPluginWithGlobal struct {
	buggy map[string]bool
}

func (b *BuggyPlugin) Init(_param0 interface{}) error {
	fail, ok := b.buggy["Init"]
	if ok && fail {
		panic("Init Failed")
	}
	return nil
}

func (b *BuggyPluginWithGlobal) Init(global PluginGlobal, config interface{}) error {
	fail, ok := b.buggy["Init"]
	if ok && fail {
		panic("Init Failed")
	}
	return nil
}

func (b *BuggyPluginWithGlobal) InitOnce(config interface{}) (global PluginGlobal, err error) {
	fail, ok := b.buggy["InitOnce"]
	if ok && fail {
		panic("InitOnce Failed")
	}
	return new(MockGlobal), nil
}

type MockGlobal struct{}

func (m *MockGlobal) Event(eventType string) {}
