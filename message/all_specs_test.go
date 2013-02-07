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
#   Mike Trinkala (trink@mozilla.com)
#
# ***** END LICENSE BLOCK *****/
package message

import (
	"bytes"
	"code.google.com/p/go-uuid/uuid"
	"fmt"
	gs "github.com/rafrombrc/gospec/src/gospec"
	"github.com/rafrombrc/gospec/src/gospec"
	"os"
	"testing"
	"time"
)

func TestAllSpecs(t *testing.T) {
	r := gospec.NewRunner()
	r.AddSpec(MessageFieldsSpec)
	r.AddSpec(MessageEqualsSpec)
	gospec.MainGoTest(r, t)
}

func getTestMessage() *Message {
	hostname, _ := os.Hostname()
	field, _ := NewField("foo", "bar", Field_RAW)
	msg := &Message{}
	msg.SetType("TEST")
	msg.SetTimestamp(time.Now().UnixNano())
	msg.SetUuid(uuid.NewRandom())
	msg.SetLogger("GoSpec")
	msg.SetSeverity(int32(6))
	msg.SetPayload("Test Payload")
	msg.SetEnvVersion("0.8")
	msg.SetPid(int32(os.Getpid()))
	msg.SetHostname(hostname)
	msg.AddField(field)

	return msg
}

func MessageFieldsSpec(c gospec.Context) {
	c.Specify("No Fields", func() {
		msg := &Message{}
		f := msg.FindFirstField("test")
		c.Expect(f, gs.IsNil)
		fa := msg.FindAllFields("test")
		c.Expect(len(fa), gs.Equals, 0)
		v, ok := msg.GetFieldValue("test")
		c.Expect(ok, gs.IsFalse)
		c.Expect(v, gs.IsNil)
	})

	c.Specify("Fields present but none match", func() {
		msg := &Message{}
		f, _ := NewField("foo", "bar", Field_RAW)
		msg.AddField(f)
		ff := msg.FindFirstField("test")
		c.Expect(ff, gs.IsNil)
		fa := msg.FindAllFields("test")
		c.Expect(len(fa), gs.Equals, 0)
		v, ok := msg.GetFieldValue("test")
		c.Expect(ok, gs.IsFalse)
		c.Expect(v, gs.IsNil)
	})

	c.Specify("Fields match", func() {
		msg := &Message{}
		f, _ := NewField("foo", "bar", Field_RAW)
		f1, _ := NewField("other", "value", Field_RAW)
		f2, _ := NewField("foo", "bar1", Field_RAW)
		msg.AddField(f)
		msg.AddField(f1)
		msg.AddField(f2)
		ff := msg.FindFirstField("foo")
		c.Expect(ff.ValueString[0], gs.Equals, "bar")
		v, ok := msg.GetFieldValue("foo")
		c.Expect(ok, gs.IsTrue)
		c.Expect(v, gs.Equals, "bar")
		fa := msg.FindAllFields("foo")
		c.Expect(len(fa), gs.Equals, 2)
		fa[0].ValueString[0] = "bar"
		fa[1].ValueString[0] = "bar1"
	})

	c.Specify("Add Bytes Field", func() {
		msg := &Message{}
		b := make([]byte, 2)
		b[0] = 'a'
		b[1] = 'b'
		f, _ := NewField("foo", b, Field_RAW)
		msg.AddField(f)
		ff := msg.FindFirstField("foo")
		c.Expect(bytes.Equal(ff.ValueBytes[0], b), gs.IsTrue)
		v, ok := msg.GetFieldValue("foo")
		c.Expect(ok, gs.IsTrue)
		c.Expect(bytes.Equal(v.([]byte), b), gs.IsTrue)
	})

	c.Specify("Add Integer Field", func() {
		msg := &Message{}
		f, _ := NewField("foo", 1, Field_RAW)
		msg.AddField(f)
		ff := msg.FindFirstField("foo")
		c.Expect(ff.ValueInteger[0], gs.Equals, int64(1))
		v, ok := msg.GetFieldValue("foo")
		c.Expect(ok, gs.IsTrue)
		c.Expect(v, gs.Equals, int64(1))
	})

	c.Specify("Add Double Field", func() {
		msg := &Message{}
		f, _ := NewField("foo", 1e9, Field_RAW)
		msg.AddField(f)
		ff := msg.FindFirstField("foo")
		c.Expect(ff.ValueDouble[0], gs.Equals, 1e9)
		v, ok := msg.GetFieldValue("foo")
		c.Expect(ok, gs.IsTrue)
		c.Expect(v, gs.Equals, 1e9)
	})

	c.Specify("Add Bool Field", func() {
		msg := &Message{}
		f, _ := NewField("foo", true, Field_RAW)
		msg.AddField(f)
		ff := msg.FindFirstField("foo")
		c.Expect(ff.ValueBool[0], gs.IsTrue)
		v, ok := msg.GetFieldValue("foo")
		c.Expect(ok, gs.IsTrue)
		c.Expect(v, gs.IsTrue)
	})
}

func MessageEqualsSpec(c gospec.Context) {
	msg0 := getTestMessage()

	c.Specify("Messages are equal", func() {
		msg1 := CopyMessage(msg0)
		c.Expect(msg0, gs.Equals, msg1)
	})

	c.Specify("Messages w/ diff severity", func() {
		msg1 := CopyMessage(msg0)
		*msg1.Severity--
		c.Expect(msg0, gs.Not(gs.Equals), msg1)
	})

	c.Specify("Messages w/ diff uuid", func() {
		msg1 := CopyMessage(msg0)
		u := uuid.NewRandom()
		copy(msg1.Uuid, u)
		c.Expect(msg0, gs.Not(gs.Equals), msg1)
	})

	c.Specify("Messages w/ diff payload", func() {
		msg1 := CopyMessage(msg0)
		*msg1.Payload = "Something completely different"
		c.Expect(msg0, gs.Not(gs.Equals), msg1)
	})

	c.Specify("Messages w/ diff number of fields", func() {
		msg1 := CopyMessage(msg0)
		f, _ := NewField("sna", "foo", Field_RAW)
		msg1.AddField(f)
		c.Expect(msg0, gs.Not(gs.Equals), msg1)
	})

	c.Specify("Messages w/ diff number of field values in a key", func() {
		msg1 := CopyMessage(msg0)
		f := msg1.FindFirstField("foo")
		f.AddValue("foo1")
		c.Expect(msg0, gs.Not(gs.Equals), msg1)
	})

	c.Specify("Messages w/ diff value in a field", func() {
		msg1 := CopyMessage(msg0)
		f := msg1.FindFirstField("foo")
		f.ValueString[0] = "bah"
		c.Expect(msg0, gs.Not(gs.Equals), msg1)
	})

	c.Specify("Messages w/ diff field key", func() {
		msg1 := CopyMessage(msg0)
		f := msg1.FindFirstField("foo")
		*f.Name = "widget"
		c.Expect(msg0, gs.Not(gs.Equals), msg1)
	})

	c.Specify("Messages w/ recurring keys", func() {
		msg0 = &Message{}
		f, _ := NewField("foo", "bar", Field_RAW)
		f1, _ := NewField("foo", "bar1", Field_RAW)
		msg0.AddField(f)
		msg0.AddField(f1)
		msg1 := CopyMessage(msg0)
		c.Expect(msg0, gs.Equals, msg1)
		foos := msg0.FindAllFields("foo")
		foos[1].ValueString[0] = "bar2"
		c.Expect(msg0, gs.Not(gs.Equals), msg1)
	})

	c.Specify("Messages can unmarshal JSON", func() {
		severity := int32(6)
		str_ts := "2013-02-05T19:39:53.916612Z"
		hostname := "Victors-MacBook-Air.local"
		pid := int32(46543)
		logger := "blah"
		msg_type := "timer"
		payload := "51"
		version := "0.8"

		sample_json := fmt.Sprintf(`{"severity": %d, "timestamp": "%s", "metlog_hostname": "%s", "fields": {"rate": 1.0, "name": "metlog.tests.test_decorators.timed_add"}, "metlog_pid": %d, "logger": "%s", "type": "%s", "payload": "%s", "env_version": "%s"}`,
			severity,
			str_ts,
			hostname,
			// TODO: add fields here
			pid,
			logger,
			msg_type,
			payload,
			version)
		m := new(Message)
		m.UnmarshalJSON([]byte(sample_json))

		t, _ := time.Parse(time.RFC3339Nano, str_ts)

		var field_value interface{}
		var expected_nil_uuid []byte

		expected_nil_uuid = make([]byte, UUID_SIZE)
		c.Expect(bytes.Compare(m.GetUuid(), expected_nil_uuid), gs.Equals, 0)
		c.Expect(m.GetTimestamp(), gs.Equals, t.UnixNano())
		c.Expect(m.GetType(), gs.Equals, msg_type)
		c.Expect(m.GetLogger(), gs.Equals, logger)
		c.Expect(m.GetSeverity(), gs.Equals, severity)
		c.Expect(m.GetPayload(), gs.Equals, payload)
		c.Expect(m.GetEnvVersion(), gs.Equals, version)
		c.Expect(m.GetPid(), gs.Equals, pid)
		c.Expect(m.GetHostname(), gs.Equals, hostname)

		field_value, _ = m.GetFieldValue("rate")
		c.Expect(field_value.(float64), gs.Equals, 1.0)

		field_value, _ = m.GetFieldValue("name")
		c.Expect(field_value.(string), gs.Equals, "metlog.tests.test_decorators.timed_add")
	})

	c.Specify("Messages can be marshalled into JSON", func() {
		var tmp interface{}

		uuid := []byte("1234")

		str_ts := "2013-02-05T19:39:53.916612Z"
		t, _ := time.Parse(time.RFC3339Nano, str_ts)
		msg_type := "timer"
		logger := "blah"
		severity := int32(6)

		payload := "51"
		version := "0.8"

		pid := int32(46543)
		hostname := "Victors-MacBook-Air.local"

		m := new(Message)
		m.SetUuid(uuid)
		m.SetTimestamp(t.UnixNano())
		m.SetType(msg_type)
		m.SetLogger(logger)
		m.SetSeverity(severity)
		m.SetPayload(payload)
		m.SetEnvVersion(version)
		m.SetPid(pid)
		m.SetHostname(hostname)
		field_map := make(map[string]interface{})
		field_map["rate"] = 1.0
		field_map["name"] = "metlog.tests.test_decorators.timed_add"
		flattenMap(field_map, m, "")

		tmp, _ = m.GetFieldValue("rate")
		c.Expect(tmp.(float64), gs.Equals, field_map["rate"])
		tmp, _ = m.GetFieldValue("name")
		c.Expect(tmp.(string), gs.Equals, field_map["name"])

		actual_json, _ := m.MarshalJSON()

		m2 := new(Message)
		m2.UnmarshalJSON(actual_json)

		var expected_nil_uuid []byte
		expected_nil_uuid = make([]byte, UUID_SIZE)

		// UUID is not encoded in the JSON blob
		c.Expect(bytes.Compare(m2.GetUuid(), expected_nil_uuid), gs.Equals, 0)
		c.Expect(m2.GetTimestamp(), gs.Equals, t.UnixNano())
		c.Expect(m2.GetType(), gs.Equals, msg_type)
		c.Expect(m2.GetLogger(), gs.Equals, logger)
		c.Expect(m2.GetSeverity(), gs.Equals, severity)
		c.Expect(m2.GetPayload(), gs.Equals, payload)
		c.Expect(m2.GetEnvVersion(), gs.Equals, version)
		c.Expect(m2.GetPid(), gs.Equals, pid)
		c.Expect(m2.GetHostname(), gs.Equals, hostname)

		var field_value interface{}
		field_value, _ = m2.GetFieldValue("rate")
		c.Expect(field_value.(float64), gs.Equals, 1.0)

		field_value, _ = m2.GetFieldValue("name")
		c.Expect(field_value.(string), gs.Equals, "metlog.tests.test_decorators.timed_add")
	})

}
