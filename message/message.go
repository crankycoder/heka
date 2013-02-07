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
#   Mike Trinkala (trink@mozilla.com)
#
# ***** END LICENSE BLOCK *****/
// Extensions to make Message more useable in our current code outside the scope
// of protocol buffers.  See message.pb.go for the actually message definition.
package message

import (
	"bytes"
	"code.google.com/p/go-uuid/uuid"
	"encoding/json"
	"errors"
	"fmt"
	simplejson "github.com/bitly/go-simplejson"
	"log"
	"reflect"
	"time"
)

const UUID_SIZE = 16

func (h *Header) SetMessageEncoding(v Header_MessageEncoding) {
	if h != nil {
		if h.MessageEncoding == nil {
			h.MessageEncoding = new(Header_MessageEncoding)
		}
		*h.MessageEncoding = v
	}
}

func (h *Header) SetMessageLength(v uint32) {
	if h != nil {
		if h.MessageLength == nil {
			h.MessageLength = new(uint32)
		}
		*h.MessageLength = v
	}
}

func (m *Message) SetUuid(v []byte) {
	if m != nil {
		if len(m.Uuid) != UUID_SIZE {
			m.Uuid = make([]byte, UUID_SIZE)
		}
		copy(m.Uuid, v)
	}
}

func (m *Message) SetTimestamp(v int64) {
	if m != nil {
		if m.Timestamp == nil {
			m.Timestamp = new(int64)
		}
		*m.Timestamp = v
	}
}

func (m *Message) SetType(v string) {
	if m != nil {
		if m.Type == nil {
			m.Type = new(string)
		}
		*m.Type = v
	}
}

func (m *Message) SetLogger(v string) {
	if m != nil {
		if m.Logger == nil {
			m.Logger = new(string)
		}
		*m.Logger = v

	}
}

func (m *Message) SetSeverity(v int32) {
	if m != nil {
		if m.Severity == nil {
			m.Severity = new(int32)
		}
		*m.Severity = v
	}
}

func (m *Message) SetPayload(v string) {
	if m != nil {
		if m.Payload == nil {
			m.Payload = new(string)
		}

		*m.Payload = v
	}
}

func (m *Message) SetEnvVersion(v string) {
	if m != nil {
		if m.EnvVersion == nil {
			m.EnvVersion = new(string)
		}
		*m.EnvVersion = v
	}
}

func (m *Message) SetPid(v int32) {
	if m != nil {
		if m.Pid == nil {
			m.Pid = new(int32)
		}
		*m.Pid = v
	}
}

func (m *Message) SetHostname(v string) {
	if m != nil {
		if m.Hostname == nil {
			m.Hostname = new(string)
		}
		*m.Hostname = v
	}
}

// Message assignment operator
func (src *Message) Copy(dst *Message) {
	if src == nil || dst == nil || src == dst {
		return
	}

	if cap(src.Uuid) > 0 {
		dst.SetUuid(src.Uuid)
	} else {
		dst.Uuid = nil
	}
	if src.Timestamp != nil {
		dst.SetTimestamp(*src.Timestamp)
	} else {
		dst.Timestamp = nil
	}
	if src.Type != nil {
		dst.SetType(*src.Type)
	} else {
		dst.Type = nil
	}
	if src.Logger != nil {
		dst.SetLogger(*src.Logger)
	} else {
		dst.Logger = nil
	}
	if src.Severity != nil {
		dst.SetSeverity(*src.Severity)
	} else {
		dst.Severity = nil
	}
	if src.Payload != nil {
		dst.SetPayload(*src.Payload)
	} else {
		dst.Payload = nil
	}
	if src.EnvVersion != nil {
		dst.SetEnvVersion(*src.EnvVersion)
	} else {
		dst.EnvVersion = nil
	}
	if src.Pid != nil {
		dst.SetPid(*src.Pid)
	} else {
		dst.Pid = nil
	}
	if src.Hostname != nil {
		dst.SetHostname(*src.Hostname)
	} else {
		dst.Hostname = nil
	}
	dst.Fields = make([]*Field, len(src.Fields))
	for i, v := range src.Fields {
		dst.Fields[i] = CopyField(v)
	}
	// ignore XXX_unrecognized
}

// Message copy constructor
func CopyMessage(src *Message) *Message {
	if src == nil {
		return nil
	}
	dst := &Message{}
	src.Copy(dst)
	return dst
}

func getValueType(v reflect.Value) (t Field_ValueType, err error) {
	switch v.Kind() {
	case reflect.String:
		t = Field_STRING
	case reflect.Array, reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			t = Field_BYTES
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		t = Field_INTEGER
	case reflect.Float32, reflect.Float64:
		t = Field_DOUBLE
	case reflect.Bool:
		t = Field_BOOL
	default:
		err = fmt.Errorf("unsupported value kind: %v type: %v", v.Kind(), v.Type())
	}
	return
}

// Adds a Field (name/value) pair to the message
func (m *Message) AddField(f *Field) {
	if m == nil {
		return
	}
	l := len(m.Fields)
	c := cap(m.Fields)
	if l == c {
		tmp := make([]*Field, l+1, c*2+1)
		copy(tmp, m.Fields)
		m.Fields = tmp
	} else {
		m.Fields = m.Fields[0 : l+1]
	}
	m.Fields[l] = f
}

// Field constructor
func NewField(name string, value interface{}, valueFormat Field_ValueFormat) (f *Field, err error) {
	v := reflect.ValueOf(value)
	t, err := getValueType(v)
	if err == nil {
		f = NewFieldInit(name, t, valueFormat)
		f.AddValue(value)
	}
	return
}

// Field initializer sets up the key, value type, and format but does not actually add a value
func NewFieldInit(name string, valueType Field_ValueType, valueFormat Field_ValueFormat) *Field {
	f := &Field{}
	f.Name = new(string)
	*f.Name = name

	f.ValueType = new(Field_ValueType)
	*f.ValueType = valueType

	f.ValueFormat = new(Field_ValueFormat)
	*f.ValueFormat = valueFormat

	return f
}

// Creates an array of values in this field, of the same type, in the order they were added
func (f *Field) AddValue(value interface{}) error {
	if f == nil {
		return fmt.Errorf("Field is nil")
	}
	v := reflect.ValueOf(value)
	t, err := getValueType(v)
	if err != nil {
		return err
	}
	if t != *f.ValueType {
		return fmt.Errorf("The field contains: %v; attempted to add %v",
			Field_ValueType_name[int32(*f.ValueType)], Field_ValueType_name[int32(t)])
	}

	switch *f.ValueType {
	case Field_STRING:
		l := len(f.ValueString)
		c := cap(f.ValueString)
		if l == c {
			tmp := make([]string, l+1, c*2+1)
			copy(tmp, f.ValueString)
			f.ValueString = tmp
		} else {
			f.ValueString = f.ValueString[0 : l+1]
		}
		f.ValueString[l] = v.String()
	case Field_BYTES:
		l := len(f.ValueBytes)
		c := cap(f.ValueBytes)
		if l == c {
			tmp := make([][]byte, l+1, c*2+1)
			copy(tmp, f.ValueBytes)
			f.ValueBytes = tmp
		} else {
			f.ValueBytes = f.ValueBytes[0 : l+1]
		}
		b := v.Bytes()
		f.ValueBytes[l] = make([]byte, len(b))
		copy(f.ValueBytes[l], b)
	case Field_INTEGER:
		l := len(f.ValueInteger)
		c := cap(f.ValueInteger)
		if l == c {
			tmp := make([]int64, l+1, c*2+1)
			copy(tmp, f.ValueInteger)
			f.ValueInteger = tmp
		} else {
			f.ValueInteger = f.ValueInteger[0 : l+1]
		}
		f.ValueInteger[l] = v.Int()
	case Field_DOUBLE:
		l := len(f.ValueDouble)
		c := cap(f.ValueDouble)
		if l == c {
			tmp := make([]float64, l+1, c*2+1)
			copy(tmp, f.ValueDouble)
			f.ValueDouble = tmp
		} else {
			f.ValueDouble = f.ValueDouble[0 : l+1]
		}
		f.ValueDouble[l] = v.Float()
	case Field_BOOL:
		l := len(f.ValueBool)
		c := cap(f.ValueBool)
		if l == c {
			tmp := make([]bool, l+1, c*2+1)
			copy(tmp, f.ValueBool)
			f.ValueBool = tmp
		} else {
			f.ValueBool = f.ValueBool[0 : l+1]
		}
		f.ValueBool[l] = v.Bool()
	}
	return nil
}

// Field copy constructor
func CopyField(src *Field) *Field {
	if src == nil {
		return nil
	}
	dst := NewFieldInit(*src.Name, *src.ValueType, *src.ValueFormat)

	if src.ValueString != nil {
		dst.ValueString = make([]string, len(src.ValueString))
		copy(dst.ValueString, src.ValueString)
	}
	if src.ValueBytes != nil {
		dst.ValueBytes = make([][]byte, len(src.ValueBytes))
		copy(dst.ValueBytes, src.ValueBytes)
	}
	if src.ValueInteger != nil {
		dst.ValueInteger = make([]int64, len(src.ValueInteger))
		copy(dst.ValueInteger, src.ValueInteger)
	}
	if src.ValueDouble != nil {
		dst.ValueDouble = make([]float64, len(src.ValueDouble))
		copy(dst.ValueDouble, src.ValueDouble)
	}
	if src.ValueBool != nil {
		dst.ValueBool = make([]bool, len(src.ValueBool))
		copy(dst.ValueBool, src.ValueBool)
	}
	return dst
}

// FindFirstField finds and returns the first field with the specified name
// if not found nil is returned
func (m *Message) FindFirstField(name string) *Field {
	if m == nil {
		return nil
	}
	if m.Fields != nil {
		for i := 0; i < len(m.Fields); i++ {
			if m.Fields[i].Name != nil && *m.Fields[i].Name == name {
				return m.Fields[i]
			}
		}
	}
	return nil
}

// GetFieldValue helper function to simplify extracting single value fields
func (m *Message) GetFieldValue(name string) (value interface{}, ok bool) {
	if m == nil {
		return
	}
	f := m.FindFirstField(name)
	if f == nil {
		return
	}
	switch *f.ValueType {
	case Field_STRING:
		if len(f.ValueString) > 0 {
			value = f.ValueString[0]
			ok = true
		}
	case Field_BYTES:
		if len(f.ValueBytes) > 0 {
			value = f.ValueBytes[0]
			ok = true
		}
	case Field_INTEGER:
		if len(f.ValueInteger) > 0 {
			value = f.ValueInteger[0]
			ok = true
		}
	case Field_DOUBLE:
		if len(f.ValueDouble) > 0 {
			value = f.ValueDouble[0]
			ok = true
		}
	case Field_BOOL:
		if len(f.ValueBool) > 0 {
			value = f.ValueBool[0]
			ok = true
		}
	}
	return
}

// FindAllFields finds and returns all the fields with the specified name
// if not found a nil slice is returned
func (m *Message) FindAllFields(name string) (all []*Field) {
	if m == nil {
		return
	}
	if m.Fields != nil {
		for _, v := range m.Fields {
			if v != nil && *v.Name == name {
				l := len(all)
				c := cap(all)
				if l == c {
					tmp := make([]*Field, l+1, c*2+1)
					copy(tmp, all)
					all = tmp
				} else {
					all = all[0 : l+1]
				}
				all[l] = v
			}
		}
	}
	return
}

// Test for message equality, for use in tests.
func (m *Message) Equals(other interface{}) bool {
	vSelf := reflect.ValueOf(m).Elem()
	vOther := reflect.ValueOf(other).Elem()

	var sField, oField reflect.Value
	for i := 0; i < vSelf.NumField(); i++ {
		sField = vSelf.Field(i)
		oField = vOther.Field(i)
		switch i {
		case 0: // uuid
			if !bytes.Equal(sField.Bytes(), oField.Bytes()) {
				return false
			}
		case 1, 2, 3, 4, 5, 6, 7, 8:
			if sField.Kind() == reflect.Ptr {
				if sField.IsNil() || oField.IsNil() {
					if !(sField.IsNil() && oField.IsNil()) {
						return false
					}
				} else {
					s := reflect.Indirect(sField)
					o := reflect.Indirect(oField)
					if s.Interface() != o.Interface() {
						return false
					}
				}
			} else {
				if sField.Interface() != oField.Interface() {
					return false
				}
			}
		case 9: // Fields
			if !reflect.DeepEqual(sField.Interface(), oField.Interface()) {
				return false
			}
		case 10: // XXX_unrecognized
			// ignore
		}
	}
	return true
}

func serialize_array(arr []interface{}) string {
	results := "["
	x := false
	for _, v := range arr {
		if x {
			results += ", "
		}
		switch interface{}(v).(type) {
		case string:
			j, _ := json.Marshal(v)
			results += string(j)
		case int, int32, int64, float32, float64:
			j, _ := json.Marshal(v)
			results += string(j)

			// TODO: handle nested arrays and maps
			/*
				case map[string]interface{}:
					var tmp_map map[string]interface{}
					tmp_map = v.(map[string]interface{})
					results += serialize_map(tmp_map)
				case []interface{}:
					var tmp_arr []interface{}
					tmp_arr = v.([]interface{})
					results += serialize_array(tmp_arr)
			*/
		}
		x = true
	}
	results += "]"
	return results
}

func serialize_map(msg_fields []*Field) string {
	results := "{"
	x := false
	for _, v := range msg_fields {
		if x {
			results += ", "
		}
		field_type := v.GetValueType()
		switch field_type {
		case Field_STRING:
			j, _ := json.Marshal(v.ValueString[0])
			k, _ := json.Marshal(v.Name)
			results += fmt.Sprintf("%s: %s", string(k), string(j))
		case Field_DOUBLE:
			j, _ := json.Marshal(v.ValueDouble[0])
			k, _ := json.Marshal(v.Name)
			results += fmt.Sprintf("%s: %s", k, string(j))
			// TODO: add all Field_* type defs here
		default:
			results += "<something_else_here>"

		}
		x = true
	}
	results += "}"
	return results
}

func (self *Message) MarshalJSON() (jsonBytes []byte, err error) {

	str_ts := time.Unix(0, *self.Timestamp).Format(time.RFC3339Nano)

	sample_json := fmt.Sprintf(`{"timestamp": "%s", "type": "%s", "logger": "%s", "severity": %d, "payload": "%s", "env_version": "%s", "metlog_pid": %d, "metlog_hostname": "%s", "fields": %s}`,
		str_ts,
		self.GetType(),
		self.GetLogger(),
		self.GetSeverity(),
		self.GetPayload(),
		self.GetEnvVersion(),
		self.GetPid(),
		self.GetHostname(),
		serialize_map(self.Fields))

	return []byte(sample_json), nil
}

func (self *Message) UnmarshalJSON(msgBytes []byte) error {
	msgJson, err := simplejson.NewJson(msgBytes)
	if err != nil {
		return err
	}

	uuidString, _ := msgJson.Get("uuid").String()
	u := uuid.Parse(uuidString)
	self.SetUuid(u)

	self.SetType(msgJson.Get("type").MustString())
	timeStr := msgJson.Get("timestamp").MustString()
	t, err := time.Parse(time.RFC3339Nano, timeStr)
	if err != nil {
		log.Printf("Timestamp parsing error: %s\n", err.Error())
		return errors.New("invalid Timestamp")
	}
	self.SetTimestamp(t.UnixNano())
	self.SetLogger(msgJson.Get("logger").MustString())
	self.SetSeverity(int32(msgJson.Get("severity").MustInt()))
	self.SetPayload(msgJson.Get("payload").MustString())
	self.SetEnvVersion(msgJson.Get("env_version").MustString())
	i, _ := msgJson.Get("metlog_pid").Int()
	self.SetPid(int32(i))
	self.SetHostname(msgJson.Get("metlog_hostname").MustString())
	fields, _ := msgJson.Get("fields").Map()
	err = flattenMap(fields, self, "")
	if err != nil {
		return err
	}
	return nil
}

func flattenMap(m map[string]interface{}, msg *Message, path string) error {
	var childPath string
	for k, v := range m {
		if len(path) == 0 {
			childPath = k
		} else {
			childPath = fmt.Sprintf("%s.%s", path, k)
		}
		err := flattenValue(v, msg, childPath)
		if err != nil {
			return err
		}
	}
	return nil
}

func flattenArray(a []interface{}, msg *Message, path string) error {
	if len(a) > 0 {
		switch a[0].(type) {
		case string, float64, bool:
			f, _ := NewField(path, a[0], Field_RAW)
			for _, v := range a[1:] {
				err := f.AddValue(v)
				if err != nil {
					return err
				}
			}
			msg.AddField(f)

		default:
			var childPath string
			fmt.Println("Calling flattenValue in flattenArray")
			for i, v := range a {
				childPath = fmt.Sprintf("%s.%d", path, i)
				err := flattenValue(v, msg, childPath)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func flattenValue(v interface{}, msg *Message, path string) error {
	switch v.(type) {
	case string, float64, bool:
		f, _ := NewField(path, v, Field_RAW)
		msg.AddField(f)
	case []interface{}:
		err := flattenArray(v.([]interface{}), msg, path)
		if err != nil {
			return err
		}
	case map[string]interface{}:
		err := flattenMap(v.(map[string]interface{}), msg, path)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("Path %s, unsupported value type: %T", path, v)
	}
	return nil
}
