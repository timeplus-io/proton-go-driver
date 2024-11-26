// Licensed to ClickHouse, Inc. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. ClickHouse, Inc. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package column

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/timeplus-io/proton-go-driver/v2/lib/binary"
)

type Json struct {
	// leaf nodes, for example '{"id": 1, "obj": { "x": "abc", "y": 2}}', the elems is:
	// <"id", []int32>,
	// <"obj.x", []string>,
	// <"obj.y", []int32>
	columns map[string]Interface
}

func (col *Json) parse() (_ Interface, err error) {
	col.columns = make(map[string]Interface)
	return col, nil
}

func (col *Json) Type() Type {
	return "json"
}

func (Json) ScanType() reflect.Type {
	return scanTypeString
}

func (col *Json) Rows() int {
	if len(col.columns) != 0 {
		for _, c := range col.columns {
			return c.Rows()
		}
	}
	return 0
}

func (col *Json) Row(i int, ptr bool) interface{} {
	json := make(map[string]interface{}, len(col.columns))
	for path, c := range col.columns {
		json[path] = c.Row(i, ptr)
	}
	return json
}

func (col *Json) ScanRow(dest interface{}, row int) error {
	switch d := dest.(type) {
	case *string:
		*d = DumpJson(NestJson(col.Row(row, false).(map[string]interface{})))
	case **string:
		**d = DumpJson(NestJson(col.Row(row, false).(map[string]interface{})))
	case *map[string]interface{}:
		*d = col.Row(row, false).(map[string]interface{})
	case **map[string]interface{}:
		**d = col.Row(row, false).(map[string]interface{})
	default:
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "json",
		}
	}
	return nil
}

func (col *Json) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	case map[string]Interface:
		if len(col.columns) == 0 {
			col.columns = v
		} else {
			old_rows := col.Rows()
			var add_rows int
			if add_rows, err = checkAndGetRows(v); err != nil {
				return nil, err
			}

			for path, c := range col.columns {
				if add_c, ok := v[path]; ok {
					c.Append(add_c)
				} else {
					for i := 0; i < add_rows; i++ {
						c.AppendRow(nil)
					}
				}
			}

			for path, add_c := range v {
				if _, ok := col.columns[path]; !ok {
					if col.columns[path], err = add_c.Type().Column(); err != nil {
						return nil, err
					}

					for i := 0; i < old_rows; i++ {
						col.columns[path].AppendRow(nil)
					}
					col.columns[path].Append(add_c)
				}
			}
		}

	default:
		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "json",
			From: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (col *Json) AppendRow(v interface{}) (err error) {
	switch v := v.(type) {
	case map[string]interface{}:
		for path, c := range col.columns {
			if add_v, ok := v[path]; ok {
				if err = c.AppendRow(add_v); err != nil {
					return err
				}
			} else {
				if err = c.AppendRow(nil); err != nil {
					return err
				}
			}
		}

		for path, add_v := range v {
			if _, ok := col.columns[path]; !ok {
				if col.columns[path], err = toType(reflect.TypeOf(add_v)).Column(); err != nil {
					return err
				}
				if err = col.columns[path].AppendRow(add_v); err != nil {
					return err
				}
			}
		}
	default:
		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   "json",
			From: fmt.Sprintf("%T", v),
		}
	}
	return nil
}

func (col *Json) Decode(decoder *binary.Decoder, rows int) (err error) {
	// deserialize json as a tuple.
	if _, err := decoder.UInt8(); err != nil {
		return err
	}

	tuple_type, err := decoder.String()
	if err != nil {
		return err
	}

	if err := deserialisation(col, tuple_type, []string{}, decoder, rows); err != nil {
		return err
	}
	return nil
}

func (col *Json) Encode(encoder *binary.Encoder) error {
	// serialize json as a tuple.
	if err := encoder.UInt8(0); err != nil {
		return err
	}
	nestedJson, err := col.nestJson()
	if err != nil {
		return err
	}

	tuple_type := nestedJson.nestedJsonType()
	if err := encoder.String(tuple_type); err != nil {
		return err
	}

	return serialisation(nestedJson, tuple_type, encoder)
}

// get full type string from nested json.
// for example:
// {"a": 1, "b": {"c": 2, "d": 3}, "e": [1,2,3,4]}
// -> "tuple(a int,b tuple(c int, d int),e array(int))"
func (json *Json) nestedJsonType() string {
	var builder strings.Builder
	builder.WriteString("tuple(")
	cnt := 0
	for key, val := range json.columns {
		builder.WriteString(fmt.Sprintf("%s ", key))
		switch v := json.columns[key].(type) {
		case *Json:
			builder.WriteString(v.nestedJsonType())
		default:
			builder.WriteString(string(val.Type()))
		}
		cnt += 1
		if cnt != len(json.columns) {
			builder.WriteString(", ")
		}
	}
	builder.WriteString(")")
	return builder.String()
}

// get a nested json format from a (path,value) json fromat.
func (col *Json) nestJson() (*Json, error) {
	root := &Json{
		columns: map[string]Interface{},
	}
	var cur *Json
	for path, value := range col.columns {
		cur = root
		parts := SplitJsonPath(path)
		for i, part := range parts {
			if strings.Contains(part, ".") {
				part = "`" + part + "`"
			}
			if i == len(parts)-1 {
				cur.columns[part] = value
				break
			}
			if _, ok := cur.columns[part]; !ok {
				cur.columns[part] = &Json{
					columns: make(map[string]Interface),
				}
			}
			var ok bool
			cur, ok = cur.columns[part].(*Json)
			if !ok {
				return nil, &Error{
					ColumnType: "json",
					Err:        fmt.Errorf("same json path with different value type"),
				}
			}
		}
	}
	return root, nil
}

var (
	_ Interface = (*Json)(nil)
)

func checkAndGetRows(json map[string]Interface) (int, error) {
	rows := -1
	for _, c := range json {
		elem_rows := c.Rows()
		if rows == -1 {
			rows = elem_rows
		} else if rows != elem_rows {
			return -1, fmt.Errorf("got inconsistent row count in json")
		}
	}

	if rows == -1 {
		return 0, nil
	}

	return rows, nil
}

func toType(t reflect.Type) Type {
	if t == nil {
		return Type("nullable(string)")
	}

	switch t.Kind() {
	case reflect.Array, reflect.Slice:
		return Type("array(" + string(toType(t.Elem())) + ")")
	case reflect.Map:
		return Type("map(" + string(toType(t.Key())) + "," + string(toType(t.Elem())) + ")")
	}
	return Type(t.Name())
}

// get full type of array, tuple, nested
func fullComplexType(builder *strings.Builder, tupleTypeWithName string) {
	st := 1
	for _, char := range tupleTypeWithName {
		if char == '(' {
			st++
		} else if char == ')' {
			st--
		}
		builder.WriteRune(char)
		if st == 0 {
			break
		}
	}
}

// deserialisation a (path,value) json fromat from a full type string.
func deserialisation(json *Json, tupleTypeWithName string, nowPath []string, decoder *binary.Decoder, rows int) error {
	typeLen := 6
	for ; typeLen < len(tupleTypeWithName) /* without ")" */ -1; typeLen += 2 {
		var nameBuilder strings.Builder
		var typeBuilder strings.Builder

		// parse name
		hasEscape := false
		for ; typeLen < len(tupleTypeWithName); typeLen++ {
			if tupleTypeWithName[typeLen] == '`' {
				hasEscape = !hasEscape
			}
			if tupleTypeWithName[typeLen] == ' ' && !hasEscape {
				typeLen++
				break
			}
			nameBuilder.WriteByte(tupleTypeWithName[typeLen])
		}
		nowPath = append(nowPath, UnescapeIfForJsonPath(nameBuilder.String()))
		if typeLen >= len(tupleTypeWithName) {
			return fmt.Errorf("parse json from tuple failed with tuple type: %s", tupleTypeWithName)
		}

		// parse sub object
		if strings.HasPrefix(tupleTypeWithName[typeLen:], "tuple(") {
			typeBuilder.WriteString("tuple(")
			fullComplexType(&typeBuilder, tupleTypeWithName[typeLen+6:])
			if err := deserialisation(json, typeBuilder.String(), nowPath, decoder, rows); err != nil {
				return err
			}
		} else {
			if strings.HasPrefix(tupleTypeWithName[typeLen:], "array(") {
				typeBuilder.WriteString("array(")
				fullComplexType(&typeBuilder, tupleTypeWithName[typeLen+6:])
			} else if strings.HasPrefix(tupleTypeWithName[typeLen:], "nested(") {
				typeBuilder.WriteString("nested(")
				fullComplexType(&typeBuilder, tupleTypeWithName[typeLen+7:])
			} else {
				for _, char := range tupleTypeWithName[typeLen:] {
					if char == ')' || char == ',' {
						break
					}
					typeBuilder.WriteRune(char)
				}
			}

			valueType, err := Type(typeBuilder.String()).Column()
			if err != nil {
				return err
			}

			path := BuildJsonPath(nowPath)
			json.columns[path] = valueType
			if err := json.columns[path].Decode(decoder, rows); err != nil {
				return err
			}
		}

		nowPath = nowPath[:len(nowPath)-1]
		typeLen += len(typeBuilder.String())
	}
	return nil
}

// serialisation a full type string from a (path, value) json format.
func serialisation(json *Json, tupleTypeWithName string, encoder *binary.Encoder) error {
	typeLen := 6
	for ; typeLen < len(tupleTypeWithName) /* without ")" */ -1; typeLen += 2 {
		var nameBuilder strings.Builder
		var typeBuilder strings.Builder

		// parse name
		has_escape := false
		for ; typeLen < len(tupleTypeWithName); typeLen++ {
			if tupleTypeWithName[typeLen] == '`' {
				has_escape = !has_escape
			}
			if tupleTypeWithName[typeLen] == ' ' && !has_escape {
				typeLen++
				break
			}
			nameBuilder.WriteByte(tupleTypeWithName[typeLen])
		}

		// parse sub object
		if strings.HasPrefix(tupleTypeWithName[typeLen:], "tuple(") {
			typeBuilder.WriteString("tuple(")
			fullComplexType(&typeBuilder, tupleTypeWithName[typeLen+6:])
			if err := serialisation(json.columns[nameBuilder.String()].(*Json), typeBuilder.String(), encoder); err != nil {
				return err
			}
		} else {
			if strings.HasPrefix(tupleTypeWithName[typeLen:], "array(") {
				typeBuilder.WriteString("array(")
				fullComplexType(&typeBuilder, tupleTypeWithName[typeLen+6:])
			} else if strings.HasPrefix(tupleTypeWithName[typeLen:], "nested(") {
				typeBuilder.WriteString("nested(")
				fullComplexType(&typeBuilder, tupleTypeWithName[typeLen+7:])
			} else {
				for _, char := range tupleTypeWithName[typeLen:] {
					if char == ')' || char == ',' {
						break
					}
					typeBuilder.WriteRune(char)
				}
			}

			if err := json.columns[nameBuilder.String()].Encode(encoder); err != nil {
				return err
			}
		}

		typeLen += len(typeBuilder.String())
	}
	return nil
}
