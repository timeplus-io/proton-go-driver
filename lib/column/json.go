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

	"github.com/timeplus-io/proton-go-driver/v2/lib/binary"
)

type Json struct {
	nullable bool

	// leaf nodes, for example '{"id": 1, "obj": { "x": "abc", "y": 2}}', the elems is:
	// <"id", []int32>,
	// <"obj.x", []string>,
	// <"obj.y", []int32>
	columns map[string]Interface
}

func (col *Json) parse(is_nullable bool) (_ Interface, err error) {
	col.nullable = is_nullable
	col.columns = make(map[string]Interface)
	return col, nil
}

func (col *Json) Type() Type {
	if col.nullable {
		return "nullable_json"
	}
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
				c.AppendRow(add_v)
			} else {
				c.AppendRow(nil)
			}
		}

		for path, add_v := range v {
			if _, ok := col.columns[path]; !ok {
				if col.columns[path], err = toType(reflect.TypeOf(add_v)).Column(); err != nil {
					return err
				}
				col.columns[path].AppendRow(add_v)
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
	var columns uint64
	if columns, err = decoder.Uvarint(); err != nil {
		return err
	}

	for i := uint64(0); i < columns; i++ {
		var parts_len uint64
		if parts_len, err = decoder.Uvarint(); err != nil {
			return err
		}

		var parts []string
		parts = make([]string, parts_len)
		for i := uint64(0); i < parts_len; i++ {
			if parts[i], err = decoder.String(); err != nil {
				return err
			}
			if _, err = decoder.Uvarint(); err != nil {
				return err
			}
			if _, err = decoder.Uvarint(); err != nil {
				return err
			}
		}

		var t string
		if t, err = decoder.String(); err != nil {
			return err
		}

		path := BuildJsonPath(parts)
		if col.columns[path], err = Type(t).Column(); err != nil {
			return err
		}
		if err = col.columns[path].Decode(decoder, rows); err != nil {
			return err
		}
	}
	return nil
}

func (col *Json) Encode(encoder *binary.Encoder) error {
	if err := encoder.Uvarint(uint64(len(col.columns))); err != nil {
		return err
	}

	for path, c := range col.columns {
		parts := SplitJsonPath(path)
		if err := encoder.Uvarint(uint64(len(parts))); err != nil {
			return err
		}

		for _, part := range parts {
			if err := encoder.String(part); err != nil {
				return err
			}
			if err := encoder.Uvarint(0); err != nil {
				return err
			}
			if err := encoder.Uvarint(0); err != nil {
				return err
			}
		}

		if err := encoder.String(string(c.Type())); err != nil {
			return err
		}

		if err := c.Encode(encoder); err != nil {
			return err
		}
	}
	return nil
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
			return -1, fmt.Errorf("Got inconsistent row count in json")
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
