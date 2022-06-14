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

package tests

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/timeplus-io/proton-go-driver/v2/lib/column"
)

func TestDumpJson(t *testing.T) {
	source_str := "hello"
	check_str_dump := "\"hello\""
	assert.Equal(t, check_str_dump, column.DumpJson(source_str))

	source_str_array := []string{"hello", "world"}
	source_str_array_dump := "[\"hello\", \"world\"]"
	assert.Equal(t, source_str_array_dump, column.DumpJson(source_str_array))

	source_array := []string{"abc", "xyz"}
	check_array_dump := "[\"abc\", \"xyz\"]"
	assert.Equal(t, check_array_dump, column.DumpJson(source_array))

	source_map := map[string]interface{}{"data": int32(1), "obj.a": int64(2), "obj.b": "hhh", "arr": []string{"abc", "xyz"}, "a.b.b.c": float32(1.0), "`a.b.b`.c": float64(2.0)}
	check_map_dump := "{\"`a.b.b`.c\": 2, \"a.b.b.c\": 1, \"arr\": [\"abc\", \"xyz\"], \"data\": 1, \"obj.a\": 2, \"obj.b\": \"hhh\"}"
	assert.Equal(t, check_map_dump, column.DumpJson(source_map))
}

func BenchmarkDumpJson(b *testing.B) {
	source_map := map[string]interface{}{"data": int32(1), "obj.a": int64(2), "obj.b": "hhh", "arr": []string{"abc", "xyz"}, "a.b.b.c": float32(1.0), "`a.b.b`.c": float64(2.0)}
	check_map_dump := "{\"`a.b.b`.c\": 2, \"a.b.b.c\": 1, \"arr\": [\"abc\", \"xyz\"], \"data\": 1, \"obj.a\": 2, \"obj.b\": \"hhh\"}"

	assert.Equal(b, check_map_dump, column.DumpJson(source_map))

	for n := 0; n < b.N; n++ {
		column.DumpJson(source_map)
	}
}

func TestNestJson(t *testing.T) {
	source_map := map[string]interface{}{"data": int32(1), "obj.a": int64(2), "obj.b": "hhh", "arr": []string{"abc", "xyz"}, "a.b.b.c": float32(1.0), "`a.b.b`.c": float64(2.0)}
	check_map_nested := map[string]interface{}{"data": int32(1), "obj": map[string]interface{}{"a": int64(2), "b": "hhh"}, "arr": []string{"abc", "xyz"}, "a": map[string]interface{}{"b": map[string]interface{}{"b": map[string]interface{}{"c": float32(1.0)}}}, "a.b.b": map[string]interface{}{"c": float64(2.0)}}
	assert.Equal(t, check_map_nested, column.NestJson(source_map))
}

func BenchmarkNestJson(b *testing.B) {
	source_map := map[string]interface{}{"data": int32(1), "obj.a": int64(2), "obj.b": "hhh", "arr": []string{"abc", "xyz"}, "a.b.b.c": float32(1.0), "`a.b.b`.c": float64(2.0)}
	check_map_nested := map[string]interface{}{"data": int32(1), "obj": map[string]interface{}{"a": int64(2), "b": "hhh"}, "arr": []string{"abc", "xyz"}, "a": map[string]interface{}{"b": map[string]interface{}{"b": map[string]interface{}{"c": float32(1.0)}}}, "a.b.b": map[string]interface{}{"c": float64(2.0)}}

	assert.Equal(b, check_map_nested, column.NestJson(source_map))

	for n := 0; n < b.N; n++ {
		column.NestJson(source_map)
	}
}

func TestSplitJsonPath(t *testing.T) {
	assert.Equal(t, []string{"a"}, column.SplitJsonPath("a"))
	assert.Equal(t, []string{"a"}, column.SplitJsonPath("`a`"))
	assert.Equal(t, []string{"a", "b"}, column.SplitJsonPath("a.b"))
	assert.Equal(t, []string{"a.b"}, column.SplitJsonPath("`a.b`"))
	assert.Equal(t, []string{"a.b.b", "c"}, column.SplitJsonPath("`a.b.b`.c"))

	assert.Equal(t, []string{"a", "b", ""}, column.SplitJsonPath("a.b."))
	assert.Equal(t, []string{"", ""}, column.SplitJsonPath("."))
}

func BenchmarkSplitJsonPath(b *testing.B) {
	source_str := "`a.b.b`.c"
	check_str_split := []string{"a.b.b", "c"}
	assert.Equal(b, check_str_split, column.SplitJsonPath(source_str))

	for n := 0; n < b.N; n++ {
		column.SplitJsonPath(source_str)
	}
}

func TestBuildJsonPath(t *testing.T) {
	assert.Equal(t, "a", column.BuildJsonPath([]string{"a"}))
	assert.Equal(t, "`a`", column.BuildJsonPath([]string{"`a`"}))
	assert.Equal(t, "a.b", column.BuildJsonPath([]string{"a", "b"}))
	assert.Equal(t, "`a.b`", column.BuildJsonPath([]string{"`a.b`"}))
	assert.Equal(t, "`a.b.b`.c", column.BuildJsonPath([]string{"a.b.b", "c"}))

	assert.Equal(t, "a.b.", column.BuildJsonPath([]string{"a", "b", ""}))
	assert.Equal(t, ".", column.BuildJsonPath([]string{"", ""}))
}

func BenchmarkBuildJsonPath(b *testing.B) {
	source_str_array := []string{"a.b.b", "c", "x", "y"}
	check_str_built := "`a.b.b`.c.x.y"
	assert.Equal(b, check_str_built, column.BuildJsonPath(source_str_array))

	for n := 0; n < b.N; n++ {
		column.BuildJsonPath(source_str_array)
	}
}

func TestEscapeIfForJsonPath(t *testing.T) {
	assert.Equal(t, "`x.y`", column.EscapeIfForJsonPath("x.y"))
	assert.Equal(t, "`\\`x.y\\`.a`", column.EscapeIfForJsonPath("`x.y`.a"))
	assert.Equal(t, "`\\`x.y\\`\\`.a`", column.EscapeIfForJsonPath("`x.y``.a"))

	assert.Equal(t, "x", column.EscapeIfForJsonPath("x"))
	assert.Equal(t, "`x.y`", column.EscapeIfForJsonPath("`x.y`"))
	assert.Equal(t, "`x.y`a", column.EscapeIfForJsonPath("`x.y`a"))
}

func BenchmarkEscapeIfForJsonPath(b *testing.B) {
	source_str := "`x.y`.a"
	check_str_escaped := "`\\`x.y\\`.a`"
	assert.Equal(b, check_str_escaped, column.EscapeIfForJsonPath(source_str))

	for n := 0; n < b.N; n++ {
		column.EscapeIfForJsonPath(source_str)
	}
}

func TestUnescapeIfForJsonPath(t *testing.T) {
	assert.Equal(t, "x.y", column.UnescapeIfForJsonPath("`x.y`"))
	assert.Equal(t, "`x.y`.a", column.UnescapeIfForJsonPath("`\\`x.y\\`.a`"))

	assert.Equal(t, "x", column.UnescapeIfForJsonPath("x"))
	assert.Equal(t, "`x.y`.a", column.UnescapeIfForJsonPath("`x.y`.a"))
	assert.Equal(t, "\\`x.y\\`.a", column.UnescapeIfForJsonPath("\\`x.y\\`.a"))
	assert.Equal(t, "`x.y`.a`", column.UnescapeIfForJsonPath("`x.y`.a`"))
	assert.Equal(t, "``", column.UnescapeIfForJsonPath("``"))
}

func BenchmarkUnescapeIfForJsonPath(b *testing.B) {
	source_str := "`\\`x.y\\`.a`"
	check_str_unescaped := "`x.y`.a"
	assert.Equal(b, check_str_unescaped, column.UnescapeIfForJsonPath(source_str))

	for n := 0; n < b.N; n++ {
		column.EscapeIfForJsonPath(source_str)
	}
}
