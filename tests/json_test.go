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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/timeplus-io/proton-go-driver/v2"
	"github.com/timeplus-io/proton-go-driver/v2/lib/column"
)

func TestJson(t *testing.T) {
	var (
		ctx       = context.Background()
		conn, err = proton.Open(&proton.Options{
			Addr: []string{"127.0.0.1:8463"},
			Auth: proton.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			},
			Compression: &proton.Compression{
				Method: proton.CompressionLZ4,
			},
			// Debug: true,
		})
	)
	if assert.NoError(t, err) {
		if err := checkMinServerVersion(conn, 1, 0); err != nil {
			t.Skip(err.Error())
			return
		}
		const ddl = `
		CREATE STREAM test_json (
			  Col1 json
		) 
		`
		defer func() {
			conn.Exec(ctx, "DROP STREAM test_json")
		}()
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_json (Col1)"); assert.NoError(t, err) {
				fmt.Println(batch)
				var (
					sourceMap = map[string]interface{}{"data": int32(1), "obj.a": int64(2), "obj.b": "hhh", "arr": []string{"abc", "xyz"}, "a.b.b.c": float32(1.0), "`a.b.b`.c": float64(2.0)}
					sourceStr = column.DumpJson(column.NestJson(sourceMap))
				)

				if err := batch.Append(sourceMap); assert.NoError(t, err) {
					if assert.NoError(t, batch.Send()) {
						var (
							resultMap map[string]interface{}
							resultStr string
						)
						time.Sleep(3 * time.Second)
						if err := conn.QueryRow(ctx, "SELECT Col1::json FROM test_json WHERE _tp_time > earliest_ts() LIMIT 1").Scan(&resultMap); assert.NoError(t, err) {
							assert.Equal(t, sourceMap, resultMap)
						}
						if err := conn.QueryRow(ctx, "SELECT Col1::json FROM test_json WHERE _tp_time > earliest_ts() LIMIT 1").Scan(&resultStr); assert.NoError(t, err) {
							assert.Equal(t, sourceStr, resultStr)
						}
					}
				}
			}
		}
	}
}

func TestComplexJson(t *testing.T) {
	var (
		ctx       = context.Background()
		conn, err = proton.Open(&proton.Options{
			Addr: []string{"127.0.0.1:8463"},
			Auth: proton.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			},
			Compression: &proton.Compression{
				Method: proton.CompressionLZ4,
			},
			// Debug: true,
		})
	)
	if !assert.NoError(t, err) {
		return
	}
	if err := checkMinServerVersion(conn, 1, 0); err != nil {
		t.Skip(err.Error())
		return
	}
	const ddl = `
	CREATE STREAM test_json (
		  Col1 json
	) 
	`
	defer func() {
		conn.Exec(ctx, "DROP STREAM test_json")
	}()

	if err := conn.Exec(ctx, ddl); !assert.NoError(t, err) {
		return
	}
	json1 := map[string]interface{}{
		"obj.a":     uint32(1),
		"obj.b":     []string{"abc", "xyz"},
		"obj.c.e":   []uint32{123544, 123546},
		"obj.c.f":   []string{"stream sql", "timeplus"},
		"a.b.b.c":   float32(1.1),
		"`a.b.b`.c": float64(23.1),
	}
	json2 := map[string]interface{}{
		"obj.a":     uint32((1 << 10) + (1 << 5)),
		"obj.b":     []string{"xxcccccc", "abcdefghijklmnopqrstuvwxyz", "2333556"},
		"obj.c.e":   []uint32{20030705, 987765},
		"obj.c.f":   []string{"stream sql", "proton+++"},
		"a.b.b.c":   float32(2.144623),
		"`a.b.b`.c": float64(123453.2),
	}

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_json (Col1)")
	if !assert.NoError(t, err) {
		return
	}
	if err := batch.Append(json1); !assert.NoError(t, err) {
		return
	}
	if err := batch.Append(json2); !assert.NoError(t, err) {
		return
	}
	if !assert.NoError(t, batch.Send()) {
		return
	}

	var (
		result1 map[string]interface{}
		result2 map[string]interface{}
	)
	time.Sleep(3 * time.Second)
	rows := conn.QueryRow(ctx, "SELECT Col1::json FROM test_json WHERE _tp_time > earliest_ts() LIMIT 2")
	if err := rows.Scan(&result1); !assert.NoError(t, err) {
		return
	}
	if err := rows.Scan(&result2); !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, json1, result1)
	assert.Equal(t, json2, result2)
}

func TestNullableJson(t *testing.T) {
	t.Skip("go driver of json haven't been implemented")
	var (
		ctx       = context.Background()
		conn, err = proton.Open(&proton.Options{
			Addr: []string{"127.0.0.1:8463"},
			Auth: proton.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			},
			Compression: &proton.Compression{
				Method: proton.CompressionLZ4,
			},
			//Debug: true,
		})
	)
	if assert.NoError(t, err) {
		const ddl = `
		CREATE STREAM test_json (
			  Col1 json
		) 
		`
		defer func() {
			conn.Exec(ctx, "DROP STREAM test_json")
		}()
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_json (* except _tp_time)"); assert.NoError(t, err) {
				var (
					sourceMap = map[string]interface{}{"data": nil}
					sourceStr = "{\"data\": <nil>}"
				)
				if err := batch.Append(sourceMap); assert.NoError(t, err) {
					if assert.NoError(t, batch.Send()) {
						var (
							resultMap map[string]interface{}
							resultStr string
						)
						if err := conn.QueryRow(ctx, "SELECT Col1 FROM test_json where Col1.data is null").Scan(&resultMap); assert.NoError(t, err) {
							assert.Equal(t, map[string]interface{}{"data": nil}, resultMap)
						}
						if err := conn.QueryRow(ctx, "SELECT Col1 FROM test_json where Col1.data is null").Scan(&resultStr); assert.NoError(t, err) {
							assert.Equal(t, sourceStr, resultStr)
						}
					}
				}
			}
		}
	}
}
