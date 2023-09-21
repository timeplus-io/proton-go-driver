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

	"github.com/stretchr/testify/assert"
	"github.com/timeplus-io/proton-go-driver/v2"
	"github.com/timeplus-io/proton-go-driver/v2/lib/column"
)

func TestJson(t *testing.T) {
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
			Debug: true,
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
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_json (* except _tp_time)"); assert.NoError(t, err) {
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
						if err := conn.QueryRow(ctx, "SELECT (* except _tp_time) FROM test_json WHERE _tp_time > earliest_ts() LIMIT 1").Scan(&resultMap); assert.NoError(t, err) {
							assert.Equal(t, sourceMap, resultMap)
						}
						if err := conn.QueryRow(ctx, "SELECT (* except _tp_time) FROM test_json WHERE _tp_time > earliest_ts() LIMIT 1").Scan(&resultStr); assert.NoError(t, err) {
							assert.Equal(t, sourceStr, resultStr)
						}
					}
				}
			}
		}
	}
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
		//if err := checkMinServerVersion(conn, 99, 99); err != nil {
		//	t.Skip(err.Error())
		//	return
		//}
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
