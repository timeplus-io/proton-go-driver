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

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestMap(t *testing.T) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{"127.0.0.1:9000"},
			Auth: clickhouse.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			},
			Compression: &clickhouse.Compression{
				Method: clickhouse.CompressionLZ4,
			},
			//Debug: true,
		})
	)
	if assert.NoError(t, err) {
		if err := checkMinServerVersion(conn, 21, 9); err != nil {
			t.Skip(err.Error())
			return
		}
		const ddl = `
		CREATE STREAM test_map (
			  Col1 map(string, uint64)
			, Col2 map(string, uint64)
			, Col3 map(string, uint64)
			, Col4 array(map(string, string))
			, Col5 map(low_cardinality(string), low_cardinality(uint64))
		) Engine Memory
		`
		defer func() {
			conn.Exec(ctx, "DROP STREAM test_map")
		}()
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_map"); assert.NoError(t, err) {
				var (
					col1Data = map[string]uint64{
						"key_col_1_1": 1,
						"key_col_1_2": 2,
					}
					col2Data = map[string]uint64{
						"key_col_2_1": 10,
						"key_col_2_2": 20,
					}
					col3Data = map[string]uint64{}
					col4Data = []map[string]string{
						map[string]string{"A": "B"},
						map[string]string{"C": "D"},
					}
					col5Data = map[string]uint64{
						"key_col_5_1": 100,
						"key_col_5_2": 200,
					}
				)
				if err := batch.Append(col1Data, col2Data, col3Data, col4Data, col5Data); assert.NoError(t, err) {
					if assert.NoError(t, batch.Send()) {
						var (
							col1 map[string]uint64
							col2 map[string]uint64
							col3 map[string]uint64
							col4 []map[string]string
							col5 map[string]uint64
						)
						if err := conn.QueryRow(ctx, "SELECT * FROM test_map").Scan(&col1, &col2, &col3, &col4, &col5); assert.NoError(t, err) {
							assert.Equal(t, col1Data, col1)
							assert.Equal(t, col2Data, col2)
							assert.Equal(t, col3Data, col3)
							assert.Equal(t, col4Data, col4)
							assert.Equal(t, col5Data, col5)
						}
					}
				}
			}
		}
	}
}

func TestColmnarMap(t *testing.T) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{"127.0.0.1:9000"},
			Auth: clickhouse.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			},
			Compression: &clickhouse.Compression{
				Method: clickhouse.CompressionLZ4,
			},
			//Debug: true,
		})
	)
	if assert.NoError(t, err) {
		if err := checkMinServerVersion(conn, 21, 9); err != nil {
			t.Skip(err.Error())
			return
		}
		const ddl = `
		CREATE STREAM test_map (
			  Col1 map(string, uint64)
			, Col2 map(string, uint64)
			, Col3 map(string, uint64)
		) Engine Memory
		`
		defer func() {
			conn.Exec(ctx, "DROP STREAM test_map")
		}()
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_map"); assert.NoError(t, err) {
				var (
					col1Data = []map[string]uint64{}
					col2Data = []map[string]uint64{}
					col3Data = []map[string]uint64{}
				)
				for i := 0; i < 100; i++ {
					col1Data = append(col1Data, map[string]uint64{
						fmt.Sprintf("key_col_1_%d_1", i): uint64(i),
						fmt.Sprintf("key_col_1_%d_2", i): uint64(i),
					})
					col2Data = append(col2Data, map[string]uint64{
						fmt.Sprintf("key_col_2_%d_1", i): uint64(i),
						fmt.Sprintf("key_col_2_%d_2", i): uint64(i),
					})
					col3Data = append(col3Data, map[string]uint64{})
				}
				if err := batch.Column(0).Append(col1Data); !assert.NoError(t, err) {
					return
				}
				if err := batch.Column(1).Append(col2Data); !assert.NoError(t, err) {
					return
				}
				if err := batch.Column(2).Append(col3Data); !assert.NoError(t, err) {
					return
				}

				if assert.NoError(t, batch.Send()) {
					var (
						col1     map[string]uint64
						col2     map[string]uint64
						col3     map[string]uint64
						col1Data = map[string]uint64{
							"key_col_1_10_1": 10,
							"key_col_1_10_2": 10,
						}
						col2Data = map[string]uint64{
							"key_col_2_10_1": 10,
							"key_col_2_10_2": 10,
						}
						col3Data = map[string]uint64{}
					)
					if err := conn.QueryRow(ctx, "SELECT * FROM test_map WHERE Col1['key_col_1_10_1'] = $1", 10).Scan(&col1, &col2, &col3); assert.NoError(t, err) {
						assert.Equal(t, col1Data, col1)
						assert.Equal(t, col2Data, col2)
						assert.Equal(t, col3Data, col3)
					}
				}
			}
		}
	}
}
