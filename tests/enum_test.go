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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/timeplus-io/proton-go-driver/v2"
)

func TestEnum(t *testing.T) {
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
			CREATE STREAM test_enum (
				  Col1 enum  ('hello'   = 1,  'world' = 2)
				, Col2 enum8 ('click'   = 5,  'house' = 25)
				, Col3 enum16('house' = 10,   'value' = 50)
				, Col4 array(enum8  ('click' = 1, 'house' = 2))
				, Col5 array(enum16 ('click' = 1, 'house' = 2))
				, Col6 array(nullable(enum8  ('click' = 1, 'house' = 2)))
				, Col7 array(nullable(enum16 ('click' = 1, 'house' = 2)))
			) 
		`
		defer func() {
			conn.Exec(ctx, "DROP STREAM test_enum")
		}()
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_enum (* except _tp_time)"); assert.NoError(t, err) {
				var (
					col1Data = "hello"
					col2Data = "click"
					col3Data = "house"
					col4Data = []string{"click", "house"}
					col5Data = []string{"house", "click"}
					col6Data = []*string{&col2Data, nil, &col3Data}
					col7Data = []*string{&col3Data, nil, &col2Data}
				)
				if err := batch.Append(
					col1Data,
					col2Data,
					col3Data,
					col4Data,
					col5Data,
					col6Data,
					col7Data,
				); assert.NoError(t, err) {
					if err := batch.Send(); assert.NoError(t, err) {
						var (
							col1 string
							col2 string
							col3 string
							col4 []string
							col5 []string
							col6 []*string
							col7 []*string
						)
						if err := conn.QueryRow(ctx, "SELECT (* except _tp_time) FROM test_enum WHERE _tp_time > earliest_ts() LIMIT 1").Scan(
							&col1, &col2, &col3, &col4,
							&col5, &col6, &col7,
						); assert.NoError(t, err) {
							assert.Equal(t, col1Data, col1)
							assert.Equal(t, col2Data, col2)
							assert.Equal(t, col3Data, col3)
							assert.Equal(t, col4Data, col4)
							assert.Equal(t, col5Data, col5)
							assert.Equal(t, col6Data, col6)
							assert.Equal(t, col7Data, col7)
						}
					}
				}
			}
		}
	}
}

func TestNullableEnum(t *testing.T) {
	t.Skip("Proton doesn't support TRUNCATE operation for streaming query")
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
			CREATE STREAM test_enum (
				  Col1 nullable(enum  ('hello'   = 1,  'world' = 2))
				, Col2 nullable(enum8 ('click'   = 5,  'house' = 25))
				, Col3 nullable(enum16('default' = 10, 'value' = 50))
			) 
		`
		defer func() {
			conn.Exec(ctx, "DROP STREAM test_enum")
		}()
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_enum (* except _tp_time)"); assert.NoError(t, err) {
				if err := batch.Append("hello", "click", "value"); assert.NoError(t, err) {
					if err := batch.Send(); assert.NoError(t, err) {
						var (
							col1 string
							col2 string
							col3 string
						)
						if err := conn.QueryRow(ctx, "SELECT (* except _tp_time) FROM test_enum WHERE _tp_time > earliest_ts() LIMIT 1").Scan(&col1, &col2, &col3); assert.NoError(t, err) {
							assert.Equal(t, "hello", col1)
							assert.Equal(t, "click", col2)
							assert.Equal(t, "value", col3)
						}
					}
				}
			}
			if err := conn.Exec(ctx, "TRUNCATE STREAM test_enum"); !assert.NoError(t, err) {
				return
			}
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_enum (* except _tp_time)"); assert.NoError(t, err) {
				if err := batch.Append("hello", nil, "value"); assert.NoError(t, err) {
					if err := batch.Send(); assert.NoError(t, err) {
						var (
							col1 *string
							col2 *string
							col3 *string
						)
						if err := conn.QueryRow(ctx, "SELECT (* except _tp_time) FROM test_enum WHERE _tp_time > earliest_ts() LIMIT 1").Scan(&col1, &col2, &col3); assert.NoError(t, err) {
							if assert.Nil(t, col2) {
								assert.Equal(t, "hello", *col1)
								assert.Equal(t, "value", *col3)
							}
						}
					}
				}
			}
		}
	}
}

func TestColumnarEnum(t *testing.T) {
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
			CREATE STREAM test_enum (
				  Col1 enum  ('hello'   = 1,  'world' = 2)
				, Col2 enum8 ('click'   = 5,  'house' = 25)
				, Col3 enum16('house' = 10,   'value' = 50)
				, Col4 array(enum8  ('click' = 1, 'house' = 2))
				, Col5 array(enum16 ('click' = 1, 'house' = 2))
				, Col6 array(nullable(enum8  ('click' = 1, 'house' = 2)))
				, Col7 array(nullable(enum16 ('click' = 1, 'house' = 2)))
			) 
		`
		defer func() {
			conn.Exec(ctx, "DROP STREAM test_enum")
		}()
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_enum (* except _tp_time)"); assert.NoError(t, err) {
				var (
					col1Data = "hello"
					col2Data = "click"
					col3Data = "house"
					col4Data = []string{"click", "house"}
					col5Data = []string{"house", "click"}
					col6Data = []*string{&col2Data, nil, &col3Data}
					col7Data = []*string{&col3Data, nil, &col2Data}
				)

				if err := batch.Column(0).Append([]string{
					col1Data, col1Data, col1Data,
				}); !assert.NoError(t, err) {
					return
				}
				if err := batch.Column(1).Append([]string{
					col2Data, col2Data, col2Data,
				}); !assert.NoError(t, err) {
					return
				}
				if err := batch.Column(2).Append([]string{
					col3Data, col3Data, col3Data,
				}); !assert.NoError(t, err) {
					return
				}
				if err := batch.Column(3).Append([][]string{
					col4Data, col4Data, col4Data,
				}); !assert.NoError(t, err) {
					return
				}
				if err := batch.Column(4).Append([][]string{
					col5Data, col5Data, col5Data,
				}); !assert.NoError(t, err) {
					return
				}
				if err := batch.Column(5).Append([][]*string{
					col6Data, col6Data, col6Data,
				}); !assert.NoError(t, err) {
					return
				}
				if err := batch.Column(6).Append([][]*string{
					col7Data, col7Data, col7Data,
				}); !assert.NoError(t, err) {
					return
				}
				if err := batch.Send(); assert.NoError(t, err) {
					var (
						col1 string
						col2 string
						col3 string
						col4 []string
						col5 []string
						col6 []*string
						col7 []*string
					)
					if err := conn.QueryRow(ctx, "SELECT (* except _tp_time) FROM test_enum WHERE _tp_time > earliest_ts() LIMIT 1").Scan(
						&col1, &col2, &col3, &col4,
						&col5, &col6, &col7,
					); assert.NoError(t, err) {
						assert.Equal(t, col1Data, col1)
						assert.Equal(t, col2Data, col2)
						assert.Equal(t, col3Data, col3)
						assert.Equal(t, col4Data, col4)
						assert.Equal(t, col5Data, col5)
						assert.Equal(t, col6Data, col6)
						assert.Equal(t, col7Data, col7)
					}
				}
			}
		}
	}
}
