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
	proton "github.com/timeplus-io/proton-go-driver/v2"
)

func TestBool(t *testing.T) {
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
			MaxOpenConns: 1,
		})
	)
	if assert.NoError(t, err) {
		if err := checkMinServerVersion(conn, 1, 0); err != nil {
			t.Skip(err.Error())
			return
		}
		const ddl = `
			CREATE STREAM test_bool (
				  Col1 bool
				, Col2 bool
				, Col3 array(bool)
				, Col4 nullable(bool)
				, Col5 array(nullable(bool))
			) 
		`
		defer func() {
			conn.Exec(ctx, "DROP STREAM test_bool")
		}()
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_bool (* except _tp_time)"); assert.NoError(t, err) {
				var val bool
				if err := batch.Append(true, false, []bool{true, false, true}, nil, []*bool{&val, nil, &val}); assert.NoError(t, err) {
					if err := batch.Send(); assert.NoError(t, err) {
						var (
							col1 bool
							col2 bool
							col3 []bool
							col4 *bool
							col5 []*bool
						)
						if err := conn.QueryRow(ctx, "SELECT (* except _tp_time) FROM test_bool WHERE _tp_time > earliest_ts() LIMIT 1").Scan(&col1, &col2, &col3, &col4, &col5); assert.NoError(t, err) {
							assert.Equal(t, true, col1)
							assert.Equal(t, false, col2)
							assert.Equal(t, []bool{true, false, true}, col3)
							if assert.Nil(t, col4) {
								assert.Equal(t, []*bool{&val, nil, &val}, col5)
							}
						}
					}
				}
			}
		}
	}
}

func TestColumnarBool(t *testing.T) {
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
			MaxOpenConns: 1,
		})
	)
	if assert.NoError(t, err) {
		if err := checkMinServerVersion(conn, 1, 0); err != nil {
			t.Skip(err.Error())
			return
		}
		const ddl = `
			CREATE STREAM test_bool (
				  ID   uint64
				, Col1 bool
				, Col2 bool
				, Col3 array(bool)
				, Col4 nullable(bool)
				, Col5 array(nullable(bool))
			) 
		`
		defer func() {
			conn.Exec(ctx, "DROP STREAM test_bool")
		}()
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			val := true
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_bool (* except _tp_time)"); assert.NoError(t, err) {
				var (
					id   []uint64
					col1 []bool
					col2 []bool
					col3 [][]bool
					col4 []*bool
					col5 [][]*bool
				)
				for i := 0; i < 1000; i++ {
					id = append(id, uint64(i))
					col1 = append(col1, true)
					col2 = append(col2, false)
					col3 = append(col3, []bool{true, false, true})
					col4 = append(col4, nil)
					col5 = append(col5, []*bool{&val, nil, &val})
				}
				{
					if err := batch.Column(0).Append(id); !assert.NoError(t, err) {
						return
					}
					if err := batch.Column(1).Append(col1); !assert.NoError(t, err) {
						return
					}
					if err := batch.Column(2).Append(col2); !assert.NoError(t, err) {
						return
					}
					if err := batch.Column(3).Append(col3); !assert.NoError(t, err) {
						return
					}
					if err := batch.Column(4).Append(col4); !assert.NoError(t, err) {
						return
					}
					if err := batch.Column(5).Append(col5); !assert.NoError(t, err) {
						return
					}
					if err := batch.Send(); assert.NoError(t, err) {
						var (
							id   uint64
							col1 bool
							col2 bool
							col3 []bool
							col4 *bool
							col5 []*bool
						)
						if err := conn.QueryRow(ctx, "SELECT (* except _tp_time) FROM test_bool WHERE _tp_time > earliest_ts() AND ID = $1 LIMIT 1", 42).Scan(&id, &col1, &col2, &col3, &col4, &col5); assert.NoError(t, err) {
							assert.Equal(t, true, col1)
							assert.Equal(t, false, col2)
							assert.Equal(t, []bool{true, false, true}, col3)
							if assert.Nil(t, col4) {
								assert.Equal(t, []*bool{&val, nil, &val}, col5)
							}
						}
					}
				}
			}
		}
	}
}
