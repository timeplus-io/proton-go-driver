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
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/timeplus-io/proton-go-driver/v2"
)

func Testlow_cardinality(t *testing.T) {
	var (
		ctx       = context.Background()
		conn, err = proton.Open(&proton.Options{
			Addr: []string{"127.0.0.1:7587"},
			Auth: proton.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			},
			Compression: &proton.Compression{
				Method: proton.CompressionLZ4,
			},
			Settings: proton.Settings{
				"allow_suspicious_low_cardinality_types": 1,
			},
			//	Debug: true,
		})
	)
	if assert.NoError(t, err) {
		if err := checkMinServerVersion(conn, 1, 0); err != nil {
			t.Skip(err.Error())
			return
		}
		const ddl = `
		CREATE STREAM test_lowcardinality (
			  Col1 low_cardinality(string)
			, Col2 low_cardinality(fixed_string(2))
			, Col3 low_cardinality(datetime)
			, Col4 low_cardinality(int32)
			, Col5 array(low_cardinality(string))
			, Col6 array(Array(low_cardinality(string)))
			, Col7 low_cardinality(nullable(string))
			, Col8 array(array(low_cardinality(nullable(string))))
		) Engine Memory
		`
		defer func() {
			conn.Exec(ctx, "DROP STREAM test_lowcardinality")
		}()
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_lowcardinality"); assert.NoError(t, err) {
				var (
					rnd       = rand.Int31()
					timestamp = time.Now()
				)
				for i := 0; i < 10; i++ {
					var (
						col1Data = timestamp.String()
						col2Data = "RU"
						col3Data = timestamp.Add(time.Duration(i) * time.Minute)
						col4Data = rnd + int32(i)
						col5Data = []string{"A", "B", "C"}
						col6Data = [][]string{
							{"Q", "W", "E"},
							{"R", "T", "Y"},
						}
						col7Data = &col2Data
						col8Data = [][]*string{
							{&col2Data, nil, &col2Data},
							{nil, &col2Data, nil},
						}
					)
					if i%2 == 0 {
						if err := batch.Append(col1Data, col2Data, col3Data, col4Data, col5Data, col6Data, col7Data, col8Data); !assert.NoError(t, err) {
							return
						}
					} else {
						if err := batch.Append(col1Data, col2Data, col3Data, col4Data, col5Data, col6Data, nil, col8Data); !assert.NoError(t, err) {
							return
						}
					}
				}
				if assert.NoError(t, batch.Send()) {
					var count uint64
					if err := conn.QueryRow(ctx, "SELECT COUNT() FROM test_lowcardinality").Scan(&count); assert.NoError(t, err) {
						assert.Equal(t, uint64(10), count)
					}
					for i := 0; i < 10; i++ {
						var (
							col1 string
							col2 string
							col3 time.Time
							col4 int32
							col5 []string
							col6 [][]string
							col7 *string
							col8 [][]*string
						)
						if err := conn.QueryRow(ctx, "SELECT * FROM test_lowcardinality WHERE Col4 = $1", rnd+int32(i)).Scan(&col1, &col2, &col3, &col4, &col5, &col6, &col7, &col8); assert.NoError(t, err) {
							assert.Equal(t, timestamp.String(), col1)
							assert.Equal(t, "RU", col2)
							assert.Equal(t, timestamp.Add(time.Duration(i)*time.Minute).Truncate(time.Second), col3)
							assert.Equal(t, rnd+int32(i), col4)
							assert.Equal(t, []string{"A", "B", "C"}, col5)
							assert.Equal(t, [][]string{
								{"Q", "W", "E"},
								{"R", "T", "Y"},
							}, col6)
							switch {
							case i%2 == 0:
								assert.Equal(t, &col2, col7)
							default:
								assert.Nil(t, col7)
							}
							col2Data := "RU"
							assert.Equal(t, [][]*string{
								{&col2Data, nil, &col2Data},
								{nil, &col2Data, nil},
							}, col8)
						}
					}
				}
			}
		}
	}
}

func TestColmnarlow_cardinality(t *testing.T) {
	var (
		ctx       = context.Background()
		conn, err = proton.Open(&proton.Options{
			Addr: []string{"127.0.0.1:7587"},
			Auth: proton.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			},
			Compression: &proton.Compression{
				Method: proton.CompressionLZ4,
			},
			Settings: proton.Settings{
				"allow_suspicious_low_cardinality_types": 1,
			},
			//	Debug: true,
		})
	)
	if assert.NoError(t, err) {
		if err := checkMinServerVersion(conn, 1, 0); err != nil {
			t.Skip(err.Error())
			return
		}
		const ddl = `
		CREATE STREAM test_lowcardinality (
			  Col1 low_cardinality(string)
			, Col2 low_cardinality(fixed_string(2))
			, Col3 low_cardinality(datetime)
			, Col4 low_cardinality(int32)
		) Engine Memory
		`
		defer func() {
			conn.Exec(ctx, "DROP STREAM test_lowcardinality")
		}()
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_lowcardinality"); assert.NoError(t, err) {
				var (
					rnd       = rand.Int31()
					timestamp = time.Now()
					col1Data  []string
					col2Data  []string
					col3Data  []time.Time
					col4Data  []int32
				)
				for i := 0; i < 10; i++ {
					col1Data = append(col1Data, timestamp.String())
					col2Data = append(col2Data, "RU")
					col3Data = append(col3Data, timestamp.Add(time.Duration(i)*time.Minute))
					col4Data = append(col4Data, rnd+int32(i))
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
				if err := batch.Column(3).Append(col4Data); !assert.NoError(t, err) {
					return
				}
				if assert.NoError(t, batch.Send()) {
					var count uint64
					if err := conn.QueryRow(ctx, "SELECT COUNT() FROM test_lowcardinality").Scan(&count); assert.NoError(t, err) {
						assert.Equal(t, uint64(10), count)
					}
					var (
						col1 string
						col2 string
						col3 time.Time
						col4 int32
					)
					if err := conn.QueryRow(ctx, "SELECT * FROM test_lowcardinality WHERE Col4 = $1", rnd+6).Scan(&col1, &col2, &col3, &col4); assert.NoError(t, err) {
						assert.Equal(t, timestamp.String(), col1)
						assert.Equal(t, "RU", col2)
						assert.Equal(t, timestamp.Add(time.Duration(6)*time.Minute).Truncate(time.Second), col3)
						assert.Equal(t, int32(rnd+6), col4)
					}
				}
			}
		}
	}
}
