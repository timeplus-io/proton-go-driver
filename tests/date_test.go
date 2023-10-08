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
	"github.com/timeplus-io/proton-go-driver/v2/types"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/timeplus-io/proton-go-driver/v2"
)

func TestDate(t *testing.T) {
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
			CREATE STREAM test_date (
				  ID   uint8
				, Col1 date
				, Col2 nullable(date)
				, Col3 array(date)
				, Col4 array(nullable(date))
			)
		`
		defer func() {
			conn.Exec(ctx, "DROP STREAM test_date")
		}()
		type result struct {
			ColID uint8 `ch:"ID"`
			Col1  types.Date
			Col2  *types.Date
			Col3  []types.Date
			Col4  []*types.Date
		}

		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_date (* except _tp_time)"); assert.NoError(t, err) {
				tim, err := time.Parse("2006-01-02 15:04:05", "2022-01-12 00:00:00")
				date := types.Date{tim}
				if !assert.NoError(t, err) {
					return
				}
				if err := batch.Append(uint8(1), date, &date, []types.Date{date}, []*types.Date{&date, nil, &date}); !assert.NoError(t, err) {
					return
				}
				if err := batch.Append(uint8(2), date, nil, []types.Date{date}, []*types.Date{nil, nil, &date}); !assert.NoError(t, err) {
					return
				}

				if err := batch.Send(); assert.NoError(t, err) {
					var (
						result1 result
						result2 result
					)
					if err := conn.QueryRow(ctx, "SELECT (* except _tp_time) FROM test_date WHERE ID = $1 AND _tp_time > earliest_ts() LIMIT 1", 1).ScanStruct(&result1); assert.NoError(t, err) {
						if assert.Equal(t, date, result1.Col1) {
							assert.Equal(t, "UTC", result1.Col1.Location().String())
							assert.Equal(t, date, *result1.Col2)
							assert.Equal(t, []types.Date{date}, result1.Col3)
							assert.Equal(t, []*types.Date{&date, nil, &date}, result1.Col4)
						}
					}
					if err := conn.QueryRow(ctx, "SELECT (* except _tp_time) FROM test_date WHERE ID = $1 AND _tp_time > earliest_ts() LIMIT 1", 2).ScanStruct(&result2); assert.NoError(t, err) {
						if assert.Equal(t, date, result2.Col1) {
							assert.Equal(t, "UTC", result2.Col1.Location().String())
							if assert.Nil(t, result2.Col2) {
								assert.Equal(t, []types.Date{date}, result2.Col3)
								assert.Equal(t, []*types.Date{nil, nil, &date}, result2.Col4)
							}
						}
					}
				}
			}
		}
	}
}

func TestNullableDate(t *testing.T) {
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
			CREATE STREAM test_date (
				  Col1 date
				, Col2 nullable(date)
			) 
		`
		defer func() {
			conn.Exec(ctx, "DROP STREAM test_date")
		}()
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_date (* except _tp_time)"); assert.NoError(t, err) {
				date, err := time.Parse("2006-01-02 15:04:05", "2022-01-12 00:00:00")
				if !assert.NoError(t, err) {
					return
				}
				if err := batch.Append(date, date); assert.NoError(t, err) {
					if err := batch.Send(); assert.NoError(t, err) {
						var (
							col1 *time.Time
							col2 *time.Time
						)
						if err := conn.QueryRow(ctx, "SELECT (* except _tp_time) FROM test_date WHERE _tp_time > earliest_ts() LIMIT 1").Scan(&col1, &col2); assert.NoError(t, err) {
							assert.Equal(t, date, *col1)
							assert.Equal(t, date, *col2)
						}
					}
				}
			}
			if err := conn.Exec(ctx, "TRUNCATE STREAM test_date"); !assert.NoError(t, err) {
				return
			}
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_date (* except _tp_time)"); assert.NoError(t, err) {
				date, err := time.Parse("2006-01-02 15:04:05", "2022-01-12 00:00:00")
				if !assert.NoError(t, err) {
					return
				}
				if err := batch.Append(date, nil); assert.NoError(t, err) {
					if err := batch.Send(); assert.NoError(t, err) {
						var (
							col1 *time.Time
							col2 *time.Time
						)
						if err := conn.QueryRow(ctx, "SELECT (* except _tp_time) FROM test_date WHERE _tp_time > earliest_ts() LIMIT 1").Scan(&col1, &col2); assert.NoError(t, err) {
							if assert.Nil(t, col2) {
								assert.Equal(t, date, *col1)
								assert.Equal(t, date.Unix(), col1.Unix())
							}
						}
					}
				}
			}
		}
	}
}

func TestColumnarDate(t *testing.T) {
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
		CREATE STREAM test_date (
			  ID   uint64
			, Col1 date
			, Col2 nullable(date)
			, Col3 array(date)
			, Col4 array(nullable(date))
		) 
		`
		defer func() {
			conn.Exec(ctx, "DROP STREAM test_date")
		}()
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_date (* except _tp_time)"); assert.NoError(t, err) {
				var (
					id       []uint64
					col1Data []types.Date
					col2Data []*types.Date
					col3Data [][]types.Date
					col4Data [][]*types.Date
				)
				tim, err := time.Parse("2006-01-02 15:04:05", "2022-01-12 00:00:00")
				date := types.Date{tim}
				if !assert.NoError(t, err) {
					return
				}
				for i := 0; i < 1000; i++ {
					id = append(id, uint64(i))
					col1Data = append(col1Data, date)
					if i%2 == 0 {
						col2Data = append(col2Data, &date)
					} else {
						col2Data = append(col2Data, nil)
					}
					col3Data = append(col3Data, []types.Date{
						date, date, date,
					})
					col4Data = append(col4Data, []*types.Date{
						&date, nil, &date,
					})
				}
				{
					if err := batch.Column(0).Append(id); !assert.NoError(t, err) {
						return
					}
					if err := batch.Column(1).Append(col1Data); !assert.NoError(t, err) {
						return
					}
					if err := batch.Column(2).Append(col2Data); !assert.NoError(t, err) {
						return
					}
					if err := batch.Column(3).Append(col3Data); !assert.NoError(t, err) {
						return
					}
					if err := batch.Column(4).Append(col4Data); !assert.NoError(t, err) {
						return
					}
				}
				if assert.NoError(t, batch.Send()) {
					var result struct {
						Col1 types.Date
						Col2 *types.Date
						Col3 []types.Date
						Col4 []*types.Date
					}
					if err := conn.QueryRow(ctx, "SELECT Col1, Col2, Col3, Col4 FROM test_date WHERE ID = $1 AND _tp_time > earliest_ts() LIMIT 1", 11).ScanStruct(&result); assert.NoError(t, err) {
						if assert.Nil(t, result.Col2) {
							assert.Equal(t, date, result.Col1)
							assert.Equal(t, []types.Date{date, date, date}, result.Col3)
							assert.Equal(t, []*types.Date{&date, nil, &date}, result.Col4)
						}
					}
				}
			}
		}
	}
}
