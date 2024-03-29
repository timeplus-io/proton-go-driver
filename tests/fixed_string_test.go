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
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/timeplus-io/proton-go-driver/v2"
)

type BinFixedString struct {
	data [10]byte
}

func (bin *BinFixedString) MarshalBinary() ([]byte, error) {
	return bin.data[:], nil
}

func (bin *BinFixedString) UnmarshalBinary(b []byte) error {
	copy(bin.data[:], b)
	return nil
}

func TestFixedString(t *testing.T) {
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
			CREATE STREAM test_fixed_string (
				Col1 fixed_string(10)
				, Col2 fixed_string(10)
				, Col3 nullable(fixed_string(10))
				, Col4 array(fixed_string(10))
				, Col5 array(nullable(fixed_string(10)))
			) 
		`
		defer func() {
			conn.Exec(ctx, "DROP STREAM test_fixed_string")
		}()
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_fixed_string (* except _tp_time)"); assert.NoError(t, err) {
				var (
					col1Data = "ClickHouse"
					col2Data = &BinFixedString{}
					col3Data = &col1Data
					col4Data = []string{"ClickHouse", "ClickHouse", "ClickHouse"}
					col5Data = []*string{&col1Data, nil, &col1Data}
				)
				if _, err := rand.Read(col2Data.data[:]); assert.NoError(t, err) {
					if err := batch.Append(col1Data, col2Data, col3Data, col4Data, col5Data); assert.NoError(t, err) {
						if assert.NoError(t, batch.Send()) {
							var (
								col1 string
								col2 BinFixedString
								col3 *string
								col4 []string
								col5 []*string
							)
							if err := conn.QueryRow(ctx, "SELECT (* except _tp_time) FROM test_fixed_string WHERE _tp_time > earliest_ts() LIMIT 1").Scan(&col1, &col2, &col3, &col4, &col5); assert.NoError(t, err) {
								assert.Equal(t, col1Data, col1)
								assert.Equal(t, col2Data.data, col2.data)
								assert.Equal(t, col3Data, col3)
								assert.Equal(t, col4Data, col4)
								assert.Equal(t, col5Data, col5)
							}
						}
					}
				}
			}
		}

		if rows, err := conn.Query(ctx, "SELECT CAST('RU' AS fixed_string(2)) FROM system.numbers_mt LIMIT 10"); assert.NoError(t, err) {
			var count int
			for rows.Next() {
				var code string
				if !assert.NoError(t, rows.Scan(&code)) || !assert.Equal(t, "RU", code) {
					return
				}
				count++
			}
			if assert.Equal(t, 10, count) && assert.NoError(t, rows.Err()) {
				assert.NoError(t, rows.Close())
			}
		}
	}
}

func TestNullableFixedString(t *testing.T) {
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
		CREATE STREAM test_fixed_string (
			  Col1 nullable(fixed_string(10))
			, Col2 nullable(fixed_string(10))
		) 
		`
		defer func() {
			conn.Exec(ctx, "DROP STREAM test_fixed_string")
		}()
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_fixed_string (* except _tp_time)"); assert.NoError(t, err) {
				var (
					col1Data = "ClickHouse"
					col2Data = &BinFixedString{}
				)
				if _, err := rand.Read(col2Data.data[:]); assert.NoError(t, err) {
					if err := batch.Append(col1Data, col2Data); assert.NoError(t, err) {
						if assert.NoError(t, batch.Send()) {
							var (
								col1 string
								col2 BinFixedString
							)
							if err := conn.QueryRow(ctx, "SELECT (* except _tp_time) FROM test_fixed_string WHERE _tp_time > earliest_ts() LIMIT 1").Scan(&col1, &col2); assert.NoError(t, err) {
								assert.Equal(t, col1Data, col1)
								assert.Equal(t, col2Data.data, col2.data)
							}
						}
					}
				}
			}
			if err := conn.Exec(ctx, "TRUNCATE STREAM test_fixed_string"); !assert.NoError(t, err) {
				return
			}
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_fixed_string (* except _tp_time)"); assert.NoError(t, err) {
				var col1Data = "ClickHouse"
				if err := batch.Append(col1Data, nil); assert.NoError(t, err) {
					if assert.NoError(t, batch.Send()) {
						var (
							col1 *string
							col2 *string
						)
						if err := conn.QueryRow(ctx, "SELECT (* except _tp_time) FROM test_fixed_string WHERE _tp_time > earliest_ts() LIMIT 1").Scan(&col1, &col2); assert.NoError(t, err) {
							if assert.Nil(t, col2) {
								assert.Equal(t, col1Data, *col1)
							}
						}
					}
				}
			}
		}
	}
}

func TestColumnarFixedString(t *testing.T) {
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
		CREATE STREAM test_fixed_string (
			  Col1 fixed_string(10)
			, Col2 fixed_string(10)
			, Col3 nullable(fixed_string(10))
			, Col4 array(fixed_string(10))
			, Col5 array(nullable(fixed_string(10)))
		) 
		`
		defer func() {
			conn.Exec(ctx, "DROP STREAM test_fixed_string")
		}()
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_fixed_string (* except _tp_time)"); assert.NoError(t, err) {
				var (
					col1Data = "ClickHouse"
					col2Data = "XXXXXXXXXX"
					col3Data = &col1Data
					col4Data = []string{"ClickHouse", "ClickHouse", "ClickHouse"}
					col5Data = []*string{&col1Data, nil, &col1Data}
				)
				if err := batch.Column(0).Append([]string{
					col1Data, col1Data, col1Data, col1Data, col1Data,
				}); !assert.NoError(t, err) {
					return
				}
				if err := batch.Column(1).Append([]string{
					col2Data, col2Data, col2Data, col2Data, col2Data,
				}); !assert.NoError(t, err) {
					return
				}
				if err := batch.Column(2).Append([]*string{
					col3Data, col3Data, col3Data, col3Data, col3Data,
				}); !assert.NoError(t, err) {
					return
				}
				if err := batch.Column(3).Append([][]string{
					col4Data, col4Data, col4Data, col4Data, col4Data,
				}); !assert.NoError(t, err) {
					return
				}
				if err := batch.Column(4).Append([][]*string{
					col5Data, col5Data, col5Data, col5Data, col5Data,
				}); !assert.NoError(t, err) {
					return
				}
				if assert.NoError(t, batch.Send()) {
					var (
						col1 string
						col2 string
						col3 *string
						col4 []string
						col5 []*string
					)
					if err := conn.QueryRow(ctx, "SELECT (* except _tp_time) FROM test_fixed_string WHERE _tp_time > earliest_ts() LIMIT 1").Scan(&col1, &col2, &col3, &col4, &col5); assert.NoError(t, err) {
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

func BenchmarkFixedString(b *testing.B) {
	var (
		ctx       = context.Background()
		conn, err = proton.Open(&proton.Options{
			Addr: []string{"127.0.0.1:8463"},
			Auth: proton.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			},
		})
	)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		conn.Exec(ctx, "DROP STREAM benchmark_fixed_string")
	}()
	if err = conn.Exec(ctx, `DROP STREAM IF EXISTS benchmark_fixed_string`); err != nil {
		b.Fatal(err)
	}
	if err = conn.Exec(ctx, `CREATE STREAM benchmark_fixed_string (Col1 uint64, Col2 fixed_string(4))`); err != nil {
		b.Fatal(err)
	}

	const rowsInBlock = 10_000_000

	for n := 0; n < b.N; n++ {
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO benchmark_fixed_string VALUES (* except _tp_time)")
		if err != nil {
			b.Fatal(err)
		}
		for i := 0; i < rowsInBlock; i++ {
			if err := batch.Append(uint64(1), "test"); err != nil {
				b.Fatal(err)
			}
		}
		if err = batch.Send(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkColumnarFixedString(b *testing.B) {
	var (
		ctx       = context.Background()
		conn, err = proton.Open(&proton.Options{
			Addr: []string{"127.0.0.1:8463"},
			Auth: proton.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			},
		})
	)
	if err != nil {
		b.Fatal(err)
	}

	defer func() {
		conn.Exec(ctx, "DROP STREAM benchmark_fixed_string")
	}()
	if err = conn.Exec(ctx, `CREATE STREAM benchmark_fixed_string (Col1 uint64, Col2 fixed_string(4))`); err != nil {
		b.Fatal(err)
	}

	const rowsInBlock = 10_000_000

	var (
		col1 []uint64
		col2 []string
	)
	for n := 0; n < b.N; n++ {
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO benchmark_fixed_string VALUES (* except _tp_time)")
		if err != nil {
			b.Fatal(err)
		}
		col1 = col1[:0]
		col2 = col2[:0]
		for i := 0; i < rowsInBlock; i++ {
			col1 = append(col1, uint64(1))
			col2 = append(col2, "test")
		}
		if err := batch.Column(0).Append(col1); err != nil {
			b.Fatal(err)
		}
		if err := batch.Column(1).Append(col2); err != nil {
			b.Fatal(err)
		}
		if err = batch.Send(); err != nil {
			b.Fatal(err)
		}
	}
}
