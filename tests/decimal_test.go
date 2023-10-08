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

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/timeplus-io/proton-go-driver/v2"
)

func TestDecimal(t *testing.T) {
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
			Settings: proton.Settings{
				"allow_experimental_bigint_types": 1,
			},
			//Debug: true,
		})
	)
	if assert.NoError(t, err) {
		if err := checkMinServerVersion(conn, 1, 0); err != nil {
			t.Skip(err.Error())
			return
		}
		const ddl = `
			CREATE STREAM test_decimal (
				  Col1 decimal32(5)
				, Col2 decimal(18,5)
				, Col3 decimal(15,3)
				, Col4 decimal128(5)
				, Col5 decimal256(5)
			) 
		`
		defer func() {
			conn.Exec(ctx, "DROP STREAM test_decimal")
		}()
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_decimal (* except _tp_time)"); assert.NoError(t, err) {
				if err := batch.Append(
					decimal.New(25, 0),
					decimal.New(30, 0),
					decimal.New(35, 0),
					decimal.New(135, 0),
					decimal.New(256, 0),
				); !assert.NoError(t, err) {
					return
				}
				if assert.NoError(t, batch.Send()) {
					var (
						col1 decimal.Decimal
						col2 decimal.Decimal
						col3 decimal.Decimal
						col4 decimal.Decimal
						col5 decimal.Decimal
					)
					if err := conn.QueryRow(ctx, "SELECT (* except _tp_time) FROM test_decimal WHERE _tp_time > earliest_ts() LIMIT 1").Scan(&col1, &col2, &col3, &col4, &col5); assert.NoError(t, err) {
						assert.True(t, decimal.New(25, 0).Equal(col1))
						assert.True(t, decimal.New(30, 0).Equal(col2))
						assert.True(t, decimal.New(35, 0).Equal(col3))
						assert.True(t, decimal.New(135, 0).Equal(col4))
						assert.True(t, decimal.New(256, 0).Equal(col5))
					}
				}
			}
		}
	}
}

func TestNullableDecimal(t *testing.T) {
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
			Settings: proton.Settings{
				"allow_experimental_bigint_types": 1,
			},
			//Debug: true,
		})
	)
	if assert.NoError(t, err) {
		if err := checkMinServerVersion(conn, 1, 0); err != nil {
			t.Skip(err.Error())
			return
		}
		const ddl = `
		CREATE STREAM test_decimal (
			  Col1 nullable(decimal32(5))
			, Col2 nullable(decimal(18,5))
			, Col3 nullable(decimal(15,3))
		) 
		`
		defer func() {
			conn.Exec(ctx, "DROP STREAM test_decimal")
		}()
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_decimal (* except _tp_time)"); assert.NoError(t, err) {
				if err := batch.Append(decimal.New(25, 0), decimal.New(30, 0), decimal.New(35, 0)); !assert.NoError(t, err) {
					return
				}
				if assert.NoError(t, batch.Send()) {
					var (
						col1 *decimal.Decimal
						col2 *decimal.Decimal
						col3 *decimal.Decimal
					)
					if err := conn.QueryRow(ctx, "SELECT (* except _tp_time) FROM test_decimal WHERE _tp_time > earliest_ts() LIMIT 1").Scan(&col1, &col2, &col3); assert.NoError(t, err) {
						assert.True(t, decimal.New(25, 0).Equal(*col1))
						assert.True(t, decimal.New(30, 0).Equal(*col2))
						assert.True(t, decimal.New(35, 0).Equal(*col3))
					}
				}
			}

			if err := conn.Exec(ctx, "TRUNCATE STREAM test_decimal"); !assert.NoError(t, err) {
				return
			}
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_decimal (* except _tp_time)"); assert.NoError(t, err) {
				if err := batch.Append(decimal.New(25, 0), nil, decimal.New(35, 0)); !assert.NoError(t, err) {
					return
				}
				if assert.NoError(t, batch.Send()) {
					var (
						col1 *decimal.Decimal
						col2 *decimal.Decimal
						col3 *decimal.Decimal
					)
					if err := conn.QueryRow(ctx, "SELECT (* except _tp_time) FROM test_decimal WHERE _tp_time > earliest_ts() LIMIT 1").Scan(&col1, &col2, &col3); assert.NoError(t, err) {
						if assert.Nil(t, col2) {
							assert.True(t, decimal.New(25, 0).Equal(*col1))
							assert.True(t, decimal.New(35, 0).Equal(*col3))
						}
					}
				}
			}
		}
	}
}
