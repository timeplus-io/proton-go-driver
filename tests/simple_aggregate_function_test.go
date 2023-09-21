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

func TestSimpleAggregateFunction(t *testing.T) {
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
		CREATE STREAM test_simple_aggregate_function (
			  Col1 uint64
			, Col2 simple_aggregate_function(sum, double)
			, Col3 simple_aggregate_function(sum_map, tuple(array(int16), array(uint64)))
		) 
		`
		defer func() {
			conn.Exec(ctx, "DROP STREAM test_simple_aggregate_function")
		}()
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_simple_aggregate_function (* except _tp_time)"); assert.NoError(t, err) {
				var (
					col1Data = uint64(42)
					col2Data = float64(256.1)
					col3Data = []interface{}{
						[]int16{1, 2, 3, 4, 5},
						[]uint64{1, 2, 3, 4, 5},
					}
				)
				if err := batch.Append(col1Data, col2Data, col3Data); !assert.NoError(t, err) {
					return
				}
				if assert.NoError(t, batch.Send()) {
					var result struct {
						Col1 uint64
						Col2 float64
						Col3 []interface{}
					}
					if err := conn.QueryRow(ctx, "SELECT (* except _tp_time) FROM test_simple_aggregate_function WHERE _tp_time > earliest_ts() LIMIT 1").ScanStruct(&result); assert.NoError(t, err) {
						assert.Equal(t, col1Data, result.Col1)
						assert.Equal(t, col2Data, result.Col2)
						assert.Equal(t, col3Data, result.Col3)
					}
				}
			}
		}
	}
}
