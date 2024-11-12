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

package std

import (
	"database/sql"
	"github.com/timeplus-io/proton-go-driver/v2/types"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStdArray(t *testing.T) {
	if conn, err := sql.Open("proton", "proton://127.0.0.1:8463"); assert.NoError(t, err) {
		const ddl = `
		CREATE STREAM test_array (
			  Col1 array(string)
			, Col2 array(array(uint32))
			, Col3 array(array(array(datetime)))
		) 
		`
		defer func() {
			conn.Exec("DROP STREAM test_array")
		}()
		if _, err := conn.Exec(ddl); assert.NoError(t, err) {
			scope, err := conn.Begin()
			if !assert.NoError(t, err) {
				return
			}
			if batch, err := scope.Prepare("INSERT INTO test_array (* except _tp_time)"); assert.NoError(t, err) {
				var (
					timestamp = types.Datetime{Time: time.Now().Truncate(time.Second)}
					col1Data  = []string{"A", "b", "c"}
					col2Data  = [][]uint32{
						{1, 2},
						{3, 87},
						{33, 3, 847},
					}
					col3Data = [][][]types.Datetime{
						{
							{
								timestamp,
								timestamp,
								timestamp,
								timestamp,
							},
						},
						{
							{
								timestamp,
								timestamp,
								timestamp,
							},
							{
								timestamp,
								timestamp,
							},
						},
					}
				)
				for i := 0; i < 10; i++ {
					if _, err := batch.Exec(col1Data, col2Data, col3Data); !assert.NoError(t, err) {
						return
					}
				}
				if assert.NoError(t, scope.Commit()) {
					if rows, err := conn.Query("SELECT (* except _tp_time) FROM test_array WHERE _tp_time > earliest_ts() LIMIT 1"); assert.NoError(t, err) {
						for rows.Next() {
							var (
								col1 []string
								col2 [][]uint32
								col3 [][][]types.Datetime
							)
							if err := rows.Scan(&col1, &col2, &col3); assert.NoError(t, err) {
								assert.Equal(t, col1Data, col1)
								assert.Equal(t, col2Data, col2)
								assert.Equal(t, col3Data, col3)
							}
						}
						if assert.NoError(t, rows.Close()) {
							assert.NoError(t, rows.Err())
						}
					}
				}
			}
		}
	}
}
