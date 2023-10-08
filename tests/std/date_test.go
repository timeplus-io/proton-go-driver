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

func TestStdDate(t *testing.T) {
	if conn, err := sql.Open("proton", "proton://127.0.0.1:8463"); assert.NoError(t, err) {
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
			conn.Exec("DROP STREAM test_date")
		}()
		type result struct {
			ColID uint8 `ch:"ID"`
			Col1  types.Date
			Col2  *types.Date
			Col3  []types.Date
			Col4  []*types.Date
		}
		if _, err := conn.Exec(ddl); assert.NoError(t, err) {
			scope, err := conn.Begin()
			if !assert.NoError(t, err) {
				return
			}
			if batch, err := scope.Prepare("INSERT INTO test_date (* except _tp_time)"); assert.NoError(t, err) {
				tim, err := time.Parse("2006-01-02 15:04:05", "2022-01-12 00:00:00")
				date := types.Date{tim}
				if !assert.NoError(t, err) {
					return
				}
				if _, err := batch.Exec(uint8(1), date, &date, []types.Date{date}, []*types.Date{&date, nil, &date}); !assert.NoError(t, err) {
					return
				}
				if _, err := batch.Exec(uint8(2), date, nil, []types.Date{date}, []*types.Date{nil, nil, &date}); !assert.NoError(t, err) {
					return
				}
				if err := scope.Commit(); assert.NoError(t, err) {
					var (
						result1 result
						result2 result
					)
					if err := conn.QueryRow("SELECT (* except _tp_time) FROM test_date WHERE _tp_time > earliest_ts() AND ID = $1 LIMIT 1", 1).Scan(
						&result1.ColID,
						&result1.Col1,
						&result1.Col2,
						&result1.Col3,
						&result1.Col4,
					); assert.NoError(t, err) {
						if assert.Equal(t, date, result1.Col1) {
							assert.Equal(t, "UTC", result1.Col1.Location().String())
							assert.Equal(t, date, *result1.Col2)
							assert.Equal(t, []types.Date{date}, result1.Col3)
							assert.Equal(t, []*types.Date{&date, nil, &date}, result1.Col4)
						}
					}
					if err := conn.QueryRow("SELECT (* except _tp_time) FROM test_date WHERE _tp_time > earliest_ts() AND ID = $1 LIMIT 1", 2).Scan(
						&result2.ColID,
						&result2.Col1,
						&result2.Col2,
						&result2.Col3,
						&result2.Col4,
					); assert.NoError(t, err) {
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
