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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStdDateTime64(t *testing.T) {
	if conn, err := sql.Open("proton", "proton://127.0.0.1:8463"); assert.NoError(t, err) {
		const ddl = `
			CREATE STREAM test_datetime64 (
				  Col1 datetime64(3)
				, Col2 datetime64(9, 'Europe/Moscow')
				, Col3 datetime64(0, 'Europe/London')
				, Col4 nullable(datetime64(3, 'Europe/Moscow'))
				, Col5 array(datetime64(3, 'Europe/Moscow'))
				, Col6 array(nullable(datetime64(3, 'Europe/Moscow')))
			) 
		`
		defer func() {
			conn.Exec("DROP STREAM test_datetime64")
		}()
		if _, err := conn.Exec(ddl); assert.NoError(t, err) {
			scope, err := conn.Begin()
			if !assert.NoError(t, err) {
				return
			}
			if batch, err := scope.Prepare("INSERT INTO test_datetime64 (* except _tp_time)"); assert.NoError(t, err) {
				var (
					datetime1 = time.Now().Truncate(time.Millisecond)
					datetime2 = time.Now().Truncate(time.Nanosecond)
					datetime3 = time.Now().Truncate(time.Second)
				)
				if _, err := batch.Exec(
					datetime1,
					datetime2,
					datetime3,
					&datetime1,
					[]time.Time{datetime1, datetime1},
					[]*time.Time{&datetime3, nil, &datetime3},
				); assert.NoError(t, err) {
					if err := scope.Commit(); assert.NoError(t, err) {
						var (
							col1 time.Time
							col2 time.Time
							col3 time.Time
							col4 *time.Time
							col5 []time.Time
							col6 []*time.Time
						)
						if err := conn.QueryRow("SELECT (* except _tp_time) FROM test_datetime64 WHERE _tp_time > earliest_ts() LIMIT 1").Scan(&col1, &col2, &col3, &col4, &col5, &col6); assert.NoError(t, err) {
							assert.Equal(t, datetime1, col1)
							assert.Equal(t, datetime2.UnixNano(), col2.UnixNano())
							assert.Equal(t, datetime3.UnixNano(), col3.UnixNano())
							if assert.Equal(t, "Europe/Moscow", col2.Location().String()) {
								assert.Equal(t, "Europe/London", col3.Location().String())
							}
							assert.Equal(t, datetime1.UnixNano(), col4.UnixNano())
							if assert.Len(t, col5, 2) {
								assert.Equal(t, "Europe/Moscow", col5[0].Location().String())
								assert.Equal(t, "Europe/Moscow", col5[1].Location().String())
							}
							if assert.Len(t, col6, 3) {
								assert.Nil(t, col6[1])
								assert.NotNil(t, col6[0])
								assert.NotNil(t, col6[2])
							}
						}
					}
				}
			}
		}
	}
}
