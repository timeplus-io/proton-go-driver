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

	"github.com/stretchr/testify/assert"
)

func TestStdWithTotals(t *testing.T) {
	const query = `
	SELECT
		number AS n
		, count()
	FROM (
		SELECT number FROM system.numbers LIMIT 100
	) GROUP BY n WITH TOTALS
	`
	if conn, err := sql.Open("proton", "proton://127.0.0.1:8463"); assert.NoError(t, err) {
		if rows, err := conn.Query(query); assert.NoError(t, err) {
			var count int
			for rows.Next() {
				count++
				var (
					n uint64
					c uint64
				)
				if !assert.NoError(t, rows.Scan(&n, &c)) {
					return
				}
			}
			if assert.Equal(t, 100, count) {
				if assert.True(t, rows.NextResultSet()) {
					var count int
					for rows.Next() {
						count++
						var (
							n, totals uint64
						)
						if assert.NoError(t, rows.Scan(&n, &totals)) {
							assert.Equal(t, uint64(0), n)
							assert.Equal(t, uint64(100), totals)
						}
					}
					assert.Equal(t, 1, count)
				}
			}
		}
	}
}
