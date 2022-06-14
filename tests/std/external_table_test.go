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
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/timeplus-io/proton-go-driver/v2"
	"github.com/timeplus-io/proton-go-driver/v2/external"
)

func TestStdExternalTable(t *testing.T) {
	table1, err := external.NewTable("external_table_1",
		external.Column("col1", "uint8"),
		external.Column("col2", "string"),
		external.Column("col3", "datetime"),
	)
	if assert.NoError(t, err) {
		for i := 0; i < 10; i++ {
			assert.NoError(t, table1.Append(uint8(i), fmt.Sprintf("value_%d", i), time.Now()))
		}
	}
	table2, err := external.NewTable("external_table_2",
		external.Column("col1", "uint8"),
		external.Column("col2", "string"),
		external.Column("col3", "datetime"),
	)
	if assert.NoError(t, err) {
		for i := 0; i < 10; i++ {
			assert.NoError(t, table2.Append(uint8(i), fmt.Sprintf("value_%d", i), time.Now()))
		}
	}
	if conn, err := sql.Open("proton", "proton://127.0.0.1:9000"); assert.NoError(t, err) {
		ctx := proton.Context(context.Background(),
			proton.WithExternalTable(table1, table2),
		)
		if rows, err := conn.QueryContext(ctx, "SELECT * FROM external_table_1"); assert.NoError(t, err) {
			for rows.Next() {
				var (
					col1 uint8
					col2 string
					col3 time.Time
				)
				if err := rows.Scan(&col1, &col2, &col3); assert.NoError(t, err) {
					t.Logf("row: col1=%d, col2=%s, col3=%s\n", col1, col2, col3)
				}
			}
			rows.Close()
		}
		var count uint64
		if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM external_table_1").Scan(&count); assert.NoError(t, err) {
			assert.Equal(t, uint64(10), count)
		}
		if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM external_table_2").Scan(&count); assert.NoError(t, err) {
			assert.Equal(t, uint64(10), count)
		}
		if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM (SELECT * FROM external_table_1 UNION ALL SELECT * FROM external_table_2)").Scan(&count); assert.NoError(t, err) {
			assert.Equal(t, uint64(20), count)
		}
	}
}
