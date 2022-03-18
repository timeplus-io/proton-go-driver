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

package issues

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestIssue482(t *testing.T) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{"127.0.0.1:9000"},
			Auth: clickhouse.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			},
			Compression: &clickhouse.Compression{
				Method: clickhouse.CompressionLZ4,
			},
			//Debug: true,
		})
	)
	if assert.NoError(t, err) {
		const query = `
			SELECT
				to_datetime('2020-02-01 00:00:00'), -- Not issued date
				to_datetime('2061-02-01 00:00:00'), -- Issued date
				to_datetime64(to_unix_timestamp(to_datetime('2064-01-01 00:00:00')), 3), -- Depend code
				to_datetime(2147483647), -- Int 32 max value to timestamp
				to_datetime(2147483648) -- Test for range over int32
		`
		var (
			notIssueDate    time.Time
			myIssueDate     time.Time
			myIssueDateTo64 time.Time
			int32MaxDate    time.Time
			int32OverDate   time.Time
		)
		err := conn.QueryRow(ctx, query).Scan(
			&notIssueDate,
			&myIssueDate,
			&myIssueDateTo64,
			&int32MaxDate,
			&int32OverDate,
		)
		if assert.NoError(t, err) {
			assert.Equal(t, "2061-02-01 00:00:00", myIssueDate.Format("2006-01-02 15:04:05"))
			assert.Equal(t, "2064-01-01 00:00:00", myIssueDateTo64.Format("2006-01-02 15:04:05"))
			assert.Equal(t, "2038-01-19 05:14:07", int32MaxDate.Format("2006-01-02 15:04:05"))
			assert.Equal(t, "2038-01-19 05:14:08", int32OverDate.Format("2006-01-02 15:04:05"))
		}
	}
}
