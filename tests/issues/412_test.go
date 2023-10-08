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

	"github.com/stretchr/testify/assert"
	"github.com/timeplus-io/proton-go-driver/v2"
)

func TestIssue412(t *testing.T) {
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
			CREATE STREAM issue_412 (
				Col1 simple_aggregate_function(max, datetime64(3, 'UTC'))
			)
		`
		defer func() {
			conn.Exec(ctx, "DROP STREAM issue_412")
		}()
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO issue_412 (* except _tp_time)"); assert.NoError(t, err) {
				datetime := time.Now().Truncate(time.Millisecond)
				if err := batch.Append(datetime); !assert.NoError(t, err) {
					return
				}
				if err := batch.Send(); assert.NoError(t, err) {
					var col1 time.Time
					if err := conn.QueryRow(ctx, "SELECT (* except _tp_time) FROM issue_412 WHERE _tp_time > earliest_ts() LIMIT 1").Scan(&col1); assert.NoError(t, err) {
						assert.Equal(t, datetime.UnixNano(), col1.UnixNano())
					}
				}
			}
		}
	}
}
