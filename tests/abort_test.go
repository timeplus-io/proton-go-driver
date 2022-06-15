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

	"github.com/timeplus-io/proton-go-driver/v2/lib/compress"

	"github.com/stretchr/testify/assert"
	"github.com/timeplus-io/proton-go-driver/v2"
)

func TestAbort(t *testing.T) {
	var (
		ctx       = context.Background()
		conn, err = proton.Open(&proton.Options{
			Addr: []string{"127.0.0.1:7587"},
			Auth: proton.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			},
			Compression: &proton.Compression{
				Method: compress.NONE,
			},
			MaxOpenConns: 1,
		})
	)
	if assert.NoError(t, err) {
		const ddl = `
		CREATE STREAM test_abort (
			Col1 uint8
		) Engine Memory
		`
		defer func() {
			conn.Exec(ctx, "DROP STREAM test_abort")
		}()
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_abort"); assert.NoError(t, err) {
				if assert.NoError(t, batch.Abort()) {
					if err := batch.Abort(); assert.Error(t, err) {
						assert.Equal(t, proton.ErrBatchAlreadySent, err)
					}
				}
			}
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_abort"); assert.NoError(t, err) {
				if assert.NoError(t, batch.Append(uint8(1))) && assert.NoError(t, batch.Send()) {
					var col1 uint8
					if err := conn.QueryRow(ctx, "SELECT * FROM test_abort SETTINGS query_mode='table'").Scan(&col1); assert.NoError(t, err) {
						assert.Equal(t, uint8(1), col1)
					}
				}
			}
		}
	}
}
