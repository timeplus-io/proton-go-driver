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

	"github.com/stretchr/testify/assert"
	"github.com/timeplus-io/proton-go-driver/v2"
)

func TestIssue502(t *testing.T) {
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
		CREATE STREAM issue_502
		(
			  Part uint8
			, Col1 uint8
			, Col2 uint8
		)
			ORDER BY Part
			PARTITION BY (Part)
		`
		defer func() {
			conn.Exec(ctx, "DROP STREAM issue_502")
		}()
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO issue_502 (* except _tp_time)"); assert.NoError(t, err) {
				for part := 0; part < 10; part++ {
					if err := batch.Append(uint8(part), uint8(part)+10, uint8(part)+20); !assert.NoError(t, err) {
						return
					}
				}
				if err := batch.Send(); assert.NoError(t, err) {
					var result []struct {
						Part uint8
						Col1 uint8
						Col2 uint8
					}
					if err := conn.Select(ctx, &result, `SELECT (* except _tp_time) FROM issue_502 WHERE _tp_time > earliest_ts() LIMIT 10`); assert.NoError(t, err) {
						if assert.Len(t, result, 10) {
							for _, v := range result {
								assert.Equal(t, uint8(v.Part)+10, v.Col1)
								assert.Equal(t, uint8(v.Part)+20, v.Col2)
							}
						}
					}
				}
			}
		}
	}
}
