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

	"github.com/paulmach/orb"
	"github.com/stretchr/testify/assert"
	"github.com/timeplus-io/proton-go-driver/v2"
)

func TestGeoRing(t *testing.T) {
	t.Skip("Geo haven't been implemented")
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
			Settings: proton.Settings{
				"allow_experimental_geo_types": 1,
			},
		})
	)
	if assert.NoError(t, err) {
		const ddl = `
		CREATE STREAM test_geo_ring (
			Col1 ring
			, Col2 array(ring)
		) 
		`
		defer func() {
			conn.Exec(ctx, "DROP STREAM test_geo_ring")
		}()
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_geo_ring (* except _tp_time)"); assert.NoError(t, err) {
				var (
					col1Data = orb.Ring{
						orb.Point{1, 2},
						orb.Point{1, 2},
					}
					col2Data = []orb.Ring{
						{
							orb.Point{1, 2},
							orb.Point{1, 2},
						},
						{
							orb.Point{1, 2},
							orb.Point{1, 2},
						},
					}
				)
				if err := batch.Append(col1Data, col2Data); assert.NoError(t, err) {
					if assert.NoError(t, batch.Send()) {
						var (
							col1 orb.Ring
							col2 []orb.Ring
						)
						if err := conn.QueryRow(ctx, "SELECT (* except _tp_time) FROM test_geo_ring WHERE _tp_time > earliest_ts() LIMIT 1").Scan(&col1, &col2); assert.NoError(t, err) {
							assert.Equal(t, col1Data, col1)
							assert.Equal(t, col2Data, col2)
						}
					}
				}
			}
		}
	}
}
