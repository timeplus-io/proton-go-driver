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

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestStdUUID(t *testing.T) {
	if conn, err := sql.Open("clickhouse", "clickhouse://127.0.0.1:9000"); assert.NoError(t, err) {
		const ddl = `
			CREATE TEMPORARY STREAM test_uuid (
				  Col1 uuid
				, Col2 uuid
			) Engine Memory
		`
		defer func() {
			conn.Exec("DROP STREAM test_uuid")
		}()
		if _, err := conn.Exec(ddl); assert.NoError(t, err) {
			scope, err := conn.Begin()
			if !assert.NoError(t, err) {
				return
			}

			if batch, err := scope.Prepare("INSERT INTO test_uuid"); assert.NoError(t, err) {
				var (
					col1Data = uuid.New()
					col2Data = uuid.New()
				)
				if _, err := batch.Exec(col1Data, col2Data); assert.NoError(t, err) {
					if assert.NoError(t, scope.Commit()) {
						var (
							col1 uuid.UUID
							col2 uuid.UUID
						)
						if err := conn.QueryRow("SELECT * FROM test_uuid").Scan(&col1, &col2); assert.NoError(t, err) {
							assert.Equal(t, col1Data, col1)
							assert.Equal(t, col2Data, col2)
						}
					}
				}
			}
		}
	}
}

func TestStdNullableUUID(t *testing.T) {
	if conn, err := sql.Open("clickhouse", "clickhouse://127.0.0.1:9000"); assert.NoError(t, err) {
		const ddl = `
			CREATE TEMPORARY STREAM test_uuid (
				  Col1 nullable(uuid)
				, Col2 nullable(uuid)
			) ENGINE = Memory
		`
		if _, err := conn.Exec(ddl); assert.NoError(t, err) {
			scope, err := conn.Begin()
			if assert.NoError(t, err) {
				return
			}
			if batch, err := conn.Prepare("INSERT INTO test_uuid"); assert.NoError(t, err) {
				var (
					col1Data = uuid.New()
					col2Data = uuid.New()
				)
				if _, err := batch.Exec(col1Data, col2Data); assert.NoError(t, err) {
					if assert.NoError(t, scope.Commit()) {
						var (
							col1 *uuid.UUID
							col2 *uuid.UUID
						)
						if err := conn.QueryRow("SELECT * FROM test_uuid").Scan(&col1, &col2); assert.NoError(t, err) {
							assert.Equal(t, col1Data, *col1)
							assert.Equal(t, col2Data, *col2)
						}
					}
				}
			}
		}
		if _, err := conn.Exec("TRUNCATE STREAM test_uuid"); !assert.NoError(t, err) {
			return
		}
		scope, err := conn.Begin()
		if assert.NoError(t, err) {
			return
		}
		if batch, err := scope.Prepare("INSERT INTO test_uuid"); assert.NoError(t, err) {
			var col1Data = uuid.New()
			if _, err := batch.Exec(col1Data, nil); assert.NoError(t, err) {
				if assert.NoError(t, scope.Commit()) {
					var (
						col1 *uuid.UUID
						col2 *uuid.UUID
					)
					if err := conn.QueryRow("SELECT * FROM test_uuid").Scan(&col1, &col2); assert.NoError(t, err) {
						if assert.Nil(t, col2) {
							assert.Equal(t, col1Data, *col1)
						}
					}
				}
			}
		}
	}
}
