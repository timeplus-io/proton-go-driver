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

	"github.com/stretchr/testify/require"
	"github.com/timeplus-io/proton-go-driver/v2"
	"github.com/timeplus-io/proton-go-driver/v2/lib/chcol"
)

var dynamicTestDate, _ = time.Parse(time.RFC3339, "2024-12-13T02:09:30.123Z")

func setupDynamicTest(t *testing.T) *sql.DB {
	conn, err := sql.Open("proton", "proton://127.0.0.1:8463")
	require.NoError(t, err)

	// Timeplus Enterprise 2.9
	if !CheckMinServerVersion(conn, 2, 9) {
		t.Skip(fmt.Errorf("unsupported proton version for dynamic type"))
		return nil
	}

	_, err = conn.ExecContext(context.Background(), "SET allow_experimental_dynamic_type = 1")
	if err != nil {
		t.Fatal(err)
		return nil
	}

	return conn
}

func TestDynamic(t *testing.T) {
	ctx := context.Background()
	conn := setupDynamicTest(t)

	const ddl = `
			CREATE STREAM IF NOT EXISTS test_std_dynamic (
				  c dynamic                  
			) Engine = MergeTree() ORDER BY tuple_cast()
		`
	_, err := conn.ExecContext(ctx, ddl)
	require.NoError(t, err)
	defer func() {
		_, err := conn.ExecContext(ctx, "DROP STREAM IF EXISTS test_std_dynamic")
		require.NoError(t, err)
	}()

	tx, err := conn.BeginTx(ctx, nil)
	require.NoError(t, err)

	batch, err := tx.PrepareContext(ctx, "INSERT INTO test_std_dynamic (c)")
	require.NoError(t, err)

	_, err = batch.ExecContext(ctx, proton.NewDynamicWithType(true, "bool"))
	require.NoError(t, err)
	colInt64 := int64(42)
	_, err = batch.ExecContext(ctx, proton.NewDynamicWithType(colInt64, "int64"))
	require.NoError(t, err)
	colString := "test"
	_, err = batch.ExecContext(ctx, proton.NewDynamicWithType(colString, "string"))
	require.NoError(t, err)
	_, err = batch.ExecContext(ctx, proton.NewDynamicWithType(dynamicTestDate, "datetime64(3)"))
	require.NoError(t, err)
	var colNil any = nil
	_, err = batch.ExecContext(ctx, colNil)
	require.NoError(t, err)
	colSliceUInt8 := []uint8{0xA, 0xB, 0xC}
	_, err = batch.ExecContext(ctx, proton.NewDynamicWithType(colSliceUInt8, "array(uint8)"))
	require.NoError(t, err)
	colSliceMapStringString := []map[string]string{{"key1": "value1", "key2": "value2"}, {"key3": "value3"}}
	_, err = batch.ExecContext(ctx, proton.NewDynamicWithType(colSliceMapStringString, "array(map(string, string))"))
	require.NoError(t, err)
	colMapStringString := map[string]string{"key1": "value1", "key2": "value2"}
	_, err = batch.ExecContext(ctx, proton.NewDynamicWithType(colMapStringString, "map(string, string)"))
	require.NoError(t, err)
	colMapStringInt64 := map[string]int64{"key1": 42, "key2": 84}
	_, err = batch.ExecContext(ctx, proton.NewDynamicWithType(colMapStringInt64, "map(string, int64)"))
	require.NoError(t, err)

	require.NoError(t, tx.Commit())

	rows, err := conn.QueryContext(ctx, "SELECT c FROM test_std_dynamic")
	require.NoError(t, err)

	var row chcol.Dynamic

	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)
	require.Equal(t, true, row.Any())

	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)
	require.Equal(t, colInt64, row.Any())

	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)
	require.Equal(t, colString, row.Any())

	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)
	// TODO: returned timezone is Local instead of UTC
	// require.Equal(t, dynamicTestDate, row.Any())

	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)
	require.Equal(t, colNil, row.Any())

	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)
	require.Equal(t, colSliceUInt8, row.Any())

	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)
	require.Equal(t, colSliceMapStringString, row.Any())

	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)
	require.Equal(t, colMapStringString, row.Any())

	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)
	require.Equal(t, colMapStringInt64, row.Any())
}

func TestDynamic_ScanWithType(t *testing.T) {
	ctx := context.Background()
	conn := setupDynamicTest(t)

	const ddl = `
			CREATE STREAM IF NOT EXISTS test_std_dynamic (
				  c dynamic                 
			) Engine = MergeTree() ORDER BY tuple_cast()
		`
	_, err := conn.ExecContext(ctx, ddl)
	require.NoError(t, err)

	defer func() {
		_, err := conn.ExecContext(ctx, "DROP STREAM IF EXISTS test_std_dynamic")
		require.NoError(t, err)
	}()

	tx, err := conn.BeginTx(ctx, nil)
	require.NoError(t, err)

	batch, err := tx.PrepareContext(ctx, "INSERT INTO test_std_dynamic (c)")
	require.NoError(t, err)

	_, err = batch.ExecContext(ctx, proton.NewDynamicWithType(true, "bool"))
	require.NoError(t, err)
	_, err = batch.ExecContext(ctx, proton.NewDynamicWithType(int64(42), "int64"))
	require.NoError(t, err)
	_, err = batch.ExecContext(ctx, nil)
	require.NoError(t, err)

	require.NoError(t, tx.Commit())

	rows, err := conn.QueryContext(ctx, "SELECT c FROM test_std_dynamic")
	require.NoError(t, err)

	var row chcol.Dynamic

	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)
	require.Equal(t, true, row.Any())
	require.Equal(t, "bool", row.Type())

	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)
	require.Equal(t, int64(42), row.Any())
	require.Equal(t, "int64", row.Type())

	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)
	require.Equal(t, nil, row.Any())
	require.Equal(t, "", row.Type())
}
