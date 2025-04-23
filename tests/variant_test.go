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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/timeplus-io/proton-go-driver/v2"
	"github.com/timeplus-io/proton-go-driver/v2/lib/driver"
)

var variantTestDate, _ = time.Parse(time.RFC3339, "2024-12-13T02:09:30.123Z")

func setupVariantTest(t *testing.T) driver.Conn {
	// SkipOnCloud(t, "cannot modify Variant settings on cloud")

	conn, err := proton.Open(&proton.Options{
		Addr: []string{"127.0.0.1:8463"},
		Auth: proton.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		Settings: proton.Settings{
			"max_execution_time":              60,
			"allow_experimental_variant_type": true,
			"allow_suspicious_variant_types":  true,
		}, Compression: &proton.Compression{
			Method: proton.CompressionLZ4,
		}})
	require.NoError(t, err)

	if err := CheckMinServerVersion(conn, 2, 9); err != nil {
		t.Skip(fmt.Errorf("unsupported timeplus version for Variant type"))
		return nil
	}

	return conn
}

func TestVariant(t *testing.T) {
	ctx := context.Background()
	conn := setupVariantTest(t)

	const ddl = `
			CREATE STREAM IF NOT EXISTS test_variant (
				  c variant(
			    	bool,
			    	int64,
			    	string,
			    	datetime64(3),
			    	array(string),
			    	array(uint8),
			    	array(map(string, string)),
			    	map(string, string),
			    	map(string, int64),
			    )                  
			) Engine = MergeTree() ORDER BY tuple_cast()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	defer func() {
		require.NoError(t, conn.Exec(ctx, "DROP STREAM IF EXISTS test_variant"))
	}()

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_variant (c)")
	require.NoError(t, err)

	require.NoError(t, batch.Append(true))
	colInt64 := int64(42)
	require.NoError(t, batch.Append(proton.NewVariantWithType(colInt64, "int64")))
	colString := "test"
	require.NoError(t, batch.Append(proton.NewVariantWithType(colString, "string")))
	require.NoError(t, batch.Append(proton.NewVariantWithType(variantTestDate, "datetime64(3)")))
	var colNil interface{} = nil
	require.NoError(t, batch.Append(colNil))
	colSliceString := []string{"a", "b"}
	require.NoError(t, batch.Append(proton.NewVariantWithType(colSliceString, "array(string)")))
	colSliceUInt8 := []uint8{0xA, 0xB, 0xC}
	require.NoError(t, batch.Append(proton.NewVariantWithType(colSliceUInt8, "array(uint8)")))
	colSliceMapStringString := []map[string]string{{"key1": "value1", "key2": "value2"}, {"key3": "value3"}}
	require.NoError(t, batch.Append(colSliceMapStringString))
	colMapStringString := map[string]string{"key1": "value1", "key2": "value2"}
	require.NoError(t, batch.Append(colMapStringString))
	colMapStringInt64 := map[string]int64{"key1": 42, "key2": 84}
	require.NoError(t, batch.Append(colMapStringInt64))
	require.NoError(t, batch.Send())

	rows, err := conn.Query(ctx, "SELECT c FROM test_variant")
	require.NoError(t, err)

	var row proton.Variant

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
	// require.Equal(t, variantTestDate, row.Any())

	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)
	require.Equal(t, colNil, row.Any())

	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)
	require.Equal(t, colSliceString, row.Any())

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

func TestVariantArray(t *testing.T) {
	ctx := context.Background()
	conn := setupVariantTest(t)

	const ddl = `
			CREATE STREAM IF NOT EXISTS test_variant (
				  c array(variant(int64))                  
			) Engine = MergeTree() ORDER BY tuple_cast()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	defer func() {
		require.NoError(t, conn.Exec(ctx, "DROP STREAM IF EXISTS test_variant"))
	}()

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_variant (c)")
	require.NoError(t, err)

	batch.Append([]proton.Variant{
		proton.NewVariantWithType(int64(42), "int64"),
		proton.NewVariantWithType(int64(84), "int64"),
	})
	require.NoError(t, batch.Send())

	rows, err := conn.Query(ctx, "SELECT c FROM test_variant")
	require.NoError(t, err)

	var arrRow []proton.Variant

	require.True(t, rows.Next())
	err = rows.Scan(&arrRow)
	require.NoError(t, err)
	require.Len(t, arrRow, 2)

	require.Equal(t, int64(42), arrRow[0].Any())
	require.Equal(t, int64(84), arrRow[1].Any())
}

func TestVariantEmptyArray(t *testing.T) {
	ctx := context.Background()
	conn := setupVariantTest(t)

	const ddl = `
			CREATE STREAM IF NOT EXISTS test_variant (
				  c array(variant(int64))                  
			) Engine = MergeTree() ORDER BY tuple_cast()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	defer func() {
		require.NoError(t, conn.Exec(ctx, "DROP STREAM IF EXISTS test_variant"))
	}()

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_variant (c)")
	require.NoError(t, err)

	batch.Append([]proton.Variant{})
	require.NoError(t, batch.Send())

	rows, err := conn.Query(ctx, "SELECT c FROM test_variant")
	require.NoError(t, err)

	var arrRow []proton.Variant

	require.True(t, rows.Next())
	err = rows.Scan(&arrRow)
	require.NoError(t, err)
	require.Len(t, arrRow, 0)
}

func TestVariant_ScanWithType(t *testing.T) {
	ctx := context.Background()
	conn := setupVariantTest(t)

	const ddl = `
			CREATE STREAM IF NOT EXISTS test_variant (
				  c variant(bool, int64)                  
			) Engine = MergeTree() ORDER BY tuple_cast()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	defer func() {
		require.NoError(t, conn.Exec(ctx, "DROP STREAM IF EXISTS test_variant"))
	}()

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_variant (c)")
	require.NoError(t, err)

	require.NoError(t, batch.Append(true))
	require.NoError(t, batch.Append(proton.NewVariantWithType(int64(42), "int64")))
	require.NoError(t, batch.Append(nil))
	require.NoError(t, batch.Send())

	rows, err := conn.Query(ctx, "SELECT c FROM test_variant")
	require.NoError(t, err)

	var row proton.Variant

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

func TestVariant_BatchFlush(t *testing.T) {
	ctx := context.Background()
	conn := setupVariantTest(t)

	const ddl = `
			CREATE STREAM IF NOT EXISTS test_variant (
				  c variant(bool, int64)                  
			) Engine = MergeTree() ORDER BY tuple_cast()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	defer func() {
		require.NoError(t, conn.Exec(ctx, "DROP STREAM IF EXISTS test_variant"))
	}()

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_variant (c)")
	require.NoError(t, err)

	vals := make([]proton.Variant, 0, 1000)
	for i := 0; i < 1000; i++ {
		if i%2 == 0 {
			vals = append(vals, proton.NewVariantWithType(int64(i), "int64"))
		} else {
			vals = append(vals, proton.NewVariantWithType(i%5 == 0, "bool"))
		}

		require.NoError(t, batch.Append(vals[i]))
		// require.NoError(t, batch.Flush())
	}
	require.NoError(t, batch.Send())

	rows, err := conn.Query(ctx, "SELECT c FROM test_variant")
	require.NoError(t, err)

	i := 0
	for rows.Next() {
		var row proton.Variant
		err = rows.Scan(&row)
		require.NoError(t, err)

		if i%2 == 0 {
			require.Equal(t, int64(i), row.Any())
			require.Equal(t, "int64", row.Type())
		} else {
			require.Equal(t, i%5 == 0, row.Any())
			require.Equal(t, "bool", row.Type())
		}

		i++
	}
}
