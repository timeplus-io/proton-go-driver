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
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/timeplus-io/proton-go-driver/v2"
	"github.com/timeplus-io/proton-go-driver/v2/lib/driver"
)

func setupJSONTest(t *testing.T) driver.Conn {
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
			"allow_experimental_dynamic_type": true,
			"allow_experimental_json_type":    true,
		}, Compression: &proton.Compression{
			Method: proton.CompressionLZ4,
		}})
	require.NoError(t, err)

	// Timeplus Enterprise 2.9
	if err := CheckMinServerVersion(conn, 2, 9); err != nil {
		t.Skip(fmt.Errorf("unsupported proton version for JSON type"))
		return nil
	}

	return conn
}

func TestJSONPaths(t *testing.T) {
	ctx := context.Background()
	conn := setupJSONTest(t)

	const ddl = `
			CREATE STREAM IF NOT EXISTS test_json (
				  c json(Name string, Age int64, KeysNumbers map(string, int64), SKIP fake.field)
			) Engine = MergeTree() ORDER BY tuple_cast()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	defer func() {
		require.NoError(t, conn.Exec(ctx, "DROP STREAM IF EXISTS test_json"))
	}()

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_json (c)")
	require.NoError(t, err)

	jsonRow := BuildTestJSONPaths()

	require.NoError(t, batch.Append(jsonRow))
	require.NoError(t, batch.Send())

	rows, err := conn.Query(ctx, "SELECT c FROM test_json")
	require.NoError(t, err)

	var row proton.JSON

	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)

	expectedValuesByPath := jsonRow.ValuesByPath()
	actualValuesByPath := row.ValuesByPath()
	for path, expectedValue := range expectedValuesByPath {
		actualValue, ok := actualValuesByPath[path]
		if !ok {
			t.Fatalf("result JSON is missing path: %s", path)
		}

		// Allow Equal func to compare values without Dynamic wrapper
		if v, ok := expectedValue.(proton.Dynamic); ok {
			expectedValue = v.Any()
		}

		if v, ok := actualValue.(proton.Dynamic); ok {
			actualValue = v.Any()
		}

		// TODO: datetime not equal due to timezone is different: Local vs UTC
		if path == "Timestamp" {
			continue
		}

		require.Equal(t, expectedValue, actualValue)
	}
}

func TestJSONArray(t *testing.T) {
	ctx := context.Background()
	conn := setupJSONTest(t)

	const ddl = `
			CREATE STREAM IF NOT EXISTS test_json (
				  c array(json)
			) Engine = MergeTree() ORDER BY tuple_cast()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	defer func() {
		require.NoError(t, conn.Exec(ctx, "DROP STREAM IF EXISTS test_json"))
	}()

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_json (c)")
	require.NoError(t, err)

	arrJsonRow := []*proton.JSON{proton.NewJSON(), BuildTestJSONPaths()}

	require.NoError(t, batch.Append(arrJsonRow))
	require.NoError(t, batch.Send())

	rows, err := conn.Query(ctx, "SELECT c FROM test_json")
	require.NoError(t, err)

	var arrRow []*proton.JSON

	require.True(t, rows.Next())
	err = rows.Scan(&arrRow)
	require.NoError(t, err)
	require.Len(t, arrRow, 2)

	actualValuesByPathEmpty := arrRow[0].ValuesByPath()
	for _, actualValue := range actualValuesByPathEmpty {
		// Allow Nil func to compare values without Dynamic wrapper
		if v, ok := actualValue.(proton.Dynamic); ok {
			actualValue = v.Any()
		}

		require.Nil(t, actualValue)
	}

	expectedValuesByPath := arrJsonRow[1].ValuesByPath()
	actualValuesByPath := arrRow[1].ValuesByPath()
	for path, expectedValue := range expectedValuesByPath {
		actualValue, ok := actualValuesByPath[path]
		if !ok {
			t.Fatalf("result JSON is missing path: %s", path)
		}

		// Allow Equal func to compare values without Dynamic wrapper
		if v, ok := expectedValue.(proton.Dynamic); ok {
			expectedValue = v.Any()
		}

		if v, ok := actualValue.(proton.Dynamic); ok {
			actualValue = v.Any()
		}

		// TODO: datetime not equal due to timezone is different: Local vs UTC
		if path == "Timestamp" {
			continue
		}

		require.Equal(t, expectedValue, actualValue)
	}
}

func TestJSONEmptyArray(t *testing.T) {
	ctx := context.Background()
	conn := setupJSONTest(t)

	const ddl = `
			CREATE STREAM IF NOT EXISTS test_json (
				  c array(json)
			) Engine = MergeTree() ORDER BY tuple_cast()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	defer func() {
		require.NoError(t, conn.Exec(ctx, "DROP STREAM IF EXISTS test_json"))
	}()

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_json (c)")
	require.NoError(t, err)

	var arrJsonRow []*proton.JSON
	require.NoError(t, batch.Append(arrJsonRow))
	require.NoError(t, batch.Send())

	rows, err := conn.Query(ctx, "SELECT c FROM test_json")
	require.NoError(t, err)

	var arrRow []*proton.JSON

	require.True(t, rows.Next())
	err = rows.Scan(&arrRow)
	require.NoError(t, err)
	require.Len(t, arrRow, 0)
}

func TestJSONStruct(t *testing.T) {
	ctx := context.Background()
	conn := setupJSONTest(t)

	const ddl = `
			CREATE STREAM IF NOT EXISTS test_json (
				  c json(Name string, Age int64, KeysNumbers map(string, int64), SKIP fake.field)
			) Engine = MergeTree() ORDER BY tuple_cast()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	defer func() {
		require.NoError(t, conn.Exec(ctx, "DROP STREAM IF EXISTS test_json"))
	}()

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_json (c)")
	require.NoError(t, err)

	inputRow := BuildTestJSONStruct()
	require.NoError(t, batch.Append(inputRow))

	inputRow2 := TestStruct{
		KeysNumbers: map[string]int64{},
		Timestamp:   JSONTestDate,
		Metadata: map[string]interface{}{
			"FieldA": "a",
			"FieldB": "b",
			"FieldC": map[string]interface{}{
				"FieldD": int64(5),
			},
			"FieldE": map[string]interface{}{
				"FieldF": "f",
			},
		},
	}
	require.NoError(t, batch.Append(inputRow2))

	require.NoError(t, batch.Send())

	rows, err := conn.Query(ctx, "SELECT c FROM test_json")
	require.NoError(t, err)

	var row TestStruct

	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)
	// The second row adds a nil value at this path. Update the inputRow for easier deep equal check
	inputRow.Metadata["FieldE"] = map[string]interface{}{
		"FieldF": nil,
	}

	// TODO: timezone is UTC vs Local
	row.Timestamp = inputRow.Timestamp

	require.Equal(t, inputRow, row)

	var row2 TestStruct

	require.True(t, rows.Next())
	err = rows.Scan(&row2)
	require.NoError(t, err)
	// Init slices for easier comparison
	inputRow2.Tags = make([]string, 0)
	inputRow2.Numbers = make([]int64, 0)

	// TODO: timezone is UTC vs Local
	row2.Timestamp = inputRow2.Timestamp

	require.Equal(t, inputRow2, row2)
}

func TestJSONFastStruct(t *testing.T) {
	ctx := context.Background()
	conn := setupJSONTest(t)

	const ddl = `
			CREATE STREAM IF NOT EXISTS test_json (
				  c json(Name string, Age int64, KeysNumbers map(string, int64), SKIP fake.field)
			) Engine = MergeTree() ORDER BY tuple_cast()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	defer func() {
		require.NoError(t, conn.Exec(ctx, "DROP STREAM IF EXISTS test_json"))
	}()

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_json (c)")
	require.NoError(t, err)

	inputRow := BuildFastTestJSONStruct()
	require.NoError(t, batch.Append(&inputRow))

	require.NoError(t, batch.Send())

	rows, err := conn.Query(ctx, "SELECT c FROM test_json")
	require.NoError(t, err)

	var row TestStruct

	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)

	// TODO: timezone is UTC vs Local
	row.Timestamp = inputRow.ts.Timestamp

	require.Equal(t, inputRow.ts, row)
}

func TestJSONString(t *testing.T) {
	t.Skip("client cannot receive JSON strings")

	ctx := context.Background()
	conn := setupJSONTest(t)

	require.NoError(t, conn.Exec(ctx, "SET output_format_native_write_json_as_string=1"))

	const ddl = `
			CREATE TABLE IF NOT EXISTS test_json (
				  c JSON(Name String, Age Int64, KeysNumbers Map(String, Int64), SKIP fake.field)
			) Engine = MergeTree() ORDER BY tuple()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	defer func() {
		require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_json"))
	}()

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_json (c)")
	require.NoError(t, err)

	inputRow := BuildTestJSONStruct()

	inputRowStr, err := json.Marshal(inputRow)
	require.NoError(t, err)
	require.NoError(t, batch.Append(inputRowStr))
	require.NoError(t, batch.Send())

	rows, err := conn.Query(ctx, "SELECT c FROM test_json")
	require.NoError(t, err)

	var row json.RawMessage

	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)

	require.Equal(t, string(inputRowStr), string(row))

	var rowStruct TestStruct
	err = json.Unmarshal(row, &rowStruct)
	require.NoError(t, err)
}

func TestJSON_BatchFlush(t *testing.T) {
	t.Skip(fmt.Errorf("server-side JSON bug"))

	ctx := context.Background()
	conn := setupJSONTest(t)

	const ddl = `
			CREATE TABLE IF NOT EXISTS test_json (
				  c JSON
			) Engine = MergeTree() ORDER BY tuple()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	defer func() {
		require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_json"))
	}()

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_json (c)")
	require.NoError(t, err)

	vals := make([]*proton.JSON, 0, 1000)
	for i := 0; i < 1000; i++ {
		row := proton.NewJSON()
		if i%2 == 0 {
			row.SetValueAtPath("a", int64(i))
			row.SetValueAtPath("b", i%5 == 0)
		} else {
			row.SetValueAtPath("c", int64(-i))
			row.SetValueAtPath("d", i%5 != 0)
		}

		vals = append(vals, row)
		require.NoError(t, batch.Append(vals[i]))
		// require.NoError(t, batch.Flush())
	}
	require.NoError(t, batch.Send())

	rows, err := conn.Query(ctx, "SELECT c FROM test_json")
	require.NoError(t, err)

	i := 0
	for rows.Next() {
		var row proton.JSON
		err = rows.Scan(&row)
		require.NoError(t, err)

		if i%2 == 0 {
			valA, ok := row.ValueAtPath("a")
			require.Equal(t, true, ok)
			_, ok = valA.(proton.Dynamic)
			require.Equal(t, true, ok)

			require.Equal(t, int64(i), valA.(proton.Dynamic).Any())
			require.Equal(t, "Int64", valA.(proton.Dynamic).Type())

			valB, ok := row.ValueAtPath("b")
			require.Equal(t, true, ok)
			_, ok = valB.(proton.Dynamic)
			require.Equal(t, true, ok)

			require.Equal(t, i%5 == 0, valB.(proton.Dynamic).Any())
			require.Equal(t, "Bool", valB.(proton.Dynamic).Type())
		} else {
			valC, ok := row.ValueAtPath("c")
			require.Equal(t, true, ok)
			_, ok = valC.(proton.Dynamic)
			require.Equal(t, true, ok)

			require.Equal(t, int64(-i), valC.(proton.Dynamic).Any())
			require.Equal(t, "Int64", valC.(proton.Dynamic).Type())

			valD, ok := row.ValueAtPath("d")
			require.Equal(t, true, ok)
			_, ok = valD.(proton.Dynamic)
			require.Equal(t, true, ok)

			require.Equal(t, i%5 != 0, valD.(proton.Dynamic).Any())
			require.Equal(t, "Bool", valD.(proton.Dynamic).Type())
		}

		i++
	}
}

func TestJSONArrayDynamic(t *testing.T) {
	ctx := context.Background()
	conn := setupJSONTest(t)

	rows, err := conn.Query(ctx, `SELECT ['{"x":5}','{"y":6}']::array(json)::dynamic AS c`)
	require.NoError(t, err)

	require.True(t, rows.Next())
}

func TestJSONArrayVariant(t *testing.T) {
	ctx := context.Background()
	conn := setupJSONTest(t)

	rows, err := conn.Query(ctx, `SELECT ['{"x":5}','{"y":6}']::array(json)::variant(array(json)) AS c`)
	require.NoError(t, err)

	require.True(t, rows.Next())
}
