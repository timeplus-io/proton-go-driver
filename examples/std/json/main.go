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

package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/timeplus-io/proton-go-driver/v2"
	proton_tests_std "github.com/timeplus-io/proton-go-driver/v2/tests/std"
)

func JSONPathsExample() error {
	ctx := context.Background()

	conn, err := sql.Open("proton", "proton://127.0.0.1:8463")
	if err != nil {
		return err
	}

	if !proton_tests_std.CheckMinServerVersion(conn, 2, 9) {
		fmt.Print("unsupported proton version for json type")
		return nil
	}

	_, err = conn.ExecContext(ctx, "SET allow_experimental_json_type = 1")
	if err != nil {
		return err
	}

	defer func() {
		conn.Exec("DROP STREAM go_json_example")
	}()

	_, err = conn.ExecContext(ctx, "DROP STREAM IF EXISTS go_json_example")
	if err != nil {
		return err
	}

	_, err = conn.ExecContext(ctx, `
		CREATE STREAM go_json_example (
		    product json
		) ENGINE = Memory
	`)
	if err != nil {
		return err
	}

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	batch, err := tx.PrepareContext(ctx, "INSERT INTO go_json_example (product)")
	if err != nil {
		return err
	}

	insertProduct := proton.NewJSON()
	insertProduct.SetValueAtPath("id", proton.NewDynamicWithType(uint64(1234), "uint64"))
	insertProduct.SetValueAtPath("name", "Book")
	insertProduct.SetValueAtPath("tags", []string{"library", "fiction"})
	insertProduct.SetValueAtPath("pricing.price", int64(750))
	insertProduct.SetValueAtPath("pricing.currency", "usd")
	insertProduct.SetValueAtPath("metadata.region", "us")
	insertProduct.SetValueAtPath("metadata.page_count", int64(852))
	insertProduct.SetValueAtPath("created_at", proton.NewDynamicWithType(time.Now().UTC().Truncate(time.Millisecond), "datetime64(3)"))

	if _, err = batch.ExecContext(ctx, insertProduct); err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return err
	}

	var selectedProduct proton.JSON

	if err = conn.QueryRowContext(ctx, "SELECT product FROM go_json_example").Scan(&selectedProduct); err != nil {
		return err
	}

	fmt.Printf("inserted product: %+v\n", insertProduct)
	fmt.Printf("selected product: %+v\n", selectedProduct)
	return nil
}

func JSONStringExample() error {
	ctx := context.Background()

	conn, err := sql.Open("proton", "proton://127.0.0.1:8463")
	if err != nil {
		return err
	}

	if !proton_tests_std.CheckMinServerVersion(conn, 2, 9) {
		fmt.Print("unsupported proton version for json type")
		return nil
	}

	_, err = conn.ExecContext(ctx, "SET allow_experimental_json_type = 1")
	if err != nil {
		return err
	}

	_, err = conn.ExecContext(ctx, "SET output_format_native_write_json_as_string = 1")
	if err != nil {
		return err
	}

	defer func() {
		conn.Exec("DROP STREAM go_json_example")
	}()

	_, err = conn.ExecContext(ctx, "DROP STREAM IF EXISTS go_json_example")
	if err != nil {
		return err
	}

	_, err = conn.ExecContext(ctx, `
		CREATE STREAM go_json_example (
		    product json
		) ENGINE = Memory
	`)
	if err != nil {
		return err
	}

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	batch, err := tx.PrepareContext(ctx, "INSERT INTO go_json_example (product)")
	if err != nil {
		return err
	}

	insertProductString := "{\"id\":1234,\"name\":\"Book\",\"tags\":[\"library\",\"fiction\"]," +
		"\"pricing\":{\"price\":750,\"currency\":\"usd\"},\"metadata\":{\"page_count\":852,\"region\":\"us\"}," +
		"\"created_at\":\"2024-12-19T11:20:04.146Z\"}"

	if _, err = batch.ExecContext(ctx, insertProductString); err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return err
	}

	var selectedProductString string

	if err = conn.QueryRowContext(ctx, "SELECT product FROM go_json_example").Scan(&selectedProductString); err != nil {
		return err
	}

	fmt.Printf("inserted product string: %s\n", insertProductString)
	fmt.Printf("selected product string: %s\n", selectedProductString)
	fmt.Printf("inserted product string matches selected product string: %t\n", insertProductString == selectedProductString)
	return nil
}

func main() {
	fmt.Println("JSONPathsExample")
	if err := JSONPathsExample(); err != nil {
		log.Fatal(err)
	}

	fmt.Println()
	fmt.Println("JSONStringExample")
	if err := JSONStringExample(); err != nil {
		log.Fatal(err)
	}
}
