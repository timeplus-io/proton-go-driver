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
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/timeplus-io/proton-go-driver/v2"
)

func example() error {
	conn, err := sql.Open("proton", "proton://127.0.0.1:8463")
	if err != nil {
		return err
	}
	const ddl = `
	CREATE STREAM example (
		  Col1 uint8
		, Col2 string
		, Col3 DateTime
	) 
	`
	if _, err := conn.Exec(`DROP STREAM IF EXISTS example`); err != nil {
		return err
	}
	if _, err := conn.Exec(ddl); err != nil {
		return err
	}
	datetime := time.Now()
	{
		scope, err := conn.Begin()
		if err != nil {
			return err
		}
		batch, err := scope.Prepare("INSERT INTO example (* except _tp_time)")
		if err != nil {
			return err
		}
		for i := 0; i < 10; i++ {
			if _, err := batch.Exec(uint8(i), "ClickHouse Inc.", datetime); err != nil {
				return err
			}
		}
		if err := scope.Commit(); err != nil {
			return err
		}
	}

	var result struct {
		Col1 uint8
		Col2 string
		Col3 time.Time
	}
	{
		if err := conn.QueryRow(`SELECT * except _tp_time FROM example WHERE _tp_time > earliest_ts() AND Col1 = $1 AND Col3 = $2 LIMIT 1`, 2, datetime).Scan(
			&result.Col1,
			&result.Col2,
			&result.Col3,
		); err != nil {
			return err
		}
		fmt.Println(result)
	}
	{
		if err := conn.QueryRow(`SELECT * except _tp_time FROM example WHERE _tp_time > earliest_ts() AND Col1 = @Col1 AND Col3 = @Col2 LIMIT 1`,
			sql.Named("Col1", 4),
			sql.Named("Col2", datetime),
		).Scan(
			&result.Col1,
			&result.Col2,
			&result.Col3,
		); err != nil {
			return err
		}
		fmt.Println(result)
	}
	return nil
}
func main() {
	if err := example(); err != nil {
		log.Fatal(err)
	}
}
