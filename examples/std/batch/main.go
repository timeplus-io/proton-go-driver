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

	"github.com/google/uuid"
	_ "github.com/timeplus-io/proton-go-driver/v2"
)

func example() error {
	conn, err := sql.Open("proton", "proton://127.0.0.1:8463?dial_timeout=1s&compress=true")
	if err != nil {
		return err
	}
	conn.SetMaxIdleConns(5)
	if err != nil {
		return err
	}
	if _, err := conn.Exec(`DROP STREAM IF EXISTS example`); err != nil {
		return err
	}
	_, err = conn.Exec(`
		CREATE STREAM IF NOT EXISTS example (
			  Col1 uint8
			, Col2 string
			, Col3 fixed_string(3)
			, Col4 uuid
			, Col5 map(string, uint8)
			, Col6 array(string)
			, Col7 tuple(string, uint8, array(map(string, string)))
			, Col8 DateTime
		) 
	`)
	if err != nil {
		return err
	}
	scope, err := conn.Begin()
	if err != nil {
		return err
	}
	batch, err := scope.Prepare("INSERT INTO example (* except _tp_time)")
	if err != nil {
		return err
	}
	for i := 0; i < 100; i++ {
		_, err := batch.Exec(
			uint8(42),
			"ClickHouse", "Inc",
			uuid.New(),
			map[string]uint8{"key": 1},             // Map(String, UInt8)
			[]string{"Q", "W", "E", "R", "T", "Y"}, // Array(String)
			[]interface{}{ // Tuple(String, UInt8, Array(Map(String, String)))
				"String Value", uint8(5), []map[string]string{
					{"key": "value"},
					{"key": "value"},
					{"key": "value"},
				},
			},
			time.Now(),
		)
		if err != nil {
			return err
		}
	}
	return scope.Commit()
}

func main() {
	start := time.Now()
	if err := example(); err != nil {
		log.Fatal(err)
	}
	fmt.Println(time.Since(start))
}
