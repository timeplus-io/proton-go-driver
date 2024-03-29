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
	"fmt"
	"log"
	"time"

	"github.com/timeplus-io/proton-go-driver/v2"
)

func main() {
	conn, err := proton.Open(&proton.Options{
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
			"max_execution_time": 60,
		},
		//Debug: true,
	})
	if err := conn.Ping(context.Background()); err != nil {
		log.Fatal(err)
	}
	var settings []struct {
		Name        string  `ch:"name"`
		Value       string  `ch:"value"`
		Changed     uint8   `ch:"changed"`
		Description string  `ch:"description"`
		Min         *string `ch:"min"`
		Max         *string `ch:"max"`
		Readonly    uint8   `ch:"readonly"`
		Type        string  `ch:"type"`
	}
	if err = conn.Select(context.Background(), &settings, "SELECT * FROM system.settings WHERE name LIKE $1 ORDER BY length(name) LIMIT 5", "%max%"); err != nil {
		log.Fatal(err)
	}
	for _, s := range settings {
		fmt.Printf("name: %s, value: %s, type=%s\n", s.Name, s.Value, s.Type)
	}

	if err = conn.Exec(context.Background(), "TRUNCATE STREAM X"); err == nil {
		panic("unexpected")
	}
	if exception, ok := err.(*proton.Exception); ok {
		fmt.Printf("Catch exception [%d]\n", exception.Code)
	}
	const ddl = `
	CREATE STREAM example (
		  Col1 uint64
		, Col2 fixed_string(2)
		, Col3 map(string, string)
		, Col4 array(string)
		, Col5 DateTime64(3)
	)
	`
	if err := conn.Exec(context.Background(), "DROP STREAM IF EXISTS example"); err != nil {
		log.Fatal(err)
	}
	if err := conn.Exec(context.Background(), ddl); err != nil {
		log.Fatal(err)
	}
	batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO example (* except _tp_time)")
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 10_000; i++ {
		err := batch.Append(
			uint64(i),
			"CH",
			map[string]string{
				"key": "value",
			},
			[]string{"A", "B", "C"},
			time.Now(),
		)
		if err != nil {
			log.Fatal(err)
		}
	}
	if err := batch.Send(); err != nil {
		log.Fatal(err)
	}
	ctx := proton.Context(context.Background(), proton.WithProgress(func(p *proton.Progress) {
		fmt.Println("progress: ", p)
	}))
	ctx, cancel := context.WithTimeout(ctx, time.Duration(10)*time.Second)
	defer cancel()
	var count uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM example WHERE _tp_time > earliest_ts() LIMIT 1").Scan(&count); err != nil {
		log.Fatal(err)
	}
	fmt.Println("count", count)
	var result struct {
		Col1  uint64
		Count uint64 `ch:"count"`
	}
	if err := conn.QueryRow(ctx, "SELECT Col1, count() AS count FROM example WHERE _tp_time > earliest_ts() AND Col1 = $1 GROUP BY Col1 LIMIT 1", 42).ScanStruct(&result); err != nil {
		log.Fatal(err)
	}
	fmt.Println("result", result)
}
