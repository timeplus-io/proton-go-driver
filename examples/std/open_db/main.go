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

func example() error {
	conn := proton.OpenDB(&proton.Options{
		Addr: []string{"127.0.0.1:8463"},
		Auth: proton.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		Settings: proton.Settings{
			"max_execution_time": 60,
		},
		DialTimeout: 5 * time.Second,
		Compression: &proton.Compression{
			proton.CompressionLZ4,
		},
		//Debug: true,
	})
	conn.SetMaxIdleConns(5)
	conn.SetMaxOpenConns(10)
	conn.SetConnMaxLifetime(time.Hour)
	ctx := proton.Context(context.Background(), proton.WithSettings(proton.Settings{
		"max_block_size": 10,
	}), proton.WithProgress(func(p *proton.Progress) {
		fmt.Println("progress: ", p)
	}))
	ctx, cancel := context.WithTimeout(ctx, time.Second*time.Duration(3))
	defer cancel()
	if err := conn.PingContext(ctx); err != nil {
		if exception, ok := err.(*proton.Exception); ok {
			fmt.Printf("Catch exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		return err
	}
	if _, err := conn.ExecContext(ctx, `DROP STREAM IF EXISTS example`); err != nil {
		return err
	}
	_, err := conn.ExecContext(ctx, `
		CREATE STREAM IF NOT EXISTS example (
			Col1 uint8,
			Col2 string,
			Col3 DateTime
		)
	`)
	if err != nil {
		return err
	}
	scope, err := conn.Begin()
	if err != nil {
		return err
	}
	{
		batch, err := scope.PrepareContext(ctx, "INSERT INTO example (Col1, Col2, Col3)")
		if err != nil {
			return err
		}
		for i := 0; i < 10; i++ {
			if _, err := batch.Exec(uint8(i), fmt.Sprintf("value_%d", i), time.Now()); err != nil {
				return err
			}
		}
	}
	if err := scope.Commit(); err != nil {
		return err
	}
	rows, err := conn.QueryContext(ctx, "SELECT Col1, Col2, Col3 FROM example  WHERE _tp_time > earliest_ts() AND Col1 >= $1 AND Col2 <> $2 AND Col3 <= $3 LIMIT 10", 0, "xxx", time.Now())
	if err != nil {
		return err
	}
	for rows.Next() {
		var (
			col1 uint8
			col2 string
			col3 time.Time
		)
		if err := rows.Scan(&col1, &col2, &col3); err != nil {
			return err
		}
		fmt.Printf("row: col1=%d, col2=%s, col3=%s\n", col1, col2, col3)
	}
	rows.Close()
	return rows.Err()
}

func main() {
	if err := example(); err != nil {
		log.Fatal(err)
	}
}
