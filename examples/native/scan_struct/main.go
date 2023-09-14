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
	conn, err := proton.Open(&proton.Options{
		Addr: []string{"127.0.0.1:8463"},
		Auth: proton.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		//Debug:           true,
		DialTimeout:     time.Second,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
		Compression: &proton.Compression{
			Method: proton.CompressionLZ4,
		},
	})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(3))
	defer cancel()
	ctx = proton.Context(ctx, proton.WithSettings(proton.Settings{
		"max_block_size": 10,
	}), proton.WithProgress(func(p *proton.Progress) {
		fmt.Println("progress: ", p)
	}))
	if err := conn.Ping(ctx); err != nil {
		if exception, ok := err.(*proton.Exception); ok {
			fmt.Printf("Catch exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		return err
	}
	if err := conn.Exec(ctx, `DROP STREAM IF EXISTS example`); err != nil {
		return err
	}
	err = conn.Exec(ctx, `
		CREATE STREAM IF NOT EXISTS example (
			Col1 uint8,
			Col2 string,
			Col3 DateTime
		)
	`)
	if err != nil {
		return err
	}
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO example (Col1, Col2, Col3)")
	if err != nil {
		return err
	}
	for i := 0; i < 10; i++ {
		if err := batch.Append(uint8(i), fmt.Sprintf("value_%d", i), time.Now()); err != nil {
			return err
		}
	}
	if err := batch.Send(); err != nil {
		return err
	}

	var result []struct {
		Col1           uint8
		Col2           string
		ColumnWithName time.Time `ch:"Col3"`
	}

	if err = conn.Select(ctx, &result, "SELECT Col1, Col2, Col3 FROM example  WHERE _tp_time > earliest_ts() LIMIT 10"); err != nil {
		return err
	}

	for _, v := range result {
		fmt.Printf("row: col1=%d, col2=%s, col3=%s\n", v.Col1, v.Col2, v.ColumnWithName)
	}

	return nil
}

func main() {
	if err := example(); err != nil {
		log.Fatal(err)
	}
}
