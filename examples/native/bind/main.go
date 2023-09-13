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
	var (
		ctx       = context.Background()
		conn, err = proton.Open(&proton.Options{
			Addr: []string{"127.0.0.1:8463"},
			Auth: proton.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			},
		})
	)
	if err != nil {
		return err
	}
	const ddl = `
	CREATE TEMPORARY STREAM example (
		  Col1 uint8
		, Col2 string
		, Col3 DateTime
	)Engine = Memory
	`
	if err := conn.Exec(ctx, ddl); err != nil {
		return err
	}
	datetime := time.Now()
	{
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO example")
		if err != nil {
			return err
		}
		for i := 0; i < 10; i++ {
			if err := batch.Append(uint8(i), "ClickHouse Inc.", datetime); err != nil {
				return err
			}
		}
		if err := batch.Send(); err != nil {
			return err
		}
	}

	var result struct {
		Col1 uint8
		Col2 string
		Col3 time.Time
	}
	{
		if err := conn.QueryRow(ctx, `SELECT * FROM example WHERE Col1 = $1 AND Col3 = $2`, 2, datetime).ScanStruct(&result); err != nil {
			return err
		}
		fmt.Println(result)
	}
	{
		if err := conn.QueryRow(ctx, `SELECT * FROM example WHERE Col1 = @Col1 AND Col3 = @Col2`,
			proton.Named("Col1", 4),
			proton.Named("Col2", datetime),
		).ScanStruct(&result); err != nil {
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
