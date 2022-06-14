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
			Addr: []string{"127.0.0.1:9000"},
			Auth: proton.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			},
			//Debug:           true,
			DialTimeout:     time.Second,
			ConnMaxLifetime: time.Hour,
		})
	)
	if err != nil {
		return err
	}
	if err := conn.Exec(ctx, `DROP STREAM IF EXISTS example`); err != nil {
		return err
	}
	err = conn.Exec(ctx, `
 		CREATE STREAM IF NOT EXISTS example (
                           timestamp datetime64(9) CODEC(Delta, ZSTD(1)),
                           traceID string CODEC(ZSTD(1)),
                           spanID string CODEC(ZSTD(1)),
                           parentSpanID string CODEC(ZSTD(1)),
                           serviceName low_cardinality(string) CODEC(ZSTD(1)),
                           name low_cardinality(string) CODEC(ZSTD(1)),
                           kind int32 CODEC(ZSTD(1)),
                           durationNano uint64 CODEC(ZSTD(1)),
                           tags array(string) CODEC(ZSTD(1)),
                           tagsKeys array(string) CODEC(ZSTD(1)),
                           tagsValues array(string) CODEC(ZSTD(1)),
                           statusCode int64 CODEC(ZSTD(1)),
                           references string CODEC(ZSTD(1)),
                           externalHttpMethod nullable(string) CODEC(ZSTD(1)),
                           externalHttpUrl nullable(string) CODEC(ZSTD(1)),
                           component nullable(string) CODEC(ZSTD(1)),
                           dbSystem nullable(string) CODEC(ZSTD(1)),
                           dbName nullable(string) CODEC(ZSTD(1)),
                           dbOperation nullable(string) CODEC(ZSTD(1)),
                           peerService nullable(string) CODEC(ZSTD(1)),
                           INDEX idx_traceID traceID TYPE bloom_filter GRANULARITY 4,
                           INDEX idx_service serviceName TYPE bloom_filter GRANULARITY 4,
                           INDEX idx_name name TYPE bloom_filter GRANULARITY 4,
                           INDEX idx_kind kind TYPE minmax GRANULARITY 4,
                           INDEX idx_tagsKeys tagsKeys TYPE bloom_filter(0.01) GRANULARITY 64,
                           INDEX idx_tagsValues tagsValues TYPE bloom_filter(0.01) GRANULARITY 64,
                           INDEX idx_duration durationNano TYPE minmax GRANULARITY 1
                         ) ENGINE MergeTree()
                         PARTITION BY to_date(timestamp)
                         ORDER BY (serviceName, -to_unix_timestamp(timestamp))
 	`)
	if err != nil {
		return err
	}
	for i := 0; i < 10; i++ {
		if err := examplePrep(ctx, conn); err != nil {
			log.Fatal(err)
		}
	}
	return err
}

func examplePrep(ctx context.Context, conn proton.Conn) error {
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO example")
	if err != nil {
		return err
	}
	return batch.Send()
}
func main() {
	start := time.Now()
	if err := example(); err != nil {
		log.Fatal(err)
	}
	fmt.Println(time.Since(start))
}
