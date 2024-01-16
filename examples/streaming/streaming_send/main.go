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
			// Debug:           true,
			DialTimeout:     time.Second,
			MaxOpenConns:    10,
			MaxIdleConns:    5,
			ConnMaxLifetime: time.Hour,
		})
	)
	ctx = proton.Context(ctx, proton.WithProgress(func(p *proton.Progress) {
		fmt.Println("progress: ", p)
	}))
	if err != nil {
		return err
	}
	if err := conn.Exec(ctx, `DROP STREAM IF EXISTS example`); err != nil {
		return err
	}
	err = conn.Exec(ctx, `
		CREATE STREAM IF NOT EXISTS example (
			  Col1 uint64
			, Col2 string
		)
	`)
	if err != nil {
		return err
	}

	buffer, err := conn.PrepareStreamingBuffer(ctx, "INSERT INTO example (* except _tp_time)")
	if err != nil {
		return err
	}
	for i := 0; i < 100; i++ {
		for j := 0; j < 100000; j++ {
			err := buffer.Append(
				uint64(i*100000+j),
				fmt.Sprintf("num_%d_%d", j, i),
			)
			if err != nil {
				return err
			}
		}
		if err := buffer.Send(); err != nil {
			return err
		}
	}
	return buffer.Close()
}

func main() {
	start := time.Now()
	if err := example(); err != nil {
		log.Fatal(err)
	}
	fmt.Println(time.Since(start))
}
