package main

import (
	"context"
	"fmt"
	"github.com/timeplus-io/proton-go-driver/v2"
	"github.com/timeplus-io/proton-go-driver/v2/lib/column"
	"log"
	"time"
)

func BufferReplaceExample() {
	var (
		ctx       = context.Background()
		conn, err = proton.Open(&proton.Options{
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
		})
	)
	ctx = proton.Context(ctx, proton.WithProgress(func(p *proton.Progress) {
		fmt.Println("progress: ", p)
	}))
	if err != nil {
		log.Fatal(err)
	}
	if err := conn.Exec(ctx, `DROP STREAM IF EXISTS example`); err != nil {
		log.Fatal(err)
	}
	err = conn.Exec(ctx, `
		CREATE STREAM IF NOT EXISTS example (
			  Col1 uint64
			, Col2 string
		)
	`)
	if err != nil {
		log.Fatal(err)
	}
	const rows = 200_000
	var (
		col1 column.UInt64 = make([]uint64, rows)
		col2 column.String = make([]string, rows)
	)
	for i := 0; i < rows; i++ {
		col1[i] = uint64(i)
		col2[i] = fmt.Sprintf("num%03d", i)
	}
	buffer, err := conn.PrepareStreamingBuffer(ctx, "INSERT INTO example (* except _tp_time)")
	err = buffer.ReplaceBy(
		&col1,
		&col2,
	)
	if err != nil {
		return
	}
	err = buffer.Send()
	if err != nil {
		return
	}
	err = buffer.Close()
	if err != nil {
		return
	}
}

func main() {
	BufferReplaceExample()
}
