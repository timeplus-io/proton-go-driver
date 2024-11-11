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
		Addr: []string{`127.0.0.1:8463`},
		Auth: proton.Auth{
			Database: `default`,
			Username: `default`,
			Password: ``,
		},
	})
	if err != nil {
		return err
	}
	if err = conn.Exec(context.Background(), `DROP STREAM IF EXISTS example`); err != nil {
		return err
	}
	if err = conn.Exec(context.Background(),
		`CREATE STREAM example(
			a int,
			b string,
			c date
		)`,
	); err != nil {
		return err
	}

	// Set a idempotent id.
	ctx := proton.Context(context.Background(), proton.WithSettings(proton.Settings{
		`idempotent_id`: `bacth1`,
	}))

	// Execute insert operation multiple times.
	for i := 0; i < 10; i++ {
		batch, err := conn.PrepareBatch(ctx, `INSERT INTO example (a,b,c) VALUES`)
		if err != nil {
			return err
		}
		for j := 0; j < 10; j++ {
			if err = batch.Append(int32(j), fmt.Sprintf("%d", j), time.Now()); err != nil {
				return err
			}
		}
		if err = batch.Send(); err != nil {
			return err
		}
	}

	// Make sure data can be accessed in historical stroage.
	time.Sleep(3 * time.Second)

	rows, err := conn.Query(context.TODO(), `SELECT COUNT(*) FROM table(example)`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var cnt uint64
		if err = rows.Scan(&cnt); err != nil {
			return err
		}
		// Only the first insertion is stored in the historical stroage.
		fmt.Println(cnt) // 10
	}
	rows.Close()
	return rows.Err()
}

func main() {
	if err := example(); err != nil {
		log.Fatal(err)
	}
}
