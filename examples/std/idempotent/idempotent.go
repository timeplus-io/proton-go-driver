package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/timeplus-io/proton-go-driver/v2"
)

func example() error {
	db, err := sql.Open(`proton`, `proton://127.0.0.1:8463`)
	if err != nil {
		return err
	}

	if _, err = db.Exec(`DROP STREAM IF EXISTS example`); err != nil {
		return err
	}
	if _, err = db.Exec(`
		CREATE STREAM example(
			a int,
			b string,
			c date
		)
	`); err != nil {
		return err
	}

	// Set a idempotent id.
	ctx := proton.Context(context.Background(), proton.WithSettings(proton.Settings{
		`idempotent_id`: `bacth1`,
	}))
	// Execute insert operation multiple times.
	for i := 0; i < 10; i++ {
		tx, err := db.Begin()
		if err != nil {
			return err
		}
		// Every time, insert 10 rows data with the same idempotent id
		st, err := tx.PrepareContext(ctx, `INSERT INTO example (a,b,c) VALUES`)
		if err != nil {
			return nil
		}
		for j := 0; j < 10; j++ {
			_, err = st.Exec(int32(j), fmt.Sprintf("%d", j), time.Now())
			if err != nil {
				return err
			}
		}
		st.Close()
		tx.Commit()
	}
	// Make sure data can be accessed in historical stroage.
	time.Sleep(3 * time.Second)

	rows, err := db.QueryContext(ctx, `SELECT COUNT(*) FROM table(example)`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var cnt int
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
