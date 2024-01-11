package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/timeplus-io/proton-go-driver/v2"
	"log"
	"math/rand"
	"time"
)

func trainModel(conn *sql.DB, ctx context.Context) (err error) {
	_, err = conn.ExecContext(ctx, "DROP stream IF EXISTS train_data;")
	if err != nil {
		return
	}
	_, err = conn.ExecContext(ctx, "CREATE STREAM train_data(param1 int, param2 int, target int) engine=Memory;")
	if err != nil {
		return
	}
	scope, err := conn.Begin()
	if err != nil {
		return
	}
	batch, err := scope.PrepareContext(ctx, "INSERT INTO train_data(param1, param2, target) VALUES")
	if err != nil {
		return
	}
	for i := int32(0); i < 100000; i++ {
		x := rand.Int31n(10000)
		y := rand.Int31n(10000)
		z := x + y
		_, err = batch.ExecContext(ctx, x, y, z)
		if err != nil {
			return
		}
	}
	err = scope.Commit()
	if err != nil {
		return
	}
	_, err = conn.ExecContext(ctx, "DROP STREAM IF EXISTS your_model;")
	if err != nil {
		return
	}
	_, err = conn.ExecContext(ctx, "CREATE STREAM your_model ENGINE = Memory AS SELECT stochastic_linear_regression_state(0.01, 1.0, 10, 'Adam', target, param1, param2) AS state FROM train_data;")
	if err != nil {
		return
	}
	return nil
}

func insertTestData(conn *sql.DB, ctx context.Context) {
	time.Sleep(time.Second)
	scope, err := conn.Begin()
	if err != nil {
		log.Fatal(err)
	}
	batch, err := scope.PrepareContext(ctx, "INSERT INTO test_data(param1, param2) VALUES")
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		x := rand.Int31n(10000)
		y := rand.Int31n(10000)
		_, err = batch.Exec(x, y)
		if err != nil {
			log.Fatal(err)
		}
	}
	err = scope.Commit()
	if err != nil {
		log.Fatal(err)
	}
}

func testModel(conn *sql.DB, ctx context.Context) (err error) {
	_, err = conn.ExecContext(ctx, "DROP STREAM IF EXISTS test_data;")
	if err != nil {
		return
	}
	_, err = conn.ExecContext(ctx, "CREATE stream test_data(param1 int, param2 int);")
	if err != nil {
		return
	}
	go insertTestData(conn, ctx)
	ctxCancel, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	rows, err := conn.QueryContext(ctxCancel, "WITH (SELECT state FROM your_model) AS model SELECT param1, param2, eval_ml_method(model, param1, param2) FROM test_data;")
	for rows.Next() {
		var param1, param2 int32
		var target float64
		err = rows.Scan(&param1, &param2, &target)
		if err != nil {
			return
		}
		fmt.Printf("(%d, %d) -> %f\n", param1, param2, target)
	}
	rows.Close()
	return rows.Err()
}

func example() error {
	conn, err := sql.Open("proton", "proton://127.0.0.1:8463?dial_timeout=1s&compress=true")
	if err != nil {
		return err
	}
	conn.SetMaxIdleConns(5)
	conn.SetMaxOpenConns(10)
	conn.SetConnMaxLifetime(time.Hour)
	ctx := proton.Context(context.Background(), proton.WithSettings(proton.Settings{
		"max_block_size": 10,
	}))
	err = trainModel(conn, ctx)
	if err != nil {
		return err
	}
	err = testModel(conn, ctx)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return nil
}

func main() {
	err := example()
	if err != nil {
		log.Fatal(err)
	}
}
