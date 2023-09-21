package main

/**
This program analyzes the data measured by the car's speed sensor every 100 milliseconds (through
random number simulation), and will show when a car is speeding.

There are 7 goroutines writing data to the database. Each writes the speed of a car every 100 milliseconds.
Main thread reads the result from the database, and print to the screen.
*/
import (
	"context"
	"database/sql"
	"github.com/timeplus-io/proton-go-driver/v2"
	"log"
	"math/rand"
	"time"
)

type SpeedingCarRcd struct {
	Id    int64
	Speed float64
	Start time.Time
	End   time.Time
}

func getConnection(ctx context.Context) (*sql.DB, context.Context) {
	conn := proton.OpenDB(&proton.Options{
		Addr: []string{"127.0.0.1:8463"},
		Auth: proton.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		Settings: proton.Settings{
			"max_execution_time": 60 * 60,
		},
		DialTimeout: 5 * time.Second,
		Compression: &proton.Compression{
			proton.CompressionLZ4,
		},
	})
	conn.SetMaxIdleConns(5)
	conn.SetMaxOpenConns(10)
	conn.SetConnMaxLifetime(time.Hour)
	ctx = proton.Context(ctx, proton.WithSettings(proton.Settings{
		"max_block_size": 10,
	}), proton.WithProgress(func(p *proton.Progress) {
	}))
	return conn, ctx
}

func initStream() {
	conn, ctx := getConnection(context.Background())
	if _, err := conn.ExecContext(ctx, "DROP STREAM IF EXISTS car"); err != nil {
		log.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "CREATE STREAM IF NOT EXISTS car(id int64, speed float64)"); err != nil {
		log.Fatal(err)
	}
}

func writer(id int64) {
	conn, ctx := getConnection(context.Background())
	for {
		speed := rand.Float64()*20 + 50
		_, err := conn.ExecContext(ctx, "INSERT INTO car (id, speed) values ($1, $2)", id, speed)
		if err != nil {
			log.Fatal(err)
		}
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}

const QueryDDL = `SELECT id, avg(speed), window_start, window_end 
FROM session(car, 1h, [speed >= 60, speed < 60)) 
GROUP BY id, window_start, window_end`

func reader() {
	conn, ctx := getConnection(context.Background())
	rows, err := conn.QueryContext(ctx, QueryDDL)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var car SpeedingCarRcd
		if err := rows.Scan(&car.Id, &car.Speed, &car.Start, &car.End); err != nil {
			log.Fatal(err)
		}
		log.Printf("%+v", car)
	}
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	initStream()
	for id := int64(0); id < int64(7); id++ {
		go writer(id)
	}
	reader()
}
