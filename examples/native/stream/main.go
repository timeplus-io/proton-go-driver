package main

/**
This program analyzes the data measured by the car's speed sensor every 100 milliseconds (through
random number simulation), and will show when a car is speeding.
There are 7 goroutines writing data to the proton. Each writes the speed of a car every 100 milliseconds.
Main thread reads the result from the proton, and print to the screen.
*/
import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/timeplus-io/proton-go-driver/v2"
)

type SpeedingCarRcd struct {
	Id    int64
	Speed float64
	Start time.Time
	End   time.Time
}

func getConnection() (proton.Conn, context.Context) {
	conn, err := proton.Open(&proton.Options{
		Addr: []string{"127.0.0.1:8463"},
		Auth: proton.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		DialTimeout:     5 * time.Second,
		MaxIdleConns:    5,
		MaxOpenConns:    10,
		ConnMaxLifetime: time.Hour,
		Compression: &proton.Compression{
			Method: proton.CompressionLZ4,
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	ctx := proton.Context(context.Background(), proton.WithProgress(func(p *proton.Progress) {
		if rand.Float32() < 0.3 {
			log.Println("progress:", p)
		}
	}))
	return conn, ctx
}

func initStream() error {
	conn, _ := getConnection()
	if err := conn.Exec(context.Background(), "DROP STREAM IF EXISTS car"); err != nil {
		return err
	}
	if err := conn.Exec(context.Background(), "CREATE STREAM IF NOT EXISTS car(id int64, speed float64)"); err != nil {
		return err
	}
	return nil
}

func writer(id int64) {
	conn, _ := getConnection()
	for {
		for i := 0; i < 20; i++ {
			speed := rand.Float64()*20 + 50
			if err := conn.Exec(context.Background(), "INSERT INTO car (id, speed) VALUES ({id:int64},{speed:float64})", proton.Named("id", strconv.FormatInt(id, 10)), proton.Named("speed", strconv.FormatFloat(speed, 'f', 2, 64))); err != nil {
				log.Fatal(err)
			}
			time.Sleep(time.Duration(100) * time.Millisecond)
		}
	}
}

func reader() {
	const Query = `
		SELECT 
			id,
			AVG(speed),
			window_start,
			window_end
		FROM 
			tumble(car, 1s) 
		GROUP BY 
			window_start, window_end, id;`
	conn, ctx := getConnection()
	ctx, cancel := context.WithCancel(ctx)
	rows, err := conn.Query(ctx, Query)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	go func() {
		time.Sleep(time.Duration(20) * time.Second)
		cancel()
	}()

	for rows.Next() {
		var car SpeedingCarRcd
		if err := rows.Scan(&car.Id, &car.Speed, &car.Start, &car.End); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("carID=%d Speed=%f Start=%s End=%s\n", car.Id, car.Speed, car.Start.Local().Format("2006-06-01-02-15:04:05"), car.End.Local().Format("2006-06-01-02-15:04:05"))
	}
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	err := initStream()
	if err != nil {
		log.Fatal(err)
	}
	for id := int64(0); id < int64(7); id++ {
		go writer(id)
	}
	reader()
}
