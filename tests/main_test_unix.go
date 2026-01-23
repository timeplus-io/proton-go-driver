//go:build !windows

package tests

import (
	"context"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	proton "github.com/timeplus-io/proton-go-driver/v2"
)

func TestMain(m *testing.M) {
	f, err := os.OpenFile("/tmp/proton-go-driver-tests.lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		panic(err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		panic(err)
	}

	cleanupTestStreams()

	code := m.Run()

	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	_ = f.Close()
	os.Exit(code)
}

func cleanupTestStreams() {
	ctx := context.Background()
	conn, err := proton.Open(&proton.Options{
		Addr: []string{"127.0.0.1:8463"},
		Auth: proton.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
	})
	if err != nil {
		return
	}

	for _, name := range []string{
		"benchmark_fixed_string",
		"benchmark_string",
		"benchmark_uuid",
		"test_abort",
		"test_append_struct",
		"test_array",
		"test_bigint",
		"test_bool",
		"test_column_interface",
		"test_date",
		"test_date32",
		"test_datetime",
		"test_datetime64",
		"test_decimal",
		"test_dynamic",
		"test_empty_query",
		"test_enum",
		"test_fixed_string",
		"test_geo_multipolygon",
		"test_geo_point",
		"test_geo_polygon",
		"test_geo_ring",
		"test_ipv4",
		"test_ipv6",
		"test_json",
		"test_lowcardinality",
		"test_lowcardinality_columnar",
		"test_map",
		"test_nested",
		"test_nullable_array",
		"test_nullable_bigint",
		"test_simple_aggregate_function",
		"test_string",
		"test_tuple",
		"test_uint8",
		"test_uuid",
		"test_variant",
	} {
		if !strings.ContainsAny(name, "`;\"\\") {
			dropStreamAndWait(ctx, conn, name)
		}
	}
}

func dropStreamAndWait(ctx context.Context, conn proton.Conn, name string) {
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		_ = conn.Exec(ctx, "DROP STREAM IF EXISTS "+name)

		var exists uint8
		if err := conn.QueryRow(ctx, "EXISTS STREAM "+name).Scan(&exists); err == nil && exists == 0 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}
