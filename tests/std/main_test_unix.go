//go:build !windows

package std

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
		"test_std_array",
		"test_std_bigint",
		"test_std_bool",
		"test_std_date",
		"test_std_date32",
		"test_std_datetime",
		"test_std_datetime64",
		"test_std_decimal",
		"test_std_dynamic",
		"test_std_enum",
		"test_std_fixed_string",
		"test_std_geo_multipolygon",
		"test_std_geo_point",
		"test_std_geo_polygon",
		"test_std_geo_ring",
		"test_std_ipv4",
		"test_std_json",
		"test_std_lowcardinality",
		"test_std_map",
		"test_std_nullable_bigint",
		"test_std_nullable_uuid",
		"test_std_uuid",
		"test_std_variant",
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
