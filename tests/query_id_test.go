package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/timeplus-io/proton-go-driver/v2"
)

func TestServerGenerateQueryID(t *testing.T) {
	var (
		idCh = make(chan string, 1)
		ctx  = proton.Context(context.Background(), proton.WithReceiveQueryID(func(id string) {
			idCh <- id
			close(idCh)
		}))

		conn, err = proton.Open(&proton.Options{
			Addr: []string{"127.0.0.1:8463"},
			Auth: proton.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			},
			/*Compression: &proton.Compression{
				Method: proton.CompressionLZ4,
			},*/
			MaxOpenConns: 1,
		})
	)

	assert.NoError(t, err)
	if _, err := conn.Query(ctx, "SELECT 123"); assert.NoError(t, err) {
		received := <-idCh
		assert.NotEmpty(t, received)
		assert.Equal(t, 36, len(received))
	}
}

func TestClientGenerateQueryID(t *testing.T) {

	var (
		idCh = make(chan string, 1)
		id   = "client_query_id"
		ctx  = proton.Context(context.Background(), proton.WithQueryID(id), proton.WithReceiveQueryID(func(id string) {
			idCh <- id
			close(idCh)
		}))

		conn, err = proton.Open(&proton.Options{
			Addr: []string{"127.0.0.1:8463"},
			Auth: proton.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			},
			/*Compression: &proton.Compression{
				Method: proton.CompressionLZ4,
			},*/
			MaxOpenConns: 1,
		})
	)

	assert.NoError(t, err)
	if _, err := conn.Query(ctx, "SELECT 123"); assert.NoError(t, err) {
		received := <-idCh
		assert.Equal(t, id, received)
	}
}
