package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/timeplus-io/proton-go-driver/v2"
	"github.com/timeplus-io/proton-go-driver/v2/lib/proto"
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
			MaxOpenConns: 1,
		})
	)

	assert.NoError(t, err)
	if _, err := conn.Query(ctx, "SELECT 123"); assert.NoError(t, err) {
		received := <-idCh
		sv, _ := conn.ServerVersion()
		if sv.Revision >= proto.DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_GENERATE_UUID {
			assert.Equal(t, 36, len(received))
		}
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
			MaxOpenConns: 1,
		})
	)

	assert.NoError(t, err)
	if _, err := conn.Query(ctx, "SELECT 123"); assert.NoError(t, err) {
		received := <-idCh
		sv, _ := conn.ServerVersion()
		if sv.Revision >= proto.DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_GENERATE_UUID {
			assert.Equal(t, id, received)
		}
	}
}
