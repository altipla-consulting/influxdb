package influxdb

import (
	"time"

	"golang.org/x/net/context"
)

type key int

var keyConnection key

// WithContext opens a new connection to a remote database and adds it to the context
func WithContext(ctx context.Context, host, database, username, password string) context.Context {
	conn := NewConnection(host, database, username, password)

	deadline, ok := ctx.Deadline()
	if ok {
		conn.timeout = deadline.Sub(time.Now())
	}

	return context.WithValue(ctx, keyConnection, conn)
}

// FromContext returns the database stored in the context
func FromContext(ctx context.Context) *Connection {
	return ctx.Value(keyConnection).(*Connection)
}
