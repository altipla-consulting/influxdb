package influxdb

import (
	"time"
)

// Connection to a remote InfluxDB server.
type Connection struct {
	host, database     string
	username, password string
	timeout            time.Duration
}

// NewConnection creates a new connection to a remote server.
func NewConnection(host, database, username, password string) *Connection {
	return &Connection{
		host:     host,
		database: database,
		username: username,
		password: password,
	}
}
