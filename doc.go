// Package influxdb allows to send data to a remote InfluxDB server and then query the data
// to obtain results. It has several improvements over the standard Go client
// (like interacting with golang.org/x/net/context) but it is incomplete. It only
// contains functions and calls we need in our applications.
package influxdb
