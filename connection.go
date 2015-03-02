package influxdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/juju/errors"
	"golang.org/x/net/context"
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

// ConnectionFromContext opens a new connection to a server including a deadline
// if the context has one.
func ConnectionFromContext(ctx context.Context, host, database, username, password string) *Connection {
	conn := NewConnection(host, database, username, password)

	deadline, ok := ctx.Deadline()
	if ok {
		conn.timeout = deadline.Sub(time.Now())
	}

	return conn
}

// WriteSerie contains the list of points of a single serie to send to the server.
type WriteSerie struct {
	Name   string
	Points []map[string]interface{}
}

type serieReq struct {
	Name    string          `json:"name"`
	Columns []string        `json:"columns"`
	Points  [][]interface{} `json:"points"`
}

// Write sends a list of points of several series to the server.
func (conn *Connection) Write(series []*WriteSerie) error {
	send := make([]*serieReq, len(series))
	for i, serie := range series {
		// Extract all column names to be consistent in every point
		cols := []string{}
		for _, p := range serie.Points {
			for k := range p {
				if !contains(cols, k) {
					cols = append(cols, k)
				}
			}
		}

		// Fill the points in sparse arrays depending on the keys
		points := make([][]interface{}, len(serie.Points))
		for i, p := range serie.Points {
			value := make([]interface{}, len(cols))
			for k, v := range p {
				value[indexOf(cols, k)] = v
			}

			points[i] = value
		}

		// Object we will send to database
		send[i] = &serieReq{
			Name:    serie.Name,
			Columns: cols,
			Points:  points,
		}
	}

	// Serialize the write
	buf := bytes.NewBuffer(nil)
	if err := json.NewEncoder(buf).Encode(send); err != nil {
		return errors.Trace(err)
	}

	// Send the write to the database
	client := &http.Client{
		Timeout: conn.timeout,
	}
	u := fmt.Sprintf("http://%s:8086/db/%s/series?u=%s&p=%s", conn.host, conn.database, conn.username, conn.password)
	resp, err := client.Post(u, "application/json", buf)
	if err != nil {
		return errors.Trace(err)
	}
	defer resp.Body.Close()

	// Error responses from the server are writed to the response.
	// A successful response has a zero length response.
	read, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return errors.Trace(err)
	}
	if len(read) > 0 {
		return errors.Errorf("request failed with message: %s", read)
	}

	return nil
}

// WriteOne sends a list of points of a single serie to the server.
func (conn *Connection) WriteOne(serie *WriteSerie) error {
	return conn.Write([]*WriteSerie{serie})
}

func contains(list []string, item string) bool {
	return indexOf(list, item) != -1
}

func indexOf(list []string, item string) int {
	for i, x := range list {
		if x == item {
			return i
		}
	}

	return -1
}

type queryResp struct {
	Name    string
	Columns []string
	Points  [][]interface{}
}

// QueryResult contains the rows returned by the server in a query.
type QueryResult struct {
	Name   string
	Points []map[string]interface{}
}

// Query sends a new query to the server and returns the result
func (conn *Connection) Query(query string) (*QueryResult, error) {
	// Build the request URL
	params := url.Values{}
	params.Add("u", conn.username)
	params.Add("p", conn.password)
	params.Add("q", query)
	params.Add("time_precision", "ms")
	u := url.URL{
		Scheme:   "http",
		Host:     fmt.Sprintf("%s:8086", conn.host),
		Path:     fmt.Sprintf("/db/%s/series", conn.database),
		RawQuery: params.Encode(),
	}

	// Send the write to the database
	client := &http.Client{
		Timeout: conn.timeout,
	}
	resp, err := client.Get(u.String())
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer resp.Body.Close()

	read, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Trace(err)
	}

	result := []*queryResp{}
	reader := bytes.NewReader(read)
	if err := json.NewDecoder(reader).Decode(&result); err != nil {
		return nil, errors.Annotatef(err, fmt.Sprintf("%s: %s", query, read))
	}

	// No results
	if len(result) == 0 {
		return &QueryResult{}, nil
	}

	points := make([]map[string]interface{}, len(result[0].Points))
	for i, point := range result[0].Points {
		points[i] = map[string]interface{}{}
		for j, col := range result[0].Columns {
			points[i][col] = point[j]
		}
	}

	return &QueryResult{
		Name:   result[0].Name,
		Points: points,
	}, nil
}
