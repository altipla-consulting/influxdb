package influxdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/juju/errors"
	"golang.org/x/net/context"
)

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
func Write(ctx context.Context, series []*WriteSerie) error {
	conn := FromContext(ctx)

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
func WriteOne(ctx context.Context, serie *WriteSerie) error {
	return Write(ctx, []*WriteSerie{serie})
}
