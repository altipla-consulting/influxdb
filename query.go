package influxdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/juju/errors"
	"golang.org/x/net/context"
)

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
func Query(ctx context.Context, query string) (*QueryResult, error) {
	conn := FromContext(ctx)

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
