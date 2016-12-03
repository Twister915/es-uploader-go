package main

import (
	"bytes"
	"fmt"
	fs "io/ioutil"
	"net/http"
)

func uploadToESFunc(elasticSearchURL string) func(*bytes.Buffer) error {
	return func(data *bytes.Buffer) (err error) {
		resp, err := http.Post(elasticSearchURL, "application/json", data)
		if err != nil {
			return
		}

		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			body, _ := fs.ReadAll(resp.Body)
			err = fmt.Errorf("There was a non-200 status code provided as a response: %d; %s", resp.StatusCode, body)
			return
		}

		return
	}
}
