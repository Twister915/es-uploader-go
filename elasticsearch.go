package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	fs "io/ioutil"
	"net/http"
	"os"
)

type ESClient struct {
	BaseURL, Index      string
	toggledBulkSettings bool
	cachedBulk          string
	http                http.Client
}

func NewESClient(baseURL, index string) (client *ESClient) {
	client = &ESClient{}
	client.BaseURL = baseURL
	client.Index = index
	client.cachedBulk = client.BaseURL + "/_bulk"
	return
}

func (client *ESClient) BulkUp(data *bytes.Buffer) (err error) {
	resp, err := http.Post(client.cachedBulk, "application/json", data)
	if err != nil {
		return
	}

	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		err = createErrorUsing(resp)
	}
	//
	// body, err := fs.ReadAll(resp.Body)
	//
	// if err != nil {
	// 	return
	// }
	//
	// fmt.Printf("%s\n", body)

	return
}

func (client *ESClient) SetupForBulk() (err error) {
	if client.toggledBulkSettings {
		return fmt.Errorf("You have already setup this client for bulk")
	}

	settings := make(map[string]string)
	settings["refresh_interval"] = "\"45s\""
	settings["number_of_replicas"] = "\"0\""
	// settings["index_concurrency"] = "\"" + strconv.Itoa(runtime.NumCPU()) + "\""

	err = client.setManySettings(&settings)
	if err != nil {
		return
	}

	client.toggledBulkSettings = true
	return
}

func (client *ESClient) Cleanup() (err error) {
	if !client.toggledBulkSettings {
		return fmt.Errorf("You have already setup this client for bulk")
	}

	settings := make(map[string]string)
	settings["refresh_interval"] = "\"1s\""
	settings["number_of_replicas"] = "\"0\""
	// settings["index_concurrency"] = "8"

	err = client.setManySettings(&settings)
	if err != nil {
		return
	}

	client.toggledBulkSettings = false
	return
}

func (client *ESClient) setManySettings(settings *map[string]string) (err error) {
	for key, value := range *settings {
		err = client.setSetting(key, value)
		if err != nil {
			return
		}
	}
	return
}

func (client *ESClient) CreateIndexIfNeeded() (err error) {
	indexURL := client.BaseURL + "/" + client.Index
	checkRequest, err := http.NewRequest("HEAD", indexURL, nil)
	if err != nil {
		return
	}

	checkResposne, err := client.http.Do(checkRequest)
	if err != nil {
		return
	}

	defer checkResposne.Body.Close()
	if checkResposne.StatusCode == 200 {
		return
	}

	createRequest, err := http.NewRequest("PUT", indexURL, nil)
	if err != nil {
		return
	}

	createResponse, err := client.http.Do(createRequest)
	if err != nil {
		return
	}

	defer createResponse.Body.Close()

	if createResponse.StatusCode != 200 {
		err = createErrorUsing(createResponse)
	}
	fmt.Fprintf(os.Stderr, "[ES] Created index %s\n", client.Index)
	return
}

const settingChangeFormat = "{\"index\":{\"%s\": %s}}"

func (client *ESClient) setSetting(setting, value string) (err error) {
	client.CreateIndexIfNeeded()
	req, err := http.NewRequest("PUT", client.BaseURL+"/"+client.Index+"/_settings", bytes.NewBufferString(fmt.Sprintf(settingChangeFormat, setting, value)))
	if err != nil {
		return
	}
	resp, err := client.http.Do(req)
	if err != nil {
		return
	}

	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		err = createErrorUsing(resp)
	}
	fmt.Fprintf(os.Stderr, "[ES] Set setting \"%s\" = %s on index %s\n", setting, value, client.Index)

	return
}

func (client *ESClient) ReadCurrentCount() (count int64, err error) {
	req, err := http.NewRequest("GET", client.BaseURL+"/"+client.Index+"/_count", nil)
	if err != nil {
		return
	}

	resp, err := client.http.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	val := make(map[string]interface{})
	body, err := fs.ReadAll(resp.Body)
	if err != nil {
		return
	}

	json.Unmarshal(body, &val)
	if val == nil {
		return 0, nil
	}

	c := val["count"]
	if c == nil {
		return 0, nil
	}
	count = int64(c.(float64))
	return
}

func createErrorUsing(resp *http.Response) error {
	body, err := fs.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return fmt.Errorf("There was a non-200 status code for the setting change... %d; %s", resp.StatusCode, body)
}
