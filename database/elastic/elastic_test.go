package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v6/esapi"
	"github.com/micro/go-micro/v2/config"
	"github.com/micro/go-micro/v2/config/source/memory"
	"testing"
)

func getConfig() (config.Config, error) {
	config, err := config.NewConfig()
	if err != nil {
		return nil, err
	}

	data := []byte(`{
		"elastic": {
			"addresses":[
				"http://127.0.0.1:9200"
			],
			"username":"elastic",
			"password":"123456"
		}
	}`)
	source := memory.NewSource(memory.WithJSON(data))

	err = config.Load(source)
	if err != nil {
		return nil, err
	}
	return config, nil
}

func TestNewElasticProvider(t *testing.T) {
	config, err := getConfig()
	if err != nil {
		t.Fatal(err)
		return
	}

	elasticClient, err := NewElasticProvider(config)
	if err != nil {
		t.Fatal(err)
		return
	}
	t.Log(elasticClient)

	req := esapi.InfoRequest{}
	res, err := req.Do(context.Background(), elasticClient)
	if err != nil {
		t.Fatal(err)
		return
	}

	defer res.Body.Close()
	fmt.Println(res.String())
}

func TestElasticCreate(t *testing.T) {

	config, err := getConfig()
	if err != nil {
		t.Fatal(err)
		return
	}

	elasticClient, err := NewElasticProvider(config)
	if err != nil {
		t.Fatal(err)
		return
	}

	body := map[string]interface{}{
		"num": 0,
		"v":   0,
		"str": "test",
	}
	jsonBody, _ := json.Marshal(body)

	req := esapi.CreateRequest{ // 如果是esapi.IndexRequest则是插入/替换
		Index:        "test_index",
		DocumentType: "_doc",
		DocumentID:   "test_1",
		Body:         bytes.NewReader(jsonBody),
	}
	res, err := req.Do(context.Background(), elasticClient)
	if err != nil {
		t.Fatal(err)
		return
	}
	defer res.Body.Close()
	fmt.Println(res.String())
}

func TestElasticUpdate(t *testing.T) {

	config, err := getConfig()
	if err != nil {
		t.Fatal(err)
		return
	}

	elasticClient, err := NewElasticProvider(config)
	if err != nil {
		t.Fatal(err)
		return
	}

	body := map[string]interface{}{
		"doc": map[string]interface{}{
			"v": 100,
		},
	}
	jsonBody, _ := json.Marshal(body)

	req := esapi.UpdateRequest{ // 如果是esapi.IndexRequest则是插入/替换
		Index:        "test_index",
		DocumentType: "_doc",
		DocumentID:   "test_1",
		Body:         bytes.NewReader(jsonBody),
	}
	res, err := req.Do(context.Background(), elasticClient)
	if err != nil {
		t.Fatal(err)
		return
	}
	defer res.Body.Close()
	fmt.Println(res.String())

}

func TestElasticXPackSQLQuery(t *testing.T) {
	config, err := getConfig()
	if err != nil {
		t.Fatal(err)
		return
	}

	elasticClient, err := NewElasticProvider(config)
	if err != nil {
		t.Fatal(err)
		return
	}

	query := map[string]interface{}{
		"query": "select  *  from test_index where str like 'te%' order by v LIMIT 1",
	}
	jsonBody, _ := json.Marshal(query)
	req := esapi.XPackSQLQueryRequest{
		Body: bytes.NewReader(jsonBody),
	}
	res, err := req.Do(context.Background(), elasticClient)
	if err != nil {
		t.Fatal(err)
		return
	}
	defer res.Body.Close()
	fmt.Println(res.String())

}

func TestElasticSearch(t *testing.T) {

}
