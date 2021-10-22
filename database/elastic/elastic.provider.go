package elastic

import (
	"errors"
	elasticsearch6 "github.com/elastic/go-elasticsearch/v6"
	"github.com/micro/go-micro/v2/config"
)

func NewElasticProvider(config config.Config) (*elasticsearch6.Client, error) {
	addresses := config.Get("elastic", "addresses").StringSlice(nil)
	if len(addresses) == 0 {
		return nil, errors.New("addresses is empty")
	}

	username := config.Get("elastic", "username").String("")
	if len(username) == 0 {
		return nil, errors.New("username is empty")
	}

	password := config.Get("elastic", "password").String("")
	if len(password) == 0 {
		return nil, errors.New("password is empty")
	}

	elasticCfg := elasticsearch6.Config{
		Addresses: addresses,
		Username:  username,
		Password:  password,
	}

	elasticClient, err := elasticsearch6.NewClient(elasticCfg)
	if err != nil {
		return nil, err
	}

	go watchConfigChange(config, elasticClient)

	return elasticClient, nil
}

func watchConfigChange(config config.Config, db *elasticsearch6.Client) {
	// TODO
}
