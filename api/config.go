package api

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
)

type JsonValue struct {
	Value string `json:"value"`
}

func Config(name string) (string, error) {
	log.Println("Api.Config: Called...")
	response, err := http.Get(fmt.Sprintf("http://localhost:8082/blue-lion/config?name=%s", name))
	if err != nil {
		return "", err
	}

	data, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return "", err
	}

	var val JsonValue
	err = json.Unmarshal(data, &val)
	if err != nil {
		return "", err
	}

	log.Println("Api.Config: Success!")
	return val.Value, nil
}
