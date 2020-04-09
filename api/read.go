package api

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
)

func Scalar(name string) (float64, error) {
	log.Println("Api.Scalar: Called...")
	response, err := http.Get(fmt.Sprintf("http://localhost:8082/blue-lion/config?name=%s", name))
	if err != nil {
		return 0.0, err
	}

	data, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return 0.0, err
	}

	var val JsonFloat64Value
	err = json.Unmarshal(data, &val)
	if err != nil {
		return 0.0, err
	}

	log.Println("Api.Scalar: Success!")
	return val.Value, nil
}

func SymbolToRefDataID(symbol string) (int, error) {
	log.Println("Api.SymbolToRefDataID: Called...")
	response, err := http.Get(fmt.Sprintf("http://localhost:8081/blue-lion/read/ref-data?symbol=%s", symbol))
	if err != nil {
		return 0, err
	}

	data, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return 0, err
	}

	var ret JsonRefData
	err = json.Unmarshal(data, &ret)
	if err != nil {
		return 0, err
	}

	log.Println("Api.SymbolToRefDataID: Success!")
	return ret.ID, nil
}
