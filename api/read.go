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

func RestSliceByString(msg string, urlFmt string, slice interface{}, param string) error {
	log.Printf("Api.%s: Called...", msg)
	response, err := http.Get(fmt.Sprintf(urlFmt, param))
	if err != nil {
		return err
	}

	data, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return err
	}

	err = json.Unmarshal(data, slice)
	if err != nil {
		return err
	}

	log.Printf("Api.%s: Success!", msg)
	return nil
}

func SimfinIncomeByTicker(ticker string, slice *[]JsonSimfinIncome) error {
	return RestSliceByString("SimfinIncomeByTicker",
		"http://localhost:8081/blue-lion/read/simfin-income?ticker=%s",
		slice,
		ticker,
	)
}

func IncomeByTicker(ticker string, slice *[]JsonIncome) error {
	return RestSliceByString("IncomeByTicker",
		"http://localhost:8081/blue-lion/read/income?ticker=%s",
		slice,
		ticker,
	)
}

func SimfinBalanceByTicker(ticker string, slice *[]JsonSimfinBalance) error {
	return RestSliceByString("SimfinBalanceByTicker",
		"http://localhost:8081/blue-lion/read/simfin-balance?ticker=%s",
		slice,
		ticker,
	)
}

func BalanceByTicker(ticker string, slice *[]JsonBalance) error {
	return RestSliceByString("BalanceByTicker",
		"http://localhost:8081/blue-lion/read/balance?ticker=%s",
		slice,
		ticker,
	)
}

func SimfinCashflowByTicker(ticker string, slice *[]JsonSimfinCashflow) error {
	return RestSliceByString("SimfinCashflowByTicker",
		"http://localhost:8081/blue-lion/read/simfin-cashflow?ticker=%s",
		slice,
		ticker,
	)
}

func CashflowByTicker(ticker string, slice *[]JsonCashflow) error {
	return RestSliceByString("CashflowByTicker",
		"http://localhost:8081/blue-lion/read/cashflow?ticker=%s",
		slice,
		ticker,
	)
}
