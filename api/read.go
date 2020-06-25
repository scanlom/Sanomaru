package api

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
)

const CONST_DIV_GROWTH = "DIV_GROWTH"

func Scalar(name string) (float64, error) {
	log.Println("Api.Scalar: Called...")
	response, err := http.Get(fmt.Sprintf("http://localhost:8081/blue-lion/read/scalar?name=%s", name))
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

func RestGetByUrl(msg string, url string, ret interface{}) error {
	log.Printf("Api.%s: Called...", msg)
	response, err := http.Get(url)
	if err != nil {
		return err
	}

	data, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return err
	}

	err = json.Unmarshal(data, ret)
	if err != nil {
		return err
	}

	log.Printf("Api.%s: Success!", msg)
	return nil
}

func MDHYearSummaryBySymbol(symbol string, date string, ret *JsonMDHYearSummary) error {
	return RestGetByUrl("MDHYearSummaryBySymbol",
		fmt.Sprintf("http://localhost:8081/blue-lion/read/market-data-historical/year-summary?symbol=%s&date=%s", symbol, date),
		ret,
	)
}

func SimfinIncomeByTicker(ticker string, slice *[]JsonSimfinIncome) error {
	return RestGetByUrl("SimfinIncomeByTicker",
		fmt.Sprintf("http://localhost:8081/blue-lion/read/simfin-income?ticker=%s", ticker),
		slice,
	)
}

func IncomeByTicker(ticker string, slice *[]JsonIncome) error {
	return RestGetByUrl("IncomeByTicker",
		fmt.Sprintf("http://localhost:8081/blue-lion/read/income?ticker=%s", ticker),
		slice,
	)
}

func SimfinBalanceByTicker(ticker string, slice *[]JsonSimfinBalance) error {
	return RestGetByUrl("SimfinBalanceByTicker",
		fmt.Sprintf("http://localhost:8081/blue-lion/read/simfin-balance?ticker=%s", ticker),
		slice,
	)
}

func BalanceByTicker(ticker string, slice *[]JsonBalance) error {
	return RestGetByUrl("BalanceByTicker",
		fmt.Sprintf("http://localhost:8081/blue-lion/read/balance?ticker=%s", ticker),
		slice,
	)
}

func SimfinCashflowByTicker(ticker string, slice *[]JsonSimfinCashflow) error {
	return RestGetByUrl("SimfinCashflowByTicker",
		fmt.Sprintf("http://localhost:8081/blue-lion/read/simfin-cashflow?ticker=%s", ticker),
		slice,
	)
}

func CashflowByTicker(ticker string, slice *[]JsonCashflow) error {
	return RestGetByUrl("CashflowByTicker",
		fmt.Sprintf("http://localhost:8081/blue-lion/read/cashflow?ticker=%s", ticker),
		slice,
	)
}

func SummaryByTicker(ticker string, slice *[]JsonSummary) error {
	return RestGetByUrl("SummaryByTicker",
		fmt.Sprintf("http://localhost:8081/blue-lion/read/summary?ticker=%s", ticker),
		slice,
	)
}

func RefDataBySymbol(symbol string, ret *JsonRefData) error {
	return RestGetByUrl("RefDataBySymbol",
		fmt.Sprintf("http://localhost:8081/blue-lion/read/ref-data?symbol=%s", symbol),
		ret,
	)
}

func ProjectionsBySymbol(symbol string, ret *JsonProjections) error {
	return RestGetByUrl("ProjectionsBySymbol",
		fmt.Sprintf("http://localhost:8081/blue-lion/read/projections?symbol=%s", symbol),
		ret,
	)
}

func MarketDataBySymbol(symbol string, ret *JsonMarketData) error {
	return RestGetByUrl("MarketDataBySymbol",
		fmt.Sprintf("http://localhost:8081/blue-lion/read/market-data?symbol=%s", symbol),
		ret,
	)
}
