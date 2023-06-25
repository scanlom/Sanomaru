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

var symbolToRefDataID = make(map[string]int)

func SymbolToRefDataID(symbol string) (int, error) {
	log.Println("Api.SymbolToRefDataID: Called...")
	if id, cached := symbolToRefDataID[symbol]; cached {
		log.Println("Api.SymbolToRefDataID: Returning cached value")
		log.Println("Api.SymbolToRefDataID: Success!")
		return id, nil
	}

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

	symbolToRefDataID[symbol] = ret.ID
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

func Projections(slice *[]JsonProjections) error {
	return RestGetByUrl("Projections",
		"http://localhost:8081/blue-lion/read/projections",
		slice,
	)
}

func MarketDataBySymbol(symbol string, ret *JsonMarketData) error {
	return RestGetByUrl("MarketDataBySymbol",
		fmt.Sprintf("http://localhost:8081/blue-lion/read/market-data?symbol=%s", symbol),
		ret,
	)
}

func MarketDataByRefDataID(refDataID int, ret *JsonMarketData) error {
	return RestGetByUrl("MarketDataByRefDataID",
		fmt.Sprintf("http://localhost:8081/blue-lion/read/market-data?refDataId=%d", refDataID),
		ret,
	)
}

func RefDataByID(id int, ret *JsonRefData) error {
	return RestGetByUrl("RefDataByID",
		fmt.Sprintf("http://localhost:8081/blue-lion/read/ref-data/%d", id),
		ret,
	)
}

func Mergers(slice *[]JsonMerger) error {
	return RestGetByUrl("Mergers",
		"http://localhost:8081/blue-lion/read/mergers",
		slice,
	)
}

func EnrichedMergersPositions(slice *[]JsonEnrichedMerger) error {
	return RestGetByUrl("EnrichedMergersPositions",
		"http://localhost:8081/blue-lion/read/enriched-mergers-positions",
		slice,
	)
}

func Positions(slice *[]JsonPosition) error {
	return RestGetByUrl("Positions",
		"http://localhost:8081/blue-lion/read/positions",
		slice,
	)
}

func PositionsBySymbolPortfolioID(symbol string, portfolioId int, ret *JsonPosition) error {
	return RestGetByUrl("PositionBySymbolPortfolioID",
		fmt.Sprintf("http://localhost:8081/blue-lion/read/positions?symbol=%s&portfolioId=%d", symbol, portfolioId),
		ret,
	)
}

func PositionsByID(id int, ret *JsonPosition) error {
	return RestGetByUrl("PositionsByID",
		fmt.Sprintf("http://localhost:8081/blue-lion/read/positions/%d", id),
		ret,
	)
}

func PositionReturnsByID(id int, ret *JsonReturns) error {
	return RestGetByUrl("PositionReturnsByID",
		fmt.Sprintf("http://localhost:8081/blue-lion/read/position-returns/%d", id),
		ret,
	)
}

func EnrichedPositionsBySymbolPortfolioID(symbol string, portfolioId int, ret *JsonEnrichedPosition) error {
	return RestGetByUrl("EnrichedPositionsBySymbolPortfolioID",
		fmt.Sprintf("http://localhost:8081/blue-lion/read/enriched-positions?symbol=%s&portfolioId=%d", symbol, portfolioId),
		ret,
	)
}

func EnrichedPositionsByID(id int, ret *JsonEnrichedPosition) error {
	return RestGetByUrl("EnrichedPositionsByID",
		fmt.Sprintf("http://localhost:8081/blue-lion/read/enriched-positions/%d", id),
		ret,
	)
}

func Portfolios(slice *[]JsonPortfolio) error {
	return RestGetByUrl("Portfolios",
		"http://localhost:8081/blue-lion/read/portfolios",
		slice,
	)
}

func EnrichedPortfoliosByID(id int, ret *JsonEnrichedPortfolio) error {
	return RestGetByUrl("EnrichedPortfoliosByID",
		fmt.Sprintf("http://localhost:8081/blue-lion/read/enriched-portfolios/%d", id),
		ret,
	)
}

func EnrichedMergersByID(id int, ret *JsonEnrichedMerger) error {
	return RestGetByUrl("EnrichedMergersByID",
		fmt.Sprintf("http://localhost:8081/blue-lion/read/enriched-mergers/%d", id),
		ret,
	)
}

func EnrichedProjectionsByID(id int, ret *JsonEnrichedProjections) error {
	return RestGetByUrl("EnrichedProjectionsByID",
		fmt.Sprintf("http://localhost:8081/blue-lion/read/enriched-projections/%d", id),
		ret,
	)
}

func PortfoliosByID(id int, ret *JsonPortfolio) error {
	return RestGetByUrl("PortfoliosByID",
		fmt.Sprintf("http://localhost:8081/blue-lion/read/portfolios/%d", id),
		ret,
	)
}

func PortfoliosHistoryByDate(date string, slice *[]JsonPortfolioHistory) error {
	return RestGetByUrl("PortfoliosHistoryByDate",
		"http://localhost:8081/blue-lion/read/portfolios-history?date="+date,
		slice,
	)
}

func PortfoliosHistoryPortfolioIDDate(portfolioId int, date string, ret *JsonPortfolioHistory) error {
	return RestGetByUrl("PortfoliosHistoryPortfolioIDDate",
		fmt.Sprintf("http://localhost:8081/blue-lion/read/portfolios-history?portfolioId=%d&date=%s", portfolioId, date),
		ret,
	)
}

func PositionsHistoryByPortfolioIDDate(portfolioId int, date string, slice *[]JsonPositionHistory) error {
	return RestGetByUrl("PositionsHistoryByPortfolioIDDate",
		fmt.Sprintf("http://localhost:8081/blue-lion/read/positions-history?portfolioId=%d&date=%s", portfolioId, date),
		slice,
	)
}

func PositionsHistoryByPositionIDDate(positionId int, date string, ret *JsonPositionHistory) error {
	return RestGetByUrl("PositionsHistoryByPositionIDDate",
		fmt.Sprintf("http://localhost:8081/blue-lion/read/positions-history?positionId=%d&date=%s", positionId, date),
		ret,
	)
}

func PositionsHistoryFirst(positionId int, date string, ret *JsonPositionHistory) error {
	return RestGetByUrl("PositionsHistoryFirst",
		fmt.Sprintf("http://localhost:8081/blue-lion/read/positions-history-first?positionId=%d", positionId),
		ret,
	)
}

func ProjectionsUpdateByID(id int) error {
	log.Println("Api.ProjectionsUpdateByID: Called...")
	_, err := http.Get(fmt.Sprintf("http://localhost:8084/blue-lion/cache/projections-update/%d", id))
	if err != nil {
		return err
	}
	log.Println("Api.ProjectionsUpdateByID: Success!")
	return nil
}