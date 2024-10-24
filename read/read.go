package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/schema"
	"github.com/scanlom/Sanomaru/api"
)

func setupRouter(router *mux.Router) {
	router.HandleFunc("/blue-lion/read/market-data/{id}", MarketDataByID).Methods("GET")
	router.HandleFunc("/blue-lion/read/market-data", MarketDataBySymbol).Queries("symbol", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/market-data", MarketDataByRefDataID).Queries("refDataId", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/market-data", MarketData).Methods("GET")
	router.HandleFunc("/blue-lion/read/enriched-market-data", EnrichedMarketDataBySymbol).Queries("symbol", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/market-data-historical/year-summary", MDHYearSummaryBySymbol).Queries("symbol", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/market-data-historical", MDHByRefDataIDDate).Queries("refDataId", "", "date", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/ref-data/focus", RefDataFocus).Methods("GET")
	router.HandleFunc("/blue-lion/read/ref-data/positions", RefDataPositions).Methods("GET")
	router.HandleFunc("/blue-lion/read/ref-data/{id}", RefDataByID).Methods("GET")
	router.HandleFunc("/blue-lion/read/ref-data", RefDataBySymbol).Queries("symbol", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/ref-data", RefData).Methods("GET")
	router.HandleFunc("/blue-lion/read/projections/{id}", ProjectionsByID).Methods("GET")
	router.HandleFunc("/blue-lion/read/projections", ProjectionsBySymbol).Queries("symbol", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/projections", Projections).Methods("GET")
	router.HandleFunc("/blue-lion/read/simfin-income/{id}", SimfinIncomeByID).Methods("GET")
	router.HandleFunc("/blue-lion/read/simfin-income", SimfinIncomeByTicker).Queries("ticker", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/income", IncomeByTicker).Queries("ticker", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/simfin-balance/{id}", SimfinBalanceByID).Methods("GET")
	router.HandleFunc("/blue-lion/read/simfin-balance", SimfinBalanceByTicker).Queries("ticker", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/balance", BalanceByTicker).Queries("ticker", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/simfin-cashflow/{id}", SimfinCashflowByID).Methods("GET")
	router.HandleFunc("/blue-lion/read/simfin-cashflow", SimfinCashflowByTicker).Queries("ticker", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/cashflow", CashflowByTicker).Queries("ticker", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/summary", SummaryByTicker).Queries("ticker", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/mergers", Mergers).Methods("GET")
	router.HandleFunc("/blue-lion/read/enriched-mergers-positions", EnrichedMergersPositions).Methods("GET")
	router.HandleFunc("/blue-lion/read/enriched-mergers-positions-total", EnrichedMergersPositionsTotal).Methods("GET")
	router.HandleFunc("/blue-lion/read/enriched-mergers-research", EnrichedMergersResearch).Methods("GET")
	router.HandleFunc("/blue-lion/read/enriched-mergers/{id}", EnrichedMergersByID).Methods("GET")
	router.HandleFunc("/blue-lion/read/enriched-mergers-journal", EnrichedMergersJournalByMergerID).Queries("mergerId", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/enriched-projections/{id}", EnrichedProjectionsByID).Methods("GET")
	router.HandleFunc("/blue-lion/read/enriched-projections", EnrichedProjectionsBySymbol).Queries("symbol", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/enriched-projections-journal", EnrichedProjectionsJournalByProjectionsID).Queries("projectionsId", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/portfolios/{id}", PortfoliosByID).Methods("GET")
	router.HandleFunc("/blue-lion/read/portfolios", Portfolios).Methods("GET")
	router.HandleFunc("/blue-lion/read/enriched-portfolios/{id}", EnrichedPortfoliosByID).Methods("GET")
	router.HandleFunc("/blue-lion/read/enriched-portfolios", EnrichedPortfolios).Methods("GET")
	router.HandleFunc("/blue-lion/read/portfolios-history", PortfoliosHistoryByPortfolioIDDate).Queries("portfolioId", "", "date", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/portfolios-history", PortfoliosHistoryByDate).Queries("date", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/portfolios-history-max-date", PortfoliosHistoryMaxDate).Methods("GET")
	router.HandleFunc("/blue-lion/read/portfolios-history-max-index", PortfoliosHistoryMaxIndexByPortfolioID).Queries("portfolioId", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/positions/{id}", PositionsByID).Methods("GET")
	router.HandleFunc("/blue-lion/read/positions", PositionsBySymbolPortfolioID).Queries("symbol", "", "portfolioId", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/positions", Positions).Methods("GET")
	router.HandleFunc("/blue-lion/read/positions-history", PositionsHistoryByPortfolioIDDate).Queries("portfolioId", "", "date", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/positions-history", PositionsHistoryByPositionIDDate).Queries("positionId", "", "date", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/positions-history-first", PositionsHistoryFirst).Queries("positionId", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/enriched-positions/{id}", EnrichedPositionsByID).Methods("GET")
	router.HandleFunc("/blue-lion/read/enriched-positions", EnrichedPositionsBySymbolPortfolioID).Queries("symbol", "", "portfolioId", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/enriched-positions", EnrichedPositionsByPortfolioID).Queries("portfolioId", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/enriched-positions-all", EnrichedPositionsAllByPortfolioID).Queries("portfolioId", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/enriched-positions-all", EnrichedPositionsAll).Methods("GET")
	router.HandleFunc("/blue-lion/read/enriched-positions-history", EnrichedPositionsHistoryByPortfolioIDDate).Queries("portfolioId", "", "date", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/portfolio-returns/{id}", PortfolioReturnsByID).Methods("GET")
	router.HandleFunc("/blue-lion/read/portfolio-returns", PortfolioReturnsByDate).Queries("date", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/portfolio-returns", PortfolioReturns).Methods("GET")
	router.HandleFunc("/blue-lion/read/position-returns/{id}", PositionReturnsByID).Methods("GET")
	router.HandleFunc("/blue-lion/read/transactions", TransactionsByPositionID).Methods("GET").Queries("positionId", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/transactions", TransactionsByPortfolioID).Methods("GET").Queries("portfolioId", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/transactions", Transactions).Methods("GET")
	router.HandleFunc("/blue-lion/read/factors", FactorsByTicker).Queries("ticker", "").Methods("GET")
	router.Methods("GET").Path("/blue-lion/read/scalar").HandlerFunc(Scalar)
}

func RestHandleGet(w http.ResponseWriter, r *http.Request, msg string, ptr interface{}, obj interface{}, table string) {
	api.Enter(msg, w, r)

	db, err := api.DbConnect()
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	err = db.Select(ptr, api.JsonToSelect(obj, table, ""))
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(ptr)

	api.Exit(msg, ptr)
}

func RestHandleGetByID(w http.ResponseWriter, r *http.Request, msg string, ptr interface{}, obj interface{}, table string) {
	api.Enter(msg, w, r)

	params := mux.Vars(r)
	id := params["id"]

	db, err := api.DbConnect()
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	err = db.Get(ptr, fmt.Sprintf("%s WHERE id=%s", api.JsonToSelect(obj, table, ""), id))
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(ptr)

	api.Exit(msg, ptr)
}

func RestHandleGetBySymbol(w http.ResponseWriter, r *http.Request, msg string, ptr interface{}, obj interface{}, table string) {
	api.Enter(msg, w, r)

	args := new(api.RestSymbolInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		api.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	refDataID, err := api.SymbolToRefDataID(args.Symbol)
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	db, err := api.DbConnect()
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	err = db.Get(ptr, fmt.Sprintf("%s WHERE ref_data_id=%d", api.JsonToSelect(obj, table, ""), refDataID))
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(ptr)

	api.Exit(msg, ptr)
}

func EnrichedProjectionsBySymbol(w http.ResponseWriter, r *http.Request) {
	api.Enter("Read-EnrichedProjectionsBySymbol", w, r)

	args := new(api.RestSymbolInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		api.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	refDataID, err := api.SymbolToRefDataID(args.Symbol)
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	db, err := api.DbConnect()
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	p := api.JsonProjections{}
	err = db.Get(&p, api.JsonToSelect(p, "projections", "")+fmt.Sprintf(" WHERE ref_data_id=%d", refDataID))
	if err != nil {
		api.ProjectionsBySymbol(args.Symbol, &p)
	}
	ep, err := api.EnrichProjections(p)
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(&ep)
	api.Exit("Read-EnrichedProjectionsBySymbol", ep)
}

func EnrichedProjectionsByID(w http.ResponseWriter, r *http.Request) {
	api.Enter("Read-EnrichedProjectionsByID", w, r)

	params := mux.Vars(r)
	id := params["id"]

	db, err := api.DbConnect()
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	p := api.JsonProjections{}
	err = db.Get(&p, api.JsonToSelect(p, "projections", "")+fmt.Sprintf(" WHERE id=%s", id))
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
	ep, err := api.EnrichProjections(p)
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(&ep)

	api.Exit("Read-EnrichedProjectionsByID", ep)
}

type EnrichedProjectionsJournalByProjectionsIDInput struct {
	ProjectionsID int `schema:"projectionsId"`
}

func EnrichedProjectionsJournalByProjectionsID(w http.ResponseWriter, r *http.Request) {
	api.Enter("Read-EnrichedProjectionsJournalByProjectionsID", w, r)

	args := new(EnrichedProjectionsJournalByProjectionsIDInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		api.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	db, err := api.DbConnect()
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret := []api.JsonEnrichedProjectionsJournal{}
	err = db.Select(&ret, api.JsonToSelect(api.JsonEnrichedProjectionsJournal{}, "projections_journal", "")+fmt.Sprintf(" WHERE projections_id=%d ORDER BY id DESC", args.ProjectionsID))
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	sort.Slice(ret, func(i, j int) bool {
		return api.DateStringToTime(ret[i].Date).After(api.DateStringToTime(ret[j].Date))
	})

	json.NewEncoder(w).Encode(&ret)

	api.Exit("Read-EnrichedProjectionsJournalByProjectionsID", ret)
}

func PositionsByID(w http.ResponseWriter, r *http.Request) {
	var ret api.JsonPosition
	RestHandleGetByID(w, r, "Read-PositionsByID", &ret, ret, "positions")
}

func ProjectionsByID(w http.ResponseWriter, r *http.Request) {
	var ret api.JsonProjections
	RestHandleGetByID(w, r, "Read-ProjectionsBySymbol", &ret, ret, "projections")
}

func ProjectionsBySymbol(w http.ResponseWriter, r *http.Request) {
	api.Enter("Read-ProjectionsBySymbol", w, r)

	args := new(api.RestSymbolInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		api.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	refDataID, err := api.SymbolToRefDataID(args.Symbol)
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	db, err := api.DbConnect()
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	var ret api.JsonProjections
	err = db.Get(&ret, fmt.Sprintf("%s WHERE ref_data_id=%d ORDER BY id DESC LIMIT 1", api.JsonToSelect(ret, "projections", ""), refDataID))
	if err != nil {
		ret.RefDataID = refDataID
		ret.Confidence = "N"
		var summary []api.JsonSummary
		err = api.SummaryByTicker(args.Symbol, &summary)
		if err != nil {
			log.Println(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		if len(summary) > 0 {
			ret.Date = summary[0].ReportDate
			ret.EPS = summary[0].EPS
			ret.DPS = summary[0].DPS
			if ret.EPS > 0 {
				ret.Payout = ret.DPS / ret.EPS
			}

			var epsCagr5yr, epsCagr10yr, roe5yr float64
			var peHighMmo5yr, peLowMmo5yr int
			api.HeadlineFromSummary(summary, &peHighMmo5yr, &peLowMmo5yr, &epsCagr5yr, &epsCagr10yr, &roe5yr)
			ret.Growth = epsCagr5yr
			ret.ROE = roe5yr
			ret.PETerminal = (peHighMmo5yr + peLowMmo5yr) / 2
			if ret.PETerminal > 18.0 { // Cap PETerminal at 18
				ret.PETerminal = 18.0
			}
			ret.EPSYr1 = ret.EPS * (1.0 + epsCagr5yr)
			ret.EPSYr2 = ret.EPSYr1 * (1.0 + epsCagr5yr)
		} else {
			ret.Date = "1900-01-01T00:00:00Z"
		}
	}

	json.NewEncoder(w).Encode(&ret)
	api.Exit("Read-ProjectionsBySymbol", &ret)
}

func MarketData(w http.ResponseWriter, r *http.Request) {
	api.Enter("MarketData", w, r)

	db, err := api.DbConnect()
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret := []api.JsonMarketData{}
	err = db.Select(&ret, "SELECT id, ref_data_id, last FROM market_data")
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	api.Exit("MarketData", ret)
}

func MarketDataByID(w http.ResponseWriter, r *http.Request) {
	var ret api.JsonMarketData
	RestHandleGetByID(w, r, "Read-MarketDataByID", &ret, ret, "market_data")
}

func MarketDataBySymbol(w http.ResponseWriter, r *http.Request) {
	var ret api.JsonMarketData
	RestHandleGetBySymbol(w, r, "Read-MarketDataBySymbol", &ret, ret, "market_data")
}

func MarketDataByRefDataID(w http.ResponseWriter, r *http.Request) {
	api.Enter("Read-MarketDataByRefDataID", w, r)

	args := new(api.RestRefDataIDInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		api.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	ret := api.JsonMarketData{}
	err = api.DbGet(&ret, api.JsonToSelect(api.JsonMarketData{}, fmt.Sprintf("market_data WHERE ref_data_id=%d", args.RefDataID), ""))
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	api.Exit("Read-MarketDataByRefDataID", ret)
}

func EnrichedMarketDataBySymbol(w http.ResponseWriter, r *http.Request) {
	api.Enter("Read-EnrichedMarketDataBySymbol", w, r)

	args := new(api.RestSymbolInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		api.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	refDataID, err := api.SymbolToRefDataID(args.Symbol)
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	var ret api.JsonEnrichedMarketData
	err = api.DbGet(&ret, fmt.Sprintf("select id, ref_data_id, last, (updated_at < current_date - interval '4 days') as stale from market_data WHERE ref_data_id=%d", refDataID))
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	api.Exit("Read-EnrichedMarketDataBySymbol", &ret)
}

func MDHYearSummaryBySymbol(w http.ResponseWriter, r *http.Request) {
	api.Enter("Read-MDHYearSummaryBySymbol", w, r)

	args := new(api.RestSymbolDateInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		api.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	refDataID, err := api.SymbolToRefDataID(args.Symbol)
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	db, err := api.DbConnect()
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret := api.JsonMDHYearSummary{}
	err = db.Get(&ret, fmt.Sprintf(
		"SELECT t1.ref_data_id, t2.close, t1.high, t1.low FROM "+
			"(SELECT ref_data_id, MAX(close) AS high, MIN(close) AS low FROM  market_data_historical "+
			"WHERE ref_data_id=%d AND date<='%s' and date > TO_DATE('%s','YYYY-MM-DD') - INTERVAL '1 year' "+
			"GROUP BY ref_data_id) t1 "+
			"JOIN "+
			"(SELECT ref_data_id, close FROM market_data_historical "+
			"WHERE ref_data_id=%d AND date = (select MAX(date) from market_data_historical where date <= '%s' and ref_data_id=%d)) t2 "+
			"ON t1.ref_data_id = t2.ref_data_id",
		refDataID, args.Date, args.Date, refDataID, args.Date, refDataID))
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	api.Exit("Read-MDHYearSummaryBySymbol", ret)
}

func MDHByRefDataIDDate(w http.ResponseWriter, r *http.Request) {
	api.Enter("Read-MDHByRefDataIDDate", w, r)

	args := new(api.RestRefDataIDDateInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		api.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	db, err := api.DbConnect()
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret := api.JsonMarketDataHistorical{}
	err = db.Get(&ret, fmt.Sprintf("SELECT id, date, ref_data_id, adj_close, close FROM market_data_historical where ref_data_id = %d and date = '%s'", args.RefDataID, args.Date))
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	api.Exit("Read-MDHByRefDataIDDate", ret)
}

func MDHBySymbol(w http.ResponseWriter, r *http.Request) {
	api.Enter("Read-MDHBySymbol", w, r)

	args := new(api.RestSymbolDateInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		api.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	refDataID, err := api.SymbolToRefDataID(args.Symbol)
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	db, err := api.DbConnect()
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret := api.JsonMDHYearSummary{}
	err = db.Get(&ret, fmt.Sprintf(
		"SELECT t1.ref_data_id, t2.close, t1.high, t1.low FROM "+
			"(SELECT ref_data_id, MAX(close) AS high, MIN(close) AS low FROM  market_data_historical "+
			"WHERE ref_data_id=%d AND date<='%s' and date > TO_DATE('%s','YYYY-MM-DD') - INTERVAL '1 year' "+
			"GROUP BY ref_data_id) t1 "+
			"JOIN "+
			"(SELECT ref_data_id, close FROM market_data_historical "+
			"WHERE ref_data_id=%d AND date = (select MAX(date) from market_data_historical where date <= '%s' and ref_data_id=%d)) t2 "+
			"ON t1.ref_data_id = t2.ref_data_id",
		refDataID, args.Date, args.Date, refDataID, args.Date, refDataID))
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	api.Exit("Read-MDHYearSummaryBySymbol", ret)
}

func RefDataPositions(w http.ResponseWriter, r *http.Request) {
	api.Enter("RefDataPositions", w, r)

	ret := []api.JsonRefData{}
	positions := []api.JsonPosition{}
	err := api.Positions(&positions)
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	for i := range positions {
		if positions[i].PricingType == api.CONST_PRICING_TYPE_BY_PRICE {
			refData := api.JsonRefData{}
			err = api.RefDataByID(positions[i].RefDataID, &refData)
			if err != nil {
				api.ErrorHttp(err, w, http.StatusInternalServerError)
				return
			}
			ret = append(ret, refData)
		}
	}
	json.NewEncoder(w).Encode(&ret)

	api.Exit("RefDataPositions", ret)
}

func RefDataFocus(w http.ResponseWriter, r *http.Request) {
	api.Enter("RefDataFocus", w, r)

	// Focus means watch list and open mergers
	ret := []api.JsonRefData{}
	projections := []api.JsonProjections{}
	err := api.Projections(&projections)
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	for i := range projections {
		if projections[i].Watch {
			refData := api.JsonRefData{}
			err = api.RefDataByID(projections[i].RefDataID, &refData)
			if err != nil {
				api.ErrorHttp(err, w, http.StatusInternalServerError)
				return
			}
			ret = append(ret, refData)
		}
	}

	mergers := []api.JsonEnrichedMerger{}
	err = api.EnrichedMergersResearch(&mergers)
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	for i := range mergers {
		if mergers[i].Status == "O" {
			refData := api.JsonRefData{}
			err = api.RefDataByID(mergers[i].TargetRefDataID, &refData)
			if err != nil {
				api.ErrorHttp(err, w, http.StatusInternalServerError)
				return
			}
			ret = append(ret, refData)
		}
	}

	json.NewEncoder(w).Encode(&ret)

	api.Exit("RefDataFocus", ret)
}

func RefData(w http.ResponseWriter, r *http.Request) {
	api.Enter("RefData", w, r)

	db, err := api.DbConnect()
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	foo := api.JsonRefData{}
	ret := []api.JsonRefData{}
	err = db.Select(&ret, fmt.Sprintf("%s, market_data m WHERE r.active=true AND r.id = m.ref_data_id ORDER BY m.updated_at ASC", api.JsonToSelect(foo, "ref_data", "r")))
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	api.Exit("RefData", ret)
}

func RefDataByID(w http.ResponseWriter, r *http.Request) {
	api.Enter("RefDataByID", w, r)

	params := mux.Vars(r)
	id := params["id"]

	db, err := api.DbConnect()
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret := api.JsonRefData{}
	err = db.Get(&ret, fmt.Sprintf("%s WHERE id=%s", api.JsonToSelect(ret, "ref_data", ""), id))
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	api.Exit("RefDataByID", ret)
}

func RefDataBySymbol(w http.ResponseWriter, r *http.Request) {
	api.Enter("RefDataBySymbol", w, r)

	args := new(api.RestSymbolInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		api.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	db, err := api.DbConnect()
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret := api.JsonRefData{}
	err = db.Get(&ret, fmt.Sprintf("%s WHERE symbol='%s'", api.JsonToSelect(ret, "ref_data", ""), args.Symbol))
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	api.Exit("RefDataBySymbol", ret)
}

type DbScalar struct {
	Name  string  `db:"name"`
	Value float64 `db:"value"`
}

type ScalarInput struct {
	Name string `schema:"name"`
}

type ScalarRet struct {
	Value float64 `json:"value"`
}

func Scalar(w http.ResponseWriter, r *http.Request) {
	api.Enter("Scalar", w, r)

	args := new(ScalarInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		api.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	db, err := api.DbConnect()
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	s := DbScalar{}
	err = db.Get(&s, fmt.Sprintf("SELECT name, value FROM scalars where name='%s'", args.Name))
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(ScalarRet{Value: s.Value})

	api.Exit("Scalar", s)
}

func SimfinIncomeByID(w http.ResponseWriter, r *http.Request) {
	api.Enter("SimfinIncomeByID", w, r)

	params := mux.Vars(r)
	id := params["id"]

	db, err := api.DbConnect()
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret := api.JsonSimfinIncome{}
	err = db.Get(&ret, api.JsonToSelect(ret, "simfin_income", "")+fmt.Sprintf(" WHERE id=%s", id))
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	api.Exit("RefDataByID", ret)
}

type SimfinIncomeInput struct {
	Ticker string `schema:"ticker"`
}

func SimfinIncomeByTicker(w http.ResponseWriter, r *http.Request) {
	api.Enter("SimfinIncomeByTicker", w, r)
	w.Header().Set("Access-Control-Allow-Origin", "*")

	args := new(SimfinIncomeInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		api.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	db, err := api.DbConnect()
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret := []api.JsonSimfinIncome{}
	err = db.Select(&ret, api.JsonToSelect(api.JsonSimfinIncome{}, "simfin_income", "")+fmt.Sprintf(" WHERE ticker='%s' ORDER BY fiscal_year DESC", args.Ticker))
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	api.Exit("SimfinIncomeByTicker", ret)
}

type IncomeInput struct {
	Ticker string `schema:"ticker"`
}

func IncomeByTicker(w http.ResponseWriter, r *http.Request) {
	api.Enter("IncomeByTicker", w, r)
	w.Header().Set("Access-Control-Allow-Origin", "*")

	args := new(IncomeInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		api.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	var simfin []api.JsonSimfinIncome
	err = api.SimfinIncomeByTicker(args.Ticker, &simfin)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	ret := []api.JsonIncome{}
	for s := range simfin {
		i := api.JsonIncome{JsonSimfinIncome: simfin[s]}
		if i.SharesDiluted > 0.0 {
			i.EPS = api.Round(float64(i.NetIncomeCommon)/float64(i.SharesDiluted), 0.01)
		}
		ret = append(ret, i)
	}
	json.NewEncoder(w).Encode(&ret)

	api.Exit("IncomeByTicker", ret)
}

func SimfinBalanceByID(w http.ResponseWriter, r *http.Request) {
	api.Enter("SimfinBalanceByID", w, r)

	params := mux.Vars(r)
	id := params["id"]

	db, err := api.DbConnect()
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret := api.JsonSimfinBalance{}
	err = db.Get(&ret, api.JsonToSelect(ret, "simfin_balance", "")+fmt.Sprintf(" WHERE id=%s", id))
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	api.Exit("SimfinBalanceByID", ret)
}

type SimfinBalanceInput struct {
	Ticker string `schema:"ticker"`
}

func SimfinBalanceByTicker(w http.ResponseWriter, r *http.Request) {
	api.Enter("SimfinBalanceByTicker", w, r)
	w.Header().Set("Access-Control-Allow-Origin", "*")

	args := new(SimfinBalanceInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		api.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	db, err := api.DbConnect()
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret := []api.JsonSimfinBalance{}
	err = db.Select(&ret, api.JsonToSelect(api.JsonSimfinBalance{}, "simfin_balance", "")+fmt.Sprintf(" WHERE ticker='%s' ORDER BY fiscal_year DESC", args.Ticker))
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	api.Exit("SimfinBalanceByTicker", ret)
}

type BalanceInput struct {
	Ticker string `schema:"ticker"`
}

func BalanceByTicker(w http.ResponseWriter, r *http.Request) {
	api.Enter("BalanceByTicker", w, r)
	w.Header().Set("Access-Control-Allow-Origin", "*")

	args := new(BalanceInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		api.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	var simfin []api.JsonSimfinBalance
	err = api.SimfinBalanceByTicker(args.Ticker, &simfin)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	ret := []api.JsonBalance{}
	for s := range simfin {
		i := api.JsonBalance{JsonSimfinBalance: simfin[s]}
		ret = append(ret, i)
	}
	json.NewEncoder(w).Encode(&ret)

	api.Exit("BalanceByTicker", ret)
}

func SimfinCashflowByID(w http.ResponseWriter, r *http.Request) {
	api.Enter("SimfinCashflowByID", w, r)

	params := mux.Vars(r)
	id := params["id"]

	db, err := api.DbConnect()
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret := api.JsonSimfinCashflow{}
	err = db.Get(&ret, api.JsonToSelect(ret, "simfin_cashflow", "")+fmt.Sprintf(" WHERE id=%s", id))
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	api.Exit("SimfinCashflowByID", ret)
}

type SimfinCashflowInput struct {
	Ticker string `schema:"ticker"`
}

func SimfinCashflowByTicker(w http.ResponseWriter, r *http.Request) {
	api.Enter("SimfinCashflowByTicker", w, r)
	w.Header().Set("Access-Control-Allow-Origin", "*")

	args := new(SimfinCashflowInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		api.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	db, err := api.DbConnect()
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret := []api.JsonSimfinCashflow{}
	err = db.Select(&ret, api.JsonToSelect(api.JsonSimfinCashflow{}, "simfin_cashflow", "")+fmt.Sprintf(" WHERE ticker='%s' ORDER BY fiscal_year DESC", args.Ticker))
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	api.Exit("SimfinCashflowByTicker", ret)
}

type CashflowInput struct {
	Ticker string `schema:"ticker"`
}

func CashflowByTicker(w http.ResponseWriter, r *http.Request) {
	api.Enter("CashflowByTicker", w, r)
	w.Header().Set("Access-Control-Allow-Origin", "*")

	args := new(CashflowInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		api.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	var simfin []api.JsonSimfinCashflow
	err = api.SimfinCashflowByTicker(args.Ticker, &simfin)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	ret := []api.JsonCashflow{}
	for s := range simfin {
		i := api.JsonCashflow{JsonSimfinCashflow: simfin[s]}
		if i.SharesBasic > 0.0 {
			i.DPS = -1.0 * api.Round(float64(i.DividendsPaid)/float64(i.SharesBasic), 0.01)
		}
		ret = append(ret, i)
	}
	json.NewEncoder(w).Encode(&ret)

	api.Exit("CashflowByTicker", ret)
}

func SummaryByTicker(w http.ResponseWriter, r *http.Request) {
	api.Enter("Read-SummaryByTicker", w, r)
	w.Header().Set("Access-Control-Allow-Origin", "*")

	args := new(api.RestTickerInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		api.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	var income []api.JsonIncome
	err = api.IncomeByTicker(args.Ticker, &income)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	var balance []api.JsonBalance
	err = api.BalanceByTicker(args.Ticker, &balance)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	var cashflow []api.JsonCashflow
	err = api.CashflowByTicker(args.Ticker, &cashflow)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	ret := []api.JsonSummary{}
	for i := range income {
		var mdhYearSummary api.JsonMDHYearSummary
		err = api.MDHYearSummaryBySymbol(args.Ticker, income[i].ReportDate, &mdhYearSummary)
		if err != nil {
			log.Println(err)
			// continue on, it's ok if we can't get historical market data
		}
		s := api.JsonSummary{}
		ebitda := income[i].NetIncome - income[i].InterestExpNet - income[i].IncomeTax - income[i].DeprAmor // Expenses are stored as negative
		s.ReportDate = income[i].ReportDate
		s.EPS = income[i].EPS
		s.SharesDiluted = income[i].SharesDiluted
		s.MarketCap = int64(mdhYearSummary.Close * float64(s.SharesDiluted))
		if income[i].Revenue > 0.0 {
			s.GrMgn = float64(income[i].GrossProfit) / float64(income[i].Revenue)
			s.OpMgn = float64(income[i].OperatingIncome) / float64(income[i].Revenue)
			s.NetMgn = float64(income[i].NetIncome) / float64(income[i].Revenue)
		}
		if s.EPS != 0.0 {
			s.PEHigh = int(math.Round(mdhYearSummary.High / s.EPS))
			s.PELow = int(math.Round(mdhYearSummary.Low / s.EPS))
		}
		if income[i].InterestExpNet < 0.0 {
			s.IntCov = float64(income[i].OperatingIncome) / float64(income[i].InterestExpNet*-1)
		}
		if i < len(balance) && ebitda > 0.0 {
			s.LTDRatio = float64(balance[i].LtDebt) / float64(ebitda)
		}
		if i < len(cashflow) {
			s.DPS = cashflow[i].DPS
		}
		// For ROE and ROA, need to make sure we're not on the last year
		if i < len(income)-1 && i < len(balance)-1 {
			if balance[i].TotalEquity+balance[i+1].TotalEquity > 0.0 {
				s.ROE = float64(income[i].NetIncome) / (float64(balance[i].TotalEquity+balance[i+1].TotalEquity) / 2.0)
			}
			if balance[i].TotalAssets+balance[i+1].TotalAssets > 0.0 {
				s.ROA = float64(income[i].NetIncome) / (float64(balance[i].TotalAssets+balance[i+1].TotalAssets) / 2.0)
			}
		}
		ret = append(ret, s)
	}
	json.NewEncoder(w).Encode(&ret)

	api.Exit("Read-SummaryByTicker", ret)
}

func FactorsByTicker(w http.ResponseWriter, r *http.Request) {
	api.Enter("Read-FactorsByTicker", w, r)
	w.Header().Set("Access-Control-Allow-Origin", "*")

	args := new(api.RestTickerInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		api.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	var income []api.JsonIncome
	err = api.IncomeByTicker(args.Ticker, &income)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	ret := []api.JsonFactors{}

	for i := range income {
		f := api.JsonFactors{}
		f.ReportDate = income[i].ReportDate
		f.Revenue = income[i].Revenue
		f.SharesDiluted = income[i].SharesDiluted
		f.EPS = income[i].EPS
		if i > 0 {
			f.RevenueGrowth = api.Round(float64(income[0].Revenue)/float64(f.Revenue), 0.0001) - 1.0
			f.RevenueCagr = math.Pow(f.RevenueGrowth+1.0, 1.0/float64(i)) - 1.0
			f.SharesDilutedGrowth = api.Round(float64(f.SharesDiluted)/float64(income[0].SharesDiluted), 0.0001) - 1.0
			f.SharesDilutedCagr = math.Pow(f.SharesDilutedGrowth+1.0, 1.0/float64(i)) - 1.0
			f.EPSGrowth = api.Round(float64(income[0].EPS)/float64(f.EPS), 0.0001) - 1.0
			f.EPSCagr = math.Pow(f.EPSGrowth+1.0, 1.0/float64(i)) - 1.0
		}
		ret = append(ret, f)
	}
	json.NewEncoder(w).Encode(&ret)

	api.Exit("Read-FactorsByTicker", ret)
}

func Mergers(w http.ResponseWriter, r *http.Request) {
	foo := api.JsonMerger{}
	ret := []api.JsonMerger{}
	RestHandleGet(w, r, "Read-Mergers", &ret, foo, "mergers")
}

func Projections(w http.ResponseWriter, r *http.Request) {
	foo := api.JsonProjections{}
	ret := []api.JsonProjections{}
	RestHandleGet(w, r, "Read-Projections", &ret, foo, "projections")
}

func Portfolios(w http.ResponseWriter, r *http.Request) {
	foo := api.JsonPortfolio{}
	ret := []api.JsonPortfolio{}
	RestHandleGet(w, r, "Read-Portfolios", &ret, foo, "portfolios WHERE active=true ORDER BY id ASC")
}

func Transactions(w http.ResponseWriter, r *http.Request) {
	foo := api.JsonTransaction{}
	ret := []api.JsonTransaction{}
	RestHandleGet(w, r, "Read-Transactions", &ret, foo, "transactions ORDER BY date DESC, id DESC LIMIT 50")
}

func TransactionsByPositionID(w http.ResponseWriter, r *http.Request) {
	api.Enter("Read-TransactionsByPositionID", w, r)

	args := new(api.RestPositionIDInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		api.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	ret := []api.JsonTransaction{}
	err = api.DbSelect(&ret, api.JsonToSelect(api.JsonTransaction{}, fmt.Sprintf("transactions WHERE position_id=%d ORDER BY date DESC, id DESC", args.PositionID), ""))
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	api.Exit("Read-TransactionsByPositionID", ret)
}

func TransactionsByPortfolioID(w http.ResponseWriter, r *http.Request) {
	api.Enter("Read-TransactionsByPortfolioID", w, r)

	args := new(api.RestPortfolioIDInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		api.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	ret := []api.JsonTransaction{}
	err = api.DbSelect(&ret, api.JsonToSelect(api.JsonTransaction{}, fmt.Sprintf("transactions WHERE portfolio_id=%d and position_id=0 ORDER BY date DESC, id DESC", args.PortfolioID), ""))
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	api.Exit("Read-TransactionsByPortfolioID", ret)
}

func PortfoliosByID(w http.ResponseWriter, r *http.Request) {
	var ret api.JsonPortfolio
	RestHandleGetByID(w, r, "Read-PortfoliosByID", &ret, ret, "portfolios")
}

func EnrichPortfolio(p api.JsonPortfolio) (api.JsonEnrichedPortfolio, error) {
	ep := api.JsonEnrichedPortfolio{JsonPortfolio: p}

	var totalPortfolio api.JsonPortfolio
	err := api.PortfoliosByID(api.CONST_PORTFOLIO_TOTAL, &totalPortfolio)
	if err != nil {
		return ep, err
	}
	if totalPortfolio.Value > 0 {
		ep.PercentTotal = api.Round(p.Value/totalPortfolio.Value, 0.0001)
	}
	if p.Value > 0 {
		ep.PercentCash = api.Round(p.Cash/p.Value, 0.0001)
		ep.PercentDebt = api.Round(p.Debt/p.Value, 0.0001)
	}

	return ep, nil
}

func EnrichedPortfolios(w http.ResponseWriter, r *http.Request) {
	api.Enter("Read-EnrichedPortfolios", w, r)

	var portfolios []api.JsonPortfolio
	err := api.Portfolios(&portfolios)
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret := []api.JsonEnrichedPortfolio{}
	for i := range portfolios {
		ep, err := EnrichPortfolio(portfolios[i])
		if err != nil {
			api.ErrorHttp(err, w, http.StatusInternalServerError)
			return
		}

		ret = append(ret, ep)
	}
	json.NewEncoder(w).Encode(&ret)

	api.Exit("Read-EnrichedPortfolios", ret)
}

func EnrichedPortfoliosByID(w http.ResponseWriter, r *http.Request) {
	api.Enter("Read-EnrichedPortfoliosByID", w, r)

	params := mux.Vars(r)
	id, err := strconv.Atoi(params["id"])
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	var portfolio api.JsonPortfolio
	err = api.PortfoliosByID(id, &portfolio)
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret, err := EnrichPortfolio(portfolio)
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	api.Exit("Read-EnrichedPortfoliosByID", ret)
}

func PortfoliosHistoryByDate(w http.ResponseWriter, r *http.Request) {
	api.Enter("Read-PortfoliosHistoryByDate", w, r)

	args := new(api.RestDateInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		api.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	db, err := api.DbConnect()
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret := []api.JsonPortfolioHistory{}
	err = db.Select(&ret, api.JsonToSelect(api.JsonPortfolioHistory{},
		fmt.Sprintf("portfolios_history WHERE date=(select max(date) from portfolios_history where date <= '%s' and value > 0)", args.Date),
		""))
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	sort.Slice(ret, func(i, j int) bool {
		return ret[i].PortfolioID < ret[j].PortfolioID
	})
	json.NewEncoder(w).Encode(&ret)

	api.Exit("Read-PortfoliosHistoryByDate", ret)
}

func PortfoliosHistoryByPortfolioIDDate(w http.ResponseWriter, r *http.Request) {
	api.Enter("Read-PortfoliosHistoryByPortfolioIDDate", w, r)

	args := new(api.RestPortfolioIDDateInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		api.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	db, err := api.DbConnect()
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret := api.JsonPortfolioHistory{}
	err = db.Get(&ret, api.JsonToSelect(api.JsonPortfolioHistory{},
		fmt.Sprintf("portfolios_history WHERE portfolio_id=%d and date=(select max(date) from portfolios_history where date <= '%s' and value > 0)", args.PortfolioID, args.Date),
		""))
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	api.Exit("Read-PortfoliosHistoryByPortfolioIDDate", ret)
}

func PortfoliosHistoryMaxIndexByPortfolioID(w http.ResponseWriter, r *http.Request) {
	api.Enter("Read-PortfoliosHistoryMaxIndexByPortfolioID", w, r)

	args := new(api.RestPortfolioIDInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		api.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	db, err := api.DbConnect()
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret := api.JsonPortfolioHistory{}
	err = db.Get(&ret, api.JsonToSelect(api.JsonPortfolioHistory{},
		fmt.Sprintf("portfolios_history WHERE portfolio_id=%d and index=(select max(index) from portfolios_history where portfolio_id=%d)", args.PortfolioID, args.PortfolioID),
		""))
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	api.Exit("Read-PortfoliosHistoryMaxIndexByPortfolioID", ret)
}

func PositionsHistoryFirst(w http.ResponseWriter, r *http.Request) {
	api.Enter("Read-PositionsHistoryFirst", w, r)

	args := new(api.RestPositionIDInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		api.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	ret := api.JsonPositionHistory{}
	err = api.DbGet(&ret, api.JsonToSelect(api.JsonPositionHistory{}, fmt.Sprintf("positions_history WHERE position_id=%d and date=(select min(date) from positions_history where position_id=%d)", args.PositionID, args.PositionID), ""))
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	api.Exit("Read-PositionsHistoryFirst", ret)
}

func PositionsHistoryByPortfolioIDDate(w http.ResponseWriter, r *http.Request) {
	api.Enter("Read-PositionsHistoryByPortfolioIDDate", w, r)

	args := new(api.RestPortfolioIDDateInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		api.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	db, err := api.DbConnect()
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret := []api.JsonPositionHistory{}
	err = db.Select(&ret, api.JsonToSelect(api.JsonPositionHistory{},
		fmt.Sprintf("positions_history WHERE portfolio_id=%d and date=(select max(date) from positions_history where date <= '%s')", args.PortfolioID, args.Date),
		""))
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	api.Exit("Read-PositionsHistoryByPortfolioIDDate", ret)
}

func PositionsHistoryByPositionIDDate(w http.ResponseWriter, r *http.Request) {
	api.Enter("Read-PositionsHistoryByPositionIDDate", w, r)

	args := new(api.RestPositionIDDateInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		api.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	db, err := api.DbConnect()
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret := api.JsonPositionHistory{}
	err = db.Get(&ret, api.JsonToSelect(api.JsonPositionHistory{},
		fmt.Sprintf("positions_history WHERE position_id=%d and date=(select max(date) from positions_history where date <= '%s')", args.PositionID, args.Date),
		""))
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	api.Exit("Read-PositionsHistoryByPositionIDDate", ret)
}

func PortfoliosHistoryMaxDate(w http.ResponseWriter, r *http.Request) {
	api.Enter("Read-PortfoliosHistoryMaxDate", w, r)

	db, err := api.DbConnect()
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret := api.RestStringOutput{}
	err = db.Get(&ret, "select max(date) as value from portfolios_history")
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	api.Exit("Read-PortfoliosHistoryMaxDate", ret)
}

func Positions(w http.ResponseWriter, r *http.Request) {
	foo := api.JsonPosition{}
	ret := []api.JsonPosition{}
	RestHandleGet(w, r, "Read-Positions", &ret, foo, "positions WHERE active=true ORDER BY portfolio_id,id ASC")
}

func PositionsBySymbolPortfolioID(w http.ResponseWriter, r *http.Request) {
	api.Enter("Read-PositionsBySymbolPortfolioID", w, r)

	args := new(api.RestSymbolPortfolioIDInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		api.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	var refData api.JsonRefData
	err = api.RefDataBySymbol(args.Symbol, &refData)
	if err != nil {
		api.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	db, err := api.DbConnect()
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret := api.JsonPosition{}
	err = db.Get(&ret, api.JsonToSelect(api.JsonPosition{}, fmt.Sprintf("positions WHERE ref_data_id=%d and portfolio_id=%d", refData.ID, args.PortfolioID), ""))
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	api.Exit("Read-PositionsBySymbolPortfolioID", ret)
}

func CalculateReturn(table string, idCol string, id int, index float64, date string, interval string, years float64) float64 {
	var start float64
	var ret float64
	query := "select index from %s where %s=%d and date=" +
		"(select max(date) from %s where %s=%d and date <= (select date('%s') - interval '%s'))"
	// If the value is not present, leave the return zero, don't handle error
	_ = api.DbGet(&start, fmt.Sprintf(query, table, idCol, id, table, idCol, id, date, interval))
	if start > 0 {
		ret = api.Round(math.Pow(index/start, 1/years)-1, 0.0001)
		log.Printf("CalculateReturn: start: %f, ret: %f", start, ret)
	}
	return ret
}

func EnrichReturns(table string, idCol string, id int, name string, value float64, index float64, tci float64, divs float64, date string) api.JsonReturns {
	r := api.JsonReturns{}
	r.ID = id
	r.Name = name
	r.OneDay = CalculateReturn(table, idCol, id, index, date, "1 day", 1)
	r.OneWeek = CalculateReturn(table, idCol, id, index, date, "1 week", 1)
	r.OneMonth = CalculateReturn(table, idCol, id, index, date, "1 month", 1)
	r.ThreeMonths = CalculateReturn(table, idCol, id, index, date, "3 months", 1)
	r.OneYear = CalculateReturn(table, idCol, id, index, date, "1 year", 1)
	r.FiveYears = CalculateReturn(table, idCol, id, index, date, "5 years", 5)
	r.TenYears = CalculateReturn(table, idCol, id, index, date, "10 years", 10)
	r.ProfitLifetime = value - tci + divs
	return r
}

func EnrichYTDPortfolioReturns(r *api.JsonReturns, value float64, index float64, tci float64, date string) error {
	yearStartDate := ""
	dateParsed, err := time.Parse("2006-01-02", date)
	if err != nil {
		return err
	}
	if dateParsed.Month() == 1 && dateParsed.Day() == 1 {
		yearStartDate = fmt.Sprintf("%d-%02d-%02d", dateParsed.Year()-1, dateParsed.Month(), dateParsed.Day())
	} else {
		yearStartDate = fmt.Sprintf("%d-%02d-%02d", dateParsed.Year(), 1, 1)
	}

	physd := api.JsonPortfolioHistory{}
	err = api.PortfoliosHistoryPortfolioIDDate(r.ID, yearStartDate, &physd)
	if err != nil {
		// If there was no position on the first of the year, that's ok, returns are just zero
		return nil
	}

	if physd.Index > 0 {
		r.YearToDate = api.Round(index/physd.Index-1, 0.0001)
	}
	r.ProfitYearToDate = value - physd.Value - (tci - physd.TotalCashInfusion)
	return nil
}

func EnrichYTDPositionReturns(r *api.JsonReturns, value float64, index float64, tci float64, divs float64, date string) error {
	yearStartDate := ""
	dateParsed, err := time.Parse("2006-01-02", date)
	if err != nil {
		return err
	}
	if dateParsed.Month() == 1 && dateParsed.Day() == 1 {
		yearStartDate = fmt.Sprintf("%d-%02d-%02d", dateParsed.Year()-1, dateParsed.Month(), dateParsed.Day())
	} else {
		yearStartDate = fmt.Sprintf("%d-%02d-%02d", dateParsed.Year(), 1, 1)
	}

	physd := api.JsonPositionHistory{}
	err = api.PositionsHistoryByPositionIDDate(r.ID, yearStartDate, &physd)
	if err != nil {
		// If there was no position on the first of the year, that's ok, returns are just zero
		return nil
	}

	if physd.Index > 0 {
		r.YearToDate = api.Round(index/physd.Index-1, 0.0001)
	}
	r.ProfitYearToDate = value - physd.Value - (tci - physd.TotalCashInfusion) + (divs - physd.AccumulatedDividends)
	return nil
}

func PortfolioReturnsByID(w http.ResponseWriter, r *http.Request) {
	api.Enter("Read-PortfolioReturnsByID", w, r)

	params := mux.Vars(r)
	id, err := strconv.Atoi(params["id"])
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	var portfolio api.JsonPortfolio
	err = api.PortfoliosByID(id, &portfolio)
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret := EnrichReturns("portfolios_history", "portfolio_id", portfolio.ID, portfolio.Name, portfolio.Value, portfolio.Index, portfolio.TotalCashInfusion,
		0, time.Now().Format("2006-01-02"))
	err = EnrichYTDPortfolioReturns(&ret, portfolio.Value, portfolio.Index, portfolio.TotalCashInfusion, time.Now().Format("2006-01-02"))
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)
	api.Exit("Read-PortfolioReturnsByID", ret)
}

func PortfolioReturns(w http.ResponseWriter, r *http.Request) {
	api.Enter("Read-PortfolioReturns", w, r)

	var portfolios []api.JsonPortfolio
	err := api.Portfolios(&portfolios)
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret := []api.JsonReturns{}
	for i := range portfolios {
		pr := EnrichReturns("portfolios_history", "portfolio_id", portfolios[i].ID, portfolios[i].Name, portfolios[i].Value, portfolios[i].Index,
			portfolios[i].TotalCashInfusion, 0, time.Now().Format("2006-01-02"))
		err = EnrichYTDPortfolioReturns(&pr, portfolios[i].Value, portfolios[i].Index, portfolios[i].TotalCashInfusion, time.Now().Format("2006-01-02"))
		if err != nil {
			api.ErrorHttp(err, w, http.StatusInternalServerError)
			return
		}

		ret = append(ret, pr)
	}
	json.NewEncoder(w).Encode(&ret)

	api.Exit("Read-PortfolioReturns", ret)
}

func PortfolioReturnsByDate(w http.ResponseWriter, r *http.Request) {
	api.Enter("Read-PortfolioReturnsByDate", w, r)

	args := new(api.RestDateInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		api.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	var portfoliosHistory []api.JsonPortfolioHistory
	err = api.PortfoliosHistoryByDate(args.Date, &portfoliosHistory)
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret := []api.JsonReturns{}
	for i := range portfoliosHistory {
		portfolio := portfoliosHistory[i].JsonPortfolio
		portfolio.ID = portfoliosHistory[i].PortfolioID
		pr := EnrichReturns("portfolios_history", "portfolio_id", portfolio.ID, portfolio.Name, portfolio.Value, portfolio.Index, portfolio.TotalCashInfusion,
			0, args.Date)
		err = EnrichYTDPortfolioReturns(&pr, portfolio.Value, portfolio.Index, portfolio.TotalCashInfusion, args.Date)
		if err != nil {
			api.ErrorHttp(err, w, http.StatusInternalServerError)
			return
		}

		ret = append(ret, pr)
	}
	json.NewEncoder(w).Encode(&ret)

	api.Exit("Read-PortfolioReturnsByDate", ret)
}

func PositionReturnsByID(w http.ResponseWriter, r *http.Request) {
	api.Enter("Read-PositionReturnsByID", w, r)

	params := mux.Vars(r)
	id, err := strconv.Atoi(params["id"])
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	var ep api.JsonEnrichedPosition
	err = api.EnrichedPositionsByID(id, &ep)
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret := EnrichReturns("positions_history", "position_id", ep.ID, ep.Symbol, ep.Value, ep.Index, ep.TotalCashInfusion, ep.AccumulatedDividends, time.Now().Format("2006-01-02"))
	err = EnrichYTDPositionReturns(&ret, ep.Value, ep.Index, ep.TotalCashInfusion, ep.AccumulatedDividends, time.Now().Format("2006-01-02"))
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(&ret)
	api.Exit("Read-PositionReturnsByID", ret)
}

func EnrichPosition(p api.JsonPosition) (api.JsonEnrichedPosition, error) {
	ep := api.JsonEnrichedPosition{JsonPosition: p}

	var refData api.JsonRefData
	err := api.RefDataByID(ep.RefDataID, &refData)
	if err != nil {
		return ep, err
	}
	ep.Symbol = refData.Symbol
	ep.Description = refData.Description

	var portfolio api.JsonPortfolio
	err = api.PortfoliosByID(p.PortfolioID, &portfolio)
	if err != nil {
		return ep, err
	}
	if ep.Active && portfolio.Value > 0 {
		ep.PercentPortfolio = api.Round(p.Value/portfolio.Value, 0.0001)
	}

	return ep, nil
}

func EnrichedPositionsAll(w http.ResponseWriter, r *http.Request) {
	api.Enter("Read-EnrichedPositionsAll", w, r)

	var positions []api.JsonPosition
	err := api.DbSelect(&positions, api.JsonToSelect(api.JsonPosition{}, "positions ORDER BY id ASC", ""))
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret := []api.JsonEnrichedPosition{}
	for i := range positions {
		ep, err := EnrichPosition(positions[i])
		if err != nil {
			api.ErrorHttp(err, w, http.StatusInternalServerError)
			return
		}

		ret = append(ret, ep)
	}
	json.NewEncoder(w).Encode(&ret)

	api.Exit("Read-EnrichedPositionsAll", ret)
}

type EnrichedPositionsByPortfolioIDInput struct {
	PortfolioID int `schema:"portfolioId"`
}

func EnrichedPositionsByPortfolioID(w http.ResponseWriter, r *http.Request) {
	EnrichedPositionsByPortfolioIDInternal(w, r, "AND active=True")
}

func EnrichedPositionsAllByPortfolioID(w http.ResponseWriter, r *http.Request) {
	EnrichedPositionsByPortfolioIDInternal(w, r, "")
}

func EnrichedPositionsByPortfolioIDInternal(w http.ResponseWriter, r *http.Request, filter string) {
	api.Enter("Read-EnrichedPositionsByPortfolioID", w, r)
	args := new(EnrichedPositionsByPortfolioIDInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		api.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	db, err := api.DbConnect()
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	positions := []api.JsonPosition{}
	err = db.Select(&positions, api.JsonToSelect(api.JsonPosition{}, "positions", "")+fmt.Sprintf(" WHERE portfolio_id=%d %s ORDER BY id DESC", args.PortfolioID, filter))
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
	ret := []api.JsonEnrichedPosition{}
	for i := range positions {
		ep, err := EnrichPosition(positions[i])
		if err != nil {
			api.ErrorHttp(err, w, http.StatusInternalServerError)
			return
		}

		ret = append(ret, ep)
	}

	sort.Slice(ret, func(i, j int) bool {
		return ret[i].Value > ret[j].Value
	})

	json.NewEncoder(w).Encode(&ret)
	api.Exit("Read-EnrichedPositionsByPortfolioID", ret)
}

func EnrichedPositionsBySymbolPortfolioID(w http.ResponseWriter, r *http.Request) {
	api.Enter("Read-EnrichedPositionsBySymbolPortfolioID", w, r)

	args := new(api.RestSymbolPortfolioIDInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		api.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	var p api.JsonPosition
	err = api.PositionsBySymbolPortfolioID(args.Symbol, args.PortfolioID, &p)
	if err != nil {
		api.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}
	ret, err := EnrichPosition(p)
	if err != nil {
		api.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	api.Exit("Read-EnrichedPositionsBySymbolPortfolioID", ret)
}

func EnrichedPositionsHistoryByPortfolioIDDate(w http.ResponseWriter, r *http.Request) {
	api.Enter("Read-EnrichedPositionsHistoryByPortfolioIDDate", w, r)

	args := new(api.RestPortfolioIDDateInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		api.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	positions := []api.JsonPositionHistory{}
	err = api.PositionsHistoryByPortfolioIDDate(args.PortfolioID, args.Date, &positions)
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
	ret := []api.JsonEnrichedPositionHistory{}
	for i := range positions {
		ep, err := EnrichPosition(positions[i].JsonPosition)
		if err != nil {
			api.ErrorHttp(err, w, http.StatusInternalServerError)
			return
		}
		eph := api.JsonEnrichedPositionHistory{JsonEnrichedPosition: ep}
		eph.Date = positions[i].Date
		eph.PortfolioID = positions[i].PortfolioID
		eph.PositionID = positions[i].PositionID
		ret = append(ret, eph)
	}

	sort.Slice(ret, func(i, j int) bool {
		return ret[i].Value > ret[j].Value
	})

	json.NewEncoder(w).Encode(&ret)
	api.Exit("Read-EnrichedPositionsHistoryByPortfolioIDDate", ret)
}

func EnrichMerger(m api.JsonMerger) (api.JsonEnrichedMerger, error) {
	em := api.JsonEnrichedMerger{JsonMerger: m}
	var acquirer api.JsonRefData
	err := api.RefDataByID(em.AcquirerRefDataID, &acquirer)
	if err != nil {
		return em, err
	}
	var target api.JsonRefData
	err = api.RefDataByID(em.TargetRefDataID, &target)
	if err != nil {
		return em, err
	}
	em.AcquirerTicker = acquirer.Symbol
	em.AcquirerDescription = acquirer.Description
	em.TargetTicker = target.Symbol
	em.TargetDescription = target.Description

	var md api.JsonMarketData
	err = api.MarketDataBySymbol(target.Symbol, &md)
	if err != nil {
		return em, err
	}
	em.Price = md.Last

	closeTime := api.DateStringToTime(em.CloseDate)
	strikeTime := api.DateStringToTime(em.AnnounceDate)
	daysToClose := closeTime.Sub(time.Now()).Hours() / 24
	fees := 0.005
	if strings.Contains(em.TargetTicker, ".HK") {
		fees = (0.0008 + 0.0013) * md.Last // 8 bps commision and 13 bps stamp on each side
	}
	if em.BreakPrice > 0 {
		em.Status = "B"
		if em.StrikePrice > 0 {
			em.StrikeReturn = api.Round((em.BreakPrice+em.Cash-fees)/em.StrikePrice-1, 0.0001)
			daysFromStrike := closeTime.Sub(strikeTime).Hours() / 24
			em.StrikeReturnAnnualized = api.Round((365/daysFromStrike)*em.StrikeReturn, 0.0001)
		}
	} else if daysToClose < 0 {
		em.Status = "C"
		if em.StrikePrice > 0 {
			em.StrikeReturn = api.Round((em.DealPrice+em.Cash-fees)/em.StrikePrice-1, 0.0001)
			daysFromStrike := closeTime.Sub(strikeTime).Hours() / 24
			em.StrikeReturnAnnualized = api.Round((365/daysFromStrike)*em.StrikeReturn, 0.0001)
		}
	} else {
		em.Status = "O"
		if em.StrikePrice > 0 {
			em.StrikeReturn = api.Round((md.Last+em.Cash-fees)/em.StrikePrice-1, 0.0001)
			daysFromStrike := time.Now().Sub(strikeTime).Hours() / 24
			em.StrikeReturnAnnualized = api.Round((365/daysFromStrike)*em.StrikeReturn, 0.0001)
		}
		em.MarketPositiveReturn = api.Round((em.DealPrice+em.Dividends-fees)/md.Last-1, 0.0001)
		em.MarketNetReturn = api.Round(
			((em.DealPrice+em.Dividends-fees-md.Last)*em.Confidence-(md.Last-em.FailPrice-em.Dividends+2*fees)*(1-em.Confidence))/md.Last, 0.0001)
		annualizeMultiple := 365 / daysToClose
		em.MarketPositiveReturnAnnualized = api.Round(annualizeMultiple*em.MarketPositiveReturn, 0.0001)
		em.MarketNetReturnAnnualized = api.Round(annualizeMultiple*em.MarketNetReturn, 0.0001)
	}

	var position api.JsonEnrichedPosition
	err = api.EnrichedPositionsBySymbolPortfolioID(em.TargetTicker, api.CONST_PORTFOLIO_RISK_ARB, &position)
	// Don't pass the error up, it's ok if this isn't a position, we just populate zero
	if err == nil {
		em.PercentPortfolio = position.PercentPortfolio
		em.PositionReturn = api.Round(position.Index/100.0-1, 0.0001)
	}

	var returns api.JsonReturns
	err = api.PositionReturnsByID(position.ID, &returns)
	// Don't pass the error up, it's ok if this isn't a position, we just populate zero
	if err == nil {
		em.ProfitLifetime = returns.ProfitLifetime
	}

	return em, nil
}

func EnrichedMergersPositions(w http.ResponseWriter, r *http.Request) {
	api.Enter("Read-EnrichedMergersPositions", w, r)
	w.Header().Set("Access-Control-Allow-Origin", "*")

	var mergers []api.JsonMerger
	err := api.Mergers(&mergers)
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret := []api.JsonEnrichedMerger{}
	for i := range mergers {
		em, err := EnrichMerger(mergers[i])
		if err != nil {
			api.ErrorHttp(err, w, http.StatusInternalServerError)
			return
		}

		if em.PercentPortfolio > 0 {
			ret = append(ret, em)
		}
	}
	sort.Slice(ret, func(i, j int) bool {
		return ret[i].PercentPortfolio > ret[j].PercentPortfolio
	})
	json.NewEncoder(w).Encode(&ret)

	api.Exit("Read-EnrichedMergersPositions", ret)
}

func EnrichedMergersPositionsTotal(w http.ResponseWriter, r *http.Request) {
	api.Enter("Read-EnrichedMergersPositionsTotal", w, r)
	w.Header().Set("Access-Control-Allow-Origin", "*")

	var mergers []api.JsonEnrichedMerger
	err := api.EnrichedMergersPositions(&mergers)
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	total := api.JsonEnrichedMerger{}
	total.TargetTicker = "Total"
	ret := []api.JsonEnrichedMerger{}
	for i := range mergers {
		total.PercentPortfolio += mergers[i].PercentPortfolio
		total.MarketNetReturn += mergers[i].MarketNetReturn * mergers[i].PercentPortfolio
		total.MarketNetReturnAnnualized += mergers[i].MarketNetReturnAnnualized * mergers[i].PercentPortfolio
		total.MarketPositiveReturn += mergers[i].MarketPositiveReturn * mergers[i].PercentPortfolio
		total.MarketPositiveReturnAnnualized += mergers[i].MarketPositiveReturnAnnualized * mergers[i].PercentPortfolio
	}
	ret = append(ret, total)
	json.NewEncoder(w).Encode(&ret)

	api.Exit("Read-EnrichedMergersPositionsTotal", ret)
}

func EnrichedMergersResearch(w http.ResponseWriter, r *http.Request) {
	api.Enter("Read-EnrichedMergersResearch", w, r)
	w.Header().Set("Access-Control-Allow-Origin", "*")

	var mergers []api.JsonMerger
	err := api.Mergers(&mergers)
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret := []api.JsonEnrichedMerger{}
	for i := range mergers {
		em, err := EnrichMerger(mergers[i])
		if err != nil {
			api.ErrorHttp(err, w, http.StatusInternalServerError)
			return
		}

		if em.PercentPortfolio <= 0 {
			ret = append(ret, em)
		}
	}

	sort.Slice(ret, func(i, j int) bool {
		if api.MergerStatusToInt(ret[i].Status) == api.MergerStatusToInt(ret[j].Status) && ret[i].MarketNetReturnAnnualized == ret[j].MarketNetReturnAnnualized {
			return ret[i].ID < ret[j].ID
		}
		if api.MergerStatusToInt(ret[i].Status) == api.MergerStatusToInt(ret[j].Status) {
			return ret[i].MarketNetReturnAnnualized > ret[j].MarketNetReturnAnnualized
		}
		return api.MergerStatusToInt(ret[i].Status) > api.MergerStatusToInt(ret[j].Status)
	})
	json.NewEncoder(w).Encode(&ret)

	api.Exit("Read-EnrichedMergersResearch", ret)
}

func EnrichedMergersByID(w http.ResponseWriter, r *http.Request) {
	api.Enter("Read-EnrichedMergersByID", w, r)

	params := mux.Vars(r)
	id := params["id"]

	db, err := api.DbConnect()
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	m := api.JsonMerger{}
	err = db.Get(&m, api.JsonToSelect(m, "mergers", "")+fmt.Sprintf(" WHERE id=%s", id))
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	em, err := EnrichMerger(m)
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(&em)

	api.Exit("Read-EnrichedMergersByID", em)
}

func EnrichedPositionsByID(w http.ResponseWriter, r *http.Request) {
	api.Enter("Read-EnrichedPositionsByID", w, r)

	params := mux.Vars(r)
	id, err := strconv.Atoi(params["id"])
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	p := api.JsonPosition{}
	err = api.PositionsByID(id, &p)
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
	ep, err := EnrichPosition(p)
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(&ep)

	api.Exit("Read-EnrichedPositionsByID", ep)
}

type EnrichedMergersJournalByMergerIDInput struct {
	MergerID int `schema:"mergerId"`
}

func EnrichedMergersJournalByMergerID(w http.ResponseWriter, r *http.Request) {
	api.Enter("Read-EnrichedMergersJournalByMergerID", w, r)

	args := new(EnrichedMergersJournalByMergerIDInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		api.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	db, err := api.DbConnect()
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret := []api.JsonEnrichedMergerJournal{}
	err = db.Select(&ret, api.JsonToSelect(api.JsonEnrichedMergerJournal{}, "mergers_journal", "")+fmt.Sprintf(" WHERE mergers_id=%d ORDER BY id DESC", args.MergerID))
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	api.Exit("Read-EnrichedMergersJournalByMergerID", ret)
}

func main() {
	log.Println("Listening on http://localhost:8081/blue-lion/read")
	router := mux.NewRouter().StrictSlash(true)
	setupRouter(router)
	log.Fatal(http.ListenAndServe(":8081", router))
}
