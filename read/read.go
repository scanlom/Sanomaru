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
	"github.com/scanlom/Sanomaru/cmn"
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
	router.HandleFunc("/blue-lion/read/positions/{id}", PositionsByID).Methods("GET")
	router.HandleFunc("/blue-lion/read/positions", PositionsBySymbolPortfolioID).Queries("symbol", "", "portfolioId", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/positions", Positions).Methods("GET")
	router.HandleFunc("/blue-lion/read/positions-history", PositionsHistoryByPortfolioIDDate).Queries("portfolioId", "", "date", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/enriched-positions/{id}", EnrichedPositionsByID).Methods("GET")
	router.HandleFunc("/blue-lion/read/enriched-positions", EnrichedPositionsBySymbolPortfolioID).Queries("symbol", "", "portfolioId", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/enriched-positions", EnrichedPositionsByPortfolioID).Queries("portfolioId", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/enriched-positions-all", EnrichedPositionsAllByPortfolioID).Queries("portfolioId", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/enriched-positions-history", EnrichedPositionsHistoryByPortfolioIDDate).Queries("portfolioId", "", "date", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/portfolio-returns/{id}", PortfolioReturnsByID).Methods("GET")
	router.HandleFunc("/blue-lion/read/portfolio-returns", PortfolioReturnsByDate).Queries("date", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/portfolio-returns", PortfolioReturns).Methods("GET")
	router.HandleFunc("/blue-lion/read/transactions", TransactionsByPositionID).Methods("GET").Queries("positionId", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/transactions", Transactions).Methods("GET")
	router.Methods("GET").Path("/blue-lion/read/scalar").HandlerFunc(Scalar)
}

func RestHandleGet(w http.ResponseWriter, r *http.Request, msg string, ptr interface{}, obj interface{}, table string) {
	cmn.Enter(msg, w, r)

	db, err := cmn.DbConnect()
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	err = db.Select(ptr, api.JsonToSelect(obj, table, ""))
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(ptr)

	cmn.Exit(msg, ptr)
}

func RestHandleGetByID(w http.ResponseWriter, r *http.Request, msg string, ptr interface{}, obj interface{}, table string) {
	cmn.Enter(msg, w, r)

	params := mux.Vars(r)
	id := params["id"]

	db, err := cmn.DbConnect()
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	err = db.Get(ptr, fmt.Sprintf("%s WHERE id=%s", api.JsonToSelect(obj, table, ""), id))
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(ptr)

	cmn.Exit(msg, ptr)
}

func RestHandleGetBySymbol(w http.ResponseWriter, r *http.Request, msg string, ptr interface{}, obj interface{}, table string) {
	cmn.Enter(msg, w, r)

	args := new(cmn.RestSymbolInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	refDataID, err := api.SymbolToRefDataID(args.Symbol)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	db, err := cmn.DbConnect()
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	err = db.Get(ptr, fmt.Sprintf("%s WHERE ref_data_id=%d", api.JsonToSelect(obj, table, ""), refDataID))
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(ptr)

	cmn.Exit(msg, ptr)
}

func EnrichedProjectionsBySymbol(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Read-EnrichedProjectionsBySymbol", w, r)

	args := new(cmn.RestSymbolInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	refDataID, err := api.SymbolToRefDataID(args.Symbol)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	db, err := cmn.DbConnect()
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	p := api.JsonProjections{}
	err = db.Get(&p, api.JsonToSelect(p, "projections", "")+fmt.Sprintf(" WHERE ref_data_id=%d", refDataID))
	if err != nil {
		api.ProjectionsBySymbol(args.Symbol, &p)
	}
	ep, err := api.EnrichProjections(p)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(&ep)
	cmn.Exit("Read-EnrichedProjectionsBySymbol", ep)
}

func EnrichedProjectionsByID(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Read-EnrichedProjectionsByID", w, r)

	params := mux.Vars(r)
	id := params["id"]

	db, err := cmn.DbConnect()
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	p := api.JsonProjections{}
	err = db.Get(&p, api.JsonToSelect(p, "projections", "")+fmt.Sprintf(" WHERE id=%s", id))
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
	ep, err := api.EnrichProjections(p)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(&ep)

	cmn.Exit("Read-EnrichedProjectionsByID", ep)
}

type EnrichedProjectionsJournalByProjectionsIDInput struct {
	ProjectionsID int `schema:"projectionsId"`
}

func EnrichedProjectionsJournalByProjectionsID(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Read-EnrichedProjectionsJournalByProjectionsID", w, r)

	args := new(EnrichedProjectionsJournalByProjectionsIDInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	db, err := cmn.DbConnect()
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret := []api.JsonEnrichedProjectionsJournal{}
	err = db.Select(&ret, api.JsonToSelect(api.JsonEnrichedProjectionsJournal{}, "projections_journal", "")+fmt.Sprintf(" WHERE projections_id=%d ORDER BY id DESC", args.ProjectionsID))
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	sort.Slice(ret, func(i, j int) bool {
		return cmn.DateStringToTime(ret[i].Date).After(cmn.DateStringToTime(ret[j].Date))
	})

	json.NewEncoder(w).Encode(&ret)

	cmn.Exit("Read-EnrichedProjectionsJournalByProjectionsID", ret)
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
	cmn.Enter("Read-ProjectionsBySymbol", w, r)

	args := new(cmn.RestSymbolInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	refDataID, err := api.SymbolToRefDataID(args.Symbol)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	db, err := cmn.DbConnect()
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
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
	cmn.Exit("Read-ProjectionsBySymbol", &ret)
}

func MarketData(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("MarketData", w, r)

	db, err := cmn.DbConnect()
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
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

	cmn.Exit("MarketData", ret)
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
	cmn.Enter("Read-MarketDataByRefDataID", w, r)

	args := new(cmn.RestRefDataIDInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	ret := api.JsonMarketData{}
	err = cmn.DbGet(&ret, api.JsonToSelect(api.JsonMarketData{}, fmt.Sprintf("market_data WHERE ref_data_id=%d", args.RefDataID), ""))
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	cmn.Exit("Read-MarketDataByRefDataID", ret)
}

func EnrichedMarketDataBySymbol(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Read-EnrichedMarketDataBySymbol", w, r)

	args := new(cmn.RestSymbolInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	refDataID, err := api.SymbolToRefDataID(args.Symbol)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	var ret api.JsonEnrichedMarketData
	err = cmn.DbGet(&ret, fmt.Sprintf("select id, ref_data_id, last, (updated_at < current_date - interval '4 days') as stale from market_data WHERE ref_data_id=%d", refDataID))
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	cmn.Exit("Read-EnrichedMarketDataBySymbol", &ret)
}

func MDHYearSummaryBySymbol(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Read-MDHYearSummaryBySymbol", w, r)

	args := new(cmn.RestSymbolDateInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	refDataID, err := api.SymbolToRefDataID(args.Symbol)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	db, err := cmn.DbConnect()
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
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

	cmn.Exit("Read-MDHYearSummaryBySymbol", ret)
}

func MDHByRefDataIDDate(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Read-MDHByRefDataIDDate", w, r)

	args := new(cmn.RestRefDataIDDateInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	db, err := cmn.DbConnect()
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
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

	cmn.Exit("Read-MDHByRefDataIDDate", ret)
}

func MDHBySymbol(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Read-MDHBySymbol", w, r)

	args := new(cmn.RestSymbolDateInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	refDataID, err := api.SymbolToRefDataID(args.Symbol)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	db, err := cmn.DbConnect()
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
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

	cmn.Exit("Read-MDHYearSummaryBySymbol", ret)
}

func RefDataFocus(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("RefDataFocus", w, r)

	db, err := cmn.DbConnect()
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	foo := api.JsonRefData{}
	ret := []api.JsonRefData{}
	err = db.Select(&ret, fmt.Sprintf("%s WHERE active=true AND focus=true", api.JsonToSelect(foo, "ref_data", "")))
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	cmn.Exit("RefDataFocus", ret)
}

func RefData(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("RefData", w, r)

	db, err := cmn.DbConnect()
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	foo := api.JsonRefData{}
	ret := []api.JsonRefData{}
	err = db.Select(&ret, fmt.Sprintf("%s, market_data m WHERE r.active=true AND r.id = m.ref_data_id ORDER BY m.updated_at ASC", api.JsonToSelect(foo, "ref_data", "r")))
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	cmn.Exit("RefData", ret)
}

func RefDataByID(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("RefDataByID", w, r)

	params := mux.Vars(r)
	id := params["id"]

	db, err := cmn.DbConnect()
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
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

	cmn.Exit("RefDataByID", ret)
}

func RefDataBySymbol(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("RefDataBySymbol", w, r)

	args := new(cmn.RestSymbolInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	db, err := cmn.DbConnect()
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
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

	cmn.Exit("RefDataBySymbol", ret)
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
	cmn.Enter("Scalar", w, r)

	args := new(ScalarInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	db, err := cmn.DbConnect()
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
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

	cmn.Exit("Scalar", s)
}

func SimfinIncomeByID(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("SimfinIncomeByID", w, r)

	params := mux.Vars(r)
	id := params["id"]

	db, err := cmn.DbConnect()
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
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

	cmn.Exit("RefDataByID", ret)
}

type SimfinIncomeInput struct {
	Ticker string `schema:"ticker"`
}

func SimfinIncomeByTicker(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("SimfinIncomeByTicker", w, r)
	w.Header().Set("Access-Control-Allow-Origin", "*")

	args := new(SimfinIncomeInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	db, err := cmn.DbConnect()
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
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

	cmn.Exit("SimfinIncomeByTicker", ret)
}

type IncomeInput struct {
	Ticker string `schema:"ticker"`
}

func IncomeByTicker(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("IncomeByTicker", w, r)
	w.Header().Set("Access-Control-Allow-Origin", "*")

	args := new(IncomeInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusBadRequest)
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
			i.EPS = cmn.Round(float64(i.NetIncomeCommon)/float64(i.SharesDiluted), 0.01)
		}
		ret = append(ret, i)
	}
	json.NewEncoder(w).Encode(&ret)

	cmn.Exit("IncomeByTicker", ret)
}

func SimfinBalanceByID(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("SimfinBalanceByID", w, r)

	params := mux.Vars(r)
	id := params["id"]

	db, err := cmn.DbConnect()
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
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

	cmn.Exit("SimfinBalanceByID", ret)
}

type SimfinBalanceInput struct {
	Ticker string `schema:"ticker"`
}

func SimfinBalanceByTicker(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("SimfinBalanceByTicker", w, r)
	w.Header().Set("Access-Control-Allow-Origin", "*")

	args := new(SimfinBalanceInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	db, err := cmn.DbConnect()
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
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

	cmn.Exit("SimfinBalanceByTicker", ret)
}

type BalanceInput struct {
	Ticker string `schema:"ticker"`
}

func BalanceByTicker(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("BalanceByTicker", w, r)
	w.Header().Set("Access-Control-Allow-Origin", "*")

	args := new(BalanceInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusBadRequest)
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

	cmn.Exit("BalanceByTicker", ret)
}

func SimfinCashflowByID(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("SimfinCashflowByID", w, r)

	params := mux.Vars(r)
	id := params["id"]

	db, err := cmn.DbConnect()
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
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

	cmn.Exit("SimfinCashflowByID", ret)
}

type SimfinCashflowInput struct {
	Ticker string `schema:"ticker"`
}

func SimfinCashflowByTicker(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("SimfinCashflowByTicker", w, r)
	w.Header().Set("Access-Control-Allow-Origin", "*")

	args := new(SimfinCashflowInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	db, err := cmn.DbConnect()
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
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

	cmn.Exit("SimfinCashflowByTicker", ret)
}

type CashflowInput struct {
	Ticker string `schema:"ticker"`
}

func CashflowByTicker(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("CashflowByTicker", w, r)
	w.Header().Set("Access-Control-Allow-Origin", "*")

	args := new(CashflowInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusBadRequest)
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
			i.DPS = -1.0 * cmn.Round(float64(i.DividendsPaid)/float64(i.SharesBasic), 0.01)
		}
		ret = append(ret, i)
	}
	json.NewEncoder(w).Encode(&ret)

	cmn.Exit("CashflowByTicker", ret)
}

func SummaryByTicker(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Read-SummaryByTicker", w, r)
	w.Header().Set("Access-Control-Allow-Origin", "*")

	args := new(cmn.RestTickerInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusBadRequest)
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

	cmn.Exit("Read-SummaryByTicker", ret)
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
	RestHandleGet(w, r, "Read-Transactions", &ret, foo, "transactions ORDER BY date DESC")
}

func TransactionsByPositionID(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Read-TransactionsByPositionID", w, r)

	args := new(cmn.RestPositionIDInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	ret := []api.JsonTransaction{}
	err = cmn.DbSelect(&ret, api.JsonToSelect(api.JsonTransaction{}, fmt.Sprintf("transactions WHERE position_id=%d ORDER BY date DESC", args.PositionID), ""))
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	cmn.Exit("Read-TransactionsByPositionID", ret)
}

func PortfoliosByID(w http.ResponseWriter, r *http.Request) {
	var ret api.JsonPortfolio
	RestHandleGetByID(w, r, "Read-PortfoliosByID", &ret, ret, "portfolios")
}

func EnrichPortfolio(p api.JsonPortfolio) (api.JsonEnrichedPortfolio, error) {
	ep := api.JsonEnrichedPortfolio{JsonPortfolio: p}

	var totalPortfolio api.JsonPortfolio
	err := api.PortfoliosByID(cmn.CONST_PORTFOLIO_TOTAL, &totalPortfolio)
	if err != nil {
		return ep, err
	}
	if totalPortfolio.Value > 0 {
		ep.PercentTotal = cmn.Round(p.Value/totalPortfolio.Value, 0.0001)
	}
	if p.Value > 0 {
		ep.PercentCash = cmn.Round(p.Cash/p.Value, 0.0001)
		ep.PercentDebt = cmn.Round(p.Debt/p.Value, 0.0001)
	}

	return ep, nil
}

func EnrichedPortfolios(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Read-EnrichedPortfolios", w, r)

	var portfolios []api.JsonPortfolio
	err := api.Portfolios(&portfolios)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret := []api.JsonEnrichedPortfolio{}
	for i := range portfolios {
		ep, err := EnrichPortfolio(portfolios[i])
		if err != nil {
			cmn.ErrorHttp(err, w, http.StatusInternalServerError)
			return
		}

		ret = append(ret, ep)
	}
	json.NewEncoder(w).Encode(&ret)

	cmn.Exit("Read-EnrichedPortfolios", ret)
}

func EnrichedPortfoliosByID(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Read-EnrichedPortfoliosByID", w, r)

	params := mux.Vars(r)
	id, err := strconv.Atoi(params["id"])
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	var portfolio api.JsonPortfolio
	err = api.PortfoliosByID(id, &portfolio)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret, err := EnrichPortfolio(portfolio)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	cmn.Exit("Read-EnrichedPortfoliosByID", ret)
}

func PortfoliosHistoryByDate(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Read-PortfoliosHistoryByDate", w, r)

	args := new(cmn.RestDateInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	db, err := cmn.DbConnect()
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret := []api.JsonPortfolioHistory{}
	err = db.Select(&ret, api.JsonToSelect(api.JsonPortfolioHistory{}, fmt.Sprintf("portfolios_history WHERE date='%s'", args.Date), ""))
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	sort.Slice(ret, func(i, j int) bool {
		return ret[i].PortfolioID < ret[j].PortfolioID
	})
	json.NewEncoder(w).Encode(&ret)

	cmn.Exit("Read-PortfoliosHistoryByDate", ret)
}

func PortfoliosHistoryByPortfolioIDDate(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Read-PortfoliosHistoryByPortfolioIDDate", w, r)

	args := new(cmn.RestPortfolioIDDateInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	db, err := cmn.DbConnect()
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret := api.JsonPortfolioHistory{}
	err = db.Get(&ret, api.JsonToSelect(api.JsonPortfolioHistory{}, fmt.Sprintf("portfolios_history WHERE portfolio_id=%d and date='%s'", args.PortfolioID, args.Date), ""))
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	cmn.Exit("Read-PortfoliosHistoryByPortfolioIDDate", ret)
}

func PositionsHistoryByPortfolioIDDate(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Read-PositionsHistoryByPortfolioIDDate", w, r)

	args := new(cmn.RestPortfolioIDDateInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	db, err := cmn.DbConnect()
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret := []api.JsonPositionHistory{}
	err = db.Select(&ret, api.JsonToSelect(api.JsonPositionHistory{}, fmt.Sprintf("positions_history WHERE portfolio_id=%d and date='%s'", args.PortfolioID, args.Date), ""))
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	cmn.Exit("Read-PositionsHistoryByPortfolioIDDate", ret)
}

func PortfoliosHistoryMaxDate(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Read-PortfoliosHistoryMaxDate", w, r)

	db, err := cmn.DbConnect()
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret := cmn.RestStringOutput{}
	err = db.Get(&ret, "select max(date) as value from portfolios_history")
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	cmn.Exit("Read-PortfoliosHistoryMaxDate", ret)
}

func Positions(w http.ResponseWriter, r *http.Request) {
	foo := api.JsonPosition{}
	ret := []api.JsonPosition{}
	RestHandleGet(w, r, "Read-Positions", &ret, foo, "positions WHERE active=true ORDER BY portfolio_id,id ASC")
}

func PositionsBySymbolPortfolioID(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Read-PositionsBySymbolPortfolioID", w, r)

	args := new(cmn.RestSymbolPortfolioIDInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	var refData api.JsonRefData
	err = api.RefDataBySymbol(args.Symbol, &refData)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	db, err := cmn.DbConnect()
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret := api.JsonPosition{}
	err = db.Get(&ret, api.JsonToSelect(api.JsonPosition{}, fmt.Sprintf("positions WHERE ref_data_id=%d and portfolio_id=%d", refData.ID, args.PortfolioID), ""))
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	cmn.Exit("Read-PositionsBySymbolPortfolioID", ret)
}

func CalculateReturn(ID int, index float64, date string, interval string, years float64) float64 {
	var start float64
	var ret float64
	query := "select index from portfolios_history where portfolio_id=%d and date=" +
		"(select max(date) from portfolios_history where portfolio_id=%d and date <= (select date('%s') - interval '%s'))"
	// If the value is not present, leave the return zero, don't handle error
	_ = cmn.DbGet(&start, fmt.Sprintf(query, ID, ID, date, interval))
	if start > 0 {
		ret = cmn.Round(math.Pow(index/start, 1/years)-1, 0.0001)
	}
	return ret
}

func EnrichPortfolioReturns(p api.JsonPortfolio, date string) (api.JsonPortfolioReturns, error) {
	pr := api.JsonPortfolioReturns{}
	pr.ID = p.ID
	pr.Name = p.Name
	pr.OneDay = CalculateReturn(p.ID, p.Index, date, "1 day", 1)
	pr.OneWeek = CalculateReturn(p.ID, p.Index, date, "1 week", 1)
	pr.OneMonth = CalculateReturn(p.ID, p.Index, date, "1 month", 1)
	pr.ThreeMonths = CalculateReturn(p.ID, p.Index, date, "3 months", 1)
	pr.OneYear = CalculateReturn(p.ID, p.Index, date, "1 year", 1)
	pr.FiveYears = CalculateReturn(p.ID, p.Index, date, "5 years", 5)
	pr.TenYears = CalculateReturn(p.ID, p.Index, date, "10 years", 10)
	pr.ProfitLifetime = p.Value - p.TotalCashInfusion

	yearStartDate := ""
	dateParsed, err := time.Parse("2006-01-02", date)
	if err != nil {
		return pr, err
	}
	if dateParsed.Month() == 1 && dateParsed.Day() == 1 {
		yearStartDate = fmt.Sprintf("%d-%02d-%02d", dateParsed.Year()-1, dateParsed.Month(), dateParsed.Day())
	} else {
		yearStartDate = fmt.Sprintf("%d-%02d-%02d", dateParsed.Year(), 1, 1)
	}

	physd := api.JsonPortfolioHistory{}
	err = api.PortfoliosHistoryPortfolioIDDate(p.ID, yearStartDate, &physd)
	if err != nil {
		return pr, err
	}

	if physd.Index > 0 {
		pr.YearToDate = cmn.Round(p.Index/physd.Index-1, 0.0001)
	}
	pr.ProfitYearToDate = p.Value - physd.Value - (p.TotalCashInfusion - physd.TotalCashInfusion)
	return pr, nil
}

func PortfolioReturnsByID(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Read-PortfolioReturnsByID", w, r)

	params := mux.Vars(r)
	id, err := strconv.Atoi(params["id"])
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	var portfolio api.JsonPortfolio
	err = api.PortfoliosByID(id, &portfolio)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret, err := EnrichPortfolioReturns(portfolio, time.Now().Format("2006-01-02"))
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)
	cmn.Exit("Read-PortfolioReturnsByID", ret)
}

func PortfolioReturns(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Read-PortfolioReturns", w, r)

	var portfolios []api.JsonPortfolio
	err := api.Portfolios(&portfolios)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret := []api.JsonPortfolioReturns{}
	for i := range portfolios {
		pr, err := EnrichPortfolioReturns(portfolios[i], time.Now().Format("2006-01-02"))
		if err != nil {
			cmn.ErrorHttp(err, w, http.StatusInternalServerError)
			return
		}

		ret = append(ret, pr)
	}
	json.NewEncoder(w).Encode(&ret)

	cmn.Exit("Read-PortfolioReturns", ret)
}

func PortfolioReturnsByDate(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Read-PortfolioReturnsByDate", w, r)

	args := new(cmn.RestDateInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	var portfoliosHistory []api.JsonPortfolioHistory
	err = api.PortfoliosHistoryByDate(args.Date, &portfoliosHistory)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret := []api.JsonPortfolioReturns{}
	for i := range portfoliosHistory {
		portfolio := portfoliosHistory[i].JsonPortfolio
		portfolio.ID = portfoliosHistory[i].PortfolioID
		pr, err := EnrichPortfolioReturns(portfolio, args.Date)
		if err != nil {
			cmn.ErrorHttp(err, w, http.StatusInternalServerError)
			return
		}

		ret = append(ret, pr)
	}
	json.NewEncoder(w).Encode(&ret)

	cmn.Exit("Read-PortfolioReturnsByDate", ret)
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
		ep.PercentPortfolio = cmn.Round(p.Value/portfolio.Value, 0.0001)
	}

	return ep, nil
}

func EnrichedPositions(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Read-EnrichedPositions", w, r)

	var positions []api.JsonPosition
	err := api.Positions(&positions)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret := []api.JsonEnrichedPosition{}
	for i := range positions {
		ep, err := EnrichPosition(positions[i])
		if err != nil {
			cmn.ErrorHttp(err, w, http.StatusInternalServerError)
			return
		}

		ret = append(ret, ep)
	}
	json.NewEncoder(w).Encode(&ret)

	cmn.Exit("Read-EnrichedPositions", ret)
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
	cmn.Enter("Read-EnrichedPositionsByPortfolioID", w, r)
	args := new(EnrichedPositionsByPortfolioIDInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	db, err := cmn.DbConnect()
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	positions := []api.JsonPosition{}
	err = db.Select(&positions, api.JsonToSelect(api.JsonPosition{}, "positions", "")+fmt.Sprintf(" WHERE portfolio_id=%d %s ORDER BY id DESC", args.PortfolioID, filter))
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
	ret := []api.JsonEnrichedPosition{}
	for i := range positions {
		ep, err := EnrichPosition(positions[i])
		if err != nil {
			cmn.ErrorHttp(err, w, http.StatusInternalServerError)
			return
		}

		ret = append(ret, ep)
	}

	sort.Slice(ret, func(i, j int) bool {
		return ret[i].Value > ret[j].Value
	})

	json.NewEncoder(w).Encode(&ret)
	cmn.Exit("Read-EnrichedPositionsByPortfolioID", ret)
}

func EnrichedPositionsBySymbolPortfolioID(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Read-EnrichedPositionsBySymbolPortfolioID", w, r)

	args := new(cmn.RestSymbolPortfolioIDInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	var p api.JsonPosition
	err = api.PositionsBySymbolPortfolioID(args.Symbol, args.PortfolioID, &p)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}
	ret, err := EnrichPosition(p)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	cmn.Exit("Read-EnrichedPositionsBySymbolPortfolioID", ret)
}

func EnrichedPositionsHistoryByPortfolioIDDate(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Read-EnrichedPositionsHistoryByPortfolioIDDate", w, r)

	args := new(cmn.RestPortfolioIDDateInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	positions := []api.JsonPositionHistory{}
	err = api.PositionsHistoryByPortfolioIDDate(args.PortfolioID, args.Date, &positions)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
	ret := []api.JsonEnrichedPositionHistory{}
	for i := range positions {
		ep, err := EnrichPosition(positions[i].JsonPosition)
		if err != nil {
			cmn.ErrorHttp(err, w, http.StatusInternalServerError)
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
	cmn.Exit("Read-EnrichedPositionsHistoryByPortfolioIDDate", ret)
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

	fees := 0.005
	if strings.Contains(em.TargetTicker, ".HK") {
		fees = (0.0008 + 0.0013) * md.Last // 8 bps commision and 13 bps stamp on each side
	}

	em.MarketPositiveReturn = cmn.Round((em.DealPrice+em.Dividends-fees)/md.Last-1, 0.0001)
	em.MarketNetReturn = cmn.Round(
		((em.DealPrice+em.Dividends-fees-md.Last)*em.Confidence-(md.Last-em.FailPrice-em.Dividends+2*fees)*(1-em.Confidence))/md.Last, 0.0001)
	closeTime := cmn.DateStringToTime(em.CloseDate)
	daysToClose := closeTime.Sub(time.Now()).Hours() / 24
	annualizeMultiple := 365 / daysToClose
	em.MarketPositiveReturnAnnualized = cmn.Round(annualizeMultiple*em.MarketPositiveReturn, 0.0001)
	em.MarketNetReturnAnnualized = cmn.Round(annualizeMultiple*em.MarketNetReturn, 0.0001)

	var position api.JsonEnrichedPosition
	err = api.EnrichedPositionsBySymbolPortfolioID(em.TargetTicker, cmn.CONST_PORTFOLIO_RISK_ARB, &position)
	// Don't pass the error up, it's ok if this isn't a position, we just populate zero
	if err == nil {
		em.PercentPortfolio = position.PercentPortfolio
	}

	if em.PercentPortfolio > 0 {
		em.Status = "P"
	} else if em.BreakPrice > 0 {
		em.Status = "B"
	} else if daysToClose < 0 {
		em.Status = "C"
	} else {
		em.Status = "O"
	}

	return em, nil
}

func EnrichedMergersPositions(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Read-EnrichedMergersPositions", w, r)
	w.Header().Set("Access-Control-Allow-Origin", "*")

	var mergers []api.JsonMerger
	err := api.Mergers(&mergers)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret := []api.JsonEnrichedMerger{}
	for i := range mergers {
		em, err := EnrichMerger(mergers[i])
		if err != nil {
			cmn.ErrorHttp(err, w, http.StatusInternalServerError)
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

	cmn.Exit("Read-EnrichedMergersPositions", ret)
}

func EnrichedMergersPositionsTotal(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Read-EnrichedMergersPositionsTotal", w, r)
	w.Header().Set("Access-Control-Allow-Origin", "*")

	var mergers []api.JsonEnrichedMerger
	err := api.EnrichedMergersPositions(&mergers)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
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

	cmn.Exit("Read-EnrichedMergersPositionsTotal", ret)
}

func EnrichedMergersResearch(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Read-EnrichedMergersResearch", w, r)
	w.Header().Set("Access-Control-Allow-Origin", "*")

	var mergers []api.JsonMerger
	err := api.Mergers(&mergers)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret := []api.JsonEnrichedMerger{}
	for i := range mergers {
		em, err := EnrichMerger(mergers[i])
		if err != nil {
			cmn.ErrorHttp(err, w, http.StatusInternalServerError)
			return
		}

		if em.PercentPortfolio <= 0 {
			ret = append(ret, em)
		}
	}

	sort.Slice(ret, func(i, j int) bool {
		if api.MergerStatusToInt(ret[i].Status) == api.MergerStatusToInt(ret[j].Status) {
			return ret[i].MarketNetReturnAnnualized > ret[j].MarketNetReturnAnnualized
		}
		return api.MergerStatusToInt(ret[i].Status) > api.MergerStatusToInt(ret[j].Status)
	})
	json.NewEncoder(w).Encode(&ret)

	cmn.Exit("Read-EnrichedMergersResearch", ret)
}

func EnrichedMergersByID(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Read-EnrichedMergersByID", w, r)

	params := mux.Vars(r)
	id := params["id"]

	db, err := cmn.DbConnect()
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
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
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(&em)

	cmn.Exit("Read-EnrichedMergersByID", em)
}

func EnrichedPositionsByID(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Read-EnrichedPositionsByID", w, r)

	params := mux.Vars(r)
	id, err := strconv.Atoi(params["id"])
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	p := api.JsonPosition{}
	err = api.PositionsByID(id, &p)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
	ep, err := EnrichPosition(p)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(&ep)

	cmn.Exit("Read-EnrichedPositionsByID", ep)
}

type EnrichedMergersJournalByMergerIDInput struct {
	MergerID int `schema:"mergerId"`
}

func EnrichedMergersJournalByMergerID(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Read-EnrichedMergersJournalByMergerID", w, r)

	args := new(EnrichedMergersJournalByMergerIDInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	db, err := cmn.DbConnect()
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
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

	cmn.Exit("Read-EnrichedMergersJournalByMergerID", ret)
}

func main() {
	log.Println("Listening on http://localhost:8081/blue-lion/read")
	router := mux.NewRouter().StrictSlash(true)
	setupRouter(router)
	log.Fatal(http.ListenAndServe(":8081", router))
}
