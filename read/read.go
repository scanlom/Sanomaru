package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/gorilla/schema"
	"github.com/scanlom/Sanomaru/api"
	"github.com/scanlom/Sanomaru/cmn"
)

func setupRouter(router *mux.Router) {
	router.HandleFunc("/blue-lion/read/market-data/{id}", MarketDataByID).Methods("GET")
	router.HandleFunc("/blue-lion/read/market-data", MarketDataBySymbol).Queries("symbol", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/market-data", MarketData).Methods("GET")
	router.HandleFunc("/blue-lion/read/market-data-historical/year-summary", MDHYearSummaryBySymbol).Queries("symbol", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/market-data-historical", MDHByRefDataIDDate).Queries("refDataId", "", "date", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/ref-data/focus", RefDataFocus).Methods("GET")
	router.HandleFunc("/blue-lion/read/ref-data/{id}", RefDataByID).Methods("GET")
	router.HandleFunc("/blue-lion/read/ref-data", RefDataBySymbol).Queries("symbol", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/ref-data", RefData).Methods("GET")
	router.HandleFunc("/blue-lion/read/projections/{id}", ProjectionsByID).Methods("GET")
	router.HandleFunc("/blue-lion/read/projections", ProjectionsBySymbol).Queries("symbol", "").Methods("GET")
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
	router.HandleFunc("/blue-lion/read/headline", HeadlineByTicker).Queries("ticker", "").Methods("GET")
	router.Methods("GET").Path("/blue-lion/read/scalar").HandlerFunc(Scalar)
}

func RestHandleGet(w http.ResponseWriter, r *http.Request, msg string, ptr interface{}, obj interface{}, table string) {
	cmn.Enter(msg, r.URL.Query())
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")

	params := mux.Vars(r)
	id := params["id"]

	db, err := cmn.DbConnect()
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	err = db.Get(ptr, fmt.Sprintf("%s WHERE id=%d", api.JsonToSelect(obj, table, ""), id))
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(ptr)

	cmn.Exit(msg, ptr)
}

func RestHandleGetBySymbol(w http.ResponseWriter, r *http.Request, msg string, ptr interface{}, obj interface{}, table string) {
	cmn.Enter(msg, r.URL.Query())
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")

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

func ProjectionsBySymbol(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Read-ProjectionsBySymbol", r.URL.Query())
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")

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
		log.Println(err)
		ret.EntryType = "D"

		var summary []api.JsonSummary
		err = api.SummaryByTicker(args.Symbol, &summary)
		if err != nil {
			log.Println(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		if len(summary) > 0 {
			ret.RefDataID = refDataID
			ret.Date = summary[0].ReportDate
			ret.EPS = summary[0].EPS
			ret.DPS = summary[0].DPS
			ret.Payout = ret.DPS / ret.EPS

			var epsCagr5yr, epsCagr10yr, roe5yr float64
			var peHighMmo5yr, peLowMmo5yr int
			HeadlineFromSummary(summary, &peHighMmo5yr, &peLowMmo5yr, &epsCagr5yr, &epsCagr10yr, &roe5yr)
			ret.Growth = epsCagr5yr
			ret.ROE = roe5yr
			ret.PETerminal = (peHighMmo5yr + peLowMmo5yr) / 2
			if ret.PETerminal > 18.0 { // Cap PETerminal at 18
				ret.PETerminal = 18.0
			}
			ret.EPSYr1 = ret.EPS * (1.0 + epsCagr5yr)
			ret.EPSYr2 = ret.EPSYr1 * (1.0 + epsCagr5yr)
		}
	} else {
		ret.EntryType = "O"
	}

	json.NewEncoder(w).Encode(&ret)
	cmn.Exit("Read-ProjectionsBySymbol", &ret)
}

func ProjectionsByID(w http.ResponseWriter, r *http.Request) {
	var ret api.JsonProjections
	RestHandleGet(w, r, "Read-ProjectionsBySymbol", &ret, ret, "projections")
}

func MarketData(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("MarketData", r.URL.Query())

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
	RestHandleGet(w, r, "Read-MarketDataByID", &ret, ret, "market_data")
}

func MarketDataBySymbol(w http.ResponseWriter, r *http.Request) {
	var ret api.JsonMarketData
	RestHandleGetBySymbol(w, r, "Read-MarketDataBySymbol", &ret, ret, "market_data")
}

func MDHYearSummaryBySymbol(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Read-MDHYearSummaryBySymbol", r.URL.Query())

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
	cmn.Enter("Read-MDHByRefDataIDDate", r.URL.Query())

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
	cmn.Enter("Read-MDHBySymbol", r.URL.Query())

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
	cmn.Enter("RefDataFocus", r.URL.Query())

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
	cmn.Enter("RefData", r.URL.Query())

	db, err := cmn.DbConnect()
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	foo := api.JsonRefData{}
	ret := []api.JsonRefData{}
	err = db.Select(&ret, fmt.Sprintf("%s, market_data m WHERE r.active=true AND r.id = m.ref_data_id ORDER BY m.updated_at ASC", api.JsonToSelect(foo, "ref_data", "r")))
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	cmn.Exit("RefData", ret)
}

func RefDataByID(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("RefDataByID", r.URL.Query())

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
	cmn.Enter("RefDataBySymbol", r.URL.Query())

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
	cmn.Enter("Scalar", r.URL.Query())

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
	cmn.Enter("SimfinIncomeByID", r.URL.Query())

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
	cmn.Enter("SimfinIncomeByTicker", r.URL.Query())
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
	cmn.Enter("IncomeByTicker", r.URL.Query())
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
	cmn.Enter("SimfinBalanceByID", r.URL.Query())

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
	cmn.Enter("SimfinBalanceByTicker", r.URL.Query())
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
	cmn.Enter("BalanceByTicker", r.URL.Query())
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
	cmn.Enter("SimfinCashflowByID", r.URL.Query())

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
	cmn.Enter("SimfinCashflowByTicker", r.URL.Query())
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
	cmn.Enter("CashflowByTicker", r.URL.Query())
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
		if i.SharesDiluted > 0.0 {
			i.DPS = -1.0 * cmn.Round(float64(i.DividendsPaid)/float64(i.SharesDiluted), 0.01)
		}
		ret = append(ret, i)
	}
	json.NewEncoder(w).Encode(&ret)

	cmn.Exit("CashflowByTicker", ret)
}

type HeadlineInput struct {
	Ticker string `schema:"ticker"`
}

func Cagr(years float64, projections api.JsonProjections, md api.JsonMarketData) float64 {
	if projections.EPS <= 0.0 || projections.Growth <= 0.0 || projections.PETerminal <= 0.0 || md.Last <= 0.0 {
		return 0.0
	}
	divBucket := 0.0
	divGrowth, _ := api.Scalar(api.CONST_DIV_GROWTH)
	eps := projections.EPS
	for i := 0.0; i < years; i++ {
		divBucket = divBucket * (1.0 + divGrowth)
		divBucket = divBucket + (eps * projections.Payout)
		eps = eps * (1.0 + projections.Growth)
	}
	ret := math.Pow(((eps*float64(projections.PETerminal))+divBucket)/md.Last, 1.0/years) - 1.0
	return math.Round(ret*100000000) / 100000000
}

func Croe(years float64, projections api.JsonProjections, md api.JsonMarketData) float64 {
	if projections.Book <= 0.0 || projections.ROE <= 0.0 || projections.PETerminal <= 0.0 || md.Last <= 0.0 {
		return 0.0
	}
	divBucket := 0.0
	divGrowth, _ := api.Scalar(api.CONST_DIV_GROWTH)
	book := projections.Book
	eps := 0.0
	for i := 0.0; i < years; i++ {
		divBucket = divBucket * (1.0 + divGrowth)
		eps = book * projections.ROE
		div := eps * projections.Payout
		divBucket += div
		book += eps - div
	}
	ret := math.Pow(((eps*float64(projections.PETerminal))+divBucket)/md.Last, 1.0/years) - 1.0
	return math.Round(ret*100000000) / 100000000
}

func HeadlineFromSummary(summary []api.JsonSummary, peHighMmo5yr *int, peLowMmo5yr *int, epsCagr5yr *float64, epsCagr10yr *float64, roe5yr *float64) {
	var sumRoe5yr float64
	var sumH, sumL, maxH, maxL, minH, minL int
	minH = math.MaxInt64
	minL = math.MaxInt64
	if len(summary) > 6 && summary[0].EPS > 0.0 && summary[1].EPS > 0.0 && summary[5].EPS > 0.0 && summary[6].EPS > 0.0 {
		first := ((summary[5].EPS + summary[6].EPS) / 2.0)
		last := ((summary[0].EPS + summary[1].EPS) / 2.0)
		*epsCagr5yr = math.Pow(last/first, 0.2) - 1.0
	}
	if len(summary) > 10 && summary[0].EPS > 0.0 && summary[10].EPS > 0.0 {
		*epsCagr10yr = math.Pow(summary[0].EPS/summary[10].EPS, 0.1) - 1.0
	}

	if len(summary) > 4 {
		for i := 0; i < 5; i++ {
			if summary[i].PEHigh > maxH {
				maxH = summary[i].PEHigh
			}
			if summary[i].PELow > maxL {
				maxL = summary[i].PELow
			}
			if summary[i].PEHigh < minH {
				minH = summary[i].PEHigh
			}
			if summary[i].PELow < minL {
				minL = summary[i].PELow
			}
			sumH += summary[i].PEHigh
			sumL += summary[i].PELow
			sumRoe5yr += summary[i].ROE
		}
		*peHighMmo5yr = int(math.Round(float64(sumH-maxH-minH) / 3.0))
		*peLowMmo5yr = int(math.Round(float64(sumL-maxL-minL) / 3.0))
		*roe5yr = sumRoe5yr / 5.0
	}
}

func HeadlineByTicker(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Read-HeadlineByTicker", r.URL.Query())
	w.Header().Set("Access-Control-Allow-Origin", "*")

	args := new(HeadlineInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	var refData api.JsonRefData
	err = api.RefDataBySymbol(args.Ticker, &refData)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	var summary []api.JsonSummary
	err = api.SummaryByTicker(args.Ticker, &summary)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	var epsCagr5yr, epsCagr10yr, epsCagr2yr, epsCagr7yr, roe5yr float64
	var price, pe, divPlusGrowth, epsYield, dpsYield, cagr5yr, cagr10yr, croe5yr, croe10yr, magic float64
	var peHighMmo5yr, peLowMmo5yr int
	HeadlineFromSummary(summary, &peHighMmo5yr, &peLowMmo5yr, &epsCagr5yr, &epsCagr10yr, &roe5yr)

	var projections api.JsonProjections
	err = api.ProjectionsBySymbol(args.Ticker, &projections)
	if err == nil {
		if len(summary) > 0 && summary[0].EPS > 0.0 && projections.EPSYr2 > 0.0 {
			epsCagr2yr = math.Pow(projections.EPSYr2/summary[0].EPS, 0.5) - 1.0
		}
		if len(summary) > 5 && summary[5].EPS > 0.0 && projections.EPSYr2 > 0.0 {
			epsCagr7yr = math.Pow(projections.EPSYr2/summary[5].EPS, 0.142857143) - 1.0
		}
	}

	var md api.JsonMarketData
	err = api.MarketDataBySymbol(args.Ticker, &md)
	if err == nil && md.Last > 0.0 {
		price = md.Last
		if projections.EPS > 0.0 {
			pe = price / projections.EPS
		}
		epsYield = projections.EPS / price
		dpsYield = projections.DPS / price
		divPlusGrowth = dpsYield + projections.Growth
		cagr5yr = Cagr(5.0, projections, md)
		croe5yr = Croe(5.0, projections, md)
		cagr10yr = Cagr(10.0, projections, md)
		croe10yr = Croe(10.0, projections, md)
	}

	if len(summary) >= 5 {
		magic = cagr5yr
		for i := 0; i < 5; i++ {
			if summary[i].NetMgn < 0.10 || summary[i].LTDRatio > 3.5 || summary[i].EPS <= 0.0 {
				magic = 0.0
				break
			}
		}
	}

	ret := api.JsonHeadline{Ticker: args.Ticker, Description: refData.Description, Sector: refData.Sector, Industry: refData.Industry,
		EPSCagr5yr: epsCagr5yr, EPSCagr10yr: epsCagr10yr, EPSCagr2yr: epsCagr2yr, EPSCagr7yr: epsCagr7yr, PEHighMMO5yr: peHighMmo5yr, PELowMMO5yr: peLowMmo5yr, ROE5yr: roe5yr,
		Price: price, PE: pe, DivPlusGrowth: divPlusGrowth, EPSYield: epsYield, DPSYield: dpsYield, CAGR5yr: cagr5yr, CAGR10yr: cagr10yr, CROE5yr: croe5yr, CROE10yr: croe10yr,
		Magic: magic}
	json.NewEncoder(w).Encode(&ret)

	cmn.Exit("Read-HeadlineByTicker", ret)
}

func SummaryByTicker(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Read-SummaryByTicker", r.URL.Query())
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

func main() {
	router := mux.NewRouter().StrictSlash(true)
	setupRouter(router)
	log.Fatal(http.ListenAndServe(":8081", router))
}
