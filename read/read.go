package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"sort"
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
	router.HandleFunc("/blue-lion/read/market-data", MarketData).Methods("GET")
	router.HandleFunc("/blue-lion/read/market-data-historical/year-summary", MDHYearSummaryBySymbol).Queries("symbol", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/market-data-historical", MDHByRefDataIDDate).Queries("refDataId", "", "date", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/ref-data/focus", RefDataFocus).Methods("GET")
	router.HandleFunc("/blue-lion/read/ref-data/{id}", RefDataByID).Methods("GET")
	router.HandleFunc("/blue-lion/read/ref-data", RefDataBySymbol).Queries("symbol", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/ref-data", RefData).Methods("GET")
	router.HandleFunc("/blue-lion/read/projections/{id}", ProjectionsByID).Methods("GET")
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
	router.HandleFunc("/blue-lion/read/enriched-mergers", EnrichedMergers).Methods("GET")
	router.HandleFunc("/blue-lion/read/enriched-mergers/{id}", EnrichedMergersByID).Methods("GET")
	router.HandleFunc("/blue-lion/read/enriched-mergers-journal", EnrichedMergersJournalByMergerID).Queries("mergerId", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/enriched-projections/{id}", EnrichedProjectionsByID).Methods("GET")
	router.HandleFunc("/blue-lion/read/enriched-projections", EnrichedProjectionsBySymbol).Queries("symbol", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/enriched-projections", EnrichedProjections).Methods("GET")
	router.HandleFunc("/blue-lion/read/enriched-projections-journal", EnrichedProjectionsJournalByProjectionsID).Queries("projectionsId", "").Methods("GET")
	router.Methods("GET").Path("/blue-lion/read/scalar").HandlerFunc(Scalar)
}

func RestHandleGet(w http.ResponseWriter, r *http.Request, msg string, ptr interface{}, obj interface{}, table string) {
	cmn.Enter(msg, w, r.URL.Query())

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
	cmn.Enter(msg, w, r.URL.Query())

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
	cmn.Enter("Read-EnrichedProjectionsBySymbol", w, r.URL.Query())

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
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
	ep, err := EnrichProjections(p)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(&ep)
	cmn.Exit("Read-EnrichedProjectionsBySymbol", ep)
}

func EnrichProjections(p api.JsonProjections) (api.JsonEnrichedProjections, error) {
	ep := api.JsonEnrichedProjections{JsonProjections: p}

	var refData api.JsonRefData
	err := api.RefDataByID(ep.RefDataID, &refData)
	if err != nil {
		return ep, nil
	}

	ep.Ticker = refData.Symbol
	ep.Description = refData.Description
	ep.Sector = refData.Sector
	ep.Industry = refData.Industry
	var summary []api.JsonSummary
	err = api.SummaryByTicker(ep.Ticker, &summary)
	if err != nil {
		return ep, nil
	}

	HeadlineFromSummary(summary, &ep.PEHighMMO5yr, &ep.PELowMMO5yr, &ep.EPSCagr5yr, &ep.EPSCagr10yr, &ep.ROE5yr)
	if len(summary) > 0 && summary[0].EPS > 0.0 && ep.EPSYr2 > 0.0 {
		ep.EPSCagr2yr = math.Pow(ep.EPSYr2/summary[0].EPS, 0.5) - 1.0
	}
	if len(summary) > 5 && summary[5].EPS > 0.0 && ep.EPSYr2 > 0.0 {
		ep.EPSCagr7yr = math.Pow(ep.EPSYr2/summary[5].EPS, 0.142857143) - 1.0
	}

	var md api.JsonMarketData
	err = api.MarketDataBySymbol(ep.Ticker, &md)

	// Don't pass the error up, it's ok if we don't get market data, we just can't calculate those fields
	if err == nil && md.Last > 0.0 {
		ep.Price = md.Last
		if ep.EPS > 0.0 {
			ep.PE = ep.Price / ep.EPS
		}
		ep.EPSYield = ep.EPS / ep.Price
		ep.DPSYield = ep.DPS / ep.Price
		ep.DivPlusGrowth = ep.DPSYield + ep.Growth
		ep.CAGR5yr = Cagr(5.0, ep.JsonProjections, md)
		ep.CROE5yr = Croe(5.0, ep.JsonProjections, md)
		ep.CAGR10yr = Cagr(10.0, ep.JsonProjections, md)
		ep.CROE10yr = Croe(10.0, ep.JsonProjections, md)
	}

	if len(summary) >= 5 {
		ep.Magic = ep.CAGR5yr
		for i := 0; i < 5; i++ {
			if summary[i].NetMgn < 0.10 || summary[i].LTDRatio > 3.5 || summary[i].EPS <= 0.0 {
				ep.Magic = 0.0
				break
			}
		}
	}

	return ep, nil
}

func EnrichedProjections(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Read-EnrichedProjections", w, r.URL.Query())

	db, err := cmn.DbConnect()
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	foo := api.JsonProjections{}
	var projections []api.JsonProjections
	err = db.Select(&projections, api.JsonToSelect(foo, "projections", ""))
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	var ret []api.JsonEnrichedProjections
	for p := range projections {
		ep, err := EnrichProjections(projections[p])
		if err != nil {
			cmn.ErrorHttp(err, w, http.StatusInternalServerError)
			return
		}
		ret = append(ret, ep)
	}

	sort.Slice(ret, func(i, j int) bool {
		return ret[i].CAGR5yr > ret[j].CAGR5yr
	})

	json.NewEncoder(w).Encode(&ret)
	cmn.Exit("Read-EnrichedProjections", &ret)
}

func EnrichedProjectionsByID(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Read-EnrichedProjectionsByID", w, r.URL.Query())

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
	ep, err := EnrichProjections(p)
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
	cmn.Enter("Read-EnrichedProjectionsJournalByProjectionsID", w, r.URL.Query())

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
		return cmn.DateStringToTime(ret[i].Date).Before(cmn.DateStringToTime(ret[j].Date))
	})

	json.NewEncoder(w).Encode(&ret)

	cmn.Exit("Read-EnrichedProjectionsJournalByProjectionsID", ret)
}

func ProjectionsByID(w http.ResponseWriter, r *http.Request) {
	var ret api.JsonProjections
	RestHandleGet(w, r, "Read-ProjectionsBySymbol", &ret, ret, "projections")
}

func MarketData(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("MarketData", w, r.URL.Query())

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
	cmn.Enter("Read-MDHYearSummaryBySymbol", w, r.URL.Query())

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
	cmn.Enter("Read-MDHByRefDataIDDate", w, r.URL.Query())

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
	cmn.Enter("Read-MDHBySymbol", w, r.URL.Query())

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
	cmn.Enter("RefDataFocus", w, r.URL.Query())

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
	cmn.Enter("RefData", w, r.URL.Query())

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
	cmn.Enter("RefDataByID", w, r.URL.Query())

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
	cmn.Enter("RefDataBySymbol", w, r.URL.Query())

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
	cmn.Enter("Scalar", w, r.URL.Query())

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
	cmn.Enter("SimfinIncomeByID", w, r.URL.Query())

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
	cmn.Enter("SimfinIncomeByTicker", w, r.URL.Query())
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
	cmn.Enter("IncomeByTicker", w, r.URL.Query())
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
	cmn.Enter("SimfinBalanceByID", w, r.URL.Query())

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
	cmn.Enter("SimfinBalanceByTicker", w, r.URL.Query())
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
	cmn.Enter("BalanceByTicker", w, r.URL.Query())
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
	cmn.Enter("SimfinCashflowByID", w, r.URL.Query())

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
	cmn.Enter("SimfinCashflowByTicker", w, r.URL.Query())
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
	cmn.Enter("CashflowByTicker", w, r.URL.Query())
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

func SummaryByTicker(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Read-SummaryByTicker", w, r.URL.Query())
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
	cmn.Enter("Read-Mergers", w, r.URL.Query())
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")

	db, err := cmn.DbConnect()
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	foo := api.JsonMerger{}
	ret := []api.JsonMerger{}
	err = db.Select(&ret, fmt.Sprintf("%s", api.JsonToSelect(foo, "mergers", "")))
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	cmn.Exit("Read-Mergers", ret)
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

	fees := 0.005
	if strings.Contains(em.TargetTicker, ".HK") {
		fees = (0.0008 + 0.0013) * md.Last // 8 bps commison and 13 bps stamp on each side
	}

	em.MarketPositiveReturn = cmn.Round((em.DealPrice+em.Dividends-fees)/md.Last-1, 0.0001)
	em.MarketNetReturn = cmn.Round(
		((em.DealPrice+em.Dividends-fees-md.Last)*em.Confidence-(md.Last-em.FailPrice-em.Dividends+2*fees)*(1-em.Confidence))/md.Last, 0.0001)
	close_time := cmn.DateStringToTime(em.CloseDate)
	annualize_multiple := 365 / (close_time.Sub(time.Now()).Hours() / 24)
	em.MarketPositiveReturnAnnualized = cmn.Round(annualize_multiple*em.MarketPositiveReturn, 0.0001)
	em.MarketNetReturnAnnualized = cmn.Round(annualize_multiple*em.MarketNetReturn, 0.0001)
	return em, nil
}

func EnrichedMergers(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Read-EnrichedMergers", w, r.URL.Query())
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

		ret = append(ret, em)
	}
	sort.Slice(ret, func(i, j int) bool {
		if ret[i].MarketNetReturnAnnualized == ret[j].MarketNetReturnAnnualized {
			return ret[i].MarketPositiveReturnAnnualized > ret[j].MarketPositiveReturnAnnualized
		}
		return ret[i].MarketNetReturnAnnualized > ret[j].MarketNetReturnAnnualized
	})
	json.NewEncoder(w).Encode(&ret)

	cmn.Exit("Read-EnrichedMergers", ret)
}

func EnrichedMergersByID(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Read-EnrichedMergersByID", w, r.URL.Query())

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

type EnrichedMergersJournalByMergerIDInput struct {
	MergerID int `schema:"mergerId"`
}

func EnrichedMergersJournalByMergerID(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Read-EnrichedMergersJournalByMergerID", w, r.URL.Query())
	w.Header().Set("Access-Control-Allow-Origin", "*")

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
	log.Fatal(http.ListenAndServe(":8081", cmn.CorsMiddleware(router)))
}
