package main

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/gorilla/schema"
	"github.com/scanlom/Sanomaru/api"
	"github.com/scanlom/Sanomaru/cmn"
	"log"
	"math"
	"net/http"
)

func setupRouter(router *mux.Router) {
	router.HandleFunc("/blue-lion/read/market-data/{id}", MarketDataByID).Methods("GET")
	router.HandleFunc("/blue-lion/read/market-data", MarketDataBySymbol).Queries("symbol", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/market-data", MarketData).Methods("GET")
	router.HandleFunc("/blue-lion/read/market-data-historical/year-summary", MDHYearSummaryBySymbol).Queries("symbol", "").Methods("GET")

	router.HandleFunc("/blue-lion/read/ref-data/{id}", RefDataByID).Methods("GET")
	router.HandleFunc("/blue-lion/read/ref-data", RefDataBySymbol).Queries("symbol", "").Methods("GET")
	router.HandleFunc("/blue-lion/read/ref-data", RefData).Methods("GET")

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
	cmn.Enter("MarketDataByID", r.URL.Query())

	params := mux.Vars(r)
	id := params["id"]

	db, err := cmn.DbConnect()
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret := api.JsonMarketData{}
	err = db.Get(&ret, fmt.Sprintf("SELECT id, ref_data_id, last FROM market_data where id=%s", id))
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	cmn.Exit("MarketDataByID", ret)
}

func MarketDataBySymbol(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("MarketDataBySymbol", r.URL.Query())

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

	ret := api.JsonMarketData{}
	err = db.Get(&ret, fmt.Sprintf("SELECT id, ref_data_id, last FROM market_data where ref_data_id=%d", refDataID))
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	cmn.Exit("MarketDataBySymbol", ret)
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
	err = db.Get(&ret, fmt.Sprintf("SELECT ref_data_id, MAX(close) AS high, MIN(close) AS low FROM  market_data_historical "+
		"WHERE ref_data_id=%d AND date<='%s' and date > TO_DATE('%s','YYYY-MM-DD') - INTERVAL '1 year' "+
		"GROUP BY ref_data_id", refDataID, args.Date, args.Date))
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	cmn.Exit("Read-MDHYearSummaryBySymbol", ret)
}

func RefData(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("RefData", r.URL.Query())

	db, err := cmn.DbConnect()
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret := []api.JsonRefData{}
	err = db.Select(&ret, "SELECT id, symbol, symbol_alpha_vantage FROM ref_data where active=true")
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
	err = db.Get(&ret, fmt.Sprintf("SELECT id, symbol, symbol_alpha_vantage FROM ref_data where id=%s", id))
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	cmn.Exit("RefDataByID", ret)
}

type RefDataInput struct {
	Symbol string `schema:"symbol"`
}

func RefDataBySymbol(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("RefDataBySymbol", r.URL.Query())

	args := new(RefDataInput)
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
	err = db.Get(&ret, fmt.Sprintf("SELECT id, symbol, symbol_alpha_vantage FROM ref_data where symbol='%s'", args.Symbol))
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
	err = db.Get(&s, "SELECT name, value FROM scalars where name='FX_JPY'")
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
	err = db.Get(&ret, api.JsonToSelect(ret, "simfin_income")+fmt.Sprintf(" WHERE id=%s", id))
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
	err = db.Select(&ret, api.JsonToSelect(api.JsonSimfinIncome{}, "simfin_income")+fmt.Sprintf(" WHERE ticker='%s' ORDER BY fiscal_year DESC", args.Ticker))
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
		i.EPS = cmn.Round(float64(i.NetIncome)/float64(i.SharesDiluted), 0.01)
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
	err = db.Get(&ret, api.JsonToSelect(ret, "simfin_balance")+fmt.Sprintf(" WHERE id=%s", id))
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
	err = db.Select(&ret, api.JsonToSelect(api.JsonSimfinBalance{}, "simfin_balance")+fmt.Sprintf(" WHERE ticker='%s' ORDER BY fiscal_year DESC", args.Ticker))
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
		//i.EPS = cmn.Round(float64(i.NetIncome)/float64(i.SharesDiluted), 0.01)
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
	err = db.Get(&ret, api.JsonToSelect(ret, "simfin_cashflow")+fmt.Sprintf(" WHERE id=%s", id))
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
	err = db.Select(&ret, api.JsonToSelect(api.JsonSimfinCashflow{}, "simfin_cashflow")+fmt.Sprintf(" WHERE ticker='%s' ORDER BY fiscal_year DESC", args.Ticker))
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
		i.DPS = -1.0 * cmn.Round(float64(i.DividendsPaid)/float64(i.SharesDiluted), 0.01)
		ret = append(ret, i)
	}
	json.NewEncoder(w).Encode(&ret)

	cmn.Exit("CashflowByTicker", ret)
}

type HeadlineInput struct {
	Ticker string `schema:"ticker"`
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

	var summary []api.JsonSummary
	err = api.SummaryByTicker(args.Ticker, &summary)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	var epsCagr5yr, epsCagr10yr float64
	var peHighMmo5yr, peLowMmo5yr int
	var sumH, sumL, maxH, maxL, minH, minL int
	minH = math.MaxInt64
	minL = math.MaxInt64
	if len(summary) > 5 {
		epsCagr5yr = math.Pow(summary[0].EPS/summary[5].EPS, 0.2) - 1.0
	}
	if len(summary) > 10 {
		epsCagr10yr = math.Pow(summary[0].EPS/summary[10].EPS, 0.1) - 1.0
	}

	if len(summary) >= 5 {
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
		}
		peHighMmo5yr = int(math.Round(float64(sumH-maxH-minH) / 3.0))
		peLowMmo5yr = int(math.Round(float64(sumL-maxL-minL) / 3.0))
	}

	ret := []api.JsonHeadline{api.JsonHeadline{Ticker: args.Ticker, EPSCagr5yr: epsCagr5yr, EPSCagr10yr: epsCagr10yr, PEHighMMO5yr: peHighMmo5yr, PELowMMO5yr: peLowMmo5yr}}
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
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		s := api.JsonSummary{}
		s.ReportDate = income[i].ReportDate
		s.EPS = income[i].EPS
		s.DPS = cashflow[i].DPS
		if s.EPS != 0.0 {
			s.PEHigh = int(math.Round(mdhYearSummary.High / s.EPS))
			s.PELow = int(math.Round(mdhYearSummary.Low / s.EPS))
		}
		// For ROE and ROA, need to make sure we're not on the last year
		if i < len(income)-1 {
			s.ROE = float64(income[i].NetIncome) / (float64(balance[i].TotalEquity+balance[i+1].TotalEquity) / 2.0)
			s.ROA = float64(income[i].NetIncome) / (float64(balance[i].TotalAssets+balance[i+1].TotalAssets) / 2.0)
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
