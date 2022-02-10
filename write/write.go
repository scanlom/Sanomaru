package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/scanlom/Sanomaru/api"
	"github.com/scanlom/Sanomaru/cmn"
)

func setupRouter(router *mux.Router) {
	router.HandleFunc("/blue-lion/write/market-data/{id}", MarketDataByID).Methods("PUT")
	router.HandleFunc("/blue-lion/write/market-data", MarketData).Methods("POST")
	router.HandleFunc("/blue-lion/write/market-data-historical", MarketDataHistorical).Methods("POST")
	router.HandleFunc("/blue-lion/write/market-data-historical/{id}", MarketDataHistoricalByID).Methods("PUT")
	router.HandleFunc("/blue-lion/write/ref-data/{id}", RefDataByID).Methods("PUT")
	router.HandleFunc("/blue-lion/write/ref-data", RefData).Methods("POST")
	router.HandleFunc("/blue-lion/write/projections/{id}", ProjectionsByID).Methods("PUT")
	router.HandleFunc("/blue-lion/write/projections", Projections).Methods("POST")
	router.HandleFunc("/blue-lion/write/simfin-income", SimfinIncome).Methods("POST")
	router.HandleFunc("/blue-lion/write/simfin-income/{id}", SimfinIncomeByIDDelete).Methods("DELETE")
	router.HandleFunc("/blue-lion/write/simfin-balance", SimfinBalance).Methods("POST")
	router.HandleFunc("/blue-lion/write/simfin-balance/{id}", SimfinBalanceByIDDelete).Methods("DELETE")
	router.HandleFunc("/blue-lion/write/simfin-cashflow", SimfinCashflow).Methods("POST")
	router.HandleFunc("/blue-lion/write/simfin-cashflow/{id}", SimfinCashflowByIDDelete).Methods("DELETE")
	router.HandleFunc("/blue-lion/write/mergers/{id}", MergersByID).Methods("PUT")
	router.HandleFunc("/blue-lion/write/enriched-mergers-journal", EnrichedMergersJournal).Methods("POST")
}

func RestHandlePost(w http.ResponseWriter, r *http.Request, msg string, ptr interface{}, obj interface{}, table string) {
	cmn.Enter(msg, r.URL.Query())
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")
	err := json.NewDecoder(r.Body).Decode(ptr)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	cmn.LogPost(msg, ptr)
	db, err := cmn.DbConnect()
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	_, err = db.NamedExec(api.JsonToNamedInsert(obj, table), ptr)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(ptr)
	cmn.Exit(msg, ptr)
}

func RestHandlePut(w http.ResponseWriter, r *http.Request, msg string, ptr interface{}, obj interface{}, table string) {
	cmn.Enter(msg, r.URL.Query())
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")
	params := mux.Vars(r)
	id := params["id"]
	err := json.NewDecoder(r.Body).Decode(ptr)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	db, err := cmn.DbConnect()
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	_, err = db.NamedExec(fmt.Sprintf("%s WHERE id=%s", api.JsonToNamedUpdate(obj, table), id), ptr)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(ptr)
	cmn.Exit(msg, ptr)
}

func RestHandleDelete(w http.ResponseWriter, r *http.Request, msg string, table string) {
	cmn.Enter(msg, r.URL.Query())
	params := mux.Vars(r)
	id := params["id"]

	db, err := cmn.DbConnect()
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	_, err = db.Exec(fmt.Sprintf("DELETE FROM %s where id=%s", table, id))
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	cmn.Exit(msg, http.StatusOK)
}

func MarketData(w http.ResponseWriter, r *http.Request) {
	var ret api.JsonMarketData
	RestHandlePost(w, r, "Write-MarketData", &ret, ret, "market_data")
}

func MarketDataByID(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("MarketDataByID", r.URL.Query())
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	params := mux.Vars(r)
	id := params["id"]

	var ret api.JsonMarketData
	log.Println(r.Body)
	err := json.NewDecoder(r.Body).Decode(&ret)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	db, err := cmn.DbConnect()
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	_, err = db.Exec("UPDATE market_data SET last=$1 WHERE id=$2", ret.Last, id)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(ret)
	cmn.Exit("MarketDataByID", ret)
}

func MarketDataHistorical(w http.ResponseWriter, r *http.Request) {
	var ret api.JsonMarketDataHistorical
	log.Println(ret)
	RestHandlePost(w, r, "Write-MarketDataHistorical", &ret, ret, "market_data_historical")
}

func MarketDataHistoricalByID(w http.ResponseWriter, r *http.Request) {
	var ret api.JsonMarketDataHistorical
	log.Println(ret)
	RestHandlePut(w, r, "Write-MarketDataHistoricalByID", &ret, ret, "market_data_historical")
}

func RefData(w http.ResponseWriter, r *http.Request) {
	var ret api.JsonRefData
	RestHandlePost(w, r, "Write-RefData", &ret, ret, "ref_data")
}

func RefDataByID(w http.ResponseWriter, r *http.Request) {
	var ret api.JsonRefData
	log.Println(ret)
	RestHandlePut(w, r, "Write-RefDataByID", &ret, ret, "ref_data")
}

func Projections(w http.ResponseWriter, r *http.Request) {
	var ret api.JsonProjections
	RestHandlePost(w, r, "Write-Projections", &ret, ret, "projections")
}

func ProjectionsByID(w http.ResponseWriter, r *http.Request) {
	var ret api.JsonProjections
	log.Println(ret)
	RestHandlePut(w, r, "Write-ProjectionsByID", &ret, ret, "projections")
}

func SimfinIncome(w http.ResponseWriter, r *http.Request) {
	var ret api.JsonSimfinIncome
	RestHandlePost(w, r, "Write-SimfinIncome", &ret, ret, "simfin_income")
}

func SimfinIncomeByIDDelete(w http.ResponseWriter, r *http.Request) {
	RestHandleDelete(w, r, "Write-SimfinIncomeByIDDelete", "simfin_income")
}

func SimfinBalance(w http.ResponseWriter, r *http.Request) {
	var ret api.JsonSimfinBalance
	RestHandlePost(w, r, "Write-SimfinBalance", &ret, ret, "simfin_balance")
}

func SimfinBalanceByIDDelete(w http.ResponseWriter, r *http.Request) {
	RestHandleDelete(w, r, "Write-SimfinBalanceByIDDelete", "simfin_balance")
}

func SimfinCashflow(w http.ResponseWriter, r *http.Request) {
	var ret api.JsonSimfinCashflow
	RestHandlePost(w, r, "Write-SimfinCashflow", &ret, ret, "simfin_cashflow")
}

func SimfinCashflowByIDDelete(w http.ResponseWriter, r *http.Request) {
	RestHandleDelete(w, r, "Write-SimfinCashflowByIDDelete", "simfin_cashflow")
}

func MergersByID(w http.ResponseWriter, r *http.Request) {
	var ret api.JsonMerger
	log.Println(ret)
	RestHandlePut(w, r, "Write-MergersByID", &ret, ret, "mergers")
}

func EnrichedMergersJournal(w http.ResponseWriter, r *http.Request) {
	var input api.JsonEnrichedMergerJournal
	cmn.Enter("Write-EnrichedMergersJournal", r.URL.Query())
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")
	err := json.NewDecoder(r.Body).Decode(&input)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
	cmn.LogPost("Write-EnrichedMergersJournal", input)

	// We've been passed the mergerId and entry, retrieve other info directly from the mergers table
	var em api.JsonEnrichedMerger
	err = api.EnrichedMergersByID(input.MergerID, &em)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
	ret := api.JsonEnrichedMergerJournal{JsonEnrichedMerger: em}
	ret.MergerID = ret.ID
	ret.ID = 0
	ret.Entry = input.Entry

	db, err := cmn.DbConnect()
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	_, err = db.NamedExec(api.JsonToNamedInsert(ret, "mergers_journal"), &ret)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(&ret)
	cmn.Exit("Write-EnrichedMergersJournal", &ret)
}

func main() {
	log.Println("Listening on http://localhost:8083/blue-lion/write")
	router := mux.NewRouter().StrictSlash(true)
	setupRouter(router)
	log.Fatal(http.ListenAndServe(":8083", cmn.CorsMiddleware(router)))
}
