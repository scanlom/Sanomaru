package main

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/gorilla/schema"
	"github.com/scanlom/Sanomaru/api"
	"github.com/scanlom/Sanomaru/cmn"
	"log"
	"net/http"
)

func setupRouter(router *mux.Router) {
	router.HandleFunc("/blue-lion/write/market-data/{id}", MarketDataByID).Methods("PUT")
	router.HandleFunc("/blue-lion/write/market-data", MarketData).Methods("POST")
	router.HandleFunc("/blue-lion/write/market-data-historical", MarketDataHistorical).Methods("POST")
	router.HandleFunc("/blue-lion/write/market-data-historical", MarketDataHistoricalBySymbolDelete).Queries("symbol", "").Methods("DELETE")
	router.HandleFunc("/blue-lion/write/simfin-income", SimfinIncome).Methods("POST")
	router.HandleFunc("/blue-lion/write/simfin-balance", SimfinBalance).Methods("POST")
	router.HandleFunc("/blue-lion/write/simfin-cashflow", SimfinCashflow).Methods("POST")
}

func RestHandlePost(w http.ResponseWriter, r *http.Request, msg string, ptr interface{}, obj interface{}, table string) {
	cmn.Enter(msg, r.URL.Query())
	w.Header().Set("Content-Type", "application/json")
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

	_, err = db.NamedExec(api.JsonToNamedInsert(obj, table), ptr)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(ptr)
	cmn.Exit(msg, ptr)
}

func MarketData(w http.ResponseWriter, r *http.Request) {
	var ret api.JsonMarketData
	RestHandlePost(w, r, "Write-MarketData", &ret, ret, "market_data")
}

func MarketDataByID(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("MarketDataByID", r.URL.Query())
	w.Header().Set("Content-Type", "application/json")
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

func MarketDataHistoricalBySymbolDelete(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Write-MarketDataHistoricalBySymbolDelete", r.URL.Query())

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

	_, err = db.Exec(fmt.Sprintf("DELETE FROM market_data_historical where ref_data_id=%d", refDataID))
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)

	cmn.Exit("Write-MarketDataHistoricalBySymbolDelete", http.StatusOK)
}

func SimfinIncome(w http.ResponseWriter, r *http.Request) {
	var ret api.JsonSimfinIncome
	RestHandlePost(w, r, "Write-SimfinIncome", &ret, ret, "simfin_income")
}

func SimfinBalance(w http.ResponseWriter, r *http.Request) {
	var ret api.JsonSimfinBalance
	RestHandlePost(w, r, "Write-SimfinBalance", &ret, ret, "simfin_balance")
}

func SimfinCashflow(w http.ResponseWriter, r *http.Request) {
	var ret api.JsonSimfinCashflow
	RestHandlePost(w, r, "Write-SimfinCashflow", &ret, ret, "simfin_cashflow")
}

func main() {
	router := mux.NewRouter().StrictSlash(true)
	setupRouter(router)
	log.Fatal(http.ListenAndServe(":8083", router))
}
