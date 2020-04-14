package main

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/scanlom/Sanomaru/api"
	"github.com/scanlom/Sanomaru/cmn"
	"log"
	"net/http"
)

func setupRouter(router *mux.Router) {
	router.HandleFunc("/blue-lion/write/market-data/{id}", MarketDataByID).Methods("PUT")
	router.HandleFunc("/blue-lion/write/market-data", MarketData).Methods("POST")
	router.HandleFunc("/blue-lion/write/simfin-income", SimfinIncome).Methods("POST")
	router.HandleFunc("/blue-lion/write/simfin-balance", SimfinBalance).Methods("POST")
	router.HandleFunc("/blue-lion/write/simfin-cashflow", SimfinCashflow).Methods("POST")
}

func MarketData(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Write-MarketData", r.URL.Query())
	w.Header().Set("Content-Type", "application/json")
	var ret api.JsonMarketData
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

	_, err = db.Exec(fmt.Sprintf("INSERT INTO market_data (ref_data_id, last) VALUES (%d, %f)", ret.RefDataID, ret.Last))
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	err = db.Get(&ret, fmt.Sprintf("SELECT id, ref_data_id, last FROM market_data WHERE ref_data_id=%d", ret.RefDataID))
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(&ret)
	cmn.Exit("Write-MarketData", ret)
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

func SimfinIncome(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Write-SimfinIncome", r.URL.Query())
	w.Header().Set("Content-Type", "application/json")
	var ret api.JsonSimfinIncome
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

	log.Println(ret)
	_, err = db.NamedExec(api.JsonToNamedInsert(ret, "simfin_income"), ret)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	// MSTODO - How should I return db entries from a post?
	/*err = db.Get(&ret, fmt.Sprintf("SELECT id, ref_data_id, last FROM market_data WHERE ref_data_id=%d", ret.RefDataID))
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}*/

	json.NewEncoder(w).Encode(&ret)
	cmn.Exit("Write-SimfinIncome", ret)
}

func SimfinBalance(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Write-SimfinBalance", r.URL.Query())
	w.Header().Set("Content-Type", "application/json")
	var ret api.JsonSimfinBalance
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

	log.Println(ret)
	_, err = db.NamedExec(api.JsonToNamedInsert(ret, "simfin_balance"), ret)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	// MSTODO - How should I return db entries from a post?
	/*err = db.Get(&ret, fmt.Sprintf("SELECT id, ref_data_id, last FROM market_data WHERE ref_data_id=%d", ret.RefDataID))
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}*/

	json.NewEncoder(w).Encode(&ret)
	cmn.Exit("Write-SimfinBalance", ret)
}

func SimfinCashflow(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Write-SimfinCashflow", r.URL.Query())
	w.Header().Set("Content-Type", "application/json")
	var ret api.JsonSimfinCashflow
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

	log.Println(ret)
	_, err = db.NamedExec(api.JsonToNamedInsert(ret, "simfin_cashflow"), ret)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	// MSTODO - How should I return db entries from a post?
	/*err = db.Get(&ret, fmt.Sprintf("SELECT id, ref_data_id, last FROM market_data WHERE ref_data_id=%d", ret.RefDataID))
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}*/

	json.NewEncoder(w).Encode(&ret)
	cmn.Exit("Write-SimfinCashflow", ret)
}

func main() {
	router := mux.NewRouter().StrictSlash(true)
	setupRouter(router)
	log.Fatal(http.ListenAndServe(":8083", router))
}
