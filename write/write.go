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
	router.HandleFunc("/blue-lion/write/enriched-mergers", EnrichedMergers).Methods("POST")
	router.HandleFunc("/blue-lion/write/enriched-mergers-journal", EnrichedMergersJournal).Methods("POST")
	router.HandleFunc("/blue-lion/write/enriched-projections-journal", EnrichedProjectionsJournal).Methods("POST")
	router.HandleFunc("/blue-lion/write/portfolios/{id}", PortfoliosByID).Methods("PUT")
	router.HandleFunc("/blue-lion/write/positions/{id}", PositionsByID).Methods("PUT")
	router.HandleFunc("/blue-lion/write/positions", Positions).Methods("POST")
	router.HandleFunc("/blue-lion/write/portfolios-history/{id}", PortfoliosHistoryByID).Methods("PUT")
	router.HandleFunc("/blue-lion/write/portfolios-history", PortfoliosHistory).Methods("POST")
	router.HandleFunc("/blue-lion/write/positions-history", PositionsHistory).Methods("POST")
}

func RestHandlePost(w http.ResponseWriter, r *http.Request, msg string, ptr interface{}, obj interface{}, table string) {
	cmn.Enter(msg, w, r)

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

func RestHandlePut(w http.ResponseWriter, r *http.Request, msg string, ptr interface{}, obj interface{}, table string) {
	cmn.Enter(msg, w, r)

	params := mux.Vars(r)
	id := params["id"]
	err := json.NewDecoder(r.Body).Decode(ptr)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	err = cmn.DbNamedExec(fmt.Sprintf("%s WHERE id=%s", api.JsonToNamedUpdate(obj, table), id), ptr)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(ptr)
	cmn.Exit(msg, ptr)
}

func RestHandleDelete(w http.ResponseWriter, r *http.Request, msg string, table string) {
	cmn.Enter(msg, w, r)

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
	cmn.Enter("MarketDataByID", w, r)

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
	RestHandlePost(w, r, "Write-MarketDataHistorical", &ret, ret, "market_data_historical")
}

func MarketDataHistoricalByID(w http.ResponseWriter, r *http.Request) {
	var ret api.JsonMarketDataHistorical
	RestHandlePut(w, r, "Write-MarketDataHistoricalByID", &ret, ret, "market_data_historical")
}

func RefData(w http.ResponseWriter, r *http.Request) {
	var ret api.JsonRefData
	RestHandlePost(w, r, "Write-RefData", &ret, ret, "ref_data")
}

func RefDataByID(w http.ResponseWriter, r *http.Request) {
	var ret api.JsonRefData
	RestHandlePut(w, r, "Write-RefDataByID", &ret, ret, "ref_data")
}

func Projections(w http.ResponseWriter, r *http.Request) {
	var ret api.JsonProjections
	RestHandlePost(w, r, "Write-Projections", &ret, ret, "projections")
}

func ProjectionsByID(w http.ResponseWriter, r *http.Request) {
	var ret api.JsonProjections
	RestHandlePut(w, r, "Write-ProjectionsByID", &ret, ret, "projections")
}

func PortfoliosByID(w http.ResponseWriter, r *http.Request) {
	var ret api.JsonPortfolio
	RestHandlePut(w, r, "Write-PortfoliosByID", &ret, ret, "portfolios")
}

func PortfoliosHistoryByID(w http.ResponseWriter, r *http.Request) {
	var ret api.JsonPortfolioHistory
	RestHandlePut(w, r, "Write-PortfoliosHistoryByID", &ret, ret, "portfolios_history")
}

func Positions(w http.ResponseWriter, r *http.Request) {
	var ret api.JsonPosition
	RestHandlePost(w, r, "Write-Positions", &ret, ret, "positions")
}

func PortfoliosHistory(w http.ResponseWriter, r *http.Request) {
	var ret api.JsonPortfolioHistory
	RestHandlePost(w, r, "Write-PortfoliosHistory", &ret, ret, "portfolios_history")
}

func PositionsHistory(w http.ResponseWriter, r *http.Request) {
	var ret api.JsonPositionHistory
	RestHandlePost(w, r, "Write-PositionsHistory", &ret, ret, "positions_history")
}

func PositionsByID(w http.ResponseWriter, r *http.Request) {
	var ret api.JsonPosition
	RestHandlePut(w, r, "Write-PositionsByID", &ret, ret, "positions")
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

func EnrichedMergers(w http.ResponseWriter, r *http.Request) {
	var input api.JsonEnrichedMerger
	cmn.Enter("Write-EnrichedMergers", w, r)

	err := json.NewDecoder(r.Body).Decode(&input)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	// We've been passed the target and acquirer symbols, so need to look up the id's
	var targetRefData api.JsonRefData
	err = api.RefDataBySymbol(input.TargetTicker, &targetRefData)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	var acquirerRefData api.JsonRefData
	err = api.RefDataBySymbol(input.AcquirerTicker, &acquirerRefData)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	// Aside from ref data, everything else from the merger passed in
	ret := api.JsonMerger(input.JsonMerger)
	ret.TargetRefDataID = targetRefData.ID
	ret.AcquirerRefDataID = acquirerRefData.ID

	db, err := cmn.DbConnect()
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	_, err = db.NamedExec(api.JsonToNamedInsert(ret, "mergers"), &ret)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(&ret)
	cmn.Exit("Write-EnrichedMergers", &ret)
}

func EnrichedMergersJournal(w http.ResponseWriter, r *http.Request) {
	var input api.JsonEnrichedMergerJournal
	cmn.Enter("Write-EnrichedMergersJournal", w, r)

	err := json.NewDecoder(r.Body).Decode(&input)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

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

func EnrichedProjectionsJournal(w http.ResponseWriter, r *http.Request) {
	var input api.JsonEnrichedProjectionsJournal
	cmn.Enter("Write-EnrichedProjectionsJournal", w, r)

	err := json.NewDecoder(r.Body).Decode(&input)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	// We've been passed the projectionsId and entry, retrieve other info directly from the projections table
	var ep api.JsonEnrichedProjections
	err = api.EnrichedProjectionsByID(input.ProjectionsID, &ep)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
	ret := api.JsonEnrichedProjectionsJournal{JsonEnrichedProjections: ep}
	ret.Date = input.Date
	ret.Entry = input.Entry

	db, err := cmn.DbConnect()
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	// First, tweek the date on the projections (on every journal entry the user is reaffirming the projections)
	_, err = db.NamedExec(fmt.Sprintf("%s WHERE id=%d", api.JsonToNamedUpdate(ret.JsonProjections, "projections"), ret.ID), &ret.JsonProjections)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	// Then, insert the projections journal (remember to flip the id, as the original one is for the projections rather than the journal)
	ret.ProjectionsID = ret.ID
	ret.ID = 0
	_, err = db.NamedExec(api.JsonToNamedInsert(ret, "projections_journal"), &ret)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(&ret)
	cmn.Exit("Write-EnrichedProjectionsJournal", &ret)
}

func main() {
	log.Println("Listening on http://localhost:8083/blue-lion/write")
	router := mux.NewRouter().StrictSlash(true)
	setupRouter(router)
	log.Fatal(http.ListenAndServe(":8083", cmn.CorsMiddleware(router)))
}
