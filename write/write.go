package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/gorilla/schema"
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
	router.HandleFunc("/blue-lion/write/enriched-mergers-journal/{id}", EnrichedMergersJournalByIDDelete).Methods("DELETE")
	router.HandleFunc("/blue-lion/write/enriched-mergers-journal/{id}", EnrichedMergersJournalByID).Methods("PUT")
	router.HandleFunc("/blue-lion/write/enriched-mergers-journal", EnrichedMergersJournal).Methods("POST")
	router.HandleFunc("/blue-lion/write/enriched-projections-journal/{id}", EnrichedProjectionsJournalByIDDelete).Methods("DELETE")
	router.HandleFunc("/blue-lion/write/enriched-projections-journal/{id}", EnrichedProjectionsJournalByID).Methods("PUT")
	router.HandleFunc("/blue-lion/write/enriched-projections-journal", EnrichedProjectionsJournal).Methods("POST")
	router.HandleFunc("/blue-lion/write/portfolios/{id}", PortfoliosByID).Methods("PUT")
	router.HandleFunc("/blue-lion/write/positions/{id}", PositionsByID).Methods("PUT")
	router.HandleFunc("/blue-lion/write/positions", Positions).Methods("POST")
	router.HandleFunc("/blue-lion/write/portfolios-history/{id}", PortfoliosHistoryByID).Methods("PUT")
	router.HandleFunc("/blue-lion/write/portfolios-history", PortfoliosHistoryByDateDelete).Methods("DELETE")
	router.HandleFunc("/blue-lion/write/portfolios-history", PortfoliosHistory).Methods("POST")
	router.HandleFunc("/blue-lion/write/positions-history/{id}", PositionsHistoryByID).Methods("PUT")
	router.HandleFunc("/blue-lion/write/positions-history", PositionsHistoryByDateDelete).Methods("DELETE")
	router.HandleFunc("/blue-lion/write/positions-history", PositionsHistory).Methods("POST")
	router.HandleFunc("/blue-lion/write/transactions", Transactions).Methods("POST")
	router.HandleFunc("/blue-lion/write/transactions/{id}", TransactionsByID).Methods("PUT")
}

func RestHandlePost(w http.ResponseWriter, r *http.Request, msg string, ptr interface{}, obj interface{}, table string) {
	cmn.Enter(msg, w, r)

	err := json.NewDecoder(r.Body).Decode(ptr)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	err = cmn.DbNamedExec(api.JsonToNamedInsert(obj, table), ptr)
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
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	cmn.Exit(msg, http.StatusOK)
}

func RestHandleDeleteByDate(w http.ResponseWriter, r *http.Request, msg string, table string) {
	cmn.Enter(msg, w, r)

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

	_, err = db.Exec(fmt.Sprintf("DELETE FROM %s where date='%s'", table, args.Date))
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
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

func Transactions(w http.ResponseWriter, r *http.Request) {
	var ret api.JsonTransaction
	RestHandlePost(w, r, "Write-Transactions", &ret, ret, "transactions")
}

func TransactionsByID(w http.ResponseWriter, r *http.Request) {
	var ret api.JsonTransaction
	RestHandlePut(w, r, "Write-TransactionsByID", &ret, ret, "transactions")
}

func Projections(w http.ResponseWriter, r *http.Request) {
	var ret api.JsonProjections
	RestHandlePost(w, r, "Write-Projections", &ret, ret, "projections")

	// Reload the projection so ID is populated correctly (unfortunately Postgres driver does not handle
	// getting this without simply doing a reselect)
	var refData api.JsonRefData
	err := api.RefDataByID(ret.RefDataID, &refData)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
	err = api.ProjectionsBySymbol(refData.Symbol, &ret)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
	err = api.ProjectionsUpdateByID(ret.ID)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
}

func ProjectionsByID(w http.ResponseWriter, r *http.Request) {
	var ret api.JsonProjections
	RestHandlePut(w, r, "Write-ProjectionsByID", &ret, ret, "projections")
	api.ProjectionsUpdateByID(ret.ID)
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

func PositionsHistoryByID(w http.ResponseWriter, r *http.Request) {
	var ret api.JsonPositionHistory
	RestHandlePut(w, r, "Write-PositionsHistoryByID", &ret, ret, "positions_history")
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

func EnrichedMergersJournalByIDDelete(w http.ResponseWriter, r *http.Request) {
	RestHandleDelete(w, r, "Write-EnrichedMergersJournalByIDDelete", "mergers_journal")
}

func EnrichedMergersJournalByID(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Write-EnrichedMergersJournalByID", w, r)
	var input api.JsonEnrichedMergerJournal
	params := mux.Vars(r)
	id := params["id"]
	err := json.NewDecoder(r.Body).Decode(&input)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	db, err := cmn.DbConnect()
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	// Only the entry can be updated
	_, err = db.Exec("UPDATE mergers_journal SET entry=$1 WHERE id=$2", input.Entry, id)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(input)
	cmn.Exit("Write-EnrichedMergersJournalByID", input)
}

func EnrichedMergersJournal(w http.ResponseWriter, r *http.Request) {
	var input api.JsonEnrichedMergerJournal
	cmn.Enter("Write-EnrichedMergersJournal", w, r)

	err := json.NewDecoder(r.Body).Decode(&input)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	// We've been passed the mergerId, date, and entry, retrieve other info directly from the mergers table
	var em api.JsonEnrichedMerger
	err = api.EnrichedMergersByID(input.MergerID, &em)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
	ret := api.JsonEnrichedMergerJournal{JsonEnrichedMerger: em}
	ret.MergerID = ret.ID
	ret.ID = 0
	ret.Date = input.Date
	ret.Entry = input.Entry

	// First, tweak the date on the merger (on every journal entry the user is reaffirming the merger)
	err = cmn.DbNamedExec(fmt.Sprintf("%s WHERE id=%d", api.JsonToNamedUpdate(ret.JsonMerger, "mergers"), ret.MergerID), &ret.JsonMerger)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	err = cmn.DbNamedExec(api.JsonToNamedInsert(ret, "mergers_journal"), &ret)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(&ret)
	cmn.Exit("Write-EnrichedMergersJournal", &ret)
}

func EnrichedProjectionsJournalByIDDelete(w http.ResponseWriter, r *http.Request) {
	RestHandleDelete(w, r, "Write-EnrichedProjectionsJournalByIDDelete", "projections_journal")
}

func PortfoliosHistoryByDateDelete(w http.ResponseWriter, r *http.Request) {
	RestHandleDeleteByDate(w, r, "Write-PortfoliosHistoryByDateDelete", "portfolios_history")
}

func PositionsHistoryByDateDelete(w http.ResponseWriter, r *http.Request) {
	RestHandleDeleteByDate(w, r, "Write-PositionsHistoryByDateDelete", "positions_history")
}

func EnrichedProjectionsJournalByID(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Write-EnrichedProjectionsJournalByID", w, r)
	var input api.JsonEnrichedProjectionsJournal
	params := mux.Vars(r)
	id := params["id"]
	err := json.NewDecoder(r.Body).Decode(&input)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	db, err := cmn.DbConnect()
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	// Only the entry can be updated
	_, err = db.Exec("UPDATE projections_journal SET entry=$1 WHERE id=$2", input.Entry, id)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(input)
	cmn.Exit("Write-EnrichedProjectionsJournalByID", input)
}

func EnrichedProjectionsJournal(w http.ResponseWriter, r *http.Request) {
	var input api.JsonEnrichedProjectionsJournal
	cmn.Enter("Write-EnrichedProjectionsJournal", w, r)

	err := json.NewDecoder(r.Body).Decode(&input)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	// We've been passed the projectionsId, date and entry, retrieve other info directly from the projections table
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

	// First, tweak the date on the projections (on every journal entry the user is reaffirming the projections)
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

	// Notify out that a projection has been updated
	api.ProjectionsUpdateByID(ret.ProjectionsID)

	json.NewEncoder(w).Encode(&ret)
	cmn.Exit("Write-EnrichedProjectionsJournal", &ret)
}

func main() {
	log.Println("Listening on http://localhost:8083/blue-lion/write")
	router := mux.NewRouter().StrictSlash(true)
	setupRouter(router)
	log.Fatal(http.ListenAndServe(":8083", cmn.CorsMiddleware(router)))
}
