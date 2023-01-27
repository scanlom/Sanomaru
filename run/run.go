package main

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"

	"github.com/scanlom/Sanomaru/api"
	"github.com/scanlom/Sanomaru/cmn"
)

func setupRouter(router *mux.Router) {
	router.HandleFunc("/blue-lion/run/job-valuation-cut", JobValuationCut).Methods("GET")
	router.HandleFunc("/blue-lion/run/execute-book", ExecuteBook).Methods("POST")
	router.HandleFunc("/blue-lion/run/execute-roll-back", ExecuteRollBack).Methods("GET")
}

func JobValuationCut(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Run-JobValuationCut", w, r)

	CONST_FX_GBP := 76.34
	CONST_FX_HKD := 7.79
	CONST_FX_JPY := 104.01
	CONST_FX_SGD := 1.35
	rates := map[int]float64{
		/*"1373.HK"*/ 76: CONST_FX_HKD,
		/*"2788.HK"*/ 3288: CONST_FX_HKD,
		/*"6670.T"*/ 2451: CONST_FX_JPY,
		/*"MRO.L"*/ 97: CONST_FX_GBP,
		/*"BATS.L"*/ 3280: CONST_FX_GBP,
		/*"U11.SI"*/ 3126: CONST_FX_SGD,
	}

	// Update price, value, index for all active by price positions
	var positions []api.JsonPosition
	err := api.Positions(&positions)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	for i := range positions {
		if positions[i].Active && cmn.CONST_PRICING_TYPE_BY_PRICE == positions[i].PricingType {
			var md api.JsonMarketData
			err = api.MarketDataByRefDataID(positions[i].RefDataID, &md)
			if err != nil {
				cmn.ErrorHttp(err, w, http.StatusInternalServerError)
				return
			}

			fx := 1.0
			if val, ok := rates[positions[i].RefDataID]; ok {
				fx = val
			}

			positions[i].Price = md.Last
			positions[i].Value = cmn.Round(positions[i].Price*(1.0/fx)*positions[i].Quantity, 0.01)
			positions[i].Index = positions[i].Value * positions[i].Divisor
			err = api.PutPosition(positions[i])
			if err != nil {
				cmn.ErrorHttp(err, w, http.StatusInternalServerError)
				return
			}
		}
	}

	// Update value, index for all active portfolios
	var portfolios []api.JsonPortfolio
	err = api.Portfolios(&portfolios)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	totalValue := 0.0
	totalValueTotalCapital := 0.0
	for i := range portfolios {
		if portfolios[i].Active && portfolios[i].ID > 1 {
			log.Printf("Updating portfolio %d", portfolios[i].ID)
			positionsTotal := 0.0
			// The number of positions is small so we aren't bothering with efficiency here. Just loop through and find the positions we want
			for j := range positions {
				if positions[j].Active && positions[j].PortfolioID == portfolios[i].ID {
					positionsTotal += positions[j].Value
				}
			}
			portfolios[i].Value = positionsTotal + portfolios[i].Cash - portfolios[i].Debt
			log.Printf("%f = %f Positions + %f Cash - %f Debt", portfolios[i].Value, positionsTotal, portfolios[i].Cash, portfolios[i].Debt)
			portfolios[i].ValueTotalCapital = positionsTotal + portfolios[i].Cash
			portfolios[i].Index = portfolios[i].Value * portfolios[i].Divisor
			portfolios[i].IndexTotalCapital = portfolios[i].ValueTotalCapital * portfolios[i].DivisorTotalCapital
			err = api.PutPortfolio(portfolios[i])
			if err != nil {
				cmn.ErrorHttp(err, w, http.StatusInternalServerError)
				return
			}
			log.Printf("Portfolio %d update complete", portfolios[i].ID)
			totalValue += portfolios[i].Value
			totalValueTotalCapital += portfolios[i].ValueTotalCapital
		}
	}

	// Update value, index for the top level portfolio
	for i := range portfolios {
		if portfolios[i].ID == 1 {
			portfolios[i].Value = totalValue + portfolios[i].Cash - portfolios[i].Debt
			portfolios[i].ValueTotalCapital = totalValueTotalCapital + portfolios[i].Cash
			portfolios[i].Index = portfolios[i].Value * portfolios[i].Divisor
			portfolios[i].IndexTotalCapital = portfolios[i].ValueTotalCapital * portfolios[i].DivisorTotalCapital
			err = api.PutPortfolio(portfolios[i])
			if err != nil {
				cmn.ErrorHttp(err, w, http.StatusInternalServerError)
				return
			}
		}
	}

	// Copy over history
	date := time.Now().Format("2006-01-02")
	api.DeletePortfoliosHistoryByDate(date)
	api.DeletePositionsHistoryByDate(date)
	for i := range portfolios {
		if portfolios[i].Active {
			history := api.JsonPortfolioHistory{JsonPortfolio: portfolios[i]}
			history.PortfolioID = history.ID
			history.ID = 0
			history.Date = date
			err = api.PostPortfolioHistory(history)
			if err != nil {
				cmn.ErrorHttp(err, w, http.StatusInternalServerError)
				return
			}
		}
	}
	for i := range positions {
		if positions[i].Active {
			history := api.JsonPositionHistory{JsonPosition: positions[i]}
			history.PositionID = history.ID
			history.ID = 0
			history.Date = date
			err = api.PostPositionHistory(history)
			if err != nil {
				cmn.ErrorHttp(err, w, http.StatusInternalServerError)
				return
			}
		}
	}

	w.WriteHeader(http.StatusOK)
	cmn.Exit("Run-JobValuationCut", nil)
}

func ExecuteBook(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Run-ExecuteBook", w, r)

	var ret api.JsonTransaction
	err := json.NewDecoder(r.Body).Decode(&ret)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	// Get the appropriate befores
	err = api.EnrichedPositionsByID(ret.PositionID, &ret.PositionBefore)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	// Do the work
	// Get the appropriate afters
	// Save down the transaction

	json.NewEncoder(w).Encode(ret)
	cmn.Exit("Run-ExecuteBook", ret)
}

func ExecuteRollBack(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Run-ExecuteRollBack", w, r)

	w.WriteHeader(http.StatusOK)
	cmn.Exit("Run-ExecuteRollBack", nil)
}

func main() {
	log.Println("Listening on http://localhost:8085/blue-lion/run")
	router := mux.NewRouter().StrictSlash(true)
	setupRouter(router)
	log.Fatal(http.ListenAndServe(":8085", cmn.CorsMiddleware(router)))
}
