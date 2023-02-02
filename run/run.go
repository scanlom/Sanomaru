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
	router.HandleFunc("/blue-lion/run/execute-book-transaction", ExecuteBookTransaction).Methods("POST")
	router.HandleFunc("/blue-lion/run/execute-roll-back-transaction", ExecuteRollBackTransaction).Methods("GET")
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

func ExecuteBookTransaction(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Run-ExecuteBookTransaction", w, r)

	var ret api.JsonTransaction
	var position api.JsonEnrichedPosition
	var portfolio api.JsonEnrichedPortfolio
	err := json.NewDecoder(r.Body).Decode(&ret)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	// Get the appropriate befores
	err = api.EnrichedPositionsByID(ret.PositionID, &position)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	err = api.EnrichedPortfoliosByID(ret.PortfolioID, &portfolio)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	// Save the befores
	ret.PositionBefore, err = json.Marshal(position)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret.PortfolioBefore, err = json.Marshal(portfolio)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	// Do the work
	switch ret.Type {
	case cmn.CONST_TXN_TYPE_BUY:
		log.Println("Run-ExecuteBookTransaction: Handling CONST_TXN_TYPE_BUY")
		portfolio.Cash -= ret.Value
		position.Quantity += ret.Quantity
		position.Value += ret.Value
		position.TotalCashInfusion += ret.Value
		position.CostBasis += ret.Value
		if position.Index == 0.0 {
			// Start index at 100.0 so we can easily check total pct gain
			position.Index = 100.0
		}
		position.Divisor = position.Index / position.Value
	case cmn.CONST_TXN_TYPE_SELL:
		log.Println("Run-ExecuteBookTransaction: Handling CONST_TXN_TYPE_SELL")
		portfolio.Cash += ret.Value
		position.Quantity -= ret.Quantity
		position.Value -= ret.Value
		position.TotalCashInfusion -= ret.Value


		/*position_history['totalCashInfusion'] -= txn['value']
		# Did we sell down to zero? Float qty's (VNE) caught us out, so compare the integer
		# Note: We don't handle CONST_PRICING_TYPE_BY_VALUE positions being sold down to zero,
		# but that's ok as we have not had any in the portfolios we track yet (so punt)
		if position_history['pricingType'] == db.CONST_PRICING_TYPE_BY_PRICE and int(txn['quantity']) == int(position_history['quantity']):
			position_history['costBasis'] = 0.0
			position_history['quantity'] = 0.0
			position_history['value'] = 0.0
			# Index is calculated one final time, and divisor is frozen at the final value
			position_history['index'] = txn['value'] * position_history['divisor']
			# There may be a sell after the last history row back when we had gaps, so force an update to the position table below in update_histories_and_currents
			position_history['forceUpdate'] = True
			log.info( "process_txn: %s (%d) sold to zero, index frozen at %f" % (txn['positionBefore']['symbol'],  position_history['positionId'], position_history['index']) )
		else:
			if position_history['pricingType'] == db.CONST_PRICING_TYPE_BY_PRICE:
				position_history['costBasis'] -= position_history['costBasis'] * (txn['quantity'] / position_history['quantity'])
			else:
				position_history['costBasis'] -= position_history['costBasis'] * (txn['value'] / position_history['value'])					
			position_history['value'] -= txn['value']
			position_history['quantity'] -= txn['quantity']
			position_history['divisor'] = position_history['index'] / position_history['value']*/
	case cmn.CONST_TXN_TYPE_DIV:
		log.Println("Run-ExecuteBookTransaction: Handling CONST_TXN_TYPE_DIV")
		portfolio.Cash += ret.Value
		position.AccumulatedDividends += ret.Value
		// A dividend may come in after a position is sold down, we've decided to drop this for final index calculation, as there is no good way to handle it
		if position.Value > 0.0 {
			// Due to the dividend the index will increase, but then we basically immediately take the dividend out
			position.Index = (position.Value + ret.Value) * position.Divisor
			position.Divisor = position.Index / position.Value
		}
	case cmn.CONST_TXN_TYPE_CI:
		log.Println("Run-ExecuteBookTransaction: Handling CONST_TXN_TYPE_CI")
	case cmn.CONST_TXN_TYPE_DI:
		log.Println("Run-ExecuteBookTransaction: Handling CONST_TXN_TYPE_DI")
	case cmn.CONST_TXN_TYPE_INT:
		log.Println("Run-ExecuteBookTransaction: Handling CONST_TXN_TYPE_INT")
	default:
		log.Println("Run-ExecuteBookTransaction: Unknown transaction type")
		// Return error
	}

	// Save the currents
	err = api.PutPosition(position.JsonPosition)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	err = api.PutPortfolio(portfolio.JsonPortfolio)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	// Get the appropriate afters
	err = api.EnrichedPositionsByID(ret.PositionID, &position)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	err = api.EnrichedPortfoliosByID(ret.PortfolioID, &portfolio)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	// Save the afters
	ret.PositionAfter, err = json.Marshal(position)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret.PortfolioAfter, err = json.Marshal(portfolio)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	// Save down the transaction
	err = api.PostTransaction(ret)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(ret)
	cmn.Exit("Run-ExecuteBookTransaction", ret)
}

func ExecuteRollBackTransaction(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Run-ExecuteRollBackTransaction", w, r)

	w.WriteHeader(http.StatusOK)
	cmn.Exit("Run-ExecuteRollBackTransaction", nil)
}

func main() {
	log.Println("Listening on http://localhost:8085/blue-lion/run")
	router := mux.NewRouter().StrictSlash(true)
	setupRouter(router)
	log.Fatal(http.ListenAndServe(":8085", cmn.CorsMiddleware(router)))
}
