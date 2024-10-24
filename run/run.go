package main

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"

	"github.com/scanlom/Sanomaru/api"
)

func setupRouter(router *mux.Router) {
	router.HandleFunc("/blue-lion/run/job-valuation-cut", JobValuationCut).Methods("GET")
	router.HandleFunc("/blue-lion/run/execute-book-transaction", ExecuteBookTransaction).Methods("POST")
	router.HandleFunc("/blue-lion/run/execute-roll-back-transaction", ExecuteRollBackTransaction).Methods("GET")
}

func JobValuationCutInternal() error {
	log.Println("JobValuationCutInternal: Called...")
	CONST_FX_EUR := 0.92
	CONST_FX_GBP := 79.34
	CONST_FX_HKD := 7.79
	CONST_FX_JPY := 148.16
	CONST_FX_SGD := 1.35
	rates := map[int]float64{
		/*"1373.HK"*/ 76: CONST_FX_HKD,
		/*"2788.HK"*/ 3288: CONST_FX_HKD,
		/*"6670.T"*/ 2451: CONST_FX_JPY,
		/*"8074.T"*/ 3303: CONST_FX_JPY,
		/*"MRO.L"*/ 97: CONST_FX_GBP,
		/*"BATS.L"*/ 3280: CONST_FX_GBP,
		/*"DWL.L"*/ 3737: CONST_FX_GBP,
		/*"U11.SI"*/ 3126: CONST_FX_SGD,
		/*"BOL.PA"*/ 55: CONST_FX_EUR,
	}

	// Update price, value, index for all active by price positions
	var positions []api.JsonPosition
	err := api.Positions(&positions)
	if err != nil {
		return err
	}

	for i := range positions {
		if positions[i].Active && api.CONST_PRICING_TYPE_BY_PRICE == positions[i].PricingType && positions[i].Quantity > api.CONST_FUDGE {
			var md api.JsonMarketData
			err = api.MarketDataByRefDataID(positions[i].RefDataID, &md)
			if err != nil {
				return err
			}

			fx := 1.0
			if val, ok := rates[positions[i].RefDataID]; ok {
				fx = val
			}

			positions[i].Price = md.Last
			positions[i].Value = api.Round(positions[i].Price*(1.0/fx)*positions[i].Quantity, 0.01)
			positions[i].Index = positions[i].Value * positions[i].Divisor
			err = api.PutPosition(positions[i])
			if err != nil {
				return err
			}
		}
	}

	// Update index for all active by value positions
	for i := range positions {
		if positions[i].Active && api.CONST_PRICING_TYPE_BY_VALUE == positions[i].PricingType && positions[i].Value > api.CONST_FUDGE {
			positions[i].Index = positions[i].Value * positions[i].Divisor
			err = api.PutPosition(positions[i])
			if err != nil {
				return err
			}
		}
	}

	// Update value, index for all active portfolios
	var portfolios []api.JsonPortfolio
	err = api.Portfolios(&portfolios)
	if err != nil {
		return err
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
				return err
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
				return err
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
				return err
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
				return err
			}
		}
	}

	log.Println("JobValuationCutInternal: Complete!")
	return nil
}

func JobValuationCut(w http.ResponseWriter, r *http.Request) {
	api.Enter("Run-JobValuationCut", w, r)

	err := JobValuationCutInternal()
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	api.Exit("Run-JobValuationCut", nil)
}

func ExecuteBookTransaction(w http.ResponseWriter, r *http.Request) {
	api.Enter("Run-ExecuteBookTransaction", w, r)

	var ret api.JsonTransaction
	var portfolio api.JsonEnrichedPortfolio
	var position api.JsonEnrichedPosition
	err := json.NewDecoder(r.Body).Decode(&ret)
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	// Get and save the appropriate befores
	err = JobValuationCutInternal() // Valuation cut first
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	err = api.EnrichedPortfoliosByID(ret.PortfolioID, &portfolio)
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret.PortfolioBefore, err = json.Marshal(portfolio)
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	if ret.PositionID > 0 {
		err = api.EnrichedPositionsByID(ret.PositionID, &position)
		if err != nil {
			api.ErrorHttp(err, w, http.StatusInternalServerError)
			return
		}

		ret.PositionBefore, err = json.Marshal(position)
		if err != nil {
			api.ErrorHttp(err, w, http.StatusInternalServerError)
			return
		}
	}

	// Do the work
	switch ret.Type {
	case api.CONST_TXN_TYPE_BUY:
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
	case api.CONST_TXN_TYPE_SELL:
		log.Println("Run-ExecuteBookTransaction: Handling CONST_TXN_TYPE_SELL")
		portfolio.Cash += ret.Value
		position.TotalCashInfusion -= ret.Value

		// Did we sell down to zero? Float qty's (VNE) caught us out in our python script, so we compare with a fudge
		if (position.PricingType == api.CONST_PRICING_TYPE_BY_PRICE && position.Quantity-ret.Quantity < api.CONST_FUDGE) ||
			(position.PricingType == api.CONST_PRICING_TYPE_BY_VALUE && position.Value-ret.Value < api.CONST_FUDGE) {
			position.CostBasis = 0.0
			position.Quantity = 0.0
			position.Value = 0.0
			// Index is calculated one final time, and divisor is frozen at the final value
			position.Index = ret.Value * position.Divisor
		} else {
			if position.PricingType == api.CONST_PRICING_TYPE_BY_PRICE {
				position.CostBasis -= position.CostBasis * (ret.Quantity / position.Quantity)
			} else {
				position.CostBasis -= position.CostBasis * (ret.Value / position.Value)
			}
			position.Quantity -= ret.Quantity
			position.Value -= ret.Value
			position.Divisor = position.Index / position.Value
		}
	case api.CONST_TXN_TYPE_DIV:
		log.Println("Run-ExecuteBookTransaction: Handling CONST_TXN_TYPE_DIV")
		portfolio.Cash += ret.Value
		position.AccumulatedDividends += ret.Value
		// A dividend may come in after a position is sold down, we've decided to drop this for final index calculation, as there is no good way to handle it
		if position.Value > 0.0 {
			// Due to the dividend the index will increase, but then we basically immediately take the dividend out
			position.Index = (position.Value + ret.Value) * position.Divisor
			position.Divisor = position.Index / position.Value
		}
	case api.CONST_TXN_TYPE_CI:
		log.Println("Run-ExecuteBookTransaction: Handling CONST_TXN_TYPE_CI")
		portfolio.Cash += ret.Value
		portfolio.TotalCashInfusion += ret.Value
		portfolio.Value += ret.Value
		portfolio.ValueTotalCapital += ret.Value
		portfolio.Divisor = portfolio.Index / portfolio.Value
		portfolio.DivisorTotalCapital = portfolio.IndexTotalCapital / portfolio.ValueTotalCapital

		// If this is a sub portfolio, update top level cash also
		if portfolio.ID != api.CONST_PORTFOLIO_TOTAL {
			var topPortfolio api.JsonEnrichedPortfolio
			err = api.EnrichedPortfoliosByID(api.CONST_PORTFOLIO_TOTAL, &topPortfolio)
			if err != nil {
				api.ErrorHttp(err, w, http.StatusInternalServerError)
				return
			}

			topPortfolio.Cash -= ret.Value

			err = api.PutPortfolio(topPortfolio.JsonPortfolio)
			if err != nil {
				api.ErrorHttp(err, w, http.StatusInternalServerError)
				return
			}
		}
	case api.CONST_TXN_TYPE_DI:
		log.Println("Run-ExecuteBookTransaction: Handling CONST_TXN_TYPE_DI")
		portfolio.Cash += ret.Value
		portfolio.Debt += ret.Value
		portfolio.ValueTotalCapital += ret.Value
		portfolio.DivisorTotalCapital = portfolio.IndexTotalCapital / portfolio.ValueTotalCapital

		// There should never be a DI on a sub portfolio
		// MSTODO: Throw error
	case api.CONST_TXN_TYPE_INT:
		log.Println("Run-ExecuteBookTransaction: Handling CONST_TXN_TYPE_INT")
		portfolio.Cash += ret.Value
	default:
		log.Println("Run-ExecuteBookTransaction: Unknown transaction type")
		// Return error
	}

	// Save the currents
	err = api.PutPortfolio(portfolio.JsonPortfolio)
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	if ret.PositionID > 0 {
		err = api.PutPosition(position.JsonPosition)
		if err != nil {
			api.ErrorHttp(err, w, http.StatusInternalServerError)
			return
		}
	}

	// Get and save the appropriate afters
	err = JobValuationCutInternal() // Valuation cut first
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	err = api.EnrichedPortfoliosByID(ret.PortfolioID, &portfolio)
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ret.PortfolioAfter, err = json.Marshal(portfolio)
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	if ret.PositionID > 0 {
		err = api.EnrichedPositionsByID(ret.PositionID, &position)
		if err != nil {
			api.ErrorHttp(err, w, http.StatusInternalServerError)
			return
		}

		ret.PositionAfter, err = json.Marshal(position)
		if err != nil {
			api.ErrorHttp(err, w, http.StatusInternalServerError)
			return
		}
	}

	// Save down the transaction
	err = api.PostTransaction(ret)
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(ret)
	api.Exit("Run-ExecuteBookTransaction", ret)
}

func ExecuteRollBackTransaction(w http.ResponseWriter, r *http.Request) {
	api.Enter("Run-ExecuteRollBackTransaction", w, r)

	w.WriteHeader(http.StatusOK)
	api.Exit("Run-ExecuteRollBackTransaction", nil)
}

func main() {
	log.Println("Listening on http://localhost:8085/blue-lion/run")
	router := mux.NewRouter().StrictSlash(true)
	setupRouter(router)
	log.Fatal(http.ListenAndServe(":8085", api.CorsMiddleware(router)))
}
