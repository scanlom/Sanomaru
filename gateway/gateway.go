package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"

	"github.com/gorilla/mux"
	"github.com/scanlom/Sanomaru/api"
	"github.com/scanlom/Sanomaru/cmn"
)

func RefDataByID(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("RefDataByID", w, r)

	params := mux.Vars(r)
	id := params["id"]

	ret := api.JsonRefData{}
	err := cmn.CacheGet(fmt.Sprintf("ref_data:%s", id), &ret)
	if err != nil {
		cmn.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	cmn.Exit("RefDataByID", ret)
}

func EnrichedMergersPositions(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Gateway-EnrichedMergersPositions", w, r)

	ret := []api.JsonEnrichedMerger{}
	keys := cmn.CacheKeys("enriched_mergers")
	for i := range keys {
		em := api.JsonEnrichedMerger{}
		err := cmn.CacheGet(keys[i], &em)
		if err == nil && em.PercentPortfolio > 0 {
			ret = append(ret, em)
		}
	}
	sort.Slice(ret, func(i, j int) bool {
		return ret[i].PercentPortfolio > ret[j].PercentPortfolio
	})
	json.NewEncoder(w).Encode(&ret)

	cmn.Exit("Gateway-EnrichedMergersPositions", ret)
}

func EnrichedMergersPositionsTotal(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Gateway-EnrichedMergersPositionsTotal", w, r)

	total := api.JsonEnrichedMerger{}
	total.TargetTicker = "Total"
	total.Active = true
	total.Status = "O"
	keys := cmn.CacheKeys("enriched_mergers")
	for i := range keys {
		em := api.JsonEnrichedMerger{}
		err := cmn.CacheGet(keys[i], &em)
		if err == nil && em.PercentPortfolio > 0 {
			total.PercentPortfolio += em.PercentPortfolio
			total.MarketNetReturn += em.MarketNetReturn * em.PercentPortfolio
			total.MarketNetReturnAnnualized += em.MarketNetReturnAnnualized * em.PercentPortfolio
			total.MarketPositiveReturn += em.MarketPositiveReturn * em.PercentPortfolio
			total.MarketPositiveReturnAnnualized += em.MarketPositiveReturnAnnualized * em.PercentPortfolio
			total.Confidence += em.Confidence * em.PercentPortfolio
			total.StrikeReturn += em.StrikeReturn * em.PercentPortfolio
			total.StrikeReturnAnnualized += em.StrikeReturnAnnualized * em.PercentPortfolio
			total.PositionReturn += em.PositionReturn * em.PercentPortfolio
			total.ProfitLifetime += em.ProfitLifetime
		}
	}
	ret := []api.JsonEnrichedMerger{}
	ret = append(ret, total)
	json.NewEncoder(w).Encode(&ret)

	cmn.Exit("Gateway-EnrichedMergersPositionsTotal", ret)
}

func EnrichedMergersResearch(w http.ResponseWriter, r *http.Request) {
	cmn.Enter("Gateway-EnrichedMergersResearch", w, r)

	ret := []api.JsonEnrichedMerger{}
	keys := cmn.CacheKeys("enriched_mergers")
	for i := range keys {
		em := api.JsonEnrichedMerger{}
		err := cmn.CacheGet(keys[i], &em)
		if err == nil && em.PercentPortfolio <= 0 {
			ret = append(ret, em)
		}
	}
	sort.Slice(ret, func(i, j int) bool {
		iTop := false
		if ret[i].Active != ret[j].Active {
			iTop = ret[i].Active
		} else if ret[i].Status == "O" {
			if ret[j].Status == "O" {
				iTop = ret[i].MarketNetReturnAnnualized > ret[j].MarketNetReturnAnnualized
			} else {
				iTop = true
			}
		} else if ret[j].Status == "O" {
			iTop = false
		} else {
			iDate := ret[i].CloseDate
			jDate := ret[j].CloseDate
			if ret[i].Status == "B" {
				iDate = ret[i].BreakDate
			}
			if ret[j].Status == "B" {
				jDate = ret[j].BreakDate
			}
			iTop = iDate > jDate
		}
		return iTop
	})
	json.NewEncoder(w).Encode(&ret)

	cmn.Exit("Gateway-EnrichedMergersResearch", ret)
}

func main() {
	log.Println("Listening on http://localhost:8086/blue-lion/gateway")
	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/blue-lion/gateway/ref-data/{id}", RefDataByID).Methods("GET")
	router.HandleFunc("/blue-lion/gateway/enriched-mergers-positions", EnrichedMergersPositions).Methods("GET")
	router.HandleFunc("/blue-lion/gateway/enriched-mergers-positions-total", EnrichedMergersPositionsTotal).Methods("GET")
	router.HandleFunc("/blue-lion/gateway/enriched-mergers-research", EnrichedMergersResearch).Methods("GET")
	log.Fatal(http.ListenAndServe(":8086", router))
}
