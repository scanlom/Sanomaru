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
	total.Status = "O"
	keys := cmn.CacheKeys("enriched_mergers")
	for i := range keys {
		em := api.JsonEnrichedMerger{}
		err := cmn.CacheGet(keys[i], &em)
		if err == nil {
			total.PercentPortfolio += em.PercentPortfolio
			total.MarketNetReturn += em.MarketNetReturn * em.PercentPortfolio
			total.MarketNetReturnAnnualized += em.MarketNetReturnAnnualized * em.PercentPortfolio
			total.MarketPositiveReturn += em.MarketPositiveReturn * em.PercentPortfolio
			total.MarketPositiveReturnAnnualized += em.MarketPositiveReturnAnnualized * em.PercentPortfolio
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
		if api.MergerStatusToInt(ret[i].Status) == api.MergerStatusToInt(ret[j].Status) {
			if ret[i].Status == "O" {
				return ret[i].MarketNetReturnAnnualized > ret[j].MarketNetReturnAnnualized
			} else if ret[i].Status == "C" {
				return ret[i].CloseDate > ret[j].CloseDate
			} else if ret[i].Status == "B" {
				return ret[i].BreakDate > ret[j].BreakDate
			}
		}
		return api.MergerStatusToInt(ret[i].Status) > api.MergerStatusToInt(ret[j].Status)
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
