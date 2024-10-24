package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/schema"
	"github.com/scanlom/Sanomaru/api"
)

func RefDataByID(w http.ResponseWriter, r *http.Request) {
	api.Enter("RefDataByID", w, r)

	params := mux.Vars(r)
	id := params["id"]

	ret := api.JsonRefData{}
	err := api.CacheGet(fmt.Sprintf("ref_data:%s", id), &ret)
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(&ret)

	api.Exit("RefDataByID", ret)
}

func EnrichedMergersPositions(w http.ResponseWriter, r *http.Request) {
	api.Enter("Gateway-EnrichedMergersPositions", w, r)

	ret := []api.JsonEnrichedMerger{}
	keys := api.CacheKeys("enriched_mergers")
	for i := range keys {
		em := api.JsonEnrichedMerger{}
		err := api.CacheGet(keys[i], &em)
		if err == nil && em.PercentPortfolio > 0 {
			ret = append(ret, em)
		}
	}
	sort.Slice(ret, func(i, j int) bool {
		return ret[i].PercentPortfolio > ret[j].PercentPortfolio
	})
	json.NewEncoder(w).Encode(&ret)

	api.Exit("Gateway-EnrichedMergersPositions", ret)
}

func EnrichedMergersPositionsTotal(w http.ResponseWriter, r *http.Request) {
	api.Enter("Gateway-EnrichedMergersPositionsTotal", w, r)

	total := api.JsonEnrichedMerger{}
	total.TargetTicker = "Total"
	total.Active = true
	total.Status = "O"
	keys := api.CacheKeys("enriched_mergers")
	for i := range keys {
		em := api.JsonEnrichedMerger{}
		err := api.CacheGet(keys[i], &em)
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

	api.Exit("Gateway-EnrichedMergersPositionsTotal", ret)
}

func EnrichedMergersResearch(w http.ResponseWriter, r *http.Request) {
	api.Enter("Gateway-EnrichedMergersResearch", w, r)

	ret := []api.JsonEnrichedMerger{}
	keys := api.CacheKeys("enriched_mergers")
	for i := range keys {
		em := api.JsonEnrichedMerger{}
		err := api.CacheGet(keys[i], &em)
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

	api.Exit("Gateway-EnrichedMergersResearch", ret)
}

func EnrichedProjectionsPositions(w http.ResponseWriter, r *http.Request) {
	api.Enter("Gateway-EnrichedProjectionsPositions", w, r)

	ret := []api.JsonEnrichedProjections{}
	keys := api.CacheKeys("enriched_projections")
	for i := range keys {
		ep := api.JsonEnrichedProjections{}
		err := api.CacheGet(keys[i], &ep)
		if err == nil && ep.PercentPortfolio > 0 {
			ret = append(ret, ep)
		}
	}
	sort.Slice(ret, func(i, j int) bool {
		return ret[i].PercentPortfolio > ret[j].PercentPortfolio
	})
	json.NewEncoder(w).Encode(&ret)

	api.Exit("Gateway-EnrichedProjectionsPositions", ret)
}

func EnrichedProjectionsPositionsTotal(w http.ResponseWriter, r *http.Request) {
	api.Enter("Gateway-EnrichedProjectionsPositionsTotal", w, r)

	var total api.JsonEnrichedProjections
	total.Ticker = "Total"
	keys := api.CacheKeys("enriched_projections")
	for i := range keys {
		ep := api.JsonEnrichedProjections{}
		err := api.CacheGet(keys[i], &ep)
		if err == nil && ep.PercentPortfolio > 0 {
			total.Growth += ep.Growth * ep.PercentPortfolio
			total.DivPlusGrowth += ep.DivPlusGrowth * ep.PercentPortfolio
			total.EPSYield += ep.EPSYield * ep.PercentPortfolio
			total.DPSYield += ep.DPSYield * ep.PercentPortfolio
			total.CAGR5yr += ep.CAGR5yr * ep.PercentPortfolio
			total.CAGR10yr += ep.CAGR10yr * ep.PercentPortfolio
			total.CROE5yr += ep.CROE5yr * ep.PercentPortfolio
			total.CROE10yr += ep.CROE10yr * ep.PercentPortfolio
			total.PercentPortfolio += ep.PercentPortfolio
		}
	}
	ret := []api.JsonEnrichedProjections{}
	ret = append(ret, total)
	json.NewEncoder(w).Encode(&ret)

	api.Exit("Gateway-EnrichedProjectionsPositionsTotal", ret)
}

func EnrichedProjectionsWatch(w http.ResponseWriter, r *http.Request) {
	api.Enter("Gateway-EnrichedProjectionsWatch", w, r)

	ret := []api.JsonEnrichedProjections{}
	keys := api.CacheKeys("enriched_projections")
	for i := range keys {
		ep := api.JsonEnrichedProjections{}
		err := api.CacheGet(keys[i], &ep)
		if err == nil && ep.Watch {
			ret = append(ret, ep)
		}
	}
	sort.Slice(ret, func(i, j int) bool {
		if api.ConfidenceToInt((ret)[i].Confidence) == api.ConfidenceToInt((ret)[j].Confidence) {
			return (ret)[i].CAGR5yr > (ret)[j].CAGR5yr
		}
		return api.ConfidenceToInt((ret)[i].Confidence) > api.ConfidenceToInt((ret)[j].Confidence)
	})
	json.NewEncoder(w).Encode(&ret)

	api.Exit("Gateway-EnrichedProjectionsWatch", ret)
}

func EnrichedProjectionsResearch(w http.ResponseWriter, r *http.Request) {
	api.Enter("Gateway-EnrichedProjectionsResearch", w, r)

	ret := []api.JsonEnrichedProjections{}
	keys := api.CacheKeys("enriched_projections")
	for i := range keys {
		ep := api.JsonEnrichedProjections{}
		err := api.CacheGet(keys[i], &ep)
		if err == nil && ep.PercentPortfolio <= 0 && !ep.Watch && ep.Active {
			ret = append(ret, ep)
		}
	}
	sort.Slice(ret, func(i, j int) bool {
		if api.ConfidenceToInt((ret)[i].Confidence) == api.ConfidenceToInt((ret)[j].Confidence) {
			return (ret)[i].CAGR5yr > (ret)[j].CAGR5yr
		}
		return api.ConfidenceToInt((ret)[i].Confidence) > api.ConfidenceToInt((ret)[j].Confidence)
	})
	json.NewEncoder(w).Encode(&ret)

	api.Exit("Gateway-EnrichedProjectionsResearch", ret)
}

func ProjectionsStats(w http.ResponseWriter, r *http.Request) {
	api.Enter("Gateway-ProjectionsStats", w, r)

	ret := api.JsonProjectionsStats{}
	ret.PW1 = true         // PW1 is true to begin and is set false if we run into something !fresh
	researchFresh := false // We need at least one fresh research
	keys := api.CacheKeys("enriched_projections")
	for i := range keys {
		ep := api.JsonEnrichedProjections{}
		err := api.CacheGet(keys[i], &ep)
		if err == nil && ep.Active {
			fresh := false
			ret.Total++
			switch api.ConfidenceToInt(ep.Confidence) {
			case api.CONST_CONFIDENCE_LOW:
				ret.Low++
			case api.CONST_CONFIDENCE_BLAH:
				ret.Blah++
			case api.CONST_CONFIDENCE_NONE:
				ret.None++
			case api.CONST_CONFIDENCE_MEDIUM:
				ret.Medium++
			case api.CONST_CONFIDENCE_HIGH:
				ret.High++
			}
			lastUpdate := api.DateStringToTime(ep.Date)
			daysSince := time.Now().Sub(lastUpdate).Hours() / 24
			if daysSince < 90 {
				fresh = true
				ret.Fresh++
			}

			if ep.PercentPortfolio > 0 {
				if !fresh {
					ret.PW1 = false
				}
			} else if ep.Watch {
				if !fresh {
					ret.PW1 = false
				}
			} else if fresh {
				researchFresh = true
			}
		}
	}

	// Last one: For PW1 to be true, we need positions + watch + 1 to be fresh
	if ret.PW1 && !researchFresh {
		ret.PW1 = false
	}
	json.NewEncoder(w).Encode(&ret)

	api.Exit("Gateway-ProjectionsStats", ret)
}

func EnrichedProjectionsBySymbol(w http.ResponseWriter, r *http.Request) {
	api.Enter("Gateway-EnrichedProjectionsBySymbol", w, r)

	args := new(api.RestSymbolInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		api.ErrorHttp(err, w, http.StatusBadRequest)
		return
	}

	refDataID, err := api.SymbolToRefDataID(args.Symbol)
	if err != nil {
		api.ErrorHttp(err, w, http.StatusInternalServerError)
		return
	}

	ep := api.JsonEnrichedProjections{}
	ids := api.CacheSMembers(fmt.Sprintf("%s:%d", "s_projections_by_ref_data_id", refDataID))
	if len(ids) <= 0 {
		// If there is no projection saved then we estimate one on the fly
		p := api.JsonProjections{}
		api.ProjectionsBySymbol(args.Symbol, &p)
		ep, err = api.EnrichProjections(p)
		if err != nil {
			api.ErrorHttp(err, w, http.StatusInternalServerError)
			return
		}
	} else {
		// If there is a projection saved an enrichment will already be cached
		err = api.CacheGet(fmt.Sprintf("%s:%d", "enriched_projections", ids[0]), &ep)
		if err != nil {
			api.ErrorHttp(err, w, http.StatusInternalServerError)
			return
		}
	}
	json.NewEncoder(w).Encode(&ep)

	api.Exit("Gateway-EnrichedProjectionsBySymbol", ep)
}

func main() {
	log.Println("Listening on http://localhost:8086/blue-lion/gateway")
	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/blue-lion/gateway/ref-data/{id}", RefDataByID).Methods("GET")
	router.HandleFunc("/blue-lion/gateway/enriched-mergers-positions", EnrichedMergersPositions).Methods("GET")
	router.HandleFunc("/blue-lion/gateway/enriched-mergers-positions-total", EnrichedMergersPositionsTotal).Methods("GET")
	router.HandleFunc("/blue-lion/gateway/enriched-mergers-research", EnrichedMergersResearch).Methods("GET")
	router.HandleFunc("/blue-lion/gateway/enriched-projections-positions", EnrichedProjectionsPositions).Methods("GET")
	router.HandleFunc("/blue-lion/gateway/enriched-projections-positions-total", EnrichedProjectionsPositionsTotal).Methods("GET")
	router.HandleFunc("/blue-lion/gateway/enriched-projections-watch", EnrichedProjectionsWatch).Methods("GET")
	router.HandleFunc("/blue-lion/gateway/enriched-projections-research", EnrichedProjectionsResearch).Methods("GET")
	router.HandleFunc("/blue-lion/gateway/projections-stats", ProjectionsStats).Methods("GET")
	router.HandleFunc("/blue-lion/gateway/enriched-projections", EnrichedProjectionsBySymbol).Queries("symbol", "").Methods("GET")
	log.Fatal(http.ListenAndServe(":8086", router))
}
