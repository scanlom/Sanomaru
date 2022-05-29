package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sort"

	"github.com/gorilla/mux"
	"github.com/scanlom/Sanomaru/api"
	"github.com/scanlom/Sanomaru/cmn"
)

func EnrichedProjections(msg string, cache []api.JsonEnrichedProjections, err error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cmn.Enter(msg, w, r)
		if err != nil {
			cmn.ErrorHttp(err, w, http.StatusInternalServerError)
			return
		}
		json.NewEncoder(w).Encode(&cache)
		cmn.Exit(msg, &cache)
	}
}

func CacheEnrichedProjections(positionsProjections *[]api.JsonEnrichedProjections, watchProjections *[]api.JsonEnrichedProjections, researchProjections *[]api.JsonEnrichedProjections) error {
	var projections []api.JsonProjections
	err := api.Projections(&projections)
	if err != nil {
		cmn.ErrorLog(err)
		return err
	}

	for p := range projections {
		ep, err := api.EnrichProjections(projections[p])
		if err != nil {
			cmn.ErrorLog(err)
			return err
		}
		if ep.PercentPortfolio > 0 {
			*positionsProjections = append(*positionsProjections, ep)
		} else if ep.Watch {
			*watchProjections = append(*watchProjections, ep)
		} else {
			*researchProjections = append(*researchProjections, ep)
		}
	}

	sort.Slice(*positionsProjections, func(i, j int) bool {
		return (*positionsProjections)[i].PercentPortfolio > (*positionsProjections)[j].PercentPortfolio
	})

	sort.Slice(*watchProjections, func(i, j int) bool {
		if api.ConfidenceToInt((*watchProjections)[i].Confidence) == api.ConfidenceToInt((*watchProjections)[j].Confidence) {
			return (*watchProjections)[i].CAGR5yr > (*watchProjections)[j].CAGR5yr
		}
		return api.ConfidenceToInt((*watchProjections)[i].Confidence) > api.ConfidenceToInt((*watchProjections)[j].Confidence)
	})

	sort.Slice(*researchProjections, func(i, j int) bool {
		if api.ConfidenceToInt((*researchProjections)[i].Confidence) == api.ConfidenceToInt((*researchProjections)[j].Confidence) {
			return (*researchProjections)[i].CAGR5yr > (*researchProjections)[j].CAGR5yr
		}
		return api.ConfidenceToInt((*researchProjections)[i].Confidence) > api.ConfidenceToInt((*researchProjections)[j].Confidence)
	})
	return nil
}

func CacheEnrichedProjectionsTotal(positionsProjectionsTotal *[]api.JsonEnrichedProjections, positionsProjections []api.JsonEnrichedProjections) {
	var ret api.JsonEnrichedProjections
	ret.Ticker = "Total"
	for i := range positionsProjections {
		ep := positionsProjections[i]
		ret.Growth += ep.Growth * ep.PercentPortfolio
		ret.DivPlusGrowth += ep.DivPlusGrowth * ep.PercentPortfolio
		ret.EPSYield += ep.EPSYield * ep.PercentPortfolio
		ret.DPSYield += ep.DPSYield * ep.PercentPortfolio
		ret.CAGR5yr += ep.CAGR5yr * ep.PercentPortfolio
		ret.CAGR10yr += ep.CAGR10yr * ep.PercentPortfolio
		ret.CROE5yr += ep.CROE5yr * ep.PercentPortfolio
		ret.CROE10yr += ep.CROE10yr * ep.PercentPortfolio
		ret.PercentPortfolio += ep.PercentPortfolio
	}
	*positionsProjectionsTotal = append(*positionsProjectionsTotal, ret)
}

func main() {
	// Build up caches
	var positionsProjections []api.JsonEnrichedProjections
	var watchProjections []api.JsonEnrichedProjections
	var researchProjections []api.JsonEnrichedProjections
	err := CacheEnrichedProjections(&positionsProjections, &watchProjections, &researchProjections)

	var positionsProjectionsTotal []api.JsonEnrichedProjections
	CacheEnrichedProjectionsTotal(&positionsProjectionsTotal, positionsProjections)

	log.Println("Listening on http://localhost:8084/blue-lion/cache")
	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/blue-lion/cache/enriched-projections-positions", EnrichedProjections("Cache-EnrichedProjectionsPositions", positionsProjections, err)).Methods("GET")
	router.HandleFunc("/blue-lion/cache/enriched-projections-positions-total", EnrichedProjections("Cache-EnrichedProjectionsPositionsTotal", positionsProjectionsTotal, err)).Methods("GET")
	router.HandleFunc("/blue-lion/cache/enriched-projections-watch", EnrichedProjections("Cache-EnrichedProjectionsWatch", watchProjections, err)).Methods("GET")
	router.HandleFunc("/blue-lion/cache/enriched-projections-research", EnrichedProjections("Cache-EnrichedProjectionsResearch", researchProjections, err)).Methods("GET")
	log.Fatal(http.ListenAndServe(":8084", router))
}
