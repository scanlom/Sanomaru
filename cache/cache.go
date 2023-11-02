package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/scanlom/Sanomaru/api"
	"github.com/scanlom/Sanomaru/cmn"
)

type ProjectionsCache struct {
	EnrichedProjections       []api.JsonEnrichedProjections
	PositionsProjections      []api.JsonEnrichedProjections
	PositionsProjectionsTotal []api.JsonEnrichedProjections
	WatchProjections          []api.JsonEnrichedProjections
	ResearchProjections       []api.JsonEnrichedProjections
	Stats                     api.JsonProjectionsStats
}

func (cache *ProjectionsCache) CacheEnrichedProjections() error {
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
		cache.EnrichedProjections = append(cache.EnrichedProjections, ep)
	}

	return nil
}

func (cache *ProjectionsCache) RefreshStatsByArray(projections []api.JsonEnrichedProjections) {
	for p := range projections {
		ep := projections[p]
		fresh := false
		cache.Stats.Total++
		switch api.ConfidenceToInt(ep.Confidence) {
		case cmn.CONST_CONFIDENCE_LOW:
			cache.Stats.Low++
		case cmn.CONST_CONFIDENCE_BLAH:
			cache.Stats.Blah++
		case cmn.CONST_CONFIDENCE_NONE:
			cache.Stats.None++
		case cmn.CONST_CONFIDENCE_MEDIUM:
			cache.Stats.Medium++
		case cmn.CONST_CONFIDENCE_HIGH:
			cache.Stats.High++
		}
		lastUpdate := cmn.DateStringToTime(ep.Date)
		daysSince := time.Now().Sub(lastUpdate).Hours() / 24
		if daysSince < 90 {
			fresh = true
			cache.Stats.Fresh++
		}

		if ep.PercentPortfolio > 0 {
			if !fresh {
				cache.Stats.PW1 = false
			}
		} else if ep.Watch {
			if !fresh {
				cache.Stats.PW1 = false
			}
		}
	}
}

func (cache *ProjectionsCache) RefreshStats() {
	cache.Stats = api.JsonProjectionsStats{}
	cache.Stats.PW1 = true // PW1 is true to begin and is set false if we run into something !fresh
	cache.RefreshStatsByArray(cache.PositionsProjections)
	cache.RefreshStatsByArray(cache.WatchProjections)
	cache.RefreshStatsByArray(cache.ResearchProjections)

	// Last one: For PW1 to be true, we need positions + watch + 1 to be fresh
	if cache.Stats.PW1 && cache.Stats.Fresh <= (len(cache.PositionsProjections)+len(cache.WatchProjections)) {
		cache.Stats.PW1 = false
	}
}

func (cache *ProjectionsCache) RefreshTotal() {
	var ret api.JsonEnrichedProjections
	ret.Ticker = "Total"
	for i := range cache.PositionsProjections {
		ep := cache.PositionsProjections[i]
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
	cache.PositionsProjectionsTotal = cache.PositionsProjectionsTotal[:0]
	cache.PositionsProjectionsTotal = append(cache.PositionsProjectionsTotal, ret)
}

func (cache *ProjectionsCache) Sift() error {
	// Clear and then Refresh
	cache.PositionsProjections = cache.PositionsProjections[:0]
	cache.WatchProjections = cache.WatchProjections[:0]
	cache.ResearchProjections = cache.ResearchProjections[:0]

	for p := range cache.EnrichedProjections {
		ep := cache.EnrichedProjections[p]
		if ep.PercentPortfolio > 0 {
			cache.PositionsProjections = append(cache.PositionsProjections, ep)
		} else if ep.Watch {
			cache.WatchProjections = append(cache.WatchProjections, ep)
		} else if ep.Active {
			cache.ResearchProjections = append(cache.ResearchProjections, ep)
		}
	}

	return nil
}

func (cache *ProjectionsCache) Sort() {
	sort.Slice(cache.PositionsProjections, func(i, j int) bool {
		return (cache.PositionsProjections)[i].PercentPortfolio > (cache.PositionsProjections)[j].PercentPortfolio
	})

	sort.Slice(cache.WatchProjections, func(i, j int) bool {
		if api.ConfidenceToInt((cache.WatchProjections)[i].Confidence) == api.ConfidenceToInt((cache.WatchProjections)[j].Confidence) {
			return (cache.WatchProjections)[i].CAGR5yr > (cache.WatchProjections)[j].CAGR5yr
		}
		return api.ConfidenceToInt((cache.WatchProjections)[i].Confidence) > api.ConfidenceToInt((cache.WatchProjections)[j].Confidence)
	})

	sort.Slice(cache.ResearchProjections, func(i, j int) bool {
		if api.ConfidenceToInt((cache.ResearchProjections)[i].Confidence) == api.ConfidenceToInt((cache.ResearchProjections)[j].Confidence) {
			return (cache.ResearchProjections)[i].CAGR5yr > (cache.ResearchProjections)[j].CAGR5yr
		}
		return api.ConfidenceToInt((cache.ResearchProjections)[i].Confidence) > api.ConfidenceToInt((cache.ResearchProjections)[j].Confidence)
	})
}

func (cache *ProjectionsCache) Init() error {
	err := cache.CacheEnrichedProjections()
	if err != nil {
		return err
	}
	cache.Sift()
	cache.Sort()
	cache.RefreshStats()
	cache.RefreshTotal()
	return nil
}

func EnrichedProjections(msg string, cache *[]api.JsonEnrichedProjections, err error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cmn.Enter(msg, w, r)
		if err != nil {
			cmn.ErrorHttp(err, w, http.StatusInternalServerError)
			return
		}
		json.NewEncoder(w).Encode(cache)
		cmn.Exit(msg, cache)
	}
}

func ProjectionsStats(msg string, cache *api.JsonProjectionsStats, err error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cmn.Enter(msg, w, r)
		if err != nil {
			cmn.ErrorHttp(err, w, http.StatusInternalServerError)
			return
		}
		json.NewEncoder(w).Encode(cache)
		cmn.Exit(msg, cache)
	}
}

func ProjectionsUpdate(cache *ProjectionsCache, err error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cmn.Enter("Cache-ProjectionsUpdate", w, r)
		if err != nil {
			cmn.ErrorHttp(err, w, http.StatusInternalServerError)
			return
		}

		params := mux.Vars(r)
		id, err := strconv.Atoi(params["id"])
		if err != nil {
			cmn.ErrorHttp(err, w, http.StatusInternalServerError)
			return
		}

		var ret api.JsonEnrichedProjections
		err = api.EnrichedProjectionsByID(id, &ret)
		if err != nil {
			cmn.ErrorHttp(err, w, http.StatusInternalServerError)
			return
		}

		found := false
		for i := range cache.EnrichedProjections {
			ep := cache.EnrichedProjections[i]
			if ep.ID == id {
				cache.EnrichedProjections[i] = ret
				found = true
			}
		}

		if !found {
			cache.EnrichedProjections = append(cache.EnrichedProjections, ret)
		}

		cache.Sift()
		cache.Sort()
		cache.RefreshStats()
		cache.RefreshTotal()

		cmn.Exit("Cache-ProjectionsUpdate", cache)
	}
}

func main() {
	// Build up caches
	var cache ProjectionsCache
	err := cache.Init()

	log.Println("Listening on http://localhost:8084/blue-lion/cache")
	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/blue-lion/cache/enriched-projections-positions", EnrichedProjections("Cache-EnrichedProjectionsPositions", &cache.PositionsProjections, err)).Methods("GET")
	router.HandleFunc("/blue-lion/cache/enriched-projections-positions-total", EnrichedProjections("Cache-EnrichedProjectionsPositionsTotal", &cache.PositionsProjectionsTotal, err)).Methods("GET")
	router.HandleFunc("/blue-lion/cache/enriched-projections-watch", EnrichedProjections("Cache-EnrichedProjectionsWatch", &cache.WatchProjections, err)).Methods("GET")
	router.HandleFunc("/blue-lion/cache/enriched-projections-research", EnrichedProjections("Cache-EnrichedProjectionsResearch", &cache.ResearchProjections, err)).Methods("GET")
	router.HandleFunc("/blue-lion/cache/projections-stats", ProjectionsStats("Cache-ProjectionsStats", &cache.Stats, err)).Methods("GET")
	router.HandleFunc("/blue-lion/cache/projections-update/{id}", ProjectionsUpdate(&cache, err)).Methods("GET")
	log.Fatal(http.ListenAndServe(":8084", router))
}
