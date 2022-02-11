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

func EnrichedProjections(cache []api.JsonEnrichedProjections, err error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cmn.Enter("Cache-EnrichedProjections", w, r.URL.Query())
		if err != nil {
			cmn.ErrorHttp(err, w, http.StatusInternalServerError)
			return
		}
		json.NewEncoder(w).Encode(&cache)
		cmn.Exit("Cache-EnrichedProjections", &cache)
	}
}

func CacheEnrichedProjections() ([]api.JsonEnrichedProjections, error) {
	var ret []api.JsonEnrichedProjections
	db, err := cmn.DbConnect()
	if err != nil {
		return ret, err
	}

	foo := api.JsonProjections{}
	var projections []api.JsonProjections
	err = db.Select(&projections, api.JsonToSelect(foo, "projections", ""))
	if err != nil {
		return ret, err
	}

	for p := range projections {
		ep, err := api.EnrichProjections(projections[p])
		if err != nil {
			return ret, err
		}
		ret = append(ret, ep)
	}

	sort.Slice(ret, func(i, j int) bool {
		if api.ConfidenceToInt(ret[i].Confidence) == api.ConfidenceToInt(ret[j].Confidence) {
			return ret[i].CAGR5yr > ret[j].CAGR5yr
		}
		return api.ConfidenceToInt(ret[i].Confidence) > api.ConfidenceToInt(ret[j].Confidence)
	})
	return ret, nil
}

func main() {
	// Build up caches
	cachedEnrichedProjections, err := CacheEnrichedProjections()

	log.Println("Listening on http://localhost:8084/blue-lion/cache")
	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/blue-lion/cache/enriched-projections", EnrichedProjections(cachedEnrichedProjections, err)).Methods("GET")
	log.Fatal(http.ListenAndServe(":8084", cmn.CorsMiddleware(router)))
}