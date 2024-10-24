package main

import (
	"fmt"
	"log"

	"github.com/scanlom/Sanomaru/api"
)

func PopulateEnrichedProjections(id int) {
	log.Printf("PopulateEnrichedProjections called for ID %d", id)

	// 1. Enrich and add
	projections := api.JsonProjections{}
	err := api.CacheGet(fmt.Sprintf("%s:%d", "projections", id), &projections)
	if err != nil {
		api.ErrorLog(err)
		return // Nothing we can do if the projections don't exist
	}
	ep, err := api.EnrichProjections(projections)
	if err != nil {
		api.ErrorLog(err)
		return // Nothing we can do if we can't enrich the projections
	}
	api.CacheSet(fmt.Sprintf("%s:%d", "enriched_projections", ep.ID), ep)

	// 2. Add secondary indices
	// NOOP

	// 3. Update graph
	// NOOP
}

func ProjectionsWork(ptr interface{}) {
	proj := *ptr.(*api.JsonProjections)

	// 1. Add secondary indices
	api.CacheSAdd(fmt.Sprintf("%s:%d", "s_projections_by_ref_data_id", proj.RefDataID), proj.ID)

	// 2. Enrich and add
	PopulateEnrichedProjections(proj.ID)
}
