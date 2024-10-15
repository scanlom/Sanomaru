package main

import (
	"fmt"

	"github.com/scanlom/Sanomaru/api"
	"github.com/scanlom/Sanomaru/cmn"
)

func PopulateEnrichedProjections(id int) {
	// 1. Enrich and add
	projections := api.JsonProjections{}
	err := cmn.CacheGet(fmt.Sprintf("%s:%d", "projections", id), &projections)
	if err != nil {
		cmn.ErrorLog(err)
		return // Nothing we can do if the projections don't exist
	}
	ep, err := api.EnrichProjections(projections)
	if err != nil {
		cmn.ErrorLog(err)
		return // Nothing we can do if we can't enrich the projections
	}
	cmn.CacheSet(fmt.Sprintf("%s:%d", "enriched_projections", ep.ID), ep)

	// 2. Add secondary indices
	// NOOP

	// 3. Update graph
	// NOOP
}

func ProjectionsWork(ptr interface{}) {
	proj := *ptr.(*api.JsonProjections)

	// 1. Add secondary indices
	// NOOP

	// 2. Enrich and add
	PopulateEnrichedProjections(proj.ID)
}
