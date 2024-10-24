package main

import (
	"fmt"
	"log"

	"github.com/scanlom/Sanomaru/api"
)

func MarketDataWork(ptr interface{}) {
	md := *ptr.(*api.JsonMarketData)
	log.Printf("market_data update for %d", md.ID)

	// 1. Add secondary indices
	api.CacheSet(fmt.Sprintf("%s:%d", "market_data_by_ref_data_id", md.RefDataID), md)
	rd := api.JsonRefData{}
	err := api.CacheGet(fmt.Sprintf("%s:%d", "ref_data", md.RefDataID), &rd)
	if err == nil {
		api.CacheSet(fmt.Sprintf("%s:%s", "market_data_by_symbol", rd.Symbol), md)
	}

	// 2. Update graph
	api.CacheSMembersAndProcess(fmt.Sprintf("%s:%d", "s_positions_by_ref_data_id", md.RefDataID), PopulateEnrichedPosition)
	api.CacheSMembersAndProcess(fmt.Sprintf("%s:%d", "s_mergers_by_ref_data_id", md.RefDataID), PopulateEnrichedMerger)
	api.CacheSMembersAndProcess(fmt.Sprintf("%s:%d", "s_projections_by_ref_data_id", md.RefDataID), PopulateEnrichedProjections)
}
