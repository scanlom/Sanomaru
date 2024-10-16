package main

import (
	"fmt"
	"log"

	"github.com/scanlom/Sanomaru/api"
	"github.com/scanlom/Sanomaru/cmn"
)

func MarketDataWork(ptr interface{}) {
	md := *ptr.(*api.JsonMarketData)
	log.Printf("market_data update for %d", md.ID)

	// 1. Add secondary indices
	cmn.CacheSet(fmt.Sprintf("%s:%d", "market_data_by_ref_data_id", md.RefDataID), md)
	rd := api.JsonRefData{}
	err := cmn.CacheGet(fmt.Sprintf("%s:%d", "ref_data", md.RefDataID), &rd)
	if err == nil {
		cmn.CacheSet(fmt.Sprintf("%s:%s", "market_data_by_symbol", rd.Symbol), md)
	}

	// 2. Update graph
	cmn.CacheSMembersAndProcess(fmt.Sprintf("%s:%d", "s_positions_by_ref_data_id", md.RefDataID), PopulateEnrichedPosition)
	cmn.CacheSMembersAndProcess(fmt.Sprintf("%s:%d", "s_mergers_by_ref_data_id", md.RefDataID), PopulateEnrichedMerger)
	cmn.CacheSMembersAndProcess(fmt.Sprintf("%s:%d", "s_projections_by_ref_data_id", md.RefDataID), PopulateEnrichedProjections)
}
