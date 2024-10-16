package main

import (
	"fmt"

	"github.com/scanlom/Sanomaru/api"
	"github.com/scanlom/Sanomaru/cmn"
)

func RefDataWork(ptr interface{}) {
	rd := *ptr.(*api.JsonRefData)

	// 1. Add secondary indices
	md := api.JsonMarketData{}
	err := cmn.CacheGet(fmt.Sprintf("%s:%d", "market_data_by_ref_data_id", rd.ID), &md)
	if err == nil {
		cmn.CacheSet(fmt.Sprintf("%s:%s", "market_data_by_symbol", rd.Symbol), md)
	}

	// 1. Update graph
	cmn.CacheSMembersAndProcess(fmt.Sprintf("%s:%d", "s_positions_by_ref_data_id", rd.ID), PopulateEnrichedPosition)
	cmn.CacheSMembersAndProcess(fmt.Sprintf("%s:%d", "s_mergers_by_ref_data_id", rd.ID), PopulateEnrichedMerger)
	cmn.CacheSMembersAndProcess(fmt.Sprintf("%s:%d", "s_projections_by_ref_data_id", rd.ID), PopulateEnrichedProjections)
}
