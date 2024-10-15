package main

import (
	"fmt"

	"github.com/scanlom/Sanomaru/api"
	"github.com/scanlom/Sanomaru/cmn"
)

func MarketDataWork(ptr interface{}) {
	md := *ptr.(*api.JsonMarketData)

	// 1. Add secondary indices
	cmn.CacheSet(fmt.Sprintf("%s:%d", "market_data_by_ref_data_id", md.RefDataID), md)
	rd := api.JsonRefData{}
	err := cmn.CacheGet(fmt.Sprintf("%s:%d", "ref_data", md.RefDataID), &rd)
	if err == nil {
		cmn.CacheSet(fmt.Sprintf("%s:%s", "market_data_by_symbol", rd.Symbol), md)
	}

	// 2. Update graph
	ids := cmn.CacheSMembers(fmt.Sprintf("%s:%d", "positions_by_ref_data_id", md.RefDataID))
	for i := range ids {
		cmn.CacheLPush(fmt.Sprintf("%s_update", "positions"), ids[i])
	}
	ids = cmn.CacheSMembers(fmt.Sprintf("%s:%d", "mergers_by_ref_data_id", md.RefDataID))
	for i := range ids {
		PopulateEnrichedMerger(ids[i])
	}
}
