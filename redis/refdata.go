package main

import (
	"fmt"

	"github.com/scanlom/Sanomaru/api"
	"github.com/scanlom/Sanomaru/cmn"
)

func RefDataWork(ptr interface{}) {
	rd := *ptr.(*api.JsonRefData)

	// 1. Add secondary indices
	cmn.CacheSet(fmt.Sprintf("%s:%s", "ref_data_by_symbol", rd.Symbol), rd)

	// 1. Update graph
	md := api.JsonMarketData{}
	err := cmn.CacheGet(fmt.Sprintf("%s:%d", "market_data_by_ref_data_id", rd.ID), &md)
	if err == nil {
		cmn.CacheLPush(fmt.Sprintf("%s_update", "market_data"), md.ID)
	}
	ids := cmn.CacheSMembers(fmt.Sprintf("%s:%d", "positions_by_ref_data_id", rd.ID))
	for i := range ids {
		cmn.CacheLPush(fmt.Sprintf("%s_update", "positions"), ids[i])
	}
	ids = cmn.CacheSMembers(fmt.Sprintf("%s:%d", "mergers_by_ref_data_id", rd.ID))
	for i := range ids {
		PopulateEnrichedMerger(ids[i])
	}
}
