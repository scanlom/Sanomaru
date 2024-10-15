package main

import (
	"fmt"
	"log"
	"math"
	"time"

	"github.com/scanlom/Sanomaru/api"
	"github.com/scanlom/Sanomaru/cmn"
)

func CalculateReturn(table string, idCol string, id int, index float64, date string, interval string, years float64) float64 {
	var start float64
	var ret float64
	query := "select index from %s where %s=%d and date=" +
		"(select max(date) from %s where %s=%d and date <= (select date('%s') - interval '%s'))"
	// If the value is not present, leave the return zero, don't handle error
	_ = cmn.DbGet(&start, fmt.Sprintf(query, table, idCol, id, table, idCol, id, date, interval))
	if start > 0 {
		ret = cmn.Round(math.Pow(index/start, 1/years)-1, 0.0001)
		log.Printf("CalculateReturn: start: %f, ret: %f", start, ret)
	}
	return ret
}

func EnrichReturns(table string, idCol string, id int, name string, value float64, index float64, tci float64, divs float64, date string) api.JsonReturns {
	r := api.JsonReturns{}
	r.ID = id
	r.Name = name
	r.OneDay = CalculateReturn(table, idCol, id, index, date, "1 day", 1)
	r.OneWeek = CalculateReturn(table, idCol, id, index, date, "1 week", 1)
	r.OneMonth = CalculateReturn(table, idCol, id, index, date, "1 month", 1)
	r.ThreeMonths = CalculateReturn(table, idCol, id, index, date, "3 months", 1)
	r.OneYear = CalculateReturn(table, idCol, id, index, date, "1 year", 1)
	r.FiveYears = CalculateReturn(table, idCol, id, index, date, "5 years", 5)
	r.TenYears = CalculateReturn(table, idCol, id, index, date, "10 years", 10)
	r.ProfitLifetime = value - tci + divs
	return r
}

func EnrichYTDPositionReturns(r *api.JsonReturns, value float64, index float64, tci float64, divs float64, date string) error {
	yearStartDate := ""
	dateParsed, err := time.Parse("2006-01-02", date)
	if err != nil {
		return err
	}
	if dateParsed.Month() == 1 && dateParsed.Day() == 1 {
		yearStartDate = fmt.Sprintf("%d-%02d-%02d", dateParsed.Year()-1, dateParsed.Month(), dateParsed.Day())
	} else {
		yearStartDate = fmt.Sprintf("%d-%02d-%02d", dateParsed.Year(), 1, 1)
	}

	physd := api.JsonPositionHistory{}
	err = api.PositionsHistoryByPositionIDDate(r.ID, yearStartDate, &physd)
	if err != nil {
		// If there was no position on the first of the year, that's ok, returns are just zero
		return nil
	}

	if physd.Index > 0 {
		r.YearToDate = cmn.Round(index/physd.Index-1, 0.0001)
	}
	r.ProfitYearToDate = value - physd.Value - (tci - physd.TotalCashInfusion) + (divs - physd.AccumulatedDividends)
	return nil
}

func PopulateEnrichedPositionReturns(id int) {
	// 1. Enrich and add
	ep := api.JsonEnrichedPosition{}
	err := cmn.CacheGet(fmt.Sprintf("%s:%d", "enriched_positions", id), &ep)
	if err != nil {
		cmn.ErrorLog(err)
		return // Nothing we can do if the enriched position doesn't exist
	}
	ret := EnrichReturns("positions_history", "position_id", ep.ID, ep.Symbol, ep.Value, ep.Index, ep.TotalCashInfusion, ep.AccumulatedDividends, time.Now().Format("2006-01-02"))
	err = EnrichYTDPositionReturns(&ret, ep.Value, ep.Index, ep.TotalCashInfusion, ep.AccumulatedDividends, time.Now().Format("2006-01-02"))
	if err != nil {
		cmn.ErrorLog(err) // Strange, but survivable
	}
	cmn.CacheSet(fmt.Sprintf("%s:%d", "position_returns", ep.ID), ret)

	// 2. Add secondary indices
	// NOOP

	// 3. Update graph
	// NOOP
}

func PopulateEnrichedPosition(id int) {
	// 1. Enrich and add
	position := api.JsonPosition{}
	err := cmn.CacheGet(fmt.Sprintf("%s:%d", "positions", id), &position)
	if err != nil {
		cmn.ErrorLog(err)
		return // Nothing we can do if the position doesn't exist
	}
	ep := api.JsonEnrichedPosition{JsonPosition: position}
	rd := api.JsonRefData{}
	err = cmn.CacheGet(fmt.Sprintf("%s:%d", "ref_data", ep.RefDataID), &rd)
	if err != nil {
		cmn.ErrorLog(err) // Strange, but survivable
	}
	ep.Symbol = rd.Symbol
	ep.Description = rd.Description
	port := api.JsonPortfolio{}
	err = cmn.CacheGet(fmt.Sprintf("%s:%d", "portfolios", ep.PortfolioID), &port)
	if err != nil {
		cmn.ErrorLog(err) // Strange, but survivable
	}
	if ep.Active && port.Value > 0 {
		ep.PercentPortfolio = cmn.Round(ep.Value/port.Value, 0.0001)
	}
	cmn.CacheSet(fmt.Sprintf("%s:%d", "enriched_positions", ep.ID), ep)

	// 2. Add secondary indices
	cmn.CacheSet(fmt.Sprintf("%s:%s:%d", "enriched_positions_by_symbol_portfolio_id", ep.Symbol, ep.PortfolioID), ep)

	// 3. Update graph
	PopulateEnrichedPositionReturns(id)
	ids := cmn.CacheSMembers(fmt.Sprintf("%s:%d", "mergers_by_ref_data_id", ep.RefDataID))
	for i := range ids {
		PopulateEnrichedMerger(ids[i])
	}
}

func PositionsWork(ptr interface{}) {
	pos := *ptr.(*api.JsonPosition)

	// 1. Add secondary indices
	cmn.CacheSAdd(fmt.Sprintf("%s:%d", "positions_by_ref_data_id", pos.RefDataID), pos.ID)

	// 2. Update graph
	PopulateEnrichedPosition(pos.ID)
}
