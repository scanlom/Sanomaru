package main

import (
	"fmt"
	"time"

	"github.com/scanlom/Sanomaru/api"
)

func EnrichYTDPortfolioReturns(r *api.JsonReturns, value float64, index float64, tci float64, date string) error {
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

	physd := api.JsonPortfolioHistory{}
	err = api.PortfoliosHistoryPortfolioIDDate(r.ID, yearStartDate, &physd)
	if err != nil {
		// If there was no position on the first of the year, that's ok, returns are just zero
		return nil
	}

	if physd.Index > 0 {
		r.YearToDate = api.Round(index/physd.Index-1, 0.0001)
	}
	r.ProfitYearToDate = value - physd.Value - (tci - physd.TotalCashInfusion)
	return nil
}

func PortfoliosWork(ptr interface{}) {
	port := *ptr.(*api.JsonPortfolio)

	// 1. Add secondary indices
	// NOOP

	// 2. Update graph
	api.CacheSMembersAndProcess(fmt.Sprintf("%s:%d", "s_positions_by_portfolio_id", port.ID), PopulateEnrichedPosition)
	// Position will then update any associated mergers
}
