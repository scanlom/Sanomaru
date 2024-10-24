package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/scanlom/Sanomaru/api"
)

func PopulateEnrichedMerger(id int) {
	// 1. Enrich and add
	merger := api.JsonMerger{}
	err := api.CacheGet(fmt.Sprintf("%s:%d", "mergers", id), &merger)
	if err != nil {
		api.ErrorLog(err)
		return // Nothing we can do if the merger doesn't exist
	}
	em := api.JsonEnrichedMerger{JsonMerger: merger}
	acquirer := api.JsonRefData{}
	err = api.CacheGet(fmt.Sprintf("%s:%d", "ref_data", em.AcquirerRefDataID), &acquirer)
	if err != nil {
		api.ErrorLog(err) // Strange, but survivable
	}
	target := api.JsonRefData{}
	err = api.CacheGet(fmt.Sprintf("%s:%d", "ref_data", em.TargetRefDataID), &target)
	if err != nil {
		api.ErrorLog(err) // Strange, but survivable
	}
	em.AcquirerTicker = acquirer.Symbol
	em.AcquirerDescription = acquirer.Description
	em.TargetTicker = target.Symbol
	em.TargetDescription = target.Description
	md := api.JsonMarketData{}
	err = api.CacheGet(fmt.Sprintf("%s:%s", "market_data_by_symbol", target.Symbol), &md)
	if err != nil {
		api.ErrorLog(err) // Strange, but survivable
	}
	em.Price = md.Last
	closeTime := api.DateStringToTime(em.CloseDate)
	strikeTime := api.DateStringToTime(em.AnnounceDate)
	daysToClose := closeTime.Sub(time.Now()).Hours() / 24
	fees := 0.005
	if strings.Contains(em.TargetTicker, ".HK") {
		fees = (0.0008 + 0.0013) * md.Last // 8 bps commision and 13 bps stamp on each side
	}
	if em.BreakPrice > 0 {
		em.Status = "B"
		if em.StrikePrice > 0 {
			em.StrikeReturn = api.Round((em.BreakPrice+em.Cash-fees)/em.StrikePrice-1, 0.0001)
			daysFromStrike := closeTime.Sub(strikeTime).Hours() / 24
			em.StrikeReturnAnnualized = api.Round((365/daysFromStrike)*em.StrikeReturn, 0.0001)
		}
	} else if daysToClose < 0 {
		em.Status = "C"
		if em.StrikePrice > 0 {
			em.StrikeReturn = api.Round((em.DealPrice+em.Cash-fees)/em.StrikePrice-1, 0.0001)
			daysFromStrike := closeTime.Sub(strikeTime).Hours() / 24
			em.StrikeReturnAnnualized = api.Round((365/daysFromStrike)*em.StrikeReturn, 0.0001)
		}
	} else {
		em.Status = "O"
		if em.StrikePrice > 0 {
			em.StrikeReturn = api.Round((md.Last+em.Cash-fees)/em.StrikePrice-1, 0.0001)
			daysFromStrike := time.Now().Sub(strikeTime).Hours() / 24
			em.StrikeReturnAnnualized = api.Round((365/daysFromStrike)*em.StrikeReturn, 0.0001)
		}
		em.MarketPositiveReturn = api.Round((em.DealPrice+em.Dividends-fees)/md.Last-1, 0.0001)
		em.MarketNetReturn = api.Round(
			((em.DealPrice+em.Dividends-fees-md.Last)*em.Confidence-(md.Last-em.FailPrice-em.Dividends+2*fees)*(1-em.Confidence))/md.Last, 0.0001)
		annualizeMultiple := 365 / daysToClose
		em.MarketPositiveReturnAnnualized = api.Round(annualizeMultiple*em.MarketPositiveReturn, 0.0001)
		em.MarketNetReturnAnnualized = api.Round(annualizeMultiple*em.MarketNetReturn, 0.0001)
	}
	ep := api.JsonEnrichedPosition{}
	err = api.CacheGet(fmt.Sprintf("%s:%s:%d", "enriched_positions_by_symbol_portfolio_id", target.Symbol, api.CONST_PORTFOLIO_RISK_ARB), &ep)
	// Don't pass the error up, it's ok if this isn't a position, we just populate zero
	if err == nil {
		em.PercentPortfolio = ep.PercentPortfolio
		em.PositionReturn = api.Round(ep.Index/100.0-1, 0.0001)
	}
	ret := api.JsonReturns{}
	err = api.CacheGet(fmt.Sprintf("%s:%d", "position_returns", ep.ID), &ret)
	// Don't pass the error up, it's ok if this isn't a position, we just populate zero
	if err == nil {
		em.ProfitLifetime = ret.ProfitLifetime
	}
	api.CacheSet(fmt.Sprintf("%s:%d", "enriched_mergers", em.ID), em)

	// 2. Add secondary indices
	// NOOP

	// 3. Update graph
	// NOOP
}

func MergersWork(ptr interface{}) {
	merger := *ptr.(*api.JsonMerger)

	// 1. Add secondary indices
	api.CacheSAdd(fmt.Sprintf("%s:%d", "s_mergers_by_ref_data_id", merger.AcquirerRefDataID), merger.ID)
	api.CacheSAdd(fmt.Sprintf("%s:%d", "s_mergers_by_ref_data_id", merger.TargetRefDataID), merger.ID)

	// 2. Update graph
	PopulateEnrichedMerger(merger.ID)
}
