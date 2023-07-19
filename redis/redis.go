package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/lib/pq"
	"github.com/scanlom/Sanomaru/api"
	"github.com/scanlom/Sanomaru/cmn"
)

// MSTODO: If there is any error I should know loud and clear
// MSTODO: threading should shutdown correctly (channel on SIGTERM)
// MSTODO: Try to do MDHSummary and other stuff from the api,
// MSTODO: Will keep this named redis.go until I'm ready to delete the current cache.go, then will swap in
// MSTODO: Should have alerting if a microservice isn't running
// MSTOMAYBE: Should i use native redis json?

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
		r.YearToDate = cmn.Round(index/physd.Index-1, 0.0001)
	}
	r.ProfitYearToDate = value - physd.Value - (tci - physd.TotalCashInfusion)
	return nil
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

func RefDataWork(ptr interface{}) {
	rd := *ptr.(*api.JsonRefData)

	// 1. Add secondary indices
	cmn.CacheSet(fmt.Sprintf("%s:%s", "ref_data_by_symbol", rd.Symbol), rd)

	// 2. Enrich and add
	// NOOP

	// 3. Update graph
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
		cmn.CacheLPush(fmt.Sprintf("%s_update", "mergers"), ids[i])
	}
}

func MarketDataWork(ptr interface{}) {
	md := *ptr.(*api.JsonMarketData)

	// 1. Add secondary indices
	cmn.CacheSet(fmt.Sprintf("%s:%d", "market_data_by_ref_data_id", md.RefDataID), md)
	rd := api.JsonRefData{}
	err := cmn.CacheGet(fmt.Sprintf("%s:%d", "ref_data", md.RefDataID), &rd)
	if err == nil {
		cmn.CacheSet(fmt.Sprintf("%s:%s", "market_data_by_symbol", rd.Symbol), md)
	}

	// 2. Enrich and add
	// NOOP

	// 3. Update graph
	ids := cmn.CacheSMembers(fmt.Sprintf("%s:%d", "positions_by_ref_data_id", md.RefDataID))
	for i := range ids {
		cmn.CacheLPush(fmt.Sprintf("%s_update", "positions"), ids[i])
	}
	ids = cmn.CacheSMembers(fmt.Sprintf("%s:%d", "mergers_by_ref_data_id", md.RefDataID))
	for i := range ids {
		cmn.CacheLPush(fmt.Sprintf("%s_update", "mergers"), ids[i])
	}
}

func PortfoliosWork(ptr interface{}) {
	// port := *ptr.(*api.JsonPortfolio)
}

func PositionsWork(ptr interface{}) {
	pos := *ptr.(*api.JsonPosition)

	// 1. Add secondary indices
	cmn.CacheSAdd(fmt.Sprintf("%s:%d", "positions_by_ref_data_id", pos.RefDataID), pos.ID)

	// 2. Enrich and add
	// First the enriched position
	ep := api.JsonEnrichedPosition{JsonPosition: pos}
	rd := api.JsonRefData{}
	err := cmn.CacheGet(fmt.Sprintf("%s:%d", "ref_data", ep.RefDataID), &rd)
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
	cmn.CacheSet(fmt.Sprintf("%s:%s:%d", "enriched_positions_by_symbol_portfolio_id", ep.Symbol, ep.PortfolioID), ep)
	// Second the position return
	ret := EnrichReturns("positions_history", "position_id", ep.ID, ep.Symbol, ep.Value, ep.Index, ep.TotalCashInfusion, ep.AccumulatedDividends, time.Now().Format("2006-01-02"))
	err = EnrichYTDPositionReturns(&ret, ep.Value, ep.Index, ep.TotalCashInfusion, ep.AccumulatedDividends, time.Now().Format("2006-01-02"))
	if err != nil {
		cmn.ErrorLog(err) // Strange, but survivable
	}
	cmn.CacheSet(fmt.Sprintf("%s:%d", "position_returns", ep.ID), ret)

	// 3. Update graph
	ids := cmn.CacheSMembers(fmt.Sprintf("%s:%d", "mergers_by_ref_data_id", pos.RefDataID))
	for i := range ids {
		cmn.CacheLPush(fmt.Sprintf("%s_update", "mergers"), ids[i])
	}
}

func MergersWork(ptr interface{}) {
	merger := *ptr.(*api.JsonMerger)

	// 1. Add secondary indices
	cmn.CacheSAdd(fmt.Sprintf("%s:%d", "mergers_by_ref_data_id", merger.AcquirerRefDataID), merger.ID)
	cmn.CacheSAdd(fmt.Sprintf("%s:%d", "mergers_by_ref_data_id", merger.TargetRefDataID), merger.ID)

	// 2. Enrich and add
	em := api.JsonEnrichedMerger{JsonMerger: merger}
	acquirer := api.JsonRefData{}
	err := cmn.CacheGet(fmt.Sprintf("%s:%d", "ref_data", em.AcquirerRefDataID), &acquirer)
	if err != nil {
		cmn.ErrorLog(err) // Strange, but survivable
	}
	target := api.JsonRefData{}
	err = cmn.CacheGet(fmt.Sprintf("%s:%d", "ref_data", em.TargetRefDataID), &target)
	if err != nil {
		cmn.ErrorLog(err) // Strange, but survivable
	}
	em.AcquirerTicker = acquirer.Symbol
	em.AcquirerDescription = acquirer.Description
	em.TargetTicker = target.Symbol
	em.TargetDescription = target.Description
	md := api.JsonMarketData{}
	err = cmn.CacheGet(fmt.Sprintf("%s:%s", "market_data_by_symbol", target.Symbol), &md)
	if err != nil {
		cmn.ErrorLog(err) // Strange, but survivable
	}
	em.Price = md.Last
	closeTime := cmn.DateStringToTime(em.CloseDate)
	strikeTime := cmn.DateStringToTime(em.AnnounceDate)
	daysToClose := closeTime.Sub(time.Now()).Hours() / 24
	fees := 0.005
	if strings.Contains(em.TargetTicker, ".HK") {
		fees = (0.0008 + 0.0013) * md.Last // 8 bps commision and 13 bps stamp on each side
	}
	if em.BreakPrice > 0 {
		em.Status = "B"
		if em.StrikePrice > 0 {
			em.StrikeReturn = cmn.Round((em.BreakPrice+em.Cash-fees)/em.StrikePrice-1, 0.0001)
			daysFromStrike := closeTime.Sub(strikeTime).Hours() / 24
			em.StrikeReturnAnnualized = cmn.Round((365/daysFromStrike)*em.StrikeReturn, 0.0001)
		}
	} else if daysToClose < 0 {
		em.Status = "C"
		if em.StrikePrice > 0 {
			em.StrikeReturn = cmn.Round((em.DealPrice+em.Cash-fees)/em.StrikePrice-1, 0.0001)
			daysFromStrike := closeTime.Sub(strikeTime).Hours() / 24
			em.StrikeReturnAnnualized = cmn.Round((365/daysFromStrike)*em.StrikeReturn, 0.0001)
		}
	} else {
		em.Status = "O"
		if em.StrikePrice > 0 {
			em.StrikeReturn = cmn.Round((md.Last+em.Cash-fees)/em.StrikePrice-1, 0.0001)
			daysFromStrike := time.Now().Sub(strikeTime).Hours() / 24
			em.StrikeReturnAnnualized = cmn.Round((365/daysFromStrike)*em.StrikeReturn, 0.0001)
		}
		em.MarketPositiveReturn = cmn.Round((em.DealPrice+em.Dividends-fees)/md.Last-1, 0.0001)
		em.MarketNetReturn = cmn.Round(
			((em.DealPrice+em.Dividends-fees-md.Last)*em.Confidence-(md.Last-em.FailPrice-em.Dividends+2*fees)*(1-em.Confidence))/md.Last, 0.0001)
		annualizeMultiple := 365 / daysToClose
		em.MarketPositiveReturnAnnualized = cmn.Round(annualizeMultiple*em.MarketPositiveReturn, 0.0001)
		em.MarketNetReturnAnnualized = cmn.Round(annualizeMultiple*em.MarketNetReturn, 0.0001)
	}
	ep := api.JsonEnrichedPosition{}
	err = cmn.CacheGet(fmt.Sprintf("%s:%s:%d", "enriched_positions_by_symbol_portfolio_id", target.Symbol, cmn.CONST_PORTFOLIO_RISK_ARB), &ep)
	// Don't pass the error up, it's ok if this isn't a position, we just populate zero
	if err == nil {
		em.PercentPortfolio = ep.PercentPortfolio
		em.PositionReturn = cmn.Round(ep.Index/100.0-1, 0.0001)
	}
	ret := api.JsonReturns{}
	err = cmn.CacheGet(fmt.Sprintf("%s:%d", "position_returns", ep.ID), &ret)
	// Don't pass the error up, it's ok if this isn't a position, we just populate zero
	if err == nil {
		em.ProfitLifetime = ret.ProfitLifetime
	}
	cmn.CacheSet(fmt.Sprintf("%s:%d", "enriched_mergers", em.ID), em)

	// 3. Update graph
	// NOOP
}

func UpdateWorker(list string, table string, ptr interface{}, obj interface{}, work func(interface{})) {
	for {
		log.Printf("Blocking on %s...", list)
		id := cmn.CacheBLPop(list)
		err := cmn.DbGet(ptr, fmt.Sprintf("%s WHERE id=%d", api.JsonToSelect(obj, table, ""), id))
		if err != nil {
			cmn.ErrorLog(err)
			continue // Strange, but a record may have been deleted, and that's survivable
		}

		cmn.CacheSet(fmt.Sprintf("%s:%d", table, id), ptr)
		work(ptr)
	}
	log.Printf("UpdateWorker %s completed", list)
}

func LoadUpdateIDList(table string) {
	ret := []api.JsonID{}
	err := cmn.DbSelect(&ret, api.JsonToSelect(api.JsonID{}, table, ""))
	if err != nil {
		cmn.ErrorLog(err)
		panic(err) // Can't survive a missing table
	}

	for i := range ret {
		cmn.CacheLPush(fmt.Sprintf("%s_update", table), ret[i].ID)
	}
}

func NotifyInsertUpdate(listener *pq.Listener) {
	for {
		select {
		case n := <-listener.Notify:
			log.Println("Received data from channel [", n.Channel, "] :")
			extra := api.JsonTableID{}
			err := json.Unmarshal([]byte(n.Extra), &extra)
			if err != nil {
				cmn.ErrorLog(err)
				panic(err) // Why are we getting a bad update
			}

			cmn.CacheLPush(fmt.Sprintf("%s_update", extra.Table), extra.ID)
		}
	}
}

func main() {
	err := cmn.CacheFlushAll()
	if err != nil {
		cmn.ErrorLog(err)
		panic(err) // Can't survive massive redis failure
	}

	listener, err := cmn.DbListen("insert_update")
	if err != nil {
		cmn.ErrorLog(err)
		panic(err) // Can't survive massive postgres failure
	}

	go NotifyInsertUpdate(listener)
	LoadUpdateIDList("ref_data")
	LoadUpdateIDList("market_data")
	LoadUpdateIDList("portfolios")
	LoadUpdateIDList("positions")
	LoadUpdateIDList("mergers")

	var wg sync.WaitGroup
	wg.Add(5)
	rd := api.JsonRefData{}
	go UpdateWorker("ref_data_update", "ref_data", &rd, rd, RefDataWork)
	md := api.JsonMarketData{}
	go UpdateWorker("market_data_update", "market_data", &md, md, MarketDataWork)
	port := api.JsonPortfolio{}
	go UpdateWorker("portfolios_update", "portfolios", &port, port, PortfoliosWork)
	pos := api.JsonPosition{}
	go UpdateWorker("positions_update", "positions", &pos, pos, PositionsWork)
	merger := api.JsonMerger{}
	go UpdateWorker("mergers_update", "mergers", &merger, merger, MergersWork)
	wg.Wait()
}
