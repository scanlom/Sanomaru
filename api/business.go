package api

import (
	"math"

	"github.com/scanlom/Sanomaru/cmn"
)

func Cagr(years float64, projections JsonProjections, md JsonMarketData) float64 {
	if projections.EPS <= 0.0 || projections.PETerminal <= 0.0 || md.Last <= 0.0 {
		return 0.0
	}
	divBucket := 0.0
	divGrowth, _ := Scalar(CONST_DIV_GROWTH)
	eps := projections.EPS
	for i := 0.0; i < years; i++ {
		divBucket = divBucket * (1.0 + divGrowth)
		divBucket = divBucket + (eps * projections.Payout)
		eps = eps * (1.0 + projections.Growth)
	}
	ret := math.Pow(((eps*float64(projections.PETerminal))+divBucket)/md.Last, 1.0/years) - 1.0
	return math.Round(ret*100000000) / 100000000
}

func Croe(years float64, projections JsonProjections, md JsonMarketData) float64 {
	if projections.Book <= 0.0 || projections.ROE <= 0.0 || projections.PETerminal <= 0.0 || md.Last <= 0.0 {
		return 0.0
	}
	divBucket := 0.0
	divGrowth, _ := Scalar(CONST_DIV_GROWTH)
	book := projections.Book
	eps := 0.0
	for i := 0.0; i < years; i++ {
		divBucket = divBucket * (1.0 + divGrowth)
		eps = book * projections.ROE
		div := eps * projections.Payout
		divBucket += div
		book += eps - div
	}
	ret := math.Pow(((eps*float64(projections.PETerminal))+divBucket)/md.Last, 1.0/years) - 1.0
	return math.Round(ret*100000000) / 100000000
}

func HeadlineFromSummary(summary []JsonSummary, peHighMmo5yr *int, peLowMmo5yr *int, epsCagr5yr *float64, epsCagr10yr *float64, roe5yr *float64) {
	var sumRoe5yr float64
	var sumH, sumL, maxH, maxL, minH, minL int
	minH = math.MaxInt64
	minL = math.MaxInt64
	if len(summary) > 6 && summary[0].EPS > 0.0 && summary[1].EPS > 0.0 && summary[5].EPS > 0.0 && summary[6].EPS > 0.0 {
		first := ((summary[5].EPS + summary[6].EPS) / 2.0)
		last := ((summary[0].EPS + summary[1].EPS) / 2.0)
		*epsCagr5yr = math.Pow(last/first, 0.2) - 1.0
	}
	if len(summary) > 10 && summary[0].EPS > 0.0 && summary[10].EPS > 0.0 {
		*epsCagr10yr = math.Pow(summary[0].EPS/summary[10].EPS, 0.1) - 1.0
	}

	if len(summary) > 4 {
		for i := 0; i < 5; i++ {
			if summary[i].PEHigh > maxH {
				maxH = summary[i].PEHigh
			}
			if summary[i].PELow > maxL {
				maxL = summary[i].PELow
			}
			if summary[i].PEHigh < minH {
				minH = summary[i].PEHigh
			}
			if summary[i].PELow < minL {
				minL = summary[i].PELow
			}
			sumH += summary[i].PEHigh
			sumL += summary[i].PELow
			sumRoe5yr += summary[i].ROE
		}
		*peHighMmo5yr = int(math.Round(float64(sumH-maxH-minH) / 3.0))
		*peLowMmo5yr = int(math.Round(float64(sumL-maxL-minL) / 3.0))
		*roe5yr = sumRoe5yr / 5.0
	}
}

func Round(x, unit float64) float64 {
	return math.Round(x/unit) * unit
}

func EnrichProjections(p JsonProjections) (JsonEnrichedProjections, error) {
	ep := JsonEnrichedProjections{JsonProjections: p}

	var refData JsonRefData
	err := RefDataByID(ep.RefDataID, &refData)
	if err != nil {
		return ep, nil
	}

	ep.Ticker = refData.Symbol
	ep.Description = refData.Description
	ep.Sector = refData.Sector
	ep.Industry = refData.Industry
	var summary []JsonSummary
	err = SummaryByTicker(ep.Ticker, &summary)
	if err != nil {
		return ep, nil
	}

	HeadlineFromSummary(summary, &ep.PEHighMMO5yr, &ep.PELowMMO5yr, &ep.EPSCagr5yr, &ep.EPSCagr10yr, &ep.ROE5yr)
	if len(summary) > 0 && summary[0].EPS > 0.0 && ep.EPSYr2 > 0.0 {
		ep.EPSCagr2yr = math.Pow(ep.EPSYr2/summary[0].EPS, 0.5) - 1.0
	}
	if len(summary) > 5 && summary[5].EPS > 0.0 && ep.EPSYr2 > 0.0 {
		ep.EPSCagr7yr = math.Pow(ep.EPSYr2/summary[5].EPS, 0.142857143) - 1.0
	}

	var md JsonMarketData
	err = MarketDataBySymbol(ep.Ticker, &md)

	// Don't pass the error up, it's ok if we don't get market data, we just can't calculate those fields
	if err == nil && md.Last > 0.0 {
		ep.Price = md.Last
		if ep.EPS > 0.0 {
			ep.PE = ep.Price / ep.EPS
		}
		ep.EPSYield = ep.EPS / ep.Price
		ep.DPSYield = ep.DPS / ep.Price
		ep.DivPlusGrowth = ep.DPSYield + ep.Growth
		ep.CAGR5yr = Cagr(5.0, ep.JsonProjections, md)
		ep.CROE5yr = Croe(5.0, ep.JsonProjections, md)
		ep.CAGR10yr = Cagr(10.0, ep.JsonProjections, md)
		ep.CROE10yr = Croe(10.0, ep.JsonProjections, md)
	}

	if len(summary) >= 5 {
		ep.Magic = ep.CAGR5yr
		for i := 0; i < 5; i++ {
			if summary[i].NetMgn < 0.10 || summary[i].LTDRatio > 3.5 || summary[i].EPS <= 0.0 {
				ep.Magic = 0.0
				break
			}
		}
	}

	var position JsonEnrichedPosition
	err = EnrichedPositionsBySymbolPortfolioID(ep.Ticker, cmn.CONST_PORTFOLIO_SELFIE, &position)
	// Don't pass the error up, it's ok if this isn't a position, we just populate zero
	if err == nil {
		ep.PercentPortfolio = position.PercentPortfolio
	}

	return ep, nil
}

func ConfidenceToInt(c string) int {
	switch c {
	case "H":
		return 5
	case "M":
		return 4
	case "N":
		return 3
	case "B":
		return 2
	case "L":
		return 1
	}
	return 0
}
