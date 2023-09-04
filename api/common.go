package api

import (
	"fmt"
	"log"
	"reflect"
	"strings"

	"github.com/jmoiron/sqlx/types"
)

type JsonFloat64Value struct {
	Value float64 `json:"value"`
}

type JsonID struct {
	ID int `json:"id" db:"id"`
}

type JsonTableID struct {
	Table string `json:"table"`
	ID    int    `json:"id"`
}

type JsonRefData struct {
	ID                 int    `json:"id" db:"id"`
	Symbol             string `json:"symbol" db:"symbol"`
	SymbolAlphaVantage string `json:"symbolAlphaVantage" db:"symbol_alpha_vantage"`
	Description        string `json:"description" db:"description"`
	Sector             string `json:"sector" db:"sector"`
	Industry           string `json:"industry" db:"industry"`
	Active             bool   `json:"active" db:"active"`
}

type JsonMarketData struct {
	ID        int     `json:"id" db:"id"`
	RefDataID int     `json:"refDataId" db:"ref_data_id"`
	Last      float64 `json:"last" db:"last"`
}

type JsonEnrichedMarketData struct {
	JsonMarketData
	Stale bool `json:"stale" db:"stale"`
}

type JsonMarketDataHistorical struct {
	ID        int     `json:"id" db:"id"`
	Date      string  `json:"date" db:"date"`
	RefDataID int     `json:"refDataId" db:"ref_data_id"`
	AdjClose  float64 `json:"adjClose" db:"adj_close"`
	Close     float64 `json:"close" db:"close"`
}

type JsonProjections struct {
	ID         int     `json:"id" db:"id"`
	RefDataID  int     `json:"refDataId" db:"ref_data_id"`
	Date       string  `json:"date" db:"date"`
	EPS        float64 `json:"eps" db:"eps"`
	DPS        float64 `json:"dps" db:"dps"`
	Growth     float64 `json:"growth" db:"growth"`
	PETerminal int     `json:"peTerminal" db:"pe_terminal"`
	Payout     float64 `json:"payout" db:"payout"`
	Book       float64 `json:"book" db:"book"`
	ROE        float64 `json:"roe" db:"roe"`
	EPSYr1     float64 `json:"epsYr1" db:"eps_yr1"`
	EPSYr2     float64 `json:"epsYr2" db:"eps_yr2"`
	Confidence string  `json:"confidence" db:"confidence"`
	Watch      bool    `json:"watch" db:"watch"`
}

type JsonMDHYearSummary struct {
	RefDataID int     `json:"refDataId" db:"ref_data_id"`
	High      float64 `json:"high" db:"high"`
	Low       float64 `json:"low" db:"low"`
	Close     float64 `json:"close" db:"close"`
}

type JsonSimfinIncome struct {
	ID                  int    `json:"id" db:"id"`
	Ticker              string `json:"ticker" db:"ticker"`
	SimfinID            int    `json:"simfinId" db:"simfin_id"`
	CCY                 string `json:"ccy" db:"currency"`
	FiscalYear          int    `json:"fiscalYear" db:"fiscal_year"`
	FiscalPeriod        string `json:"fiscalPeriod" db:"fiscal_period"`
	ReportDate          string `json:"reportDate" db:"report_date"`
	PublishDate         string `json:"publishDate" db:"publish_date"`
	RestatedDate        string `json:"restatedDate" db:"restated_date"`
	SharesBasic         int64  `json:"sharesBasic" db:"shares_basic"`
	SharesDiluted       int64  `json:"sharesDiluted" db:"shares_diluted"`
	Revenue             int64  `json:"revenue" db:"revenue"`
	CostRevenue         int64  `json:"costRevenue" db:"cost_revenue"`
	GrossProfit         int64  `json:"grossProfit" db:"gross_profit"`
	OperatingExpenses   int64  `json:"operatingExpenses" db:"operating_expenses"`
	SellingGenAdmin     int64  `json:"sellingGenAdmin" db:"selling_gen_admin"`
	ResearchDev         int64  `json:"researchDev" db:"research_dev"`
	DeprAmor            int64  `json:"deprAmor" db:"depr_amor"`
	OperatingIncome     int64  `json:"operatingIncome" db:"operating_income"`
	NonOperatingIncome  int64  `json:"nonOperatingIncome" db:"non_operating_income"`
	InterestExpNet      int64  `json:"interestExpNet" db:"interest_exp_net"`
	PretaxIncomeLossAdj int64  `json:"pretaxIncomeLossAdj" db:"pretax_income_loss_adj"`
	AbnormGainLoss      int64  `json:"abnormGainLoss" db:"abnorm_gain_loss"`
	PretaxIncomeLoss    int64  `json:"pretaxIncomeLoss" db:"pretax_income_loss"`
	IncomeTax           int64  `json:"incomeTax" db:"income_tax"`
	IncomeAffilNetTax   int64  `json:"incomeAffilNetTax" db:"income_affil_net_tax"`
	IncomeContOp        int64  `json:"incomeContOp" db:"income_cont_op"`
	NetExtrGainLoss     int64  `json:"netExtrGainLoss" db:"net_extr_gain_loss"`
	NetIncome           int64  `json:"netIncome" db:"net_income"`
	NetIncomeCommon     int64  `json:"netIncomeCommon" db:"net_income_common"`
	EntryType           string `json:"entryType" db:"entry_type"`
}

type JsonIncome struct {
	JsonSimfinIncome
	EPS float64 `json:"eps"`
}

type JsonSimfinBalance struct {
	ID                int    `json:"id" db:"id"`
	Ticker            string `json:"ticker" db:"ticker"`
	SimfinID          int    `json:"simfinId" db:"simfin_id"`
	CCY               string `json:"ccy" db:"currency"`
	FiscalYear        int    `json:"fiscalYear" db:"fiscal_year"`
	FiscalPeriod      string `json:"fiscalPeriod" db:"fiscal_period"`
	ReportDate        string `json:"reportDate" db:"report_date"`
	PublishDate       string `json:"publishDate" db:"publish_date"`
	RestatedDate      string `json:"restatedDate" db:"restated_date"`
	SharesBasic       int64  `json:"sharesBasic" db:"shares_basic"`
	SharesDiluted     int64  `json:"sharesDiluted" db:"shares_diluted"`
	CashEquivStInvest int64  `json:"cashEquivStInvest" db:"cash_equiv_st_invest"`
	AccNotesRecv      int64  `json:"accNotesRecv" db:"acc_notes_recv"`
	Inventories       int64  `json:"inventories" db:"inventories"`
	TotalCurAssets    int64  `json:"totalCurAssets" db:"total_cur_assets"`
	PropPlantEquipNet int64  `json:"propPlantEquipNet" db:"prop_plant_equip_net"`
	LtInvestRecv      int64  `json:"ltInvestRecv" db:"lt_invest_recv"`
	OtherLtAssets     int64  `json:"otherLtAssets" db:"other_lt_assets"`
	TotalNoncurAssets int64  `json:"totalNoncurAssets" db:"total_noncur_assets"`
	TotalAssets       int64  `json:"totalAssets" db:"total_assets"`
	PayablesAccruals  int64  `json:"payablesAccruals" db:"payables_accruals"`
	StDebt            int64  `json:"stDebt" db:"st_debt"`
	TotalCurLiab      int64  `json:"totalCurLiab" db:"total_cur_liab"`
	LtDebt            int64  `json:"ltDebt" db:"lt_debt"`
	TotalNoncurLiab   int64  `json:"totalNoncurLiab" db:"total_noncur_liab"`
	TotalLiabilities  int64  `json:"totalLiabilities" db:"total_liabilities"`
	PreferredEquity   int64  `json:"preferredEquity" db:"preferred_equity"`
	ShareCapitalAdd   int64  `json:"shareCapitalAdd" db:"share_capital_add"`
	TreasuryStock     int64  `json:"treasuryStock" db:"treasury_stock"`
	RetainedEarnings  int64  `json:"retainedEarnings" db:"retained_earnings"`
	TotalEquity       int64  `json:"totalEquity" db:"total_equity"`
	TotalLiabEquity   int64  `json:"totalLiabEquity" db:"total_liab_equity"`
	EntryType         string `json:"entryType" db:"entry_type"`
}

type JsonBalance struct {
	JsonSimfinBalance
}

type JsonSimfinCashflow struct {
	ID                   int    `json:"id" db:"id"`
	Ticker               string `json:"ticker" db:"ticker"`
	SimfinID             int    `json:"simfinId" db:"simfin_id"`
	CCY                  string `json:"ccy" db:"currency"`
	FiscalYear           int    `json:"fiscalYear" db:"fiscal_year"`
	FiscalPeriod         string `json:"fiscalPeriod" db:"fiscal_period"`
	ReportDate           string `json:"reportDate" db:"report_date"`
	PublishDate          string `json:"publishDate" db:"publish_date"`
	RestatedDate         string `json:"restatedDate" db:"restated_date"`
	SharesBasic          int64  `json:"sharesBasic" db:"shares_basic"`
	SharesDiluted        int64  `json:"sharesDiluted" db:"shares_diluted"`
	NetIncomeStart       int64  `json:"netIncomeStart" db:"net_income_start"`
	DeprAmor             int64  `json:"deprAmor" db:"depr_amor"`
	NonCashItems         int64  `json:"nonCashItems" db:"non_cash_items"`
	ChgWorkingCapital    int64  `json:"chgWorkingCapital" db:"chg_working_capital"`
	ChgAccountsRecv      int64  `json:"chgAccountsRecv" db:"chg_accounts_recv"`
	ChgInventories       int64  `json:"chgInventories" db:"chg_inventories"`
	ChgAccPayable        int64  `json:"chgAccPayable" db:"chg_acc_payable"`
	ChgOther             int64  `json:"chgOther" db:"chg_other"`
	NetCashOps           int64  `json:"netCashOps" db:"net_cash_ops"`
	ChgFixAssetsInt      int64  `json:"chgFixAssetsInt" db:"chg_fix_assets_intcapex"`
	NetChgLtInvest       int64  `json:"netChgLtInvest" db:"net_chg_lt_invest"`
	NetCashAcqDivest     int64  `json:"netCashAcqDivest" db:"net_cash_acq_divest"`
	NetCashInv           int64  `json:"netCashInv" db:"net_cash_inv"`
	DividendsPaid        int64  `json:"dividendsPaid" db:"dividends_paid"`
	CashRepayDebt        int64  `json:"cashRepayDebt" db:"cash_repay_debt"`
	CashRepurchaseEquity int64  `json:"cashRepurchaseEquity" db:"cash_repurchase_equity"`
	NetCashFin           int64  `json:"netCashFin" db:"net_cash_fin"`
	EffectFxRates        int64  `json:"effectFxRates" db:"effect_fx_rates"`
	NetChgCash           int64  `json:"netChgCash" db:"net_chg_cash"`
	EntryType            string `json:"entryType" db:"entry_type"`
}

type JsonCashflow struct {
	JsonSimfinCashflow
	DPS float64 `json:"dps"`
}

type JsonProjectionsStats struct {
	Total  int  `json:"total"`
	High   int  `json:"high"`
	Medium int  `json:"medium"`
	None   int  `json:"none"`
	Blah   int  `json:"blah"`
	Low    int  `json:"low"`
	Fresh  int  `json:"fresh"`
	PW1    bool `json:"pw1"`
}

type JsonEnrichedProjections struct {
	JsonProjections
	// Ref Data
	Ticker      string  `json:"ticker" db:"ticker"`
	Description string  `json:"description" db:"description"`
	Sector      string  `json:"sector" db:"sector"`
	Industry    string  `json:"industry" db:"industry"`
	Price       float64 `json:"price" db:"price"`
	// Position
	PercentPortfolio float64 `json:"percentPortfolio" db:"percent_portfolio"`
	// Derived - Financials
	EPSCagr5yr   float64 `json:"epsCagr5yr" db:"eps_cagr_5yr"`
	EPSCagr10yr  float64 `json:"epsCagr10yr" db:"eps_cagr_10yr"`
	PEHighMMO5yr int     `json:"peHighMmo5yr" db:"pe_high_mmo_5yr"`
	PELowMMO5yr  int     `json:"peLowMmo5yr" db:"pe_low_mmo_5yr"`
	ROE5yr       float64 `json:"roe5yr" db:"roe_5yr"`
	// Derived - Projections
	PE            float64 `json:"pe" db:"pe"`
	EPSCagr2yr    float64 `json:"epsCagr2yr" db:"eps_cagr_2yr"`
	EPSCagr7yr    float64 `json:"epsCagr7yr" db:"eps_cagr_7yr"`
	DivPlusGrowth float64 `json:"divPlusGrowth" db:"div_plus_growth"`
	EPSYield      float64 `json:"epsYield" db:"eps_yield"`
	DPSYield      float64 `json:"dpsYield" db:"dps_yield"`
	CAGR5yr       float64 `json:"cagr5yr" db:"cagr_5yr"`
	CAGR10yr      float64 `json:"cagr10yr" db:"cagr_10yr"`
	CROE5yr       float64 `json:"croe5yr" db:"croe_5yr"`
	CROE10yr      float64 `json:"croe10yr" db:"croe_10yr"`
	Magic         float64 `json:"magic" db:"magic"`
}

type JsonEnrichedProjectionsJournal struct {
	JsonEnrichedProjections
	ProjectionsID int    `json:"projectionsId" db:"projections_id"`
	Entry         string `json:"entry" db:"entry"`
}

type JsonSummary struct {
	ReportDate    string  `json:"reportDate"`
	EPS           float64 `json:"eps"`
	DPS           float64 `json:"dps"`
	PEHigh        int     `json:"peHigh"`
	PELow         int     `json:"peLow"`
	ROE           float64 `json:"roe"`
	ROA           float64 `json:"roa"`
	GrMgn         float64 `json:"grMgn"`
	OpMgn         float64 `json:"opMgn"`
	NetMgn        float64 `json:"netMgn"`
	LTDRatio      float64 `json:"ltdRatio"`
	IntCov        float64 `json:"intCov"`
	MarketCap     int64   `json:"marketCap"`
	SharesDiluted int64   `json:"sharesDiluted" db:"shares_diluted"`
}

type JsonMerger struct {
	ID                int     `json:"id" db:"id"`
	Date              string  `json:"date" db:"date"`
	AcquirerRefDataID int     `json:"acquirerRefDataId" db:"acquirer_ref_data_id"`
	TargetRefDataID   int     `json:"targetRefDataId" db:"target_ref_data_id"`
	DealPrice         float64 `json:"dealPrice" db:"deal_price"`
	FailPrice         float64 `json:"failPrice" db:"fail_price"`
	BreakPrice        float64 `json:"breakPrice" db:"break_price"`
	StrikePrice       float64 `json:"strikePrice" db:"strike_price"`
	AnnounceDate      string  `json:"announceDate" db:"announce_date"`
	MeetingDate       string  `json:"meetingDate" db:"meeting_date"`
	CloseDate         string  `json:"closeDate" db:"close_date"`
	BreakDate         string  `json:"breakDate" db:"break_date"`
	Confidence        float64 `json:"confidence" db:"confidence"`
	Dividends         float64 `json:"dividends" db:"dividends"`
	Cash              float64 `json:"cash" db:"cash"`
}

type JsonEnrichedMerger struct {
	JsonMerger
	AcquirerTicker                 string  `json:"acquirerTicker" db:"acquirer_ticker"`
	AcquirerDescription            string  `json:"acquirerDescription" db:"acquirer_description"`
	TargetTicker                   string  `json:"targetTicker" db:"target_ticker"`
	TargetDescription              string  `json:"targetDescription" db:"target_description"`
	Price                          float64 `json:"price" db:"price"`
	MarketPositiveReturn           float64 `json:"marketPositiveReturn" db:"market_positive_return"`
	MarketNetReturn                float64 `json:"marketNetReturn" db:"market_net_return"`
	MarketPositiveReturnAnnualized float64 `json:"marketPositiveReturnAnnualized" db:"market_positive_return_annualized"`
	MarketNetReturnAnnualized      float64 `json:"marketNetReturnAnnualized" db:"market_net_return_annualized"`
	StrikeReturn                   float64 `json:"strikeReturn" db:"strike_return"`
	StrikeReturnAnnualized         float64 `json:"strikeReturnAnnualized" db:"strike_return_annualized"`
	PercentPortfolio               float64 `json:"percentPortfolio" db:"percent_portfolio"`
	Status                         string  `json:"status" db:"status"`
	// Returns
	PositionReturn float64 `json:"positionReturn" db:"position_return"`
	ProfitLifetime float64 `json:"profitLifetime" db:"profit_lifetime"`
}

type JsonEnrichedMergerJournal struct {
	JsonEnrichedMerger
	MergerID int    `json:"mergerId" db:"mergers_id"`
	Entry    string `json:"entry" db:"entry"`
}

type JsonPortfolio struct {
	ID                  int     `json:"id" db:"id"`
	Name                string  `json:"name" db:"name"`
	Value               float64 `json:"value" db:"value"`
	Index               float64 `json:"index" db:"index"`
	Divisor             float64 `json:"divisor" db:"divisor"`
	Cash                float64 `json:"cash" db:"cash"`
	Debt                float64 `json:"debt" db:"debt"`
	ValueTotalCapital   float64 `json:"valueTotalCapital" db:"value_total_capital"`
	IndexTotalCapital   float64 `json:"indexTotalCapital" db:"index_total_capital"`
	DivisorTotalCapital float64 `json:"divisorTotalCapital" db:"divisor_total_capital"`
	TotalCashInfusion   float64 `json:"totalCashInfusion" db:"total_cash_infusion"`
	Model               float64 `json:"model" db:"model"`
	Active              bool    `json:"active" db:"active"`
}

type JsonEnrichedPortfolio struct {
	JsonPortfolio
	PercentTotal float64 `json:"percentTotal"`
	PercentCash  float64 `json:"percentCash"`
	PercentDebt  float64 `json:"percentDebt"`
}

type JsonReturns struct {
	ID               int     `json:"id"`
	Name             string  `json:"name"`
	OneDay           float64 `json:"oneDay"`
	OneWeek          float64 `json:"oneWeek"`
	OneMonth         float64 `json:"oneMonth"`
	ThreeMonths      float64 `json:"threeMonths"`
	OneYear          float64 `json:"oneYear"`
	FiveYears        float64 `json:"fiveYears"`
	TenYears         float64 `json:"tenYears"`
	YearToDate       float64 `json:"yearToDate"`
	ProfitYearToDate float64 `json:"profitYearToDate"`
	ProfitLifetime   float64 `json:"profitLifetime"`
}

type JsonPortfolioHistory struct {
	JsonPortfolio
	Date        string `json:"date" db:"date"`
	PortfolioID int    `json:"portfolioId" db:"portfolio_id"`
}

type JsonPosition struct {
	ID                   int     `json:"id" db:"id"`
	RefDataID            int     `json:"refDataId" db:"ref_data_id"`
	PortfolioID          int     `json:"portfolioId" db:"portfolio_id"`
	Quantity             float64 `json:"quantity" db:"quantity"`
	Price                float64 `json:"price" db:"price"`
	Value                float64 `json:"value" db:"value"`
	Index                float64 `json:"index" db:"index"`
	Divisor              float64 `json:"divisor" db:"divisor"`
	CostBasis            float64 `json:"costBasis" db:"cost_basis"`
	TotalCashInfusion    float64 `json:"totalCashInfusion" db:"total_cash_infusion"`
	AccumulatedDividends float64 `json:"accumulatedDividends" db:"accumulated_dividends"`
	Model                float64 `json:"model" db:"model"`
	PricingType          int     `json:"pricingType" db:"pricing_type"`
	Active               bool    `json:"active" db:"active"`
}

type JsonPositionHistory struct {
	JsonPosition
	Date       string `json:"date" db:"date"`
	PositionID int    `json:"positionId" db:"position_id"`
}

type JsonEnrichedPosition struct {
	JsonPosition
	Symbol           string  `json:"symbol" db:"symbol"`
	Description      string  `json:"description" db:"description"`
	PercentPortfolio float64 `json:"percentPortfolio"`
}

type JsonEnrichedPositionHistory struct {
	JsonEnrichedPosition
	Date       string `json:"date" db:"date"`
	PositionID int    `json:"positionId" db:"position_id"`
}

type JsonTransaction struct {
	ID              int            `json:"id" db:"id"`
	Date            string         `json:"date" db:"date"`
	Type            int            `json:"type" db:"type"`
	SubType         int            `json:"subType" db:"sub_type"`
	PositionID      int            `json:"positionId" db:"position_id"`
	PortfolioID     int            `json:"portfolioId" db:"portfolio_id"`
	Value           float64        `json:"value" db:"value"`
	Quantity        float64        `json:"quantity" db:"quantity"`
	PositionBefore  types.JSONText `json:"positionBefore" db:"position_before"`
	PositionAfter   types.JSONText `json:"positionAfter" db:"position_after"`
	PortfolioBefore types.JSONText `json:"portfolioBefore" db:"portfolio_before"`
	PortfolioAfter  types.JSONText `json:"portfolioAfter" db:"portfolio_after"`
	Note            string         `json:"note" db:"note"`
}

func JsonToNamedInsertInternal(t reflect.Type, cols *string, params *string) {
	for i := 0; i < t.NumField(); i++ {
		if t.Field(i).Type.Kind() == reflect.Struct {
			JsonToNamedInsertInternal(t.Field(i).Type, cols, params)
		} else {
			tag := t.Field(i).Tag.Get("db")
			if len(tag) > 0 && tag != "id" {
				*cols += tag + ","
				*params += ":" + tag + ","
			}
		}
	}
}

func JsonToNamedInsert(val interface{}, table string) string {
	var cols string
	var params string
	t := reflect.TypeOf(val)

	// Sub Structures must be handled recursively
	JsonToNamedInsertInternal(t, &cols, &params)

	cols = strings.TrimRight(cols, ",")
	params = strings.TrimRight(params, ",")
	ret := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, cols, params)
	return ret
}

func JsonToNamedUpdateInternal(t reflect.Type, update *string) {
	for i := 0; i < t.NumField(); i++ {
		if t.Field(i).Type.Kind() == reflect.Struct {
			JsonToNamedUpdateInternal(t.Field(i).Type, update)
		} else {
			tag := t.Field(i).Tag.Get("db")
			if len(tag) > 0 && tag != "id" {
				*update += tag + "=:" + tag + ","
			}
		}
	}
}

func JsonToNamedUpdate(val interface{}, table string) string {
	var update string
	t := reflect.TypeOf(val)

	// Sub Structures must be handled recursively
	JsonToNamedUpdateInternal(t, &update)

	update = strings.TrimRight(update, ",")
	ret := fmt.Sprintf("UPDATE %s SET %s", table, update)
	return ret
}

func JsonToSelectInternal(t reflect.Type, prefix string, cols *string) {
	for i := 0; i < t.NumField(); i++ {
		if t.Field(i).Type.Kind() == reflect.Struct {
			JsonToSelectInternal(t.Field(i).Type, prefix, cols)
		} else {
			tag := t.Field(i).Tag.Get("db")
			if len(tag) > 0 {
				if len(prefix) > 0 {
					*cols += prefix + "."
				}
				*cols += tag + ","
			}
		}
	}
}

func JsonToSelect(val interface{}, table string, prefix string) string {
	var cols string
	t := reflect.TypeOf(val)

	// Sub Structures must be handled recursively
	JsonToSelectInternal(t, prefix, &cols)

	cols = strings.TrimRight(cols, ",")
	ret := fmt.Sprintf("SELECT %s FROM %s", cols, table)
	if len(prefix) > 0 {
		ret += " " + prefix
	}
	log.Println(ret)
	return ret
}
