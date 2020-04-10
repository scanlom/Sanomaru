package api

import (
	"reflect"
	"strings"
)

type JsonStringValue struct {
	Value string `json:"value"`
}

type JsonFloat64Value struct {
	Value float64 `json:"value"`
}

type JsonRefData struct {
	ID                 int    `json:"id" db:"id"`
	Symbol             string `json:"symbol" db:"symbol"`
	SymbolAlphaVantage string `json:"symbolAlphaVantage" db:"symbol_alpha_vantage"`
}

type JsonMarketData struct {
	ID        int     `json:"id" db:"id"`
	RefDataID int     `json:"refDataId" db:"ref_data_id"`
	Last      float64 `json:"last" db:"last"`
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
	IncomeContOp        int64  `json:"incomeContOp" db:"income_cont_op"`
	NetExtrGainLoss     int64  `json:"netExtrGainLoss" db:"net_extr_gain_loss"`
	NetIncome           int64  `json:"netIncome" db:"net_income"`
	NetIncomeCommon     int64  `json:"netIncomeCommon" db:"net_income_common"`
}

func JsonToNamedInsert(val JsonSimfinIncome) string {
	var cols string
	var params string
	t := reflect.TypeOf(val)
	for i := 0; i < t.NumField(); i++ {
		tag := t.Field(i).Tag.Get("db")
		if tag != "id" {
			cols += tag + ","
			params += ":" + tag + ","
		}
	}
	cols = strings.TrimRight(cols, ",")
	params = strings.TrimRight(params, ",")
	ret := "INSERT INTO simfin_income (" + cols + ") VALUES (" + params + ")"
	return ret
}

func JsonToSelect(val JsonSimfinIncome) string {
	var cols string
	t := reflect.TypeOf(val)
	for i := 0; i < t.NumField(); i++ {
		tag := t.Field(i).Tag.Get("db")
		cols += tag + ","
	}
	cols = strings.TrimRight(cols, ",")
	ret := "SELECT " + cols + " FROM simfin_income"
	return ret
}
