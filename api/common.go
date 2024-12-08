package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/types"
	"github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"gopkg.in/yaml.v2"
)

const CONST_PRICING_TYPE_BY_PRICE = 1
const CONST_PRICING_TYPE_BY_VALUE = 2

const CONST_PORTFOLIO_TOTAL = 1
const CONST_PORTFOLIO_SELFIE = 2
const CONST_PORTFOLIO_OAK = 3
const CONST_PORTFOLIO_MANAGED = 4
const CONST_PORTFOLIO_RISK_ARB = 5
const CONST_PORTFOLIO_TRADE_FIN = 6
const CONST_PORTFOLIO_QUICK = 7
const CONST_PORTFOLIO_PORTFOLIO = 8
const CONST_PORTFOLIO_NONE = 99

const CONST_TXN_TYPE_BUY = 1
const CONST_TXN_TYPE_SELL = 2
const CONST_TXN_TYPE_DIV = 3
const CONST_TXN_TYPE_CI = 4
const CONST_TXN_TYPE_DI = 5
const CONST_TXN_TYPE_INT = 6

const CONST_CONFIDENCE_LOW = 1
const CONST_CONFIDENCE_BLAH = 2
const CONST_CONFIDENCE_NONE = 3
const CONST_CONFIDENCE_MEDIUM = 4
const CONST_CONFIDENCE_HIGH = 5

const CONST_FX_GBP = 76.34
const CONST_FX_HKD = 7.79
const CONST_FX_ILS = 342
const CONST_FX_INR = 73.52
const CONST_FX_JPY = 104.01
const CONST_FX_SGD = 1.35

const CONST_FUDGE = 0.0004

var CONST_FX_MAP = map[string]float64{
	"1373.HK":    CONST_FX_HKD,
	"2788.HK":    CONST_FX_HKD,
	"6670.T":     CONST_FX_JPY,
	"ASALCBR.NS": CONST_FX_INR,
	"MRO.L":      CONST_FX_GBP,
	"BATS.L":     CONST_FX_GBP,
	"TEVA.TA":    CONST_FX_ILS,
	"U11.SI":     CONST_FX_SGD,
}

type RestSymbolInput struct {
	Symbol string `schema:"symbol"`
}

type RestSymbolPortfolioIDInput struct {
	Symbol      string `schema:"symbol"`
	PortfolioID int    `schema:"portfolioId"`
}

type RestPositionIDInput struct {
	PositionID int `schema:"positionId"`
}

type RestPortfolioIDInput struct {
	PortfolioID int `schema:"portfolioId"`
}

type RestRefDataIDInput struct {
	RefDataID int `schema:"refDataId"`
}
type RestTickerInput struct {
	Ticker string `schema:"ticker"`
}

type RestDateInput struct {
	Date string `schema:"date"`
}

type RestSymbolDateInput struct {
	Symbol string `schema:"symbol"`
	Date   string `schema:"date"`
}

type RestPortfolioIDDateInput struct {
	PortfolioID int    `schema:"portfolioId"`
	Date        string `schema:"date"`
}

type RestPositionIDDateInput struct {
	PositionID int    `schema:"positionId"`
	Date       string `schema:"date"`
}

type RestRefDataIDDateInput struct {
	RefDataID int    `schema:"refDataId"`
	Date      string `schema:"date"`
}

type RestStringOutput struct {
	Value string `json:"value"`
}

var db *sqlx.DB
var cache *redis.Client
var ctx context.Context

func Enter(name string, w http.ResponseWriter, r *http.Request) {
	log.Printf("%s: Called...", name)
	log.Printf("%s: URI: %s", name, r.URL.RequestURI())
	var bodyBytes []byte
	bodyBytes, _ = ioutil.ReadAll(r.Body)
	r.Body = ioutil.NopCloser(bytes.NewBuffer(bodyBytes))
	bodyString := string(bodyBytes)
	log.Printf("%s: Body Arguments: %s", name, bodyString)
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PATCH, PUT, DELETE, OPTIONS")
	w.Header().Set("Content-Type", "application/json")
}

func Exit(name string, ret interface{}) {
	log.Printf("%s: Returned %v", name, ret)
	log.Printf("%s: Complete!", name)
}

func ErrorHttp(err error, w http.ResponseWriter, code int) {
	function, file, line, _ := runtime.Caller(1) // Ignoring the error as we are in an error handler anyway
	log.Printf("ERROR: File: %s  Function: %s Line: %d", file, runtime.FuncForPC(function).Name(), line)
	log.Printf("ERROR: %s", err)
	w.WriteHeader(code)
}

func ErrorLog(err error) {
	function, file, line, _ := runtime.Caller(1) // Ignoring the error as we are in an error handler anyway
	log.Printf("ERROR: File: %s  Function: %s Line: %d", file, runtime.FuncForPC(function).Name(), line)
	log.Printf("ERROR: %s", err)
}

func CacheConnect() {
	if cache != nil {
		log.Println("CacheConnect: Returned cached connection")
		return
	}

	log.Println("CacheConnect: Acquiring connection")
	cache = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	ctx = context.Background()
}

func CacheClose() {
	err := cache.Close()
	if err != nil {
		ErrorLog(err)
		panic(err) // Can't survive massive redis failure
	} else {
		log.Println("CacheClose: Closed connection")
	}
}

func CacheRPush(key string, values ...interface{}) {
	CacheConnect()
	err := cache.RPush(ctx, key, values).Err()
	if err != nil {
		ErrorLog(err)
		panic(err) // Can't survive massive redis failure
	}
}

func CacheLPush(key string, values ...interface{}) {
	CacheConnect()
	err := cache.LPush(ctx, key, values).Err()
	if err != nil {
		ErrorLog(err)
		panic(err) // Can't survive massive redis failure
	}
}

func CacheBLPop(key string) int {
	CacheConnect()
	str := cache.BLPop(ctx, 0, key).Val()[1]
	id, _ := strconv.Atoi(str)
	return id
}

func CacheSet(key string, obj interface{}) {
	CacheConnect()
	ret, _ := json.Marshal(obj)
	err := cache.Set(ctx, key, ret, 0).Err()
	if err != nil {
		ErrorLog(err)
		panic(err) // Can't survive massive redis failure
	}
}

func CacheGet(key string, ptr interface{}) error {
	CacheConnect()
	cmd := cache.Get(ctx, key)
	if cmd.Err() != nil {
		return cmd.Err() // Likely simply a cache miss
	}
	bytes := []byte(cmd.Val())
	err := json.Unmarshal(bytes, ptr)
	return err
}

func CacheSAdd(key string, id int) {
	CacheConnect()
	err := cache.SAdd(ctx, key, fmt.Sprintf("%d", id)).Err()
	if err != nil {
		ErrorLog(err)
		panic(err) // Can't survive massive redis failure
	}
}

type process func(int)

func CacheSMembersAndProcess(key string, process process) {
	ids := CacheSMembers(key)
	for i := range ids {
		process(ids[i])
	}
}

func CacheSMembers(key string) []int {
	CacheConnect()
	ret := []int{}
	cmd := cache.SMembers(ctx, key)
	if cmd.Err() != nil {
		ErrorLog(cmd.Err())
		return ret // Likely simply a cache miss
	}
	members := cmd.Val()
	for i := range members {
		member, _ := strconv.Atoi(members[i])
		ret = append(ret, member)
	}
	return ret
}

func CacheKeys(key string) []string {
	CacheConnect()
	cmd := cache.Keys(ctx, fmt.Sprintf("%s:*", key))
	if cmd.Err() != nil {
		ErrorLog(cmd.Err())
		return []string{} // Likely simply a cache miss
	}
	return cmd.Val()
}

func CacheFlushAll() error {
	CacheConnect()
	return cache.FlushAll(ctx).Err()
}

func CacheWait() {
	time.Sleep(1 * time.Second)
}

func DbConnect() (*sqlx.DB, error) {
	if db != nil {
		log.Println("DbConnect: Returned cached connection")
		return db, nil
	}

	log.Println("DbConnect: Acquiring connection")
	c, err := Config("database.connect")
	if err != nil {
		return nil, err
	}
	db, err = sqlx.Connect("postgres", c)
	return db, err
}

func DbGet(dest interface{}, query string) error {
	db, err := DbConnect()
	if err != nil {
		return err
	}
	log.Printf("DbGet: %s", query)
	err = db.Get(dest, query)
	return err
}

func DbSelect(dest interface{}, query string) error {
	db, err := DbConnect()
	if err != nil {
		return err
	}
	log.Printf("DbSelect: %s", query)
	err = db.Select(dest, query)
	return err
}

func DbNamedExec(query string, ptr interface{}) error {
	db, err := DbConnect()
	if err != nil {
		return err
	}
	log.Printf("DbNamedExec: %s", query)
	_, err = db.NamedExec(query, ptr)
	return err
}

func DbListen(channel string) (*pq.Listener, error) {
	reportProblem := func(ev pq.ListenerEventType, err error) {
		if err != nil {
			ErrorLog(err)
		}
	}

	c, err := Config("database.connect")
	if err != nil {
		return nil, err
	}

	listener := pq.NewListener(c, 10*time.Second, time.Minute, reportProblem)
	err = listener.Listen(channel)
	return listener, err
}

func Round(x, unit float64) float64 {
	return math.Round(x/unit) * unit
}

func DateStringToTime(date string) time.Time {
	t, _ := time.Parse("2006-01-02T15:04:05Z", date)
	return t
}

func CorsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("CorsMiddleware: Http Method: %s", r.Method)

		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "*")
		w.Header().Set("Access-Control-Allow-Headers", "*")
		w.Header().Set("Content-Type", "application/json")

		if r.Method == "OPTIONS" {
			log.Println("CorsMiddleware: Short Circuit, returning OK")
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

type JsonStringValue struct {
	Value string `json:"value"`
}

type Yaml struct {
	Email struct {
		Server   string `yaml:"server"`
		Port     int    `yaml:"port"`
		User     string `yaml:"user"`
		Password string `yaml:"password"`
	} `yaml:"email"`
	Database struct {
		Connect string `yaml:"connect"`
	} `yaml:"database"`
}

type ConfigInput struct {
	Name string `schema:"name"`
}

type ConfigRet struct {
	Value string `json:"value"`
}

func Config(name string) (string, error) {
	log.Println("Api.Config: Called...")

	yamlFile, err := os.ReadFile("/home/scanlom/.Sanomaru")
	if err != nil {
		log.Printf("Config: /home/scanlom/.Sanomaru err #%v ", err)
		panic(err) // Can't survive missing config
	}

	var y Yaml
	err = yaml.Unmarshal(yamlFile, &y)
	if err != nil {
		log.Printf("Config: Unmarshal: %v", err)
		panic(err) // Can't survive missing config
	}

	result := ""
	if name == "database.connect" {
		result = y.Database.Connect
	} else {
		log.Printf("Config: Unknown Name: %s", name)
		panic(err) // Can't survive missing config
	}

	log.Println("Api.Config: Success!")
	return result, nil
}

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
	Active      bool    `json:"active" db:"active"`
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
	Active            bool    `json:"active" db:"active"`
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

type JsonFactors struct {
	ReportDate          string  `json:"reportDate"`
	Revenue             int64   `json:"revenue"`
	RevenueGrowth       float64 `json:"revenueGrowth"`
	RevenueCagr         float64 `json:"revenueCagr"`
	NetMgn              float64 `json:"netMgn"`
	NetMgnGrowth        float64 `json:"NetMgnGrowth"`
	NetMgnCagr          float64 `json:"NetMgnCagr"`
	SharesDiluted       int64   `json:"sharesDiluted"`
	SharesDilutedGrowth float64 `json:"sharesDilutedGrowth"`
	SharesDilutedCagr   float64 `json:"sharesDilutedCagr"`
	EPS                 float64 `json:"eps"`
	EPSGrowth           float64 `json:"epsGrowth"`
	EPSCagr             float64 `json:"epsCagr"`
}

type JsonConversion struct {
	ReportDate           string  `json:"reportDate"`
	NetCashOps           float64 `json:"netCashOps"`
	NetChgCash           float64 `json:"netChgCash"`
	NetCashInv           float64 `json:"netCashInv"`
	DividendsPaid        float64 `json:"dividendsPaid"`
	CashRepayDebt        float64 `json:"cashRepayDebt"`
	CashRepurchaseEquity float64 `json:"cashRepurchaseEquity"`
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
