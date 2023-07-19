package cmn

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"runtime"
	"strconv"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/redis/go-redis/v9"
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

func Config(name string) (string, error) {
	log.Println("Api.Config: Called...")
	response, err := http.Get(fmt.Sprintf("http://localhost:8082/blue-lion/config?name=%s", name))
	if err != nil {
		return "", err
	}

	data, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return "", err
	}

	var val JsonStringValue
	err = json.Unmarshal(data, &val)
	if err != nil {
		return "", err
	}

	log.Println("Api.Config: Success!")
	return val.Value, nil
}
