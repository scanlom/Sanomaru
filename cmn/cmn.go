package cmn

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"runtime"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

const CONST_PORTFOLIO_TOTAL = 1
const CONST_PORTFOLIO_SELFIE = 2

type RestSymbolInput struct {
	Symbol string `schema:"symbol"`
}

type RestSymbolPortfolioIDInput struct {
	Symbol      string `schema:"symbol"`
	PortfolioID int    `schema:"portfolioId"`
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

type RestRefDataIDDateInput struct {
	RefDataID int    `schema:"refDataId"`
	Date      string `schema:"date"`
}

type RestStringOutput struct {
	Value string `json:"value"`
}

var db *sqlx.DB

func Enter(name string, w http.ResponseWriter, r *http.Request) {
	log.Printf("%s: Called...", name)
	log.Printf("%s: Query Arguments: %v", name, r.URL.Query())
	var bodyBytes []byte
	bodyBytes, _ = ioutil.ReadAll(r.Body)
	r.Body = ioutil.NopCloser(bytes.NewBuffer(bodyBytes))
	bodyString := string(bodyBytes)
	log.Printf("%s: Body Arguments: %s", name, bodyString)
	w.Header().Set("Access-Control-Allow-Origin", "*")
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

func DbNamedExec(query string, ptr interface{}) error {
	db, err := DbConnect()
	if err != nil {
		return err
	}
	log.Printf("DbNamedExec: %s", query)
	_, err = db.NamedExec(query, ptr)
	return err
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
		if r.Method == "OPTIONS" {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PATCH, PUT, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
			w.Header().Set("Content-Type", "application/json")
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
