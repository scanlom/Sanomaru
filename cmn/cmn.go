package cmn

import (
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/scanlom/Sanomaru/api"
	"log"
	"math"
	"net/http"
)

type RestSymbolInput struct {
	Symbol string `schema:"symbol"`
}

type RestTickerInput struct {
	Ticker string `schema:"ticker"`
}

type RestSymbolDateInput struct {
	Symbol string `schema:"symbol"`
	Date   string `schema:"date"`
}

var db *sqlx.DB

func Enter(name string, args interface{}) {
	log.Printf("%s: Called...", name)
	log.Printf("%s: Arguments: %v", name, args)
}

func Exit(name string, ret interface{}) {
	log.Printf("%s: Returned %v", name, ret)
	log.Printf("%s: Complete!", name)
}

func ErrorHttp(err error, w http.ResponseWriter, code int) {
	log.Println(err)
	w.WriteHeader(code)
	return
}

func DbConnect() (*sqlx.DB, error) {
	if db != nil {
		log.Println("DbConnect: Returned cached connection")
		return db, nil
	}

	log.Println("DbConnect: Acquiring connection")
	c, err := api.Config("database.connect")
	if err != nil {
		return nil, err
	}
	db, err = sqlx.Connect("postgres", c)
	return db, err
}

func Round(x, unit float64) float64 {
	return math.Round(x/unit) * unit
}
