package main

import (
	"log"
	"math"
	"strconv"
	"encoding/json"
	"net/http"
	"github.com/gorilla/mux"
)

const CONST_DIV_GROWTH = 0.0981

type CagrRet struct {
    Cagr      float64   `json:"cagr"`
}

func setupRouter(router *mux.Router) {
	router.
		Methods("GET").
		Path("/blue-lion/utils/cagr").
		HandlerFunc(Cagr)
}

func Cagr(w http.ResponseWriter, r *http.Request) {
	log.Println("Cagr called...")

	argYears, ok := r.URL.Query()["years"]
    	if !ok {
		log.Println("Url Param 'years' is missing")
	return
    	}

	years, err := strconv.ParseFloat(argYears[0],64)
	if err != nil { return }

	argEps, ok := r.URL.Query()["eps"]
    	if !ok {
		log.Println("Url Param 'eps' is missing")
	return
    	}
	
	eps, err := strconv.ParseFloat(argEps[0],64)

	argPayout, ok := r.URL.Query()["payout"]
    	if !ok {
		log.Println("Url Param 'payout' is missing")
	return
    	}
	
	payout, err := strconv.ParseFloat(argPayout[0],64)

	argGrowth, ok := r.URL.Query()["growth"]
    	if !ok {
		log.Println("Url Param 'growth' is missing")
	return
    	}
	
	growth, err := strconv.ParseFloat(argGrowth[0],64)

	argPeterminal, ok := r.URL.Query()["peterminal"]
    	if !ok {
		log.Println("Url Param 'peterminal' is missing")
	return
    	}
	
	peterminal, err := strconv.ParseFloat(argPeterminal[0],64)

	argPrice, ok := r.URL.Query()["price"]
    	if !ok {
		log.Println("Url Param 'price' is missing")
	return
    	}
	
	price, err := strconv.ParseFloat(argPrice[0],64)

	div_bucket := 0.0
	for i:=0.0; i<years; i++ {
		div_bucket = div_bucket * (1.0 + CONST_DIV_GROWTH)
		div_bucket = div_bucket + (eps * payout)
		eps = eps * (1.0 + growth)
	}
	result :=  math.Pow(((eps * peterminal) + div_bucket) / price, 1.0 / years) - 1.0
	result = math.Round(result * 100000000) / 100000000

	log.Println("Cagr complete!")
	json.NewEncoder(w).Encode(CagrRet{Cagr: result})
}

func main() {
	router := mux.NewRouter().StrictSlash(true)
	setupRouter(router)
	log.Fatal(http.ListenAndServe(":8080", router))
}
