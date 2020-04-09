package main

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"github.com/gorilla/schema"
	"github.com/scanlom/Sanomaru/api"
	"log"
	"math"
	"net/http"
	"strings"
)

const CONST_DIV_GROWTH = "DIV_GROWTH"
const CONST_CONFIDENCE_NONE = "NONE"
const CONST_CONFIDENCE_LOW = "LOW"
const CONST_CONFIDENCE_MEDIUM = "MEDIUM"
const CONST_CONFIDENCE_HIGH = "HIGH"

type CagrInput struct {
	Years      float64 `schema:"years"`
	Eps        float64 `schema:"eps"`
	Payout     float64 `schema:"payout"`
	Growth     float64 `schema:"growth"`
	PeTerminal float64 `schema:"peterminal"`
	Price      float64 `schema:"price"`
}

type CagrRet struct {
	Cagr float64 `json:"cagr"`
}

type ConfidenceInput struct {
	Research string `schema:"research"`
}

type ConfidenceRet struct {
	Confidence string `json:"confidence"`
}

func setupRouter(router *mux.Router) {
	router.
		Methods("GET").
		Path("/blue-lion/utils/cagr").
		HandlerFunc(Cagr)
	router.
		Methods("GET").
		Path("/blue-lion/utils/confidence").
		HandlerFunc(Confidence)
}

func Confidence(w http.ResponseWriter, r *http.Request) {
	log.Println("Confidence called...")

	args := new(ConfidenceInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		log.Println(err)
		return
	}

	result := CONST_CONFIDENCE_NONE
	if strings.Contains(args.Research, CONST_CONFIDENCE_HIGH) {
		result = CONST_CONFIDENCE_HIGH
	} else if strings.Contains(args.Research, CONST_CONFIDENCE_MEDIUM) {
		result = CONST_CONFIDENCE_MEDIUM
	} else if strings.Contains(args.Research, CONST_CONFIDENCE_LOW) {
		result = CONST_CONFIDENCE_LOW
	}

	json.NewEncoder(w).Encode(ConfidenceRet{Confidence: result})
	log.Println("Confidence complete!")
}

func Cagr(w http.ResponseWriter, r *http.Request) {
	log.Println("Cagr called...")

	args := new(CagrInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		log.Println(err)
		return
	}

	div_bucket := 0.0
	div_growth, err := api.Scalar(CONST_DIV_GROWTH)
	eps := args.Eps
	for i := 0.0; i < args.Years; i++ {
		div_bucket = div_bucket * (1.0 + div_growth)
		div_bucket = div_bucket + (eps * args.Payout)
		eps = eps * (1.0 + args.Growth)
	}
	result := math.Pow(((eps*args.PeTerminal)+div_bucket)/args.Price, 1.0/args.Years) - 1.0
	result = math.Round(result*100000000) / 100000000

	json.NewEncoder(w).Encode(CagrRet{Cagr: result})
	log.Println("Cagr complete!")
}

func main() {
	router := mux.NewRouter().StrictSlash(true)
	setupRouter(router)
	log.Fatal(http.ListenAndServe(":8080", router))
}
