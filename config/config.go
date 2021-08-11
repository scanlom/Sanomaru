package main

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"github.com/gorilla/schema"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"net/http"
)

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

func setupRouter(router *mux.Router) {
	router.
		Methods("GET").
		Path("/blue-lion/config").
		HandlerFunc(Config)
}

func Config(w http.ResponseWriter, r *http.Request) {
	log.Println("Config: Called...")
	log.Printf("Config: Arguments: %s", r.URL.Query())

	args := new(ConfigInput)
	decoder := schema.NewDecoder()
	err := decoder.Decode(args, r.URL.Query())
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	yamlFile, err := ioutil.ReadFile("/home/scanlom/.Sanomaru")
	if err != nil {
		log.Printf("Config: /home/scanlom/.Sanomaru err #%v ", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	var y Yaml
	err = yaml.Unmarshal(yamlFile, &y)
	if err != nil {
		log.Printf("Config: Unmarshal: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	result := ""
	if args.Name == "database.connect" {
		result = y.Database.Connect
	} else {
		log.Printf("Config: Unknown Name: %s", args.Name)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	ret := ConfigRet{Value: result}
	json.NewEncoder(w).Encode(ret)
	log.Printf("Config: Returned %v", ret)
	log.Println("Config: Complete!")
}

func main() {
	log.Println("Listening on http://localhost:8082/blue-lion/config")
	router := mux.NewRouter().StrictSlash(true)
	setupRouter(router)
	log.Fatal(http.ListenAndServe(":8082", router))
}
