package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

func RestPostByUrl(msg string, url string, data interface{}) error {
	log.Printf("Api.%s: Called...", msg)
	log.Printf("Api.%s: %s", msg, url)

    json, err := json.Marshal(data)
    if err != nil {
        return err
    }
	
    req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(json))
    if err != nil {
        return err
    }
    
    req.Header.Set("Content-Type", "application/json; charset=utf-8")
	client := &http.Client{}
    resp, err := client.Do(req)
    if err != nil {
        return err
    }

	log.Printf("Api.%s: Complete, status code %s", msg, resp.Status)
	return nil
}

func RestPutByUrl(msg string, url string, data interface{}) error {
	log.Printf("Api.%s: Called...", msg)
	log.Printf("Api.%s: %s", msg, url)

    json, err := json.Marshal(data)
    if err != nil {
        return err
    }
	
    req, err := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(json))
    if err != nil {
        return err
    }
    
    req.Header.Set("Content-Type", "application/json; charset=utf-8")
	client := &http.Client{}
    resp, err := client.Do(req)
    if err != nil {
        return err
    }

	log.Printf("Api.%s: Complete, status code %s", msg, resp.Status)
	return nil
}

func RestDeleteByUrl(msg string, url string) error {
	log.Printf("Api.%s: Called...", msg)
	log.Printf("Api.%s: %s", msg, url)

    req, err := http.NewRequest(http.MethodDelete, url, nil)
    if err != nil {
        return err
    }
    
    req.Header.Set("Content-Type", "application/json; charset=utf-8")
	client := &http.Client{}
    resp, err := client.Do(req)
    if err != nil {
        return err
    }

	log.Printf("Api.%s: Complete, status code %s", msg, resp.Status)
	return nil
}

func PostPortfolioHistory(data JsonPortfolioHistory) error {
	return RestPostByUrl("PostPortfolioHistory",
		fmt.Sprintf("http://localhost:8083/blue-lion/write/portfolios-history"),
		data,
	)
}

func PostPositionHistory(data JsonPositionHistory) error {
	return RestPostByUrl("PostPositionHistory",
		fmt.Sprintf("http://localhost:8083/blue-lion/write/positions-history"),
		data,
	)
}

func PutPosition(data JsonPosition) error {
	return RestPutByUrl("PutPosition",
		fmt.Sprintf("http://localhost:8083/blue-lion/write/positions/%d", data.ID),
		data,
	)
}

func PutPortfolio(data JsonPortfolio) error {
	return RestPutByUrl("PutPortfolio",
		fmt.Sprintf("http://localhost:8083/blue-lion/write/portfolios/%d", data.ID),
		data,
	)
}

func DeletePositionsHistoryByDate(date string) error {
	return RestDeleteByUrl("DeletePositionsHistoryByDate",
		fmt.Sprintf("http://localhost:8083/blue-lion/write/positions-history?date=%s", date),
	)
}

func DeletePortfoliosHistoryByDate(date string) error {
	return RestDeleteByUrl("DeletePortfoliosHistoryByDate",
		fmt.Sprintf("http://localhost:8083/blue-lion/write/portfolios-history?date=%s", date),
	)
}