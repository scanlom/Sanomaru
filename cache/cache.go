package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/lib/pq"
	"github.com/scanlom/Sanomaru/api"
)

const CONST_DIE_SIGNAL = -1

// MSTODO: If there is any error I should know loud and clear
// MSTODO: Try to do MDHSummary and other stuff from the api,
// MSTODO: Should have alerting if a microservice isn't running
// MSTODO: Clean up directory structure
// MSTODO: Gateway should do minimal job, maybe only sort
// MSTODO: When I update a projection, angular should update after redis cache updates

func UpdateWorker(wg *sync.WaitGroup, list string, table string, ptr interface{}, obj interface{}, work func(interface{})) {
	defer wg.Done()
	for {
		log.Printf("Blocking on %s...", list)
		id := api.CacheBLPop(list)
		if id == CONST_DIE_SIGNAL {
			break
		}
		err := api.DbGet(ptr, fmt.Sprintf("%s WHERE id=%d", api.JsonToSelect(obj, table, ""), id))
		if err != nil {
			api.ErrorLog(err)
			continue // Strange, but a record may have been deleted, and that's survivable
		}

		api.CacheSet(fmt.Sprintf("%s:%d", table, id), ptr)
		work(ptr)
	}
	log.Printf("UpdateWorker %s completed", list)
}

func LoadUpdateIDList(table string) {
	ret := []api.JsonID{}
	err := api.DbSelect(&ret, api.JsonToSelect(api.JsonID{}, table, ""))
	if err != nil {
		api.ErrorLog(err)
		panic(err) // Can't survive a missing table
	}

	for i := range ret {
		api.CacheRPush(fmt.Sprintf("%s_update", table), ret[i].ID)
	}
}

func NotifyInsertUpdate(wg *sync.WaitGroup, listener *pq.Listener) {
	defer wg.Done()
	for n := range listener.Notify {
		log.Println("Received data from channel [", n.Channel, "] :")
		extra := api.JsonTableID{}
		err := json.Unmarshal([]byte(n.Extra), &extra)
		if err != nil {
			api.ErrorLog(err)
			panic(err) // Why are we getting a bad update
		}
		log.Printf("Table %s, ID %d", extra.Table, extra.ID)
		api.CacheRPush(fmt.Sprintf("%s_update", extra.Table), extra.ID)
	}
	log.Printf("NotifyInsertUpdate complete")
}

// This channel's only job is to wait for SIGINT/TERM, after which main will handle cleanup
func WaitToDie() <-chan struct{} {
	end := make(chan struct{})
	s := make(chan os.Signal, 1)
	signal.Notify(s, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-s
		close(end)
	}()
	return end
}

func main() {
	// Start clean
	err := api.CacheFlushAll()
	if err != nil {
		api.ErrorLog(err)
		panic(err) // Can't survive massive redis failure
	}

	var wg sync.WaitGroup
	wg.Add(7)

	// Listen for db inserts and updates
	listener, err := api.DbListen("insert_update")
	if err != nil {
		api.ErrorLog(err)
		panic(err) // Can't survive massive postgres failure
	}
	go NotifyInsertUpdate(&wg, listener)

	// Cache the universe
	LoadUpdateIDList("ref_data")
	LoadUpdateIDList("market_data")
	LoadUpdateIDList("portfolios")
	LoadUpdateIDList("positions")
	LoadUpdateIDList("projections")
	LoadUpdateIDList("mergers")

	// Start our workers
	rd := api.JsonRefData{}
	go UpdateWorker(&wg, "ref_data_update", "ref_data", &rd, rd, RefDataWork)
	md := api.JsonMarketData{}
	go UpdateWorker(&wg, "market_data_update", "market_data", &md, md, MarketDataWork)
	port := api.JsonPortfolio{}
	go UpdateWorker(&wg, "portfolios_update", "portfolios", &port, port, PortfoliosWork)
	pos := api.JsonPosition{}
	go UpdateWorker(&wg, "positions_update", "positions", &pos, pos, PositionsWork)
	proj := api.JsonProjections{}
	go UpdateWorker(&wg, "projections_update", "projections", &proj, proj, ProjectionsWork)
	merger := api.JsonMerger{}
	go UpdateWorker(&wg, "mergers_update", "mergers", &merger, merger, MergersWork)

	// Wait to die
	end := WaitToDie()
	<-end
	log.Print("Shutdown...")
	err = listener.Close()
	if err != nil {
		api.ErrorLog(err)
	}
	api.CacheLPush("ref_data_update", CONST_DIE_SIGNAL)
	api.CacheLPush("market_data_update", CONST_DIE_SIGNAL)
	api.CacheLPush("portfolios_update", CONST_DIE_SIGNAL)
	api.CacheLPush("positions_update", CONST_DIE_SIGNAL)
	api.CacheLPush("projections_update", CONST_DIE_SIGNAL)
	api.CacheLPush("mergers_update", CONST_DIE_SIGNAL)
	wg.Wait()
	api.CacheClose()
	log.Print("Shutdown complete")
}
