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
	"github.com/scanlom/Sanomaru/cmn"
)

const CONST_DIE_SIGNAL = -1

// MSTODO: If there is any error I should know loud and clear
// MSTODO: Try to do MDHSummary and other stuff from the api,
// MSTODO: Will keep this named redis.go until I'm ready to delete the current cache.go, then will swap in
// MSTODO: Should have alerting if a microservice isn't running
// MSTODO: Clean up directory structure
// MSTODO: Gateway should do minimal job, maybe only sort

func UpdateWorker(wg *sync.WaitGroup, list string, table string, ptr interface{}, obj interface{}, work func(interface{})) {
	defer wg.Done()
	for {
		log.Printf("Blocking on %s...", list)
		id := cmn.CacheBLPop(list)
		if id == CONST_DIE_SIGNAL {
			break
		}
		err := cmn.DbGet(ptr, fmt.Sprintf("%s WHERE id=%d", api.JsonToSelect(obj, table, ""), id))
		if err != nil {
			cmn.ErrorLog(err)
			continue // Strange, but a record may have been deleted, and that's survivable
		}

		cmn.CacheSet(fmt.Sprintf("%s:%d", table, id), ptr)
		work(ptr)
	}
	log.Printf("UpdateWorker %s completed", list)
}

func LoadUpdateIDList(table string) {
	ret := []api.JsonID{}
	err := cmn.DbSelect(&ret, api.JsonToSelect(api.JsonID{}, table, ""))
	if err != nil {
		cmn.ErrorLog(err)
		panic(err) // Can't survive a missing table
	}

	for i := range ret {
		cmn.CacheLPush(fmt.Sprintf("%s_update", table), ret[i].ID)
	}
}

func NotifyInsertUpdate(wg *sync.WaitGroup, listener *pq.Listener) {
	defer wg.Done()
	for n := range listener.Notify {
		log.Println("Received data from channel [", n.Channel, "] :")
		extra := api.JsonTableID{}
		err := json.Unmarshal([]byte(n.Extra), &extra)
		if err != nil {
			cmn.ErrorLog(err)
			panic(err) // Why are we getting a bad update
		}

		cmn.CacheLPush(fmt.Sprintf("%s_update", extra.Table), extra.ID)
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
	err := cmn.CacheFlushAll()
	if err != nil {
		cmn.ErrorLog(err)
		panic(err) // Can't survive massive redis failure
	}

	var wg sync.WaitGroup
	wg.Add(7)

	// Listen for db inserts and updates
	listener, err := cmn.DbListen("insert_update")
	if err != nil {
		cmn.ErrorLog(err)
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
		cmn.ErrorLog(err)
	}
	cmn.CacheLPush("ref_data_update", CONST_DIE_SIGNAL)
	cmn.CacheLPush("market_data_update", CONST_DIE_SIGNAL)
	cmn.CacheLPush("portfolios_update", CONST_DIE_SIGNAL)
	cmn.CacheLPush("positions_update", CONST_DIE_SIGNAL)
	cmn.CacheLPush("mergers_update", CONST_DIE_SIGNAL)
	wg.Wait()
	cmn.CacheClose()
	log.Print("Shutdown complete")
}
