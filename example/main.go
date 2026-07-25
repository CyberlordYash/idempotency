// Command example runs a tiny server that wraps a slow "create charge" handler
// with the idempotency middleware, so you can watch retries get coalesced.
package main

import (
	"fmt"
	"log"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/CyberlordYash/idempotency"
)

func main() {
	var charges atomic.Int64

	// Pretend this talks to a payment provider and takes a moment. The sleep is
	// what makes a concurrent retry actually arrive mid-flight.
	charge := func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
		id := charges.Add(1)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"charge_id": %d}`+"\n", id)
	}

	idem := idempotency.New(idempotency.Options{
		TTL:         time.Minute,
		WaitTimeout: 10 * time.Second,
	})

	mux := http.NewServeMux()
	mux.Handle("POST /charge", idem.Middleware(http.HandlerFunc(charge)))

	addr := ":8080"
	log.Printf("listening on %s", addr)
	log.Print("  same key twice          -> one charge, second reply is replayed")
	log.Print("  two at once, same key   -> second blocks ~2s, then replays the first")
	log.Print("  same key, different body-> 422")
	log.Print("  no Idempotency-Key      -> 400")
	log.Printf(`  curl -i -XPOST localhost%s/charge -H 'Idempotency-Key: demo' -d '{"amt":100}'`, addr)

	log.Fatal(http.ListenAndServe(addr, mux))
}
