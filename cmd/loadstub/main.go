// Command loadstub is the provider node of the load tests (deploy/docker/compose.load.yml): the simulator with instant
// pairing and a configurable send latency, plus delivery counters (GET /_stats) so the load generator can verify, across real
// processes, that nothing was delivered twice or out of order. It is never part of a production deployment.
package main

import (
	"log"
	"net/http"
	"os"
	"time"

	"github.com/relayplane/relayplane/internal/simulator"
)

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func main() {
	key := os.Getenv("AUTHENTICATION_API_KEY")
	if key == "" {
		log.Fatal("AUTHENTICATION_API_KEY is required")
	}
	d, _ := time.ParseDuration(env("SEND_LATENCY", "50ms"))
	addr := env("LISTEN", ":8080")
	log.Printf("loadstub listening on %s (send latency %s)", addr, d)
	log.Fatal(http.ListenAndServe(addr, simulator.New(simulator.Config{APIKey: key, SendLatency: d, AutoPair: true}).Handler()))
}
