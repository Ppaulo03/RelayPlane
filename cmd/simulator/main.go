// Command simulator runs a drivable stand-in for a WhatsApp provider node (see internal/simulator). Point RelayPlane's
// PROVIDER_NODES at it instead of an Evolution node, and drive the user's side through /_sim/... For development and CI
// only; never deploy it to production.
package main

import (
	"log"
	"net/http"
	"os"
	"strconv"
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
	latency, _ := time.ParseDuration(env("SEND_LATENCY", "0s"))
	delay, _ := time.ParseDuration(env("SIM_RECEIPT_DELAY", "300ms"))
	auto, _ := strconv.ParseBool(env("SIM_AUTO_PAIR", "false"))
	receipts, _ := strconv.ParseBool(env("SIM_AUTO_RECEIPTS", "false"))
	sim := simulator.New(simulator.Config{APIKey: key, SendLatency: latency, AutoPair: auto, AutoReceipts: receipts, ReceiptDelay: delay,
		WebhookBase: os.Getenv("SIM_WEBHOOK_BASE")})
	addr := env("LISTEN", ":8080")
	log.Printf("simulator listening on %s (auto-pair=%v auto-receipts=%v)", addr, auto, receipts)
	log.Fatal(http.ListenAndServe(addr, sim.Handler()))
}
