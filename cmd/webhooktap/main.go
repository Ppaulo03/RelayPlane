// Command webhooktap records what a real provider node (Evolution) sends BEFORE RelayPlane normalizes it, for the real-number
// spike (docs/REAL-NUMBER-SPIKE.md). It sits between the node and the gateway:
//
//	Evolution --POST /webhooks/...--> webhooktap --forward--> gateway
//	RelayPlane webhook delivery  ---POST /sink--> webhooktap   (the normalized events a consumer would receive)
//
// Raw captures are written as JSON lines under CAPTURE_DIR. They contain real phone numbers, names and message text: keep
// them local (the directory is git-ignored) and run tools/spike/sanitize.py before sharing anything. The node token header is
// never written. For development only.
package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type record struct {
	At      time.Time           `json:"at"`
	Kind    string              `json:"kind"` // provider | sink
	Method  string              `json:"method"`
	Path    string              `json:"path"`
	Query   map[string][]string `json:"query,omitempty"`
	Headers map[string][]string `json:"headers,omitempty"`
	Body    json.RawMessage     `json:"body"`
	Status  int                 `json:"forward_status,omitempty"`
}

type tap struct {
	mu  sync.Mutex
	out *os.File
}

func (t *tap) write(r record) {
	t.mu.Lock()
	defer t.mu.Unlock()
	b, _ := json.Marshal(r)
	_, _ = t.out.Write(append(b, '\n'))
}

// safeHeaders drops credentials: only descriptive headers are kept.
func safeHeaders(h http.Header) map[string][]string {
	out := map[string][]string{}
	for k, v := range h {
		lk := strings.ToLower(k)
		if lk == "x-relayplane-token" || lk == "authorization" || lk == "apikey" || lk == "cookie" || strings.Contains(lk, "signature") {
			continue
		}
		out[k] = v
	}
	return out
}

func rawBody(b []byte) json.RawMessage {
	if json.Valid(b) {
		return json.RawMessage(b)
	}
	q, _ := json.Marshal(string(b))
	return q
}

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (s *statusRecorder) WriteHeader(c int) { s.code = c; s.ResponseWriter.WriteHeader(c) }

func main() {
	target := os.Getenv("TARGET")
	if target == "" {
		log.Fatal("TARGET (the gateway URL) is required")
	}
	dir := os.Getenv("CAPTURE_DIR")
	if dir == "" {
		dir = "/captures"
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Fatal(err)
	}
	f, err := os.OpenFile(filepath.Join(dir, "webhooks.jsonl"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		log.Fatal(err)
	}
	t := &tap{out: f}
	u, err := url.Parse(target)
	if err != nil {
		log.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(u)

	mux := http.NewServeMux()
	// what RelayPlane would deliver to a consumer's webhook (normalized events)
	mux.HandleFunc("POST /sink", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 4<<20))
		t.write(record{At: time.Now().UTC(), Kind: "sink", Method: r.Method, Path: r.URL.Path, Headers: safeHeaders(r.Header), Body: rawBody(body)})
		w.WriteHeader(http.StatusNoContent)
	})
	// everything else is the provider talking to the gateway: record, then forward untouched
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 32<<20))
		r.Body = io.NopCloser(bytes.NewReader(body))
		rec := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
		proxy.ServeHTTP(rec, r)
		t.write(record{At: time.Now().UTC(), Kind: "provider", Method: r.Method, Path: r.URL.Path, Query: r.URL.Query(), Headers: safeHeaders(r.Header), Body: rawBody(body), Status: rec.code})
	})
	addr := os.Getenv("LISTEN")
	if addr == "" {
		addr = ":9000"
	}
	log.Printf("webhooktap on %s -> %s, capturing to %s/webhooks.jsonl", addr, target, dir)
	log.Fatal(http.ListenAndServe(addr, mux))
}
