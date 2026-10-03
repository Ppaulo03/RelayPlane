// Command loadstub is a stand-in for a provider node, used ONLY by load tests (deploy/docker/compose.load.yml).
// It speaks the subset of the Evolution v2 HTTP API the adapter uses, pairs every instance instantly, adds a configurable
// latency to sends and records what it received so the load generator can verify, across real processes, that nothing was
// delivered twice or out of order. It is never part of a production deployment.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

type stub struct {
	mu         sync.Mutex
	key        string
	latency    time.Duration
	instances  map[string]string // name -> connection state
	sent       map[string]int    // instance + "\x00" + text -> count
	last       map[string]string // instance -> last text
	sends      int
	duplicates int
	reordered  int
}

func main() {
	d, _ := time.ParseDuration(env("SEND_LATENCY", "50ms"))
	s := &stub{key: os.Getenv("AUTHENTICATION_API_KEY"), latency: d, instances: map[string]string{}, sent: map[string]int{}, last: map[string]string{}}
	if s.key == "" {
		log.Fatal("AUTHENTICATION_API_KEY is required")
	}
	log.Printf("loadstub listening on %s (send latency %s)", env("LISTEN", ":8080"), d)
	log.Fatal(http.ListenAndServe(env("LISTEN", ":8080"), http.HandlerFunc(s.serve)))
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func apiErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]any{"status": code, "error": http.StatusText(code), "response": map[string]any{"message": []string{msg}}})
}

func (s *stub) serve(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/" {
		writeJSON(w, 200, map[string]any{"status": 200, "message": "Welcome to the Evolution API, it is working!", "version": "2.3.7"})
		return
	}
	if r.Header.Get("apikey") != s.key {
		apiErr(w, 401, "Unauthorized")
		return
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if parts[0] == "_stats" {
		s.mu.Lock()
		defer s.mu.Unlock()
		writeJSON(w, 200, map[string]any{"sends": s.sends, "duplicates": s.duplicates, "reordered": s.reordered, "instances": len(s.instances)})
		return
	}
	if len(parts) < 2 {
		apiErr(w, 404, "route not found")
		return
	}
	route, name := parts[0]+"/"+parts[1], ""
	if len(parts) > 2 {
		name, _ = url.PathUnescape(parts[2])
	}
	switch {
	case route == "instance/create" && r.Method == "POST":
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		n, _ := body["instanceName"].(string)
		s.mu.Lock()
		_, exists := s.instances[n]
		if !exists {
			s.instances[n] = "open" // paired instantly: no QR in a load test
		}
		s.mu.Unlock()
		if exists {
			apiErr(w, 403, fmt.Sprintf("This name %q is already in use.", n))
			return
		}
		writeJSON(w, 201, map[string]any{"instance": map[string]any{"instanceName": n, "instanceId": "uuid-" + n, "status": "created"}, "hash": "h"})
	case route == "instance/fetchInstances":
		s.mu.Lock()
		defer s.mu.Unlock()
		var out []map[string]any
		for n, st := range s.instances {
			if want := r.URL.Query().Get("instanceName"); want == "" || want == n {
				out = append(out, map[string]any{"id": "uuid-" + n, "name": n, "connectionStatus": st, "ownerJid": "5562999999999@s.whatsapp.net"})
			}
		}
		writeJSON(w, 200, out)
	case route == "instance/connectionState", route == "instance/connect", route == "instance/logout", route == "instance/delete",
		route == "message/sendText", route == "message/sendMedia", route == "message/sendWhatsAppAudio":
		s.instanceRoute(w, r, route, name)
	default:
		apiErr(w, 404, "route not found")
	}
}

func (s *stub) instanceRoute(w http.ResponseWriter, r *http.Request, route, name string) {
	s.mu.Lock()
	st, ok := s.instances[name]
	s.mu.Unlock()
	if !ok {
		apiErr(w, 404, fmt.Sprintf("The %q instance does not exist", name))
		return
	}
	switch route {
	case "instance/connectionState", "instance/connect":
		writeJSON(w, 200, map[string]any{"instance": map[string]any{"instanceName": name, "state": st}})
	case "instance/logout":
		s.mu.Lock()
		s.instances[name] = "close"
		s.mu.Unlock()
		writeJSON(w, 200, map[string]any{"status": "SUCCESS", "error": false})
	case "instance/delete":
		s.mu.Lock()
		delete(s.instances, name)
		s.mu.Unlock()
		writeJSON(w, 200, map[string]any{"status": "SUCCESS", "error": false})
	default: // sends
		if st != "open" {
			apiErr(w, 400, "Connection Closed")
			return
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		text, _ := body["text"].(string)
		time.Sleep(s.latency) // the provider's own processing time; concurrent across instances
		s.mu.Lock()
		s.sends++
		k := name + "\x00" + text
		if s.sent[k]++; s.sent[k] > 1 {
			s.duplicates++
		}
		if prev := s.last[name]; text != "" && prev != "" && text < prev {
			s.reordered++
		}
		if text != "" {
			s.last[name] = text
		}
		n := s.sends
		s.mu.Unlock()
		writeJSON(w, 201, map[string]any{"key": map[string]any{"remoteJid": "x@s.whatsapp.net", "fromMe": true, "id": fmt.Sprintf("3EB0%08d", n)}, "status": "PENDING"})
	}
}
