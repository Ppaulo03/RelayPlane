// Command loadgen drives a running RelayPlane stack (gateway + N workers + reconciler as separate processes) through its
// public HTTP API and verifies the outcome across processes with the counters of the load stub (cmd/loadstub).
// It exits non-zero when anything was lost, duplicated, reordered or ended in an unexpected state.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	gateway       = flag.String("gateway", "http://127.0.0.1:8080", "gateway base URL")
	adminKey      = flag.String("admin-key", os.Getenv("ADMIN_API_KEY"), "admin API key")
	instances     = flag.Int("instances", 20, "instances to create")
	messages      = flag.Int("messages", 2000, "total messages")
	stubs         = flag.String("stubs", "", "comma separated url=apikey of the load stubs to read /_stats from")
	allowUnk      = flag.Bool("allow-unknown", false, "UNKNOWN is a legitimate end state (workers are killed mid-send); delivered count may then exceed ACCEPTED by at most the UNKNOWN count")
	webhookListen = flag.String("webhook-listen", "", "address to listen on for the tenant webhook sink, e.g. 0.0.0.0:18090 (enables the webhook check)")
	webhookURL    = flag.String("webhook-url", "", "URL the gateway/worker containers use to reach the sink, e.g. http://host.docker.internal:18090/hook")
	webhookFail   = flag.Float64("webhook-fail-rate", 0.1, "fraction of webhook requests the sink answers with 500 (exercises retries)")
	timeout       = flag.Duration("timeout", 5*time.Minute, "how long to wait for the delivery to finish")
)

var hc = &http.Client{Timeout: 30 * time.Second}

func call(method, path, key, idem string, in, out any) (int, error) {
	var body io.Reader
	if in != nil {
		b, _ := json.Marshal(in)
		body = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, *gateway+path, body)
	req.Header.Set("Authorization", "Bearer "+key)
	if idem != "" {
		req.Header.Set("Idempotency-Key", idem)
	}
	resp, err := hc.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if out != nil && resp.StatusCode < 300 {
		if err := json.Unmarshal(raw, out); err != nil {
			return resp.StatusCode, err
		}
	}
	if resp.StatusCode >= 300 {
		return resp.StatusCode, fmt.Errorf("%s %s: %d %s", method, path, resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	return resp.StatusCode, nil
}

type stubStats struct{ Sends, Duplicates, Reordered int }

// readStubs sums the counters of every load stub ("url=apikey,url=apikey").
func readStubs(spec string) (stubStats, error) {
	var tot stubStats
	for _, s := range strings.Split(spec, ",") {
		u, k, ok := strings.Cut(strings.TrimSpace(s), "=")
		if !ok {
			continue
		}
		req, _ := http.NewRequest("GET", strings.TrimRight(u, "/")+"/_stats", nil)
		req.Header.Set("apikey", k)
		resp, err := hc.Do(req)
		if err != nil {
			return tot, err
		}
		var st stubStats
		_ = json.NewDecoder(resp.Body).Decode(&st)
		resp.Body.Close()
		tot.Sends, tot.Duplicates, tot.Reordered = tot.Sends+st.Sends, tot.Duplicates+st.Duplicates, tot.Reordered+st.Reordered
	}
	return tot, nil
}

func fatal(f string, a ...any) { fmt.Fprintf(os.Stderr, "loadgen: "+f+"\n", a...); os.Exit(1) }

func main() {
	flag.Parse()
	if *adminKey == "" {
		fatal("ADMIN_API_KEY / -admin-key is required")
	}
	run := fmt.Sprint(time.Now().UnixNano())

	var tenant struct {
		ID     string `json:"id"`
		APIKey string `json:"api_key"`
	}
	if _, err := call("POST", "/api/v1/tenants", *adminKey, "", map[string]string{"name": "load-" + run}, &tenant); err != nil {
		fatal("%v", err)
	}
	// a load test must not be shaped by the anti-ban limits: lift the per-tenant policy
	_, _ = call("PUT", "/api/v1/tenants/"+tenant.ID+"/rate-policy", *adminKey, "", map[string]any{}, nil)

	var sink *webhookSink
	if *webhookListen != "" {
		if *webhookURL == "" {
			fatal("-webhook-url is required with -webhook-listen")
		}
		var err error
		if sink, err = startSink(*webhookListen, *webhookFail); err != nil {
			fatal("webhook sink: %v", err)
		}
		var sub struct {
			Secret string `json:"secret"`
		}
		if _, err := call("POST", "/api/v1/subscriptions", tenant.APIKey, "", map[string]any{"url": *webhookURL, "event_types": []string{"message.outbound_status"}}, &sub); err != nil {
			fatal("create subscription: %v", err)
		}
		sink.setSecret(sub.Secret)
		fmt.Printf("webhook subscription created (sink on %s, %.0f%% injected failures)\n", *webhookListen, *webhookFail*100)
	}

	ids := make([]string, *instances)
	for i := range ids {
		var r struct {
			ID string `json:"id"`
		}
		if _, err := call("POST", "/api/v1/instances", tenant.APIKey, fmt.Sprintf("inst-%s-%d", run, i), map[string]string{"name": fmt.Sprintf("load-%d", i)}, &r); err != nil {
			fatal("create instance: %v", err)
		}
		ids[i] = r.ID
	}
	deadline := time.Now().Add(2 * time.Minute)
	for _, id := range ids {
		for {
			var in struct {
				Observed string `json:"observed_state"`
			}
			if _, err := call("GET", "/api/v1/instances/"+id, tenant.APIKey, "", nil, &in); err == nil && in.Observed == "CONNECTED" {
				break
			}
			if time.Now().After(deadline) {
				fatal("instance %s never became CONNECTED", id)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
	fmt.Printf("%d instances CONNECTED\n", len(ids))

	per := *messages / len(ids)
	var mu sync.Mutex
	var accept []time.Duration
	byInst := make([][]string, len(ids))
	var failures []string
	var wg sync.WaitGroup
	start := time.Now()
	for k, id := range ids {
		wg.Add(1)
		go func(k int, id string) {
			defer wg.Done()
			for j := 0; j < per; j++ {
				t0 := time.Now()
				var r struct {
					MessageID string `json:"message_id"`
				}
				_, err := call("POST", "/api/v1/messages/send", tenant.APIKey, fmt.Sprintf("m-%s-%d-%d", run, k, j),
					map[string]any{"instance_id": id, "to": "5562999999999", "type": "text", "payload": map[string]string{"text": fmt.Sprintf("m%05d", j)}}, &r)
				mu.Lock()
				if err != nil {
					failures = append(failures, err.Error())
				} else {
					accept = append(accept, time.Since(t0))
					byInst[k] = append(byInst[k], r.MessageID)
				}
				mu.Unlock()
			}
		}(k, id)
	}
	wg.Wait()
	acceptedIn := time.Since(start)

	// Delivery time is read from the provider side (the stubs' counters), which does not depend on how fast this
	// program can poll statuses: the moment the stubs have received every accepted message.
	var deliveredIn time.Duration
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		if *stubs == "" {
			return
		}
		want := 0
		for _, l := range byInst {
			want += len(l)
		}
		for time.Since(start) < *timeout {
			if st, err := readStubs(*stubs); err == nil && st.Sends >= want {
				deliveredIn = time.Since(start)
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	final := map[string]int{}
	pending := map[string]bool{}
	total := 0
	for _, l := range byInst {
		for _, m := range l {
			pending[m] = true
			total++
		}
	}
	for time.Since(start) < *timeout && len(pending) > 0 {
		for m := range pending {
			var msg struct {
				Status string `json:"status"`
			}
			if _, err := call("GET", "/api/v1/messages/"+m, tenant.APIKey, "", nil, &msg); err != nil {
				continue
			}
			if msg.Status != "QUEUED" && msg.Status != "DISPATCHING" {
				final[msg.Status]++
				delete(pending, m)
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	done := time.Since(start)

	sort.Slice(accept, func(i, j int) bool { return accept[i] < accept[j] })
	pct := func(p float64) time.Duration {
		if len(accept) == 0 {
			return 0
		}
		return accept[int(float64(len(accept)-1)*p)].Round(100 * time.Microsecond)
	}
	<-watchDone
	delivered := "n/a"
	if deliveredIn > 0 {
		delivered = fmt.Sprintf("%s (%.0f msg/s)", deliveredIn.Round(time.Millisecond), float64(total)/deliveredIn.Seconds())
	}
	fmt.Printf("%d messages / %d instances: accepted in %s (%.0f msg/s; accept p50=%s p95=%s p99=%s); delivered to the provider in %s; statuses settled in %s; states=%v\n",
		total, len(ids), acceptedIn.Round(time.Millisecond), float64(total)/acceptedIn.Seconds(), pct(.5), pct(.95), pct(.99),
		delivered, done.Round(time.Millisecond), final)

	okStates := final["ACCEPTED"] == total
	if *allowUnk {
		okStates = final["ACCEPTED"]+final["UNKNOWN"] == total
	}
	bad := len(failures) > 0 || len(pending) > 0 || !okStates
	for _, f := range failures[:min(len(failures), 3)] {
		fmt.Println("send failure:", f)
	}
	if len(pending) > 0 {
		fmt.Printf("%d messages never reached a terminal state\n", len(pending))
	}
	if sink != nil {
		var all []string
		for _, l := range byInst {
			all = append(all, l...)
		}
		// every message reaches ACCEPTED (or UNKNOWN when its worker died mid-send): the tenant must be told, through the
		// webhook, despite the injected endpoint failures and worker kills. Retries back off, so allow generous time.
		accepted := []string{"ACCEPTED"}
		if *allowUnk {
			accepted = append(accepted, "UNKNOWN")
		}
		deadline := time.Now().Add(*timeout)
		for time.Now().Before(deadline) && len(sink.missing(all, accepted...)) > 0 {
			time.Sleep(500 * time.Millisecond)
		}
		miss := sink.missing(all, accepted...)
		fmt.Printf("webhooks: %s; %d of %d messages never got their status event\n", sink.summary(), len(miss), len(all))
		for _, id := range miss[:min(len(miss), 5)] {
			fmt.Println("no status event for message", id)
		}
		bad = bad || len(miss) > 0 || sink.badSignatures() > 0
	}
	st, serr := readStubs(*stubs)
	if serr != nil {
		fatal("stubs: %v", serr)
	}
	sends, dups, reord := st.Sends, st.Duplicates, st.Reordered
	if *stubs != "" {
		fmt.Printf("provider side: %d sends, %d duplicates, %d out of order\n", sends, dups, reord)
		// an UNKNOWN message may or may not have reached the provider before its worker died
		lo, hi := total, total
		if *allowUnk {
			lo, hi = final["ACCEPTED"], final["ACCEPTED"]+final["UNKNOWN"]
		}
		bad = bad || dups > 0 || reord > 0 || sends < lo || sends > hi
	}
	if bad {
		fatal("FAILED")
	}
	fmt.Println("OK: every message delivered exactly once, in order")
}
