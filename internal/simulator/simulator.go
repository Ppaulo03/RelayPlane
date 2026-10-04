// Package simulator is a stand-in for a WhatsApp provider node (Evolution API v2 subset) that a developer or a CI job can
// DRIVE: it behaves like a node towards RelayPlane (instances, pairing, sends, webhooks in the real payload shape) and
// exposes a control API (/_sim/...) to play the other side: scan the QR, receive a message from a user (optionally
// quoting one of our messages), deliver receipts, drop the connection, inject failures.
//
// It lets the conversation agent (or any consumer) run end to end against the REAL RelayPlane binaries without a WhatsApp
// number. It is for development, CI and load tests only; it is never part of a production deployment.
package simulator

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Config configures a simulator.
type Config struct {
	// APIKey is the node API key RelayPlane presents (header "apikey"); the control API uses the same key.
	APIKey string
	// Version is reported by GET / (RelayPlane refuses untested provider versions). Default 2.3.7.
	Version string
	// SendLatency is added to every send (the provider's own processing time).
	SendLatency time.Duration
	// AutoPair makes every created instance CONNECTED at once (load tests, no QR step).
	AutoPair bool
	// AutoReceipts makes the simulator report "delivered" for every message it accepted, after ReceiptDelay.
	AutoReceipts bool
	ReceiptDelay time.Duration
	// WebhookBase, when set, replaces scheme://host of the webhook URL RelayPlane configured (to run the simulator
	// outside the compose network, e.g. on the host, towards a gateway published on localhost).
	WebhookBase string
	// Now is the clock (tests).
	Now func() time.Time
	// HTTP is the client used for webhooks.
	HTTP *http.Client
}

// Simulator is the node.
type Simulator struct {
	cfg Config

	mu        sync.Mutex
	instances map[string]*instanceState
	faults    []string // kinds queued for the next provider API calls
	seq       int

	// counters for load tests (GET /_stats)
	sends, duplicates, reordered int
	seen                         map[string]int
	lastText                     map[string]string
	attachments                  map[string]*attachment // by provider message id
	downloads                    int
	presences                    []Presence
	reads                        []ReadMark
}

type webhookConfig struct {
	URL     string
	Headers map[string]string
}

// Sent is a message the simulator accepted from RelayPlane.
type Sent struct {
	ID       string    `json:"id"`
	Instance string    `json:"instance"`
	To       string    `json:"to"`
	Type     string    `json:"type"`
	Text     string    `json:"text,omitempty"`
	MediaURL string    `json:"media_url,omitempty"`
	At       time.Time `json:"at"`
	// Quote: the message this one quotes. A real node only quotes when the request carries the quoted MESSAGE (it keeps no
	// history to look it up in); a bare id is silently ignored and QuoteDropped says so.
	QuotedID     string `json:"quoted_id,omitempty"`
	QuotedText   string `json:"quoted_text,omitempty"`
	QuotedFromMe bool   `json:"quoted_from_me,omitempty"`
	QuoteDropped bool   `json:"quote_dropped,omitempty"`
}

type instanceState struct {
	name    string
	state   string // connecting | open | close
	owner   string
	webhook *webhookConfig
	sent    []Sent
}

// New returns a simulator.
func New(cfg Config) *Simulator {
	if cfg.Version == "" {
		cfg.Version = "2.3.7"
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.HTTP == nil {
		cfg.HTTP = &http.Client{Timeout: 10 * time.Second}
	}
	return &Simulator{cfg: cfg, instances: map[string]*instanceState{}, seen: map[string]int{}, lastText: map[string]string{}, attachments: map[string]*attachment{}}
}

func (s *Simulator) newID(prefix string) string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	s.mu.Lock()
	s.seq++
	n := s.seq
	s.mu.Unlock()
	return fmt.Sprintf("%s%06d%s", prefix, n, strings.ToUpper(hex.EncodeToString(b)))
}

// ---- HTTP ----

// Handler serves the node API and the control API.
func (s *Simulator) Handler() http.Handler { return http.HandlerFunc(s.serve) }

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func apiErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]any{"status": code, "error": http.StatusText(code), "response": map[string]any{"message": []string{msg}}})
}

func (s *Simulator) serve(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/" {
		writeJSON(w, 200, map[string]any{"status": 200, "message": "Welcome to the Evolution API, it is working!", "version": s.cfg.Version})
		return
	}
	if r.Header.Get("apikey") != s.cfg.APIKey {
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
	if parts[0] == "_sim" {
		s.control(w, r, parts[1:])
		return
	}
	if len(parts) < 2 {
		apiErr(w, 404, "route not found")
		return
	}
	if s.injected(w) {
		return
	}
	name := ""
	if len(parts) > 2 {
		name, _ = url.PathUnescape(parts[2])
	}
	route := parts[0] + "/" + parts[1]
	switch {
	case route == "instance/create" && r.Method == http.MethodPost:
		s.create(w, r)
	case route == "instance/fetchInstances":
		s.fetch(w, r)
	default:
		s.instanceRoute(w, r, route, name)
	}
}

// injected answers the call with a queued fault, if any, and reports whether it did.
func (s *Simulator) injected(w http.ResponseWriter) bool {
	s.mu.Lock()
	if len(s.faults) == 0 {
		s.mu.Unlock()
		return false
	}
	kind := s.faults[0]
	s.faults = s.faults[1:]
	s.mu.Unlock()
	switch kind {
	case "unavailable": // throttled before the node accepted it: provably not executed
		apiErr(w, 429, "Too Many Requests")
	case "auth":
		apiErr(w, 401, "Unauthorized")
	case "not_found":
		apiErr(w, 404, "The instance does not exist")
	case "server_error":
		apiErr(w, 500, "Internal Server Error")
	case "ambiguous": // the node crashes mid-request: connection dropped, no response
		if hj, ok := w.(http.Hijacker); ok {
			if c, _, err := hj.Hijack(); err == nil {
				_ = c.Close()
			}
		}
	default:
		apiErr(w, 500, "unknown injected fault "+kind)
	}
	return true
}

func (s *Simulator) create(w http.ResponseWriter, r *http.Request) {
	var body struct {
		InstanceName string `json:"instanceName"`
		Webhook      *struct {
			URL     string            `json:"url"`
			Headers map[string]string `json:"headers"`
		} `json:"webhook"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	s.mu.Lock()
	if _, ok := s.instances[body.InstanceName]; ok {
		s.mu.Unlock()
		apiErr(w, 403, fmt.Sprintf("This name %q is already in use.", body.InstanceName))
		return
	}
	inst := &instanceState{name: body.InstanceName, state: "connecting"}
	if s.cfg.AutoPair {
		inst.state, inst.owner = "open", "5562999999999@s.whatsapp.net"
	}
	if body.Webhook != nil && body.Webhook.URL != "" {
		inst.webhook = &webhookConfig{URL: s.rewrite(body.Webhook.URL), Headers: body.Webhook.Headers}
	}
	s.instances[body.InstanceName] = inst
	s.mu.Unlock()
	writeJSON(w, 201, map[string]any{"instance": map[string]any{"instanceName": body.InstanceName, "instanceId": "uuid-" + body.InstanceName, "status": "created"}, "hash": "h"})
}

// rewrite applies Config.WebhookBase to a webhook URL.
func (s *Simulator) rewrite(raw string) string {
	if s.cfg.WebhookBase == "" {
		return raw
	}
	u, err := url.Parse(raw)
	b, err2 := url.Parse(s.cfg.WebhookBase)
	if err != nil || err2 != nil {
		return raw
	}
	u.Scheme, u.Host = b.Scheme, b.Host
	return u.String()
}

func (s *Simulator) fetch(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []map[string]any
	for n, i := range s.instances {
		if want := r.URL.Query().Get("instanceName"); want == "" || want == n {
			out = append(out, map[string]any{"id": "uuid-" + n, "name": n, "connectionStatus": i.state, "ownerJid": i.owner})
		}
	}
	writeJSON(w, 200, out)
}

func (s *Simulator) instanceRoute(w http.ResponseWriter, r *http.Request, route, name string) {
	s.mu.Lock()
	inst, ok := s.instances[name]
	state := ""
	if ok {
		state = inst.state
	}
	s.mu.Unlock()
	if !ok {
		apiErr(w, 404, fmt.Sprintf("The %q instance does not exist", name))
		return
	}
	switch route {
	case "instance/connectionState":
		writeJSON(w, 200, map[string]any{"instance": map[string]any{"instanceName": name, "state": state}})
	case "instance/connect":
		if state == "open" {
			writeJSON(w, 200, map[string]any{"instance": map[string]any{"instanceName": name, "state": "open"}})
			return
		}
		writeJSON(w, 200, map[string]any{"pairingCode": "ABCD1234", "code": "2@sim-qr", "base64": "data:image/png;base64,AAAA", "count": 1})
	case "instance/logout":
		if state != "open" {
			apiErr(w, 400, "Instance is not connected")
			return
		}
		s.mu.Lock()
		inst.state, inst.owner = "close", ""
		s.mu.Unlock()
		writeJSON(w, 200, map[string]any{"status": "SUCCESS", "error": false})
	case "instance/delete":
		s.mu.Lock()
		delete(s.instances, name)
		s.mu.Unlock()
		writeJSON(w, 200, map[string]any{"status": "SUCCESS", "error": false})
	case "message/sendText", "message/sendMedia", "message/sendWhatsAppAudio":
		s.send(w, r, inst, route)
	case "chat/getBase64FromMediaMessage":
		s.download(w, r)
	case "chat/sendPresence":
		s.presence(w, r, inst)
	case "chat/markMessageAsRead":
		s.markRead(w, r, inst)
	default:
		apiErr(w, 404, "route not found")
	}
}

// download answers Evolution's getBase64FromMediaMessage: the request carries the message (the node keeps none), the
// answer is the decrypted content in base64.
func (s *Simulator) download(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Message struct {
			Key         struct{ ID string } `json:"key"`
			MessageType string              `json:"messageType"`
			Message     map[string]any      `json:"message"`
		} `json:"message"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&in); err != nil || in.Message.Message == nil {
		apiErr(w, 400, "invalid media request")
		return
	}
	s.mu.Lock()
	s.downloads++
	a, ok := s.attachments[in.Message.Key.ID]
	if ok && a.failures > 0 {
		a.failures--
		ok = false
	}
	s.mu.Unlock()
	if !ok {
		apiErr(w, 400, "Error: Failed to download media (404): the attachment is no longer available")
		return
	}
	writeJSON(w, 200, map[string]any{"mediaType": in.Message.MessageType, "fileName": a.filename, "mimetype": a.mime,
		"size": map[string]any{"fileLength": len(a.content)}, "base64": base64.StdEncoding.EncodeToString(a.content), "buffer": nil})
}

// Presence is one "typing…" the simulator was asked to show.
type Presence struct {
	Instance string `json:"instance"`
	To       string `json:"to"`
	State    string `json:"state"`
	DelayMS  int64  `json:"delay_ms"`
}

// ReadMark is one message RelayPlane marked as read.
type ReadMark struct {
	Instance string `json:"instance"`
	Chat     string `json:"chat"`
	ID       string `json:"id"`
	FromMe   bool   `json:"from_me"`
}

// presence answers Evolution's sendPresence: number, presence and delay are all required (the real node 400s without them).
func (s *Simulator) presence(w http.ResponseWriter, r *http.Request, inst *instanceState) {
	var in struct {
		Number   string   `json:"number"`
		Presence string   `json:"presence"`
		Delay    *float64 `json:"delay"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&in); err != nil || in.Number == "" || in.Delay == nil ||
		!map[string]bool{"unavailable": true, "available": true, "composing": true, "recording": true, "paused": true}[in.Presence] {
		apiErr(w, 400, "invalid presence request: number, presence and delay are required")
		return
	}
	if inst.state != "open" {
		apiErr(w, 400, "Connection Closed")
		return
	}
	s.mu.Lock()
	s.presences = append(s.presences, Presence{Instance: inst.name, To: in.Number, State: in.Presence, DelayMS: int64(*in.Delay)})
	s.mu.Unlock()
	writeJSON(w, 201, map[string]any{"presence": in.Presence})
}

func (s *Simulator) markRead(w http.ResponseWriter, r *http.Request, inst *instanceState) {
	var in struct {
		ReadMessages []struct {
			ID        string `json:"id"`
			FromMe    *bool  `json:"fromMe"`
			RemoteJid string `json:"remoteJid"`
		} `json:"readMessages"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&in); err != nil || len(in.ReadMessages) == 0 {
		apiErr(w, 400, "readMessages is required")
		return
	}
	for _, m := range in.ReadMessages {
		if m.ID == "" || m.FromMe == nil || m.RemoteJid == "" {
			apiErr(w, 400, "each read message needs id, fromMe and remoteJid")
			return
		}
	}
	if inst.state != "open" {
		apiErr(w, 400, "Connection Closed")
		return
	}
	s.mu.Lock()
	for _, m := range in.ReadMessages {
		s.reads = append(s.reads, ReadMark{Instance: inst.name, Chat: m.RemoteJid, ID: m.ID, FromMe: *m.FromMe})
	}
	s.mu.Unlock()
	writeJSON(w, 201, map[string]any{"message": "Read messages", "read": "success"})
}

// Presences and Reads return what the node was asked to show/mark (tests).
func (s *Simulator) Presences(name string) []Presence {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Presence
	for _, p := range s.presences {
		if p.Instance == name {
			out = append(out, p)
		}
	}
	return out
}

func (s *Simulator) Reads(name string) []ReadMark {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []ReadMark
	for _, m := range s.reads {
		if m.Instance == name {
			out = append(out, m)
		}
	}
	return out
}

// MediaDownloads is how many times the node was asked for an attachment (tests).
func (s *Simulator) MediaDownloads() int { s.mu.Lock(); defer s.mu.Unlock(); return s.downloads }

func (s *Simulator) send(w http.ResponseWriter, r *http.Request, inst *instanceState, route string) {
	s.mu.Lock()
	state := inst.state
	s.mu.Unlock()
	if state != "open" {
		apiErr(w, 400, "Connection Closed")
		return
	}
	var body map[string]any
	_ = json.NewDecoder(r.Body).Decode(&body)
	str := func(k string) string { v, _ := body[k].(string); return v }
	typ, text, media := "text", str("text"), ""
	switch route {
	case "message/sendMedia":
		typ, text, media = str("mediatype"), str("caption"), str("media")
	case "message/sendWhatsAppAudio":
		typ, media = "audio", str("audio")
	}
	if s.cfg.SendLatency > 0 {
		time.Sleep(s.cfg.SendLatency) // the provider's own processing time; concurrent across instances
	}
	id := s.newID("3EB0")
	rec := Sent{ID: id, Instance: inst.name, To: str("number"), Type: typ, Text: text, MediaURL: media, At: s.cfg.Now().UTC()}
	if q, ok := body["quoted"].(map[string]any); ok {
		key, _ := q["key"].(map[string]any)
		qid, _ := key["id"].(string)
		fromMe, _ := key["fromMe"].(bool)
		if msg, ok := q["message"].(map[string]any); ok && qid != "" {
			qtext, _ := msg["conversation"].(string)
			rec.QuotedID, rec.QuotedText, rec.QuotedFromMe = qid, qtext, fromMe
		} else {
			rec.QuoteDropped = true
		}
	}
	s.mu.Lock()
	inst.sent = append(inst.sent, rec)
	s.sends++
	k := inst.name + "\x00" + text
	if s.seen[k]++; s.seen[k] > 1 {
		s.duplicates++
	}
	if prev := s.lastText[inst.name]; text != "" && prev != "" && text < prev {
		s.reordered++
	}
	if text != "" {
		s.lastText[inst.name] = text
	}
	s.mu.Unlock()
	if s.cfg.AutoReceipts {
		go func() {
			time.Sleep(s.cfg.ReceiptDelay)
			_, _ = s.Receipt(inst.name, id, "delivered")
		}()
	}
	writeJSON(w, 201, map[string]any{"key": map[string]any{"remoteJid": str("number") + "@s.whatsapp.net", "fromMe": true, "id": id}, "status": "PENDING"})
}

// ---- webhooks (the simulator plays the provider towards RelayPlane) ----

// emit POSTs one Evolution-shaped webhook to the URL RelayPlane configured for the instance and returns the gateway's
// status. It retries a few times on transport errors (the gateway may still be starting).
func (s *Simulator) emit(name, event string, data any) (int, error) {
	s.mu.Lock()
	inst, ok := s.instances[name]
	var wh *webhookConfig
	if ok {
		wh = inst.webhook
	}
	s.mu.Unlock()
	if !ok {
		return 0, fmt.Errorf("instance %q does not exist", name)
	}
	if wh == nil {
		return 0, fmt.Errorf("instance %q has no webhook configured (was it created by RelayPlane?)", name)
	}
	raw, err := json.Marshal(map[string]any{"event": event, "instance": name, "data": data, "date_time": s.cfg.Now().UTC().Format(time.RFC3339)})
	if err != nil {
		return 0, err
	}
	var last error
	for attempt := 0; attempt < 4; attempt++ {
		req, _ := http.NewRequest(http.MethodPost, wh.URL, bytes.NewReader(raw))
		req.Header.Set("Content-Type", "application/json")
		for k, v := range wh.Headers {
			req.Header.Set(k, v)
		}
		resp, err := s.cfg.HTTP.Do(req)
		if err == nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
			resp.Body.Close()
			return resp.StatusCode, nil
		}
		last = err
		time.Sleep(time.Duration(attempt+1) * 150 * time.Millisecond)
	}
	return 0, last
}

// Scan plays the user scanning the QR code: the instance becomes connected and the node reports it.
func (s *Simulator) Scan(name string) (int, error) {
	s.mu.Lock()
	inst, ok := s.instances[name]
	if ok {
		inst.state, inst.owner = "open", "5562999999999@s.whatsapp.net"
	}
	s.mu.Unlock()
	if !ok {
		return 0, fmt.Errorf("instance %q does not exist", name)
	}
	return s.emit(name, "connection.update", map[string]any{"state": "open", "statusReason": 200})
}

// Disconnect plays the WhatsApp socket dying. loggedOut simulates the user logging the device out from the phone.
func (s *Simulator) Disconnect(name string, loggedOut bool) (int, error) {
	s.mu.Lock()
	inst, ok := s.instances[name]
	if ok {
		inst.state = "close"
	}
	s.mu.Unlock()
	if !ok {
		return 0, fmt.Errorf("instance %q does not exist", name)
	}
	reason := 428
	if loggedOut {
		reason = 401
	}
	return s.emit(name, "connection.update", map[string]any{"state": "close", "statusReason": reason})
}

// Inbound describes a message a user sends to the connected number.
type Inbound struct {
	From     string `json:"from"`      // sender number, e.g. 5562988887777
	Text     string `json:"text"`      // text or caption
	Type     string `json:"type"`      // text (default) | image | audio | video | document
	ReplyTo  string `json:"reply_to"`  // provider id of the message being quoted, or "last_sent"
	Group    bool   `json:"group"`     // sent in a group chat
	PushName string `json:"push_name"` // sender's display name
	// Timestamp is when the user wrote it (the provider's stamp); default: now.
	Timestamp time.Time `json:"timestamp"`
	ID        string    `json:"id"`
	// Media fields (type != text)
	Mimetype string `json:"mimetype"`
	Filename string `json:"filename"`
	Seconds  int    `json:"seconds"`
	URL      string `json:"url"`
	// Content is what a download of the attachment returns (default: a few bytes). DeclaredSize overrides the size the
	// message announces (fileLength), to play a huge attachment without holding it. FailDownloads makes the first N
	// downloads of this attachment fail as an attachment that WhatsApp no longer has.
	Content       []byte `json:"content"`
	DeclaredSize  int64  `json:"declared_size"`
	FailDownloads int    `json:"fail_downloads"`
}

type attachment struct {
	content  []byte
	mime     string
	filename string
	failures int
}

// InboundResult is what the simulator reports back.
type InboundResult struct {
	ID            string `json:"id"`
	GatewayStatus int    `json:"gateway_status"`
	ReplyTo       string `json:"reply_to,omitempty"`
}

// Receive plays a user sending a message: it POSTs a messages.upsert webhook with the real Evolution payload shape.
func (s *Simulator) Receive(name string, in Inbound) (InboundResult, error) {
	if in.From == "" {
		in.From = "5562988887777"
	}
	if in.Type == "" {
		in.Type = "text"
	}
	if in.ID == "" {
		in.ID = s.newID("SIM")
	}
	if in.Timestamp.IsZero() {
		in.Timestamp = s.cfg.Now()
	}
	reply := in.ReplyTo
	if reply == "last_sent" {
		s.mu.Lock()
		if inst, ok := s.instances[name]; ok && len(inst.sent) > 0 {
			reply = inst.sent[len(inst.sent)-1].ID
		} else {
			reply = ""
		}
		s.mu.Unlock()
		if reply == "" {
			return InboundResult{}, fmt.Errorf("instance %q has not sent anything to quote", name)
		}
	}
	ctxInfo := map[string]any{}
	if reply != "" {
		ctxInfo = map[string]any{"stanzaId": reply, "participant": "5562999999999@s.whatsapp.net", "quotedMessage": map[string]any{"conversation": "(quoted)"}}
	}
	jid := in.From + "@s.whatsapp.net"
	key := map[string]any{"remoteJid": jid, "fromMe": false, "id": in.ID}
	if in.Group {
		key["remoteJid"] = "120363000000000001@g.us"
		key["participant"] = jid
	}
	var msgType string
	var message map[string]any
	withCtx := func(body map[string]any) map[string]any {
		if reply != "" {
			body["contextInfo"] = ctxInfo
		}
		return body
	}
	switch in.Type {
	case "text":
		if reply == "" {
			msgType, message = "conversation", map[string]any{"conversation": in.Text}
		} else {
			msgType, message = "extendedTextMessage", map[string]any{"extendedTextMessage": withCtx(map[string]any{"text": in.Text})}
		}
	case "image", "video", "audio", "document":
		msgType = in.Type + "Message"
		mt := in.Mimetype
		if mt == "" {
			mt = map[string]string{"image": "image/jpeg", "video": "video/mp4", "audio": "audio/ogg; codecs=opus", "document": "application/pdf"}[in.Type]
		}
		content := in.Content
		if len(content) == 0 {
			content = []byte("simulated " + in.Type + " content")
		}
		size := in.DeclaredSize
		if size <= 0 {
			size = int64(len(content))
		}
		s.mu.Lock()
		s.attachments[in.ID] = &attachment{content: content, mime: mt, filename: in.Filename, failures: in.FailDownloads}
		s.mu.Unlock()
		// the shapes Evolution really sends: fileLength is a protobuf Long, mediaKey a serialised byte buffer
		body := map[string]any{"mimetype": mt, "url": in.URL, "directPath": "/v/t62.sim/" + in.ID,
			"fileLength": map[string]any{"low": size & 0xffffffff, "high": size >> 32, "unsigned": true},
			"mediaKey":   map[string]any{"0": 1, "1": 2, "2": 3}, "fileSha256": "c2ltdWxhdGVk"}
		if in.Text != "" {
			body["caption"] = in.Text
		}
		if in.Filename != "" {
			body["fileName"] = in.Filename
		}
		if in.Type == "audio" {
			body["seconds"], body["ptt"] = in.Seconds, true
		}
		message = map[string]any{msgType: withCtx(body)}
	default:
		return InboundResult{}, fmt.Errorf("unsupported message type %q", in.Type)
	}
	data := map[string]any{"key": key, "pushName": in.PushName, "messageType": msgType, "message": message, "messageTimestamp": in.Timestamp.Unix()}
	st, err := s.emit(name, "messages.upsert", data)
	return InboundResult{ID: in.ID, GatewayStatus: st, ReplyTo: reply}, err
}

// Receipt plays the provider reporting the state of a message RelayPlane sent: delivered | read | failed.
// messageID may be "last_sent".
func (s *Simulator) Receipt(name, messageID, status string) (int, error) {
	if messageID == "last_sent" {
		s.mu.Lock()
		inst, ok := s.instances[name]
		if ok && len(inst.sent) > 0 {
			messageID = inst.sent[len(inst.sent)-1].ID
		}
		s.mu.Unlock()
	}
	ev := map[string]string{"delivered": "DELIVERY_ACK", "read": "READ", "failed": "ERROR"}[status]
	if ev == "" {
		return 0, fmt.Errorf("unknown receipt status %q (delivered|read|failed)", status)
	}
	return s.emit(name, "messages.update", map[string]any{"keyId": messageID, "status": ev})
}

// SentMessages returns what the instance has accepted from RelayPlane.
func (s *Simulator) SentMessages(name string) []Sent {
	s.mu.Lock()
	defer s.mu.Unlock()
	if inst, ok := s.instances[name]; ok {
		return append([]Sent(nil), inst.sent...)
	}
	return nil
}

// InjectFaults queues failures for the next provider API calls: unavailable | auth | not_found | server_error | ambiguous.
func (s *Simulator) InjectFaults(kinds ...string) {
	s.mu.Lock()
	s.faults = append(s.faults, kinds...)
	s.mu.Unlock()
}

// Reset forgets every instance and fault.
func (s *Simulator) Reset() {
	s.mu.Lock()
	s.instances, s.faults, s.seen, s.lastText = map[string]*instanceState{}, nil, map[string]int{}, map[string]string{}
	s.sends, s.duplicates, s.reordered = 0, 0, 0
	s.mu.Unlock()
}

// ---- control API ----

func (s *Simulator) control(w http.ResponseWriter, r *http.Request, p []string) {
	decode := func(v any) bool {
		if r.Body == nil {
			return true
		}
		dec := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
		if err := dec.Decode(v); err != nil && err != io.EOF {
			apiErr(w, 400, "invalid JSON body: "+err.Error())
			return false
		}
		return true
	}
	reply := func(status int, err error, extra map[string]any) {
		if err != nil {
			apiErr(w, 502, err.Error())
			return
		}
		out := map[string]any{"gateway_status": status}
		for k, v := range extra {
			out[k] = v
		}
		writeJSON(w, 200, out)
	}
	switch {
	case len(p) == 1 && p[0] == "instances" && r.Method == http.MethodGet:
		s.mu.Lock()
		out := []map[string]any{}
		for n, i := range s.instances {
			out = append(out, map[string]any{"name": n, "state": i.state, "sent": len(i.sent), "webhook": i.webhook != nil})
		}
		s.mu.Unlock()
		writeJSON(w, 200, map[string]any{"instances": out})
	case len(p) == 1 && p[0] == "reset" && r.Method == http.MethodPost:
		s.Reset()
		w.WriteHeader(http.StatusNoContent)
	case len(p) == 1 && p[0] == "faults" && r.Method == http.MethodPost:
		var in struct {
			Next []string `json:"next"`
		}
		if decode(&in) {
			s.InjectFaults(in.Next...)
			w.WriteHeader(http.StatusNoContent)
		}
	case len(p) == 3 && p[0] == "instances" && p[2] == "sent" && r.Method == http.MethodGet:
		writeJSON(w, 200, map[string]any{"sent": s.SentMessages(p[1])})
	case len(p) == 3 && p[0] == "instances" && p[2] == "scan" && r.Method == http.MethodPost:
		st, err := s.Scan(p[1])
		reply(st, err, nil)
	case len(p) == 3 && p[0] == "instances" && p[2] == "disconnect" && r.Method == http.MethodPost:
		var in struct {
			LoggedOut bool `json:"logged_out"`
		}
		if decode(&in) {
			st, err := s.Disconnect(p[1], in.LoggedOut)
			reply(st, err, nil)
		}
	case len(p) == 3 && p[0] == "instances" && p[2] == "inbound" && r.Method == http.MethodPost:
		var in Inbound
		if decode(&in) {
			res, err := s.Receive(p[1], in)
			reply(res.GatewayStatus, err, map[string]any{"id": res.ID, "reply_to": res.ReplyTo})
		}
	case len(p) == 3 && p[0] == "instances" && p[2] == "receipt" && r.Method == http.MethodPost:
		var in struct {
			MessageID string `json:"message_id"`
			Status    string `json:"status"`
		}
		if decode(&in) {
			st, err := s.Receipt(p[1], in.MessageID, in.Status)
			reply(st, err, nil)
		}
	default:
		apiErr(w, 404, "unknown control route")
	}
}
