package v2

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"time"

	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/events"
	"github.com/relayplane/relayplane/internal/core/ownership"
	"github.com/relayplane/relayplane/internal/ports"
)

// mediaKinds maps Evolution's message body keys to the attachment kinds RelayPlane reports.
var mediaKinds = map[string]string{
	"imageMessage": "image", "videoMessage": "video", "audioMessage": "audio", "documentMessage": "document", "stickerMessage": "sticker",
}

// mediaKeep lists what the download needs; thumbnails, waveforms and contextInfo are dropped from the stored reference.
var mediaKeep = []string{"url", "directPath", "mediaKey", "fileSha256", "fileEncSha256", "mimetype", "fileLength", "mediaKeyTimestamp", "fileName", "seconds", "ptt"}

// mediaOf describes the attachment of an inbound message (nil when there is none). The reference is the exact message
// Evolution's getBase64FromMediaMessage accepts: with the message given in the request it needs no stored copy (the nodes
// run with DATABASE_SAVE_DATA_NEW_MESSAGE=false). It holds the media key, so it never leaves the control plane.
func mediaOf(d upsertData) *events.InboundMedia {
	for key, kind := range mediaKinds {
		body, ok := d.Message[key].(map[string]any)
		if !ok {
			continue
		}
		str := func(k string) string { s, _ := body[k].(string); return s }
		name := str("fileName")
		if name == "" {
			name = str("title")
		}
		pruned := map[string]any{}
		for _, k := range mediaKeep {
			if v, ok := body[k]; ok {
				pruned[k] = v
			}
		}
		ref, err := json.Marshal(map[string]any{
			"key":         map[string]any{"id": d.Key.ID, "remoteJid": d.Key.RemoteJid, "fromMe": d.Key.FromMe},
			"message":     map[string]any{key: pruned},
			"messageType": key,
		})
		if err != nil {
			return nil
		}
		return &events.InboundMedia{Ref: ref, Media: events.MessageMedia{Kind: kind, MimeType: str("mimetype"), Size: longValue(body["fileLength"]),
			Filename: name, Seconds: int(longValue(body["seconds"]))}}
	}
	return nil
}

// longValue reads a number as Evolution serialises it: a JSON number, a numeric string or a protobuf Long
// ({"low":4719,"high":0,"unsigned":true}).
func longValue(v any) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case string:
		i, _ := strconv.ParseInt(n, 10, 64)
		return i
	case map[string]any:
		low, _ := n["low"].(float64)
		high, _ := n["high"].(float64)
		return int64(uint32(int64(high)))<<32 | int64(uint32(int64(low)))
	}
	return 0
}

type mediaResponse struct {
	MediaType string `json:"mediaType"`
	FileName  string `json:"fileName"`
	Mimetype  string `json:"mimetype"`
	Base64    string `json:"base64"`
}

var _ ports.MediaDownloader = (*Provider)(nil)

// DownloadMedia asks the node to fetch and decrypt the attachment (Evolution answers with base64 inside JSON, so the
// whole content passes through memory: maxBytes is enforced on the declared size before and on the response here).
func (p *Provider) DownloadMedia(ctx context.Context, a ownership.Assignment, ref json.RawMessage, maxBytes int64) (*ports.DownloadedMedia, error) {
	var msg map[string]any
	if err := json.Unmarshal(ref, &msg); err != nil || msg["message"] == nil {
		return nil, fmt.Errorf("%w: unusable media reference", errs.ErrProviderRejected)
	}
	// base64 is 4/3 of the content; add room for the JSON around it
	limit := maxBytes*4/3 + 64<<10
	resp, err := p.c.doWith(ctx, a.NodeID, "POST", "/chat/getBase64FromMediaMessage/"+url.PathEscape(a.InstanceID), nil,
		map[string]any{"message": msg, "convertToMp4": false}, opManage, true, callLimits{timeout: 2 * time.Minute, maxBytes: limit})
	if err != nil {
		return nil, err
	}
	var out mediaResponse
	if err := p.c.decode(resp, &out); err != nil {
		return nil, err
	}
	raw, err := base64.StdEncoding.DecodeString(out.Base64)
	if err != nil || len(raw) == 0 {
		return nil, fmt.Errorf("%w: the node returned no usable content", errs.ErrProviderRejected)
	}
	if int64(len(raw)) > maxBytes {
		return nil, fmt.Errorf("%w: media is %d bytes, the limit is %d", errs.ErrPayloadTooLarge, len(raw), maxBytes)
	}
	return &ports.DownloadedMedia{Body: readCloser(raw), Size: int64(len(raw)), ContentType: out.Mimetype, Filename: out.FileName}, nil
}

func readCloser(b []byte) io.ReadCloser { return io.NopCloser(bytes.NewReader(b)) }

var (
	_ ports.PresenceSender = (*Provider)(nil)
	_ ports.ReadMarker     = (*Provider)(nil)
)

// SendPresence shows "typing…" / "recording…" to a contact. Evolution holds the HTTP call for `delay` milliseconds (it
// keeps the state for that long and then pauses by itself), so the call takes about `duration`.
func (p *Provider) SendPresence(ctx context.Context, a ownership.Assignment, to string, state ports.PresenceState, duration time.Duration) error {
	_, err := p.c.doWith(ctx, a.NodeID, "POST", "/chat/sendPresence/"+url.PathEscape(a.InstanceID), nil,
		map[string]any{"number": to, "presence": string(state), "delay": duration.Milliseconds()}, opManage, true,
		callLimits{timeout: duration + 20*time.Second})
	return err
}

// MarkRead marks messages the chat sent us as read. Evolution wants {id, fromMe, remoteJid} for each (it drops the
// participant of a group message, so this is meant for direct chats).
func (p *Provider) MarkRead(ctx context.Context, a ownership.Assignment, chat string, ids []string) error {
	items := make([]map[string]any, 0, len(ids))
	for _, id := range ids {
		items = append(items, map[string]any{"id": id, "fromMe": false, "remoteJid": chatJID(chat)})
	}
	_, err := p.c.do(ctx, a.NodeID, "POST", "/chat/markMessageAsRead/"+url.PathEscape(a.InstanceID), nil,
		map[string]any{"readMessages": items}, opManage, true)
	return err
}
