package chatsdk_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/chatsdk"
)

const bridgeSecret = boardsync.Secret("bridge-secret")

func TestSignV1Vector(t *testing.T) {
	body := []byte(`{"events":[]}`)
	got := chatsdk.SignV1(bridgeSecret, 1727000400, "nonce-0123456789", body)
	want := "v1=829108018c214008cf6a10f86f5722bd682afac72cd3ce0c4660765ff4c58308"
	if got != want {
		t.Fatalf("SignV1 = %q, want %q", got, want)
	}
}

func TestVerifyV1(t *testing.T) {
	body := []byte(`{"events":[]}`)
	now := time.Unix(1727000400, 0)
	good := func() (string, string, string) {
		return "1727000400", "nonce-0123456789", chatsdk.SignV1(bridgeSecret, 1727000400, "nonce-0123456789", body)
	}
	timestamp, nonce, signature := good()
	if err := chatsdk.VerifyV1(bridgeSecret, timestamp, nonce, signature, body, now, 0); err != nil {
		t.Fatalf("valid: %v", err)
	}
	cases := []struct {
		name      string
		timestamp string
		nonce     string
		signature string
		body      []byte
		now       time.Time
	}{
		{name: "missing timestamp", nonce: nonce, signature: signature, body: body, now: now},
		{name: "malformed timestamp", timestamp: "soon", nonce: nonce, signature: signature, body: body, now: now},
		{name: "stale timestamp", timestamp: "1726990000", nonce: nonce, signature: signature, body: body, now: now},
		{name: "future timestamp", timestamp: "1727004000", nonce: nonce, signature: signature, body: body, now: now},
		{name: "short nonce", timestamp: timestamp, nonce: "short", signature: signature, body: body, now: now},
		{name: "nonce whitespace", timestamp: timestamp, nonce: "nonce with space", signature: signature, body: body, now: now},
		{name: "missing signature", timestamp: timestamp, nonce: nonce, body: body, now: now},
		{name: "wrong scheme", timestamp: timestamp, nonce: nonce, signature: "v2=" + strings.TrimPrefix(signature, "v1="), body: body, now: now},
		{name: "bad hex", timestamp: timestamp, nonce: nonce, signature: "v1=zzzz", body: body, now: now},
		{name: "tampered body", timestamp: timestamp, nonce: nonce, signature: signature, body: []byte(`{"events":[1]}`), now: now},
		{name: "wrong secret", timestamp: timestamp, nonce: nonce, signature: chatsdk.SignV1("other", 1727000400, nonce, body), body: body, now: now},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := chatsdk.VerifyV1(bridgeSecret, tc.timestamp, tc.nonce, tc.signature, tc.body, tc.now, 0)
			if !errors.Is(err, boardsync.ErrUnauthorized) {
				t.Fatalf("err = %v, want ErrUnauthorized", err)
			}
		})
	}
	if err := (chatsdk.VerifyV1("", timestamp, nonce, signature, body, now, 0)); !errors.Is(err, boardsync.ErrUnauthorized) {
		t.Fatalf("empty secret = %v", err)
	}
}

func TestVerifyRequestV1UsesHeaders(t *testing.T) {
	body := []byte(`{}`)
	header := http.Header{}
	header.Set(chatsdk.HeaderTimestamp, "1727000400")
	header.Set(chatsdk.HeaderNonce, "nonce-0123456789")
	header.Set(chatsdk.HeaderSignature, chatsdk.SignV1(bridgeSecret, 1727000400, "nonce-0123456789", body))
	if err := chatsdk.VerifyRequestV1(bridgeSecret, header, body, time.Unix(1727000400, 0), 0); err != nil {
		t.Fatalf("VerifyRequestV1: %v", err)
	}
}

func TestDecodeInbound(t *testing.T) {
	event := boardsync.Event{
		Kind: boardsync.KindMessage, Type: boardsync.TypeMessage,
		Channel: "C0123", Thread: "C0123:42", Message: "42", Text: "hello",
		Author: boardsync.Author{ID: "telegram:user:7", Name: "Ada"},
	}
	batch := chatsdk.InboundBatch{
		Version: "1",
		Events:  []chatsdk.InboundEvent{{Dedupe: "m1", Event: event}},
	}
	body, err := json.Marshal(batch)
	if err != nil {
		t.Fatal(err)
	}
	records, err := chatsdk.DecodeInbound(body, "telegram", "telegram")
	if err != nil {
		t.Fatalf("DecodeInbound: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("records = %d", len(records))
	}
	got := records[0]
	if got.Dedupe != "m1" || got.Event.ID != "m1" {
		t.Fatalf("identity = %+v", got)
	}
	if got.Event.Key != "telegram" || got.Event.Platform != "telegram" {
		t.Fatalf("pinning = %+v", got.Event)
	}
	if err := got.Validate(); err != nil {
		t.Fatalf("record validate: %v", err)
	}

	// NULs and oversized text are cleaned, not stored raw.
	batch.Events[0].Event.Text = "a\x00b"
	body, _ = json.Marshal(batch)
	records, err = chatsdk.DecodeInbound(body, "telegram", "telegram")
	if err != nil {
		t.Fatal(err)
	}
	if strings.ContainsRune(records[0].Event.Text, 0) {
		t.Fatal("NUL survived the bridge")
	}
}

func TestDecodeInboundRefusals(t *testing.T) {
	event := func() boardsync.Event {
		return boardsync.Event{
			Kind: boardsync.KindMessage, Type: boardsync.TypeMessage,
			Channel: "C0123", Thread: "C0123:42", Message: "42", Text: "hello",
			Author: boardsync.Author{ID: "telegram:user:7", Name: "Ada"},
		}
	}
	encode := func(batch chatsdk.InboundBatch) []byte {
		body, err := json.Marshal(batch)
		if err != nil {
			t.Fatal(err)
		}
		return body
	}
	cases := []struct {
		name string
		body []byte
	}{
		{name: "empty", body: nil},
		{name: "not json", body: []byte(`{`)},
		{name: "wrong version", body: encode(chatsdk.InboundBatch{Version: "2", Events: []chatsdk.InboundEvent{{Dedupe: "m1", Event: event()}}})},
		{name: "no events field", body: []byte(`{}`)},
		{name: "empty dedupe", body: encode(chatsdk.InboundBatch{Events: []chatsdk.InboundEvent{{Event: event()}}})},
		{name: "duplicate dedupe", body: encode(chatsdk.InboundBatch{Events: []chatsdk.InboundEvent{{Dedupe: "m1", Event: event()}, {Dedupe: "m1", Event: event()}}})},
		{name: "id mismatch", body: func() []byte {
			e := event()
			e.ID = "other"
			return encode(chatsdk.InboundBatch{Events: []chatsdk.InboundEvent{{Dedupe: "m1", Event: e}}})
		}()},
		{name: "key mismatch", body: func() []byte {
			e := event()
			e.Key = "github"
			return encode(chatsdk.InboundBatch{Events: []chatsdk.InboundEvent{{Dedupe: "m1", Event: e}}})
		}()},
		{name: "platform mismatch", body: func() []byte {
			e := event()
			e.Platform = "slack"
			return encode(chatsdk.InboundBatch{Events: []chatsdk.InboundEvent{{Dedupe: "m1", Event: e}}})
		}()},
		{name: "invalid event", body: func() []byte {
			e := event()
			e.Message = ""
			return encode(chatsdk.InboundBatch{Events: []chatsdk.InboundEvent{{Dedupe: "m1", Event: e}}})
		}()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := chatsdk.DecodeInbound(tc.body, "telegram", "telegram"); !errors.Is(err, boardsync.ErrBadRequest) {
				t.Fatalf("err = %v, want ErrBadRequest", err)
			}
		})
	}
	tooMany := make([]chatsdk.InboundEvent, chatsdk.MaxEvents+1)
	for i := range tooMany {
		tooMany[i] = chatsdk.InboundEvent{Dedupe: "m" + string(rune('a'+i%26)) + string(rune('0'+i/26)), Event: event()}
	}
	// Distinct dedupes but over the batch limit.
	for i := range tooMany {
		tooMany[i].Dedupe = "m" + strings.Repeat("x", i+1)
	}
	if _, err := chatsdk.DecodeInbound(encode(chatsdk.InboundBatch{Events: tooMany}), "telegram", "telegram"); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("too many = %v", err)
	}
	oversized := encode(chatsdk.InboundBatch{Events: []chatsdk.InboundEvent{{Dedupe: "m1", Event: event()}}})
	oversized = append(oversized, make([]byte, chatsdk.MaxBodyBytes)...)
	if _, err := chatsdk.DecodeInbound(oversized, "telegram", "telegram"); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("oversized = %v", err)
	}
}

func TestDecodeInboundRejectsUnstorableIDs(t *testing.T) {
	base := func() boardsync.Event {
		return boardsync.Event{
			Kind: boardsync.KindMessage, Type: boardsync.TypeMessage,
			Channel: "C0123", Thread: "C0123:42", Message: "42", Text: "hello",
			Author: boardsync.Author{ID: "telegram:user:7", Name: "Ada"},
		}
	}
	cases := []struct {
		name   string
		mutate func(*boardsync.Event)
		dedupe string
	}{
		{name: "nul channel", mutate: func(e *boardsync.Event) { e.Channel = "C0\x00123" }},
		{name: "newline thread", mutate: func(e *boardsync.Event) { e.Thread = "C0123:\n42" }},
		{name: "nul message", mutate: func(e *boardsync.Event) { e.Message = "4\x002" }},
		{name: "nul author", mutate: func(e *boardsync.Event) { e.Author.ID = "telegram:\x00bot" }},
		{name: "nul emoji", mutate: func(e *boardsync.Event) {
			e.Kind = boardsync.KindReaction
			e.Type = boardsync.TypeReactionAdded
			e.Emoji = "x\x00y"
			e.Added = true
		}},
		{name: "overlong channel", mutate: func(e *boardsync.Event) { e.Channel = strings.Repeat("c", 301) }},
		{name: "overlong author", mutate: func(e *boardsync.Event) { e.Author.ID = strings.Repeat("a", 301) }},
		{name: "nul dedupe", dedupe: "m\x001"},
		{name: "nul raw", mutate: func(e *boardsync.Event) { e.Raw = json.RawMessage(`{"x":"\u0000"}`) }},
		{name: "nul file name", mutate: func(e *boardsync.Event) { e.Files = []boardsync.File{{Name: "a\x00b"}} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			event := base()
			if tc.mutate != nil {
				tc.mutate(&event)
			}
			dedupe := "m1"
			if tc.dedupe != "" {
				dedupe = tc.dedupe
			}
			batch := chatsdk.InboundBatch{Version: "1", Events: []chatsdk.InboundEvent{{Dedupe: dedupe, Event: event}}}
			body, err := json.Marshal(batch)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := chatsdk.DecodeInbound(body, "telegram", "telegram"); !errors.Is(err, boardsync.ErrBadRequest) {
				t.Fatalf("err = %v, want ErrBadRequest", err)
			}
		})
	}
}

func TestBridgeErrorMappings(t *testing.T) {
	cases := map[chatsdk.ErrorCode]int{
		chatsdk.ErrorAuth:        http.StatusUnauthorized,
		chatsdk.ErrorRateLimited: http.StatusTooManyRequests,
		chatsdk.ErrorTransient:   http.StatusServiceUnavailable,
		chatsdk.ErrorUnsupported: http.StatusUnprocessableEntity,
		chatsdk.ErrorPermanent:   http.StatusUnprocessableEntity,
	}
	for code, status := range cases {
		if !code.Valid() || code.HTTPStatus() != status {
			t.Fatalf("%s status = %d, want %d", code, code.HTTPStatus(), status)
		}
		err := &chatsdk.BridgeError{Code: code}
		if code == chatsdk.ErrorRateLimited || code == chatsdk.ErrorTransient {
			if boardsync.IsPermanent(err) {
				t.Fatalf("%s should be retryable", code)
			}
		} else if !boardsync.IsPermanent(err) {
			t.Fatalf("%s should be permanent", code)
		}
	}
	if chatsdk.ErrorCodeOf(&chatsdk.BridgeError{Code: chatsdk.ErrorAuth}) != chatsdk.ErrorAuth {
		t.Fatal("ErrorCodeOf lost auth")
	}
}

func TestVerifyV1WindowAndNonceBoundaries(t *testing.T) {
	body := []byte(`{}`)
	now := time.Unix(1727000400, 0)
	window := 5 * time.Minute
	check := func(timestamp int64, nonce string, want error) {
		t.Helper()
		signature := chatsdk.SignV1(bridgeSecret, timestamp, nonce, body)
		err := chatsdk.VerifyV1(bridgeSecret, strconv.FormatInt(timestamp, 10), nonce, signature, body, now, window)
		if want != nil {
			if !errors.Is(err, want) {
				t.Fatalf("timestamp %d nonce %d: err = %v", timestamp, len(nonce), err)
			}
			return
		}
		if err != nil {
			t.Fatalf("timestamp %d nonce %d: %v", timestamp, len(nonce), err)
		}
	}
	check(now.Unix()-int64(window/time.Second), strings.Repeat("a", 8), nil)
	check(now.Unix()+int64(window/time.Second), strings.Repeat("a", 128), nil)
	check(now.Unix()-int64(window/time.Second)-1, "nonce-0123456789", boardsync.ErrUnauthorized)
	check(now.Unix()+int64(window/time.Second)+1, "nonce-0123456789", boardsync.ErrUnauthorized)
	check(now.Unix(), strings.Repeat("a", 7), boardsync.ErrUnauthorized)
	check(now.Unix(), strings.Repeat("a", 129), boardsync.ErrUnauthorized)
}

func TestDecodeInboundBoundaryLimits(t *testing.T) {
	event := func(dedupe string) boardsync.Event {
		return boardsync.Event{
			Kind: boardsync.KindMessage, Type: boardsync.TypeMessage,
			Channel: "C0123", Thread: "C0123:42", Message: "42", Text: "hello",
			Author: boardsync.Author{ID: "telegram:user:7", Name: "Ada"},
		}
	}
	makeBatch := func(count int, dedupeLen int) []byte {
		events := make([]chatsdk.InboundEvent, count)
		for i := range events {
			dedupe := fmt.Sprintf("m%d", i)
			if dedupeLen > 0 {
				dedupe = strings.Repeat("d", dedupeLen)
			}
			events[i] = chatsdk.InboundEvent{Dedupe: dedupe, Event: event(dedupe)}
		}
		body, err := json.Marshal(chatsdk.InboundBatch{Version: "1", Events: events})
		if err != nil {
			t.Fatal(err)
		}
		return body
	}
	if _, err := chatsdk.DecodeInbound(makeBatch(chatsdk.MaxEvents, 0), "telegram", "telegram"); err != nil {
		t.Fatalf("%d events: %v", chatsdk.MaxEvents, err)
	}
	if _, err := chatsdk.DecodeInbound(makeBatch(chatsdk.MaxEvents+1, 0), "telegram", "telegram"); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("%d events = %v", chatsdk.MaxEvents+1, err)
	}
	if _, err := chatsdk.DecodeInbound(makeBatch(1, 300), "telegram", "telegram"); err != nil {
		t.Fatalf("300-byte dedupe: %v", err)
	}
	if _, err := chatsdk.DecodeInbound(makeBatch(1, 301), "telegram", "telegram"); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("301-byte dedupe = %v", err)
	}
	// Exactly the body cap is accepted; one byte more is refused.
	body := makeBatch(1, 0)
	body = append(body, []byte(strings.Repeat(" ", chatsdk.MaxBodyBytes-len(body)))...)
	if len(body) != chatsdk.MaxBodyBytes {
		t.Fatalf("body = %d", len(body))
	}
	if _, err := chatsdk.DecodeInbound(body, "telegram", "telegram"); err != nil {
		t.Fatalf("exact body cap: %v", err)
	}
	if _, err := chatsdk.DecodeInbound(append(body, ' '), "telegram", "telegram"); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("body cap + 1 = %v", err)
	}
	// Text at the cap is kept; one rune over is clamped, not refused.
	batch := chatsdk.InboundBatch{Version: "1", Events: []chatsdk.InboundEvent{{Dedupe: "text", Event: event("text")}}}
	batch.Events[0].Event.Text = strings.Repeat("x", boardsync.MaxTextBytes)
	encoded, _ := json.Marshal(batch)
	records, err := chatsdk.DecodeInbound(encoded, "telegram", "telegram")
	if err != nil || len(records[0].Event.Text) != boardsync.MaxTextBytes {
		t.Fatalf("text cap = %d, %v", len(records[0].Event.Text), err)
	}
	batch.Events[0].Event.Text += "x"
	encoded, _ = json.Marshal(batch)
	records, err = chatsdk.DecodeInbound(encoded, "telegram", "telegram")
	if err != nil || len(records[0].Event.Text) > boardsync.MaxTextBytes {
		t.Fatalf("text cap + 1 = %d, %v", len(records[0].Event.Text), err)
	}
}

func TestNewRequiresSelfIDs(t *testing.T) {
	if _, err := chatsdk.New(chatsdk.Config{Key: "telegram", Adapter: &chatsdk.FuncAdapter{PlatformName: "telegram"}}); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("no self IDs = %v", err)
	}
	client, err := chatsdk.NewClient(chatsdk.ClientConfig{
		BaseURL: "http://127.0.0.1:9", Secret: bridgeSecret, Key: "telegram",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := chatsdk.NewRemoteAdapter(client, "telegram", boardsync.Capabilities{}); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("remote without self IDs = %v", err)
	}
}
