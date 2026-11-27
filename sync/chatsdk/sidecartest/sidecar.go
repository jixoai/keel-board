// Package sidecartest is a reference Chat SDK sidecar for tests. It speaks
// the bridge v1 HTTP contract, delegates to a small adapter interface, and
// records what the board sent. It is test-only: production code must never
// import it, and the production server never wires it in.
package sidecartest

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/chatsdk"
)

// Adapter is the minimum a fake Chat SDK adapter implements. Optional
// interfaces below model the operations a platform may support.
type Adapter interface {
	Platform() string
	Send(ctx context.Context, key boardsync.Key, target boardsync.ResolvedTarget, msg boardsync.OutboundMessage) (boardsync.Posted, error)
}

// Editor, Deleter, Reactor, ChannelResolver, and TargetResolver are the
// optional shapes a fake adapter may implement. An adapter that doesn't is
// answered with `unsupported`, exactly as the real bridge contract says.
type (
	Editor interface {
		Edit(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, message string, msg boardsync.OutboundMessage) error
	}
	Deleter interface {
		Delete(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, message string) error
	}
	Reactor interface {
		React(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, message, emoji string, added bool) error
	}
	ChannelResolver interface {
		ResolveChannel(ctx context.Context, key boardsync.Key, name string) (string, error)
	}
	TargetResolver interface {
		ResolveTarget(ctx context.Context, key boardsync.Key, target string) (boardsync.ResolvedTarget, error)
	}
)

// Sidecar is an httptest server implementing the sidecar endpoints.
type Sidecar struct {
	Key      boardsync.Key
	Platform boardsync.Platform
	Secret   boardsync.Secret
	Adapter  Adapter
	Server   *httptest.Server

	mu         sync.Mutex
	delivered  []chatsdk.DeliverRequest
	resolved   []chatsdk.ResolveRequest
	byDelivery map[string]boardsync.Posted
	now        func() time.Time
}

// New starts a sidecar for one entry. Close it when the test ends.
func New(key boardsync.Key, platform boardsync.Platform, secret boardsync.Secret, adapter Adapter) (*Sidecar, error) {
	if !key.Valid() {
		return nil, fmt.Errorf("sidecartest: %q isn't a sync key", key)
	}
	if strings.TrimSpace(string(platform)) == "" {
		return nil, errors.New("sidecartest: a sidecar needs a platform")
	}
	if secret.Empty() {
		return nil, errors.New("sidecartest: a sidecar needs a secret")
	}
	if adapter == nil {
		return nil, errors.New("sidecartest: a sidecar needs an adapter")
	}
	sidecar := &Sidecar{
		Key: key, Platform: platform, Secret: secret, Adapter: adapter,
		byDelivery: make(map[string]boardsync.Posted),
		now:        time.Now,
	}
	sidecar.Server = httptest.NewServer(http.HandlerFunc(sidecar.handle))
	return sidecar, nil
}

// URL is the configured base URL a board client uses.
func (s *Sidecar) URL() string { return s.Server.URL }

// Close stops the fixture.
func (s *Sidecar) Close() { s.Server.Close() }

// Deliveries returns the delivery requests received so far.
func (s *Sidecar) Deliveries() []chatsdk.DeliverRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]chatsdk.DeliverRequest(nil), s.delivered...)
}

// Resolves returns the resolve requests received so far.
func (s *Sidecar) Resolves() []chatsdk.ResolveRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]chatsdk.ResolveRequest(nil), s.resolved...)
}

// PushResult is what the board answered to a normalized event push.
type PushResult struct {
	Status int
	Body   []byte
}

// PushInbound signs and posts a normalized batch to a board's bridge route.
// boardURL is the test server's origin, never a value taken from an event.
func (s *Sidecar) PushInbound(ctx context.Context, boardURL string, events []chatsdk.InboundEvent) (PushResult, error) {
	batch := chatsdk.InboundBatch{Version: chatsdk.BridgeVersion, Events: events}
	body, err := json.Marshal(batch)
	if err != nil {
		return PushResult{}, err
	}
	return s.PushRaw(ctx, boardURL, body, time.Now().Unix(), newNonce())
}

// PushRaw signs and posts an exact body, for bad-signature and replay tests.
func (s *Sidecar) PushRaw(ctx context.Context, boardURL string, body []byte, timestamp int64, nonce string) (PushResult, error) {
	endpoint := strings.TrimRight(boardURL, "/") + "/v1/board/sync/bridge/" + string(s.Key) + "/events"
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return PushResult{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(chatsdk.HeaderTimestamp, fmt.Sprintf("%d", timestamp))
	request.Header.Set(chatsdk.HeaderNonce, nonce)
	request.Header.Set(chatsdk.HeaderSignature, chatsdk.SignV1(s.Secret, timestamp, nonce, body))
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return PushResult{}, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, chatsdk.MaxResponseBytes))
	if err != nil {
		return PushResult{}, err
	}
	return PushResult{Status: response.StatusCode, Body: data}, nil
}

func (s *Sidecar) handle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, chatsdk.MaxBodyBytes+1))
	if err != nil || len(body) > chatsdk.MaxBodyBytes {
		http.Error(w, "too large", http.StatusRequestEntityTooLarge)
		return
	}
	if err := chatsdk.VerifyRequestV1(s.Secret, r.Header, body, s.now(), chatsdk.DefaultWindow); err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	switch r.URL.Path {
	case "/deliver":
		s.deliver(w, body)
	case "/resolve":
		s.resolve(w, body)
	default:
		http.NotFound(w, r)
	}
}

func (s *Sidecar) deliver(w http.ResponseWriter, body []byte) {
	var request chatsdk.DeliverRequest
	if err := json.Unmarshal(body, &request); err != nil {
		s.writeError(w, boardsync.ErrBadRequest)
		return
	}
	if request.Key != s.Key || request.Target.Platform != s.Platform {
		s.writeError(w, boardsync.ErrBadRequest)
		return
	}
	if err := request.Validate(); err != nil {
		s.writeError(w, err)
		return
	}
	s.mu.Lock()
	s.delivered = append(s.delivered, request)
	if posted, ok := s.byDelivery[request.DeliveryID]; ok {
		s.mu.Unlock()
		// A retry is answered with the first result, not posted twice.
		writeJSON(w, http.StatusOK, posted)
		return
	}
	s.mu.Unlock()
	var (
		posted boardsync.Posted
		err    error
	)
	switch request.Kind {
	case chatsdk.DeliverMessage:
		posted, err = s.Adapter.Send(context.Background(), s.Key, request.Target, boardsync.OutboundMessage{
			Markdown: request.Body.Markdown, Sender: request.Body.Sender, External: request.Body.External,
			BoardMessage: request.Body.BoardMessage, BoardThread: request.Body.BoardThread,
		})
	case chatsdk.DeliverEdit:
		editor, ok := s.Adapter.(Editor)
		if !ok {
			err = fmt.Errorf("%w: adapter has no editor", boardsync.ErrUnsupported)
			break
		}
		err = editor.Edit(context.Background(), s.Key, request.Target.Ref(), request.Message, boardsync.OutboundMessage{
			Markdown: request.Body.Markdown, Sender: request.Body.Sender, External: request.Body.External,
			BoardMessage: request.Body.BoardMessage, BoardThread: request.Body.BoardThread,
		})
		posted = boardsync.Posted{Thread: request.Target.Thread, Message: request.Message}
	case chatsdk.DeliverDelete:
		deleter, ok := s.Adapter.(Deleter)
		if !ok {
			err = fmt.Errorf("%w: adapter has no deleter", boardsync.ErrUnsupported)
			break
		}
		err = deleter.Delete(context.Background(), s.Key, request.Target.Ref(), request.Message)
		posted = boardsync.Posted{Thread: request.Target.Thread, Message: request.Message}
	case chatsdk.DeliverReaction:
		reactor, ok := s.Adapter.(Reactor)
		if !ok {
			err = fmt.Errorf("%w: adapter has no reactor", boardsync.ErrUnsupported)
			break
		}
		err = reactor.React(context.Background(), s.Key, request.Target.Ref(), request.Message, request.Emoji, request.Added)
		posted = boardsync.Posted{Thread: request.Target.Thread, Message: request.Message}
	default:
		err = fmt.Errorf("%w: unknown delivery kind", boardsync.ErrBadRequest)
	}
	if err != nil {
		s.writeError(w, err)
		return
	}
	if err := posted.Validate(); err != nil {
		s.writeError(w, fmt.Errorf("%w: adapter answered %v", boardsync.ErrUnsupported, err))
		return
	}
	s.mu.Lock()
	s.byDelivery[request.DeliveryID] = posted
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, posted)
}

func (s *Sidecar) resolve(w http.ResponseWriter, body []byte) {
	var request chatsdk.ResolveRequest
	if err := json.Unmarshal(body, &request); err != nil || request.Validate() != nil {
		s.writeError(w, boardsync.ErrBadRequest)
		return
	}
	s.mu.Lock()
	s.resolved = append(s.resolved, request)
	s.mu.Unlock()
	if request.Name != "" {
		resolver, ok := s.Adapter.(ChannelResolver)
		if !ok {
			s.writeError(w, fmt.Errorf("%w: adapter has no channel resolver", boardsync.ErrUnsupported))
			return
		}
		channel, err := resolver.ResolveChannel(context.Background(), s.Key, request.Name)
		if err != nil {
			s.writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, chatsdk.ResolveResponse{Channel: channel})
		return
	}
	resolver, ok := s.Adapter.(TargetResolver)
	if !ok {
		s.writeError(w, fmt.Errorf("%w: adapter has no target resolver", boardsync.ErrUnsupported))
		return
	}
	target, err := resolver.ResolveTarget(context.Background(), s.Key, request.Target)
	if err != nil {
		s.writeError(w, err)
		return
	}
	if target.Key == "" {
		target.Key = s.Key
	}
	if target.Platform == "" {
		target.Platform = s.Platform
	}
	writeJSON(w, http.StatusOK, chatsdk.ResolveResponse{Target: &target})
}

func (s *Sidecar) writeError(w http.ResponseWriter, err error) {
	var code chatsdk.ErrorCode
	switch {
	case errors.Is(err, boardsync.ErrUnauthorized):
		code = chatsdk.ErrorAuth
	case errors.Is(err, boardsync.ErrUnsupported):
		code = chatsdk.ErrorUnsupported
	case errors.Is(err, boardsync.ErrRateLimited):
		code = chatsdk.ErrorRateLimited
	case boardsync.IsPermanent(err):
		code = chatsdk.ErrorPermanent
	default:
		code = chatsdk.ErrorTransient
	}
	writeJSON(w, code.HTTPStatus(), chatsdk.BridgeError{Code: code})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func newNonce() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return fmt.Sprintf("fixture-%d", time.Now().UnixNano())
	}
	return base64.RawURLEncoding.EncodeToString(raw[:])
}
