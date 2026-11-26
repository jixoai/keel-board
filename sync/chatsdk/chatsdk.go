// Package chatsdk is the generic bridge for platforms that don't have a
// native adapter. It keeps the same contract as the built-in adapters: verify
// a webhook, normalize events, send a message. A platform plugs in an
// Adapter; this package does not load or wrap any JavaScript Chat SDK.
package chatsdk

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
)

// Adapter is a platform implementation the bridge delegates to.
type Adapter interface {
	// Platform names the platform in IDs and labels.
	Platform() string
	// Verify authenticates a webhook before anything is parsed.
	Verify(ctx context.Context, req *boardsync.WebhookRequest) error
	// Decode normalizes what a verified webhook brought.
	Decode(ctx context.Context, key boardsync.Key, req *boardsync.WebhookRequest) (DecodeResult, error)
	// Send posts one message.
	Send(ctx context.Context, key boardsync.Key, target boardsync.ResolvedTarget, msg boardsync.OutboundMessage) (boardsync.Posted, error)
}

// DecodeResult is what an adapter's webhook produced.
type DecodeResult struct {
	Records  []boardsync.Record
	Response boardsync.WebhookResponse
}

// Editor, Deleter, Reactor, ChannelResolver, TargetResolver, RootFetcher,
// Linker, FileFetcher, Permalinker are optional adapter capabilities. The
// bridge type-asserts for them.
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
	RootFetcher interface {
		FetchRoot(ctx context.Context, key boardsync.Key, thread string) (*boardsync.RootDraft, error)
	}
	Linker interface {
		Locate(url string) (thread string, message string, ok bool)
		URL(thread, message string) (string, error)
	}
	FileFetcher interface {
		FetchFile(ctx context.Context, key boardsync.Key, file boardsync.File) ([]byte, error)
	}
	Permalinker interface {
		Permalink(ctx context.Context, key boardsync.Key, event boardsync.Event) (string, error)
	}
)

// Config configures one bridge entry.
type Config struct {
	Key     boardsync.Key
	Adapter Adapter
	// Caps defaults to StartThreads false, edits/deletes/reactions true if the
	// adapter implements them.
	Caps boardsync.Capabilities
}

// Provider implements boardsync.Provider over an Adapter.
type Provider struct {
	cfg     Config
	adapter Adapter
	caps    boardsync.Capabilities
}

// New returns a bridge provider.
func New(cfg Config) (*Provider, error) {
	if cfg.Adapter == nil {
		return nil, fmt.Errorf("%w: chatsdk needs an adapter", boardsync.ErrBadRequest)
	}
	if cfg.Key != "" && !cfg.Key.Valid() {
		return nil, fmt.Errorf("%w: %q isn't a sync key", boardsync.ErrBadRequest, cfg.Key)
	}
	return &Provider{cfg: cfg, adapter: cfg.Adapter, caps: cfg.Caps}, nil
}

// Platform implements boardsync.Provider.
func (p *Provider) Platform() boardsync.Platform {
	return boardsync.Platform(p.adapter.Platform())
}

// Capabilities implements boardsync.Provider.
func (p *Provider) Capabilities() boardsync.Capabilities { return p.caps }

// Receive delegates verification and decoding to the adapter, then pins the
// entry key and platform on every record.
func (p *Provider) Receive(ctx context.Context, key boardsync.Key, req *boardsync.WebhookRequest) (boardsync.Intake, error) {
	if p.cfg.Key != "" && key != p.cfg.Key {
		return boardsync.Intake{}, fmt.Errorf("%w: entry %q isn't this adapter's %q", boardsync.ErrBadRequest, key, p.cfg.Key)
	}
	if req == nil {
		return boardsync.Intake{}, fmt.Errorf("%w: empty chatsdk webhook", boardsync.ErrBadRequest)
	}
	if err := p.adapter.Verify(ctx, req); err != nil {
		return boardsync.Intake{}, err
	}
	decoded, err := p.adapter.Decode(ctx, key, req)
	if err != nil {
		return boardsync.Intake{}, err
	}
	for i := range decoded.Records {
		if decoded.Records[i].Event.Key == "" {
			decoded.Records[i].Event.Key = key
		}
		if decoded.Records[i].Event.Platform == "" {
			decoded.Records[i].Event.Platform = p.Platform()
		}
		if decoded.Records[i].Event.ID == "" {
			decoded.Records[i].Event.ID = decoded.Records[i].Dedupe
		}
	}
	if decoded.Response.Status == 0 {
		decoded.Response = boardsync.OK()
	}
	return boardsync.Intake{Records: decoded.Records, Response: decoded.Response}, nil
}

// ResolveChannel delegates to the adapter; without a resolver a name is its
// own ID, as most Chat SDK adapters treat channel IDs.
func (p *Provider) ResolveChannel(ctx context.Context, key boardsync.Key, name string) (string, error) {
	if p.cfg.Key != "" && key != p.cfg.Key {
		return "", fmt.Errorf("%w: entry %q isn't this adapter's %q", boardsync.ErrBadRequest, key, p.cfg.Key)
	}
	if resolver, ok := p.adapter.(ChannelResolver); ok {
		return resolver.ResolveChannel(ctx, key, name)
	}
	return name, nil
}

// ResolveTarget delegates to the adapter; without a resolver a target can't
// be read safely, so it is refused rather than guessed.
func (p *Provider) ResolveTarget(ctx context.Context, key boardsync.Key, target string) (boardsync.ResolvedTarget, error) {
	if p.cfg.Key != "" && key != p.cfg.Key {
		return boardsync.ResolvedTarget{}, fmt.Errorf("%w: entry %q isn't this adapter's %q", boardsync.ErrBadRequest, key, p.cfg.Key)
	}
	resolver, ok := p.adapter.(TargetResolver)
	if !ok {
		return boardsync.ResolvedTarget{}, fmt.Errorf("%w: chatsdk adapter %s has no target resolver", boardsync.ErrUnsupported, p.Platform())
	}
	resolved, err := resolver.ResolveTarget(ctx, key, target)
	if err != nil {
		return boardsync.ResolvedTarget{}, err
	}
	if resolved.Key == "" {
		resolved.Key = key
	}
	if resolved.Platform == "" {
		resolved.Platform = p.Platform()
	}
	return resolved, resolved.Validate()
}

// Post delegates to the adapter.
func (p *Provider) Post(ctx context.Context, key boardsync.Key, target boardsync.ResolvedTarget, msg boardsync.OutboundMessage) (boardsync.Posted, error) {
	if p.cfg.Key != "" && key != p.cfg.Key {
		return boardsync.Posted{}, fmt.Errorf("%w: entry %q isn't this adapter's %q", boardsync.ErrBadRequest, key, p.cfg.Key)
	}
	if err := target.Validate(); err != nil {
		return boardsync.Posted{}, err
	}
	return p.adapter.Send(ctx, key, target, msg)
}

// Edit implements boardsync.Editor when the adapter does.
func (p *Provider) Edit(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, message string, msg boardsync.OutboundMessage) error {
	editor, ok := p.adapter.(Editor)
	if !ok {
		return fmt.Errorf("%w: %s can't edit", boardsync.ErrUnsupported, p.Platform())
	}
	return editor.Edit(ctx, key, thread, message, msg)
}

// Delete implements boardsync.Deleter when the adapter does.
func (p *Provider) Delete(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, message string) error {
	deleter, ok := p.adapter.(Deleter)
	if !ok {
		return fmt.Errorf("%w: %s can't delete", boardsync.ErrUnsupported, p.Platform())
	}
	return deleter.Delete(ctx, key, thread, message)
}

// React implements boardsync.Reactor when the adapter does.
func (p *Provider) React(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, message, emoji string, added bool) error {
	reactor, ok := p.adapter.(Reactor)
	if !ok {
		return fmt.Errorf("%w: %s can't react", boardsync.ErrUnsupported, p.Platform())
	}
	return reactor.React(ctx, key, thread, message, emoji, added)
}

// FetchRoot implements boardsync.RootFetcher when the adapter does.
func (p *Provider) FetchRoot(ctx context.Context, key boardsync.Key, thread string) (*boardsync.RootDraft, error) {
	fetcher, ok := p.adapter.(RootFetcher)
	if !ok {
		return nil, fmt.Errorf("%w: %s can't fetch a root", boardsync.ErrUnsupported, p.Platform())
	}
	return fetcher.FetchRoot(ctx, key, thread)
}

// FetchFile implements boardsync.FileFetcher when the adapter does.
func (p *Provider) FetchFile(ctx context.Context, key boardsync.Key, file boardsync.File) ([]byte, error) {
	fetcher, ok := p.adapter.(FileFetcher)
	if !ok {
		return nil, fmt.Errorf("%w: %s can't fetch files", boardsync.ErrUnsupported, p.Platform())
	}
	return fetcher.FetchFile(ctx, key, file)
}

// Locate implements boardsync.Linker when the adapter does.
func (p *Provider) Locate(url string) (string, string, bool) {
	linker, ok := p.adapter.(Linker)
	if !ok {
		return "", "", false
	}
	return linker.Locate(url)
}

// URL implements boardsync.Linker when the adapter does.
func (p *Provider) URL(thread, message string) (string, error) {
	linker, ok := p.adapter.(Linker)
	if !ok {
		return "", nil
	}
	return linker.URL(thread, message)
}

// Permalink implements boardsync.Permalinker when the adapter does.
func (p *Provider) Permalink(ctx context.Context, key boardsync.Key, event boardsync.Event) (string, error) {
	linker, ok := p.adapter.(Permalinker)
	if !ok {
		return "", nil
	}
	return linker.Permalink(ctx, key, event)
}

// HMACVerifier is a generic webhook verifier: HMAC-SHA256 of the raw body,
// sent in a header with an optional "sha256=" prefix, and an optional
// timestamp header checked against a tolerance.
type HMACVerifier struct {
	Secret          boardsync.Secret
	Header          string
	Prefix          string
	TimestampHeader string
	Tolerance       time.Duration
	Now             func() time.Time
}

// Verify implements verification for a generic webhook.
func (v HMACVerifier) Verify(_ context.Context, req *boardsync.WebhookRequest) error {
	if v.Secret.Empty() {
		return fmt.Errorf("%w: webhook secret is unset", boardsync.ErrUnauthorized)
	}
	if req == nil {
		return fmt.Errorf("%w: no webhook request", boardsync.ErrUnauthorized)
	}
	header := v.Header
	if header == "" {
		header = "X-Webhook-Signature"
	}
	signature := req.Header.Get(header)
	if signature == "" {
		return fmt.Errorf("%w: no %s header", boardsync.ErrUnauthorized, header)
	}
	prefix := v.Prefix
	if prefix == "" {
		prefix = "sha256="
	}
	if strings.HasPrefix(signature, prefix) {
		signature = strings.TrimPrefix(signature, prefix)
	}
	if v.TimestampHeader != "" {
		now := time.Now
		if v.Now != nil {
			now = v.Now
		}
		raw := req.Header.Get(v.TimestampHeader)
		seconds, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return fmt.Errorf("%w: %s isn't a unix timestamp", boardsync.ErrUnauthorized, v.TimestampHeader)
		}
		tolerance := v.Tolerance
		if tolerance <= 0 {
			tolerance = 5 * time.Minute
		}
		skew := now().Sub(time.Unix(seconds, 0))
		if skew < 0 {
			skew = -skew
		}
		if skew > tolerance {
			return fmt.Errorf("%w: webhook timestamp is outside the %s window", boardsync.ErrUnauthorized, tolerance)
		}
	}
	mac := hmac.New(sha256.New, []byte(v.Secret.Reveal()))
	mac.Write(req.Body)
	expected := hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(expected), []byte(signature)) {
		return fmt.Errorf("%w: webhook signature didn't verify", boardsync.ErrUnauthorized)
	}
	return nil
}

// FuncAdapter is an Adapter made of functions, for tests and small platforms.
type FuncAdapter struct {
	PlatformName string
	VerifyFunc   func(ctx context.Context, req *boardsync.WebhookRequest) error
	DecodeFunc   func(ctx context.Context, key boardsync.Key, req *boardsync.WebhookRequest) (DecodeResult, error)
	SendFunc     func(ctx context.Context, key boardsync.Key, target boardsync.ResolvedTarget, msg boardsync.OutboundMessage) (boardsync.Posted, error)
	EditFunc     func(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, message string, msg boardsync.OutboundMessage) error
	DeleteFunc   func(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, message string) error
	ReactFunc    func(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, message, emoji string, added bool) error
	ChannelFunc  func(ctx context.Context, key boardsync.Key, name string) (string, error)
	TargetFunc   func(ctx context.Context, key boardsync.Key, target string) (boardsync.ResolvedTarget, error)
	RootFunc     func(ctx context.Context, key boardsync.Key, thread string) (*boardsync.RootDraft, error)
	LocateFunc   func(url string) (string, string, bool)
	URLFunc      func(thread, message string) (string, error)
}

// Platform implements Adapter.
func (a *FuncAdapter) Platform() string { return a.PlatformName }

// Verify implements Adapter.
func (a *FuncAdapter) Verify(ctx context.Context, req *boardsync.WebhookRequest) error {
	if a.VerifyFunc == nil {
		return fmt.Errorf("%w: adapter has no verifier", boardsync.ErrUnauthorized)
	}
	return a.VerifyFunc(ctx, req)
}

// Decode implements Adapter.
func (a *FuncAdapter) Decode(ctx context.Context, key boardsync.Key, req *boardsync.WebhookRequest) (DecodeResult, error) {
	if a.DecodeFunc == nil {
		return DecodeResult{}, fmt.Errorf("%w: adapter has no decoder", boardsync.ErrBadRequest)
	}
	return a.DecodeFunc(ctx, key, req)
}

// Send implements Adapter.
func (a *FuncAdapter) Send(ctx context.Context, key boardsync.Key, target boardsync.ResolvedTarget, msg boardsync.OutboundMessage) (boardsync.Posted, error) {
	if a.SendFunc == nil {
		return boardsync.Posted{}, fmt.Errorf("%w: adapter has no sender", boardsync.ErrUnsupported)
	}
	return a.SendFunc(ctx, key, target, msg)
}

// Edit implements Editor when EditFunc is set.
func (a *FuncAdapter) Edit(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, message string, msg boardsync.OutboundMessage) error {
	if a.EditFunc == nil {
		return fmt.Errorf("%w: adapter can't edit", boardsync.ErrUnsupported)
	}
	return a.EditFunc(ctx, key, thread, message, msg)
}

// Delete implements Deleter when DeleteFunc is set.
func (a *FuncAdapter) Delete(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, message string) error {
	if a.DeleteFunc == nil {
		return fmt.Errorf("%w: adapter can't delete", boardsync.ErrUnsupported)
	}
	return a.DeleteFunc(ctx, key, thread, message)
}

// React implements Reactor when ReactFunc is set.
func (a *FuncAdapter) React(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, message, emoji string, added bool) error {
	if a.ReactFunc == nil {
		return fmt.Errorf("%w: adapter can't react", boardsync.ErrUnsupported)
	}
	return a.ReactFunc(ctx, key, thread, message, emoji, added)
}

// ResolveChannel implements ChannelResolver when ChannelFunc is set.
func (a *FuncAdapter) ResolveChannel(ctx context.Context, key boardsync.Key, name string) (string, error) {
	if a.ChannelFunc == nil {
		return name, nil
	}
	return a.ChannelFunc(ctx, key, name)
}

// ResolveTarget implements TargetResolver when TargetFunc is set. Without it,
// a target is unreadable.
func (a *FuncAdapter) ResolveTarget(ctx context.Context, key boardsync.Key, target string) (boardsync.ResolvedTarget, error) {
	if a.TargetFunc == nil {
		return boardsync.ResolvedTarget{}, fmt.Errorf("%w: adapter has no target resolver", boardsync.ErrUnsupported)
	}
	return a.TargetFunc(ctx, key, target)
}

// FetchRoot implements RootFetcher when RootFunc is set.
func (a *FuncAdapter) FetchRoot(ctx context.Context, key boardsync.Key, thread string) (*boardsync.RootDraft, error) {
	if a.RootFunc == nil {
		return nil, fmt.Errorf("%w: adapter can't fetch a root", boardsync.ErrUnsupported)
	}
	return a.RootFunc(ctx, key, thread)
}

// Locate implements Linker when LocateFunc is set.
func (a *FuncAdapter) Locate(url string) (string, string, bool) {
	if a.LocateFunc == nil {
		return "", "", false
	}
	return a.LocateFunc(url)
}

// URL implements Linker when URLFunc is set.
func (a *FuncAdapter) URL(thread, message string) (string, error) {
	if a.URLFunc == nil {
		return "", nil
	}
	return a.URLFunc(thread, message)
}

// Interface checks.
var (
	_ boardsync.Provider    = (*Provider)(nil)
	_ boardsync.Editor      = (*Provider)(nil)
	_ boardsync.Deleter     = (*Provider)(nil)
	_ boardsync.Reactor     = (*Provider)(nil)
	_ boardsync.RootFetcher = (*Provider)(nil)
	_ boardsync.Linker      = (*Provider)(nil)
	_ boardsync.FileFetcher = (*Provider)(nil)
	_ boardsync.Permalinker = (*Provider)(nil)
)
