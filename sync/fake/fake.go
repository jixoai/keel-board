// Package fake provides an in-memory Provider and Host for tests. It speaks
// no platform and performs no I/O: every call is recorded and every answer is
// programmable.
package fake

import (
	"context"
	"errors"
	"sync"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
)

// Provider is a programmable sync.Provider. It implements the optional
// interfaces too, so a test can exercise edits, deletes, reactions, links, and
// roots; return an error from one to simulate a platform that can't.
type Provider struct {
	PlatformName string
	Caps         boardsync.Capabilities
	// SelfIDsValue is what SelfIDs returns; empty means no identity.
	SelfIDsValue []string

	ReceiveFunc        func(ctx context.Context, key boardsync.Key, req *boardsync.WebhookRequest) (boardsync.Intake, error)
	ResolveChannelFunc func(ctx context.Context, key boardsync.Key, name string) (string, error)
	ResolveTargetFunc  func(ctx context.Context, key boardsync.Key, target string) (boardsync.ResolvedTarget, error)
	PostFunc           func(ctx context.Context, key boardsync.Key, target boardsync.ResolvedTarget, msg boardsync.OutboundMessage) (boardsync.Posted, error)
	EditFunc           func(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, message string, msg boardsync.OutboundMessage) error
	DeleteFunc         func(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, message string) error
	ReactFunc          func(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, message, emoji string, added bool) error
	PermalinkFunc      func(ctx context.Context, key boardsync.Key, event boardsync.Event) (string, error)
	FetchRootFunc      func(ctx context.Context, key boardsync.Key, thread string) (*boardsync.RootDraft, error)
	FetchFileFunc      func(ctx context.Context, key boardsync.Key, file boardsync.File) ([]byte, error)
	UploadFilesFunc    func(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, files []boardsync.OutboundFile) ([]string, error)
	TypingFunc         func(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, latest, status string) error
	LabelsFunc         func(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, labels, before map[string]string) error
	LocateFunc         func(url string) (thread string, message string, ok bool)
	URLFunc            func(thread, message string) (string, error)
	ReadSourceFunc     func(ctx context.Context, key boardsync.Key, target string, opts boardsync.ReadOptions) (boardsync.SourceRead, error)
	JoinSourceFunc     func(ctx context.Context, key boardsync.Key, target string) (boardsync.SourceJoin, error)

	mu          sync.Mutex
	receives    []ReceiveCall
	resolves    []ResolveCall
	targets     []TargetCall
	posts       []PostCall
	edits       []EditCall
	deletes     []DeleteCall
	reactions   []ReactCall
	uploads     []UploadCall
	typings     []TypingCall
	labels      []LabelsCall
	permalink   []PermalinkCall
	starts      []StartCall
	sourceReads []SourceReadCall
	sourceJoins []SourceJoinCall
}

// ReceiveCall records one webhook.
type ReceiveCall struct {
	Key boardsync.Key
	Req *boardsync.WebhookRequest
}

// ResolveCall records one channel lookup.
type ResolveCall struct {
	Key  boardsync.Key
	Name string
}

// TargetCall records one target resolution.
type TargetCall struct {
	Key    boardsync.Key
	Target string
}

// PostCall records one post.
type PostCall struct {
	Key    boardsync.Key
	Target boardsync.ResolvedTarget
	Msg    boardsync.OutboundMessage
}

// EditCall records one edit.
type EditCall struct {
	Key     boardsync.Key
	Thread  boardsync.ThreadRef
	Message string
	Msg     boardsync.OutboundMessage
}

// DeleteCall records one delete.
type DeleteCall struct {
	Key     boardsync.Key
	Thread  boardsync.ThreadRef
	Message string
}

// ReactCall records one reaction.
type ReactCall struct {
	Key     boardsync.Key
	Thread  boardsync.ThreadRef
	Message string
	Emoji   string
	Added   bool
}

// LabelsCall records one labels-only change.
type LabelsCall struct {
	Key    boardsync.Key
	Thread boardsync.ThreadRef
	Labels map[string]string
	Before map[string]string
}

// UploadCall records one file upload.
type UploadCall struct {
	Key    boardsync.Key
	Thread boardsync.ThreadRef
	Files  []boardsync.OutboundFile
	IDs    []string
}

// TypingCall records one typing status.
type TypingCall struct {
	Key    boardsync.Key
	Thread boardsync.ThreadRef
	Latest string
	Status string
}

// PermalinkCall records one permalink lookup.
type PermalinkCall struct {
	Key   boardsync.Key
	Event boardsync.Event
}

// StartCall is kept for symmetry with a future typing fake.
type StartCall struct {
	Key     boardsync.Key
	Channel string
}

// SourceReadCall records one on-demand source read.
type SourceReadCall struct {
	Key    boardsync.Key
	Target string
	Opts   boardsync.ReadOptions
}

// SourceJoinCall records one opt-in join.
type SourceJoinCall struct {
	Key    boardsync.Key
	Target string
}

// New returns a fake provider for platform with the given capabilities.
func New(platform string, caps boardsync.Capabilities) *Provider {
	return &Provider{PlatformName: platform, Caps: caps}
}

// Platform implements boardsync.Provider.
func (p *Provider) Platform() boardsync.Platform { return boardsync.Platform(p.PlatformName) }

// Capabilities implements boardsync.Provider.
func (p *Provider) Capabilities() boardsync.Capabilities { return p.Caps }

// SelfIDs implements boardsync.SelfIdentifier.
func (p *Provider) SelfIDs() []string { return append([]string(nil), p.SelfIDsValue...) }

// Receive implements boardsync.Provider.
func (p *Provider) Receive(ctx context.Context, key boardsync.Key, req *boardsync.WebhookRequest) (boardsync.Intake, error) {
	p.mu.Lock()
	p.receives = append(p.receives, ReceiveCall{Key: key, Req: req})
	p.mu.Unlock()
	if p.ReceiveFunc == nil {
		return boardsync.Intake{Response: boardsync.OK()}, nil
	}
	return p.ReceiveFunc(ctx, key, req)
}

// ResolveChannel implements boardsync.Provider.
func (p *Provider) ResolveChannel(ctx context.Context, key boardsync.Key, name string) (string, error) {
	p.mu.Lock()
	p.resolves = append(p.resolves, ResolveCall{Key: key, Name: name})
	p.mu.Unlock()
	if p.ResolveChannelFunc != nil {
		return p.ResolveChannelFunc(ctx, key, name)
	}
	return name, nil
}

// ResolveTarget implements boardsync.Provider.
func (p *Provider) ResolveTarget(ctx context.Context, key boardsync.Key, target string) (boardsync.ResolvedTarget, error) {
	p.mu.Lock()
	p.targets = append(p.targets, TargetCall{Key: key, Target: target})
	p.mu.Unlock()
	if p.ResolveTargetFunc != nil {
		return p.ResolveTargetFunc(ctx, key, target)
	}
	return boardsync.ResolvedTarget{
		Key: key, Platform: p.Platform(), Channel: target, Thread: target, Kind: boardsync.TargetThread,
	}, nil
}

// Post implements boardsync.Provider.
func (p *Provider) Post(ctx context.Context, key boardsync.Key, target boardsync.ResolvedTarget, msg boardsync.OutboundMessage) (boardsync.Posted, error) {
	p.mu.Lock()
	p.posts = append(p.posts, PostCall{Key: key, Target: target, Msg: msg})
	p.mu.Unlock()
	if p.PostFunc != nil {
		return p.PostFunc(ctx, key, target, msg)
	}
	thread := target.Thread
	if target.Kind == boardsync.TargetChannel {
		thread = target.Channel + ":1"
	}
	return boardsync.Posted{Thread: thread, Message: "m1"}, nil
}

// Edit implements boardsync.Editor.
func (p *Provider) Edit(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, message string, msg boardsync.OutboundMessage) error {
	p.mu.Lock()
	p.edits = append(p.edits, EditCall{Key: key, Thread: thread, Message: message, Msg: msg})
	p.mu.Unlock()
	if p.EditFunc != nil {
		return p.EditFunc(ctx, key, thread, message, msg)
	}
	return nil
}

// Delete implements boardsync.Deleter.
func (p *Provider) Delete(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, message string) error {
	p.mu.Lock()
	p.deletes = append(p.deletes, DeleteCall{Key: key, Thread: thread, Message: message})
	p.mu.Unlock()
	if p.DeleteFunc != nil {
		return p.DeleteFunc(ctx, key, thread, message)
	}
	return nil
}

// React implements boardsync.Reactor.
func (p *Provider) React(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, message, emoji string, added bool) error {
	p.mu.Lock()
	p.reactions = append(p.reactions, ReactCall{Key: key, Thread: thread, Message: message, Emoji: emoji, Added: added})
	p.mu.Unlock()
	if p.ReactFunc != nil {
		return p.ReactFunc(ctx, key, thread, message, emoji, added)
	}
	return nil
}

// Labels implements boardsync.Labeler.
func (p *Provider) Labels(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, labels, before map[string]string) error {
	p.mu.Lock()
	p.labels = append(p.labels, LabelsCall{Key: key, Thread: thread, Labels: labels, Before: before})
	p.mu.Unlock()
	if p.LabelsFunc != nil {
		return p.LabelsFunc(ctx, key, thread, labels, before)
	}
	return nil
}

// Permalink implements boardsync.Permalinker.
func (p *Provider) Permalink(ctx context.Context, key boardsync.Key, event boardsync.Event) (string, error) {
	p.mu.Lock()
	p.permalink = append(p.permalink, PermalinkCall{Key: key, Event: event})
	p.mu.Unlock()
	if p.PermalinkFunc != nil {
		return p.PermalinkFunc(ctx, key, event)
	}
	return "https://example.test/" + string(key) + "/" + event.Message, nil
}

// FetchRoot implements boardsync.RootFetcher.
func (p *Provider) FetchRoot(ctx context.Context, key boardsync.Key, thread string) (*boardsync.RootDraft, error) {
	if p.FetchRootFunc != nil {
		return p.FetchRootFunc(ctx, key, thread)
	}
	return nil, boardsync.ErrNotFound
}

// FetchFile implements boardsync.FileFetcher. Without a function it behaves
// like an adapter that can't fetch: ErrUnsupported, so an import keeps the
// file's metadata.
func (p *Provider) FetchFile(ctx context.Context, key boardsync.Key, file boardsync.File) ([]byte, error) {
	if p.FetchFileFunc != nil {
		return p.FetchFileFunc(ctx, key, file)
	}
	return nil, boardsync.ErrUnsupported
}

// UploadFiles implements boardsync.FileUploader.
func (p *Provider) UploadFiles(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, files []boardsync.OutboundFile) ([]string, error) {
	p.mu.Lock()
	call := UploadCall{Key: key, Thread: thread, Files: append([]boardsync.OutboundFile(nil), files...)}
	p.uploads = append(p.uploads, call)
	p.mu.Unlock()
	if p.UploadFilesFunc != nil {
		return p.UploadFilesFunc(ctx, key, thread, files)
	}
	return []string{}, nil
}

// Typing implements boardsync.Typist.
func (p *Provider) Typing(ctx context.Context, key boardsync.Key, thread boardsync.ThreadRef, latest, status string) error {
	p.mu.Lock()
	p.typings = append(p.typings, TypingCall{Key: key, Thread: thread, Latest: latest, Status: status})
	p.mu.Unlock()
	if p.TypingFunc != nil {
		return p.TypingFunc(ctx, key, thread, latest, status)
	}
	return nil
}

// Locate implements boardsync.Linker.
func (p *Provider) Locate(url string) (string, string, bool) {
	if p.LocateFunc != nil {
		return p.LocateFunc(url)
	}
	return "", "", false
}

// URL implements boardsync.Linker.
func (p *Provider) URL(thread, message string) (string, error) {
	if p.URLFunc != nil {
		return p.URLFunc(thread, message)
	}
	return "", nil
}

// ReadSource implements boardsync.SourceReader. Without a function it behaves
// like an adapter that can't read sources: ErrUnsupported.
func (p *Provider) ReadSource(ctx context.Context, key boardsync.Key, target string, opts boardsync.ReadOptions) (boardsync.SourceRead, error) {
	p.mu.Lock()
	p.sourceReads = append(p.sourceReads, SourceReadCall{Key: key, Target: target, Opts: opts})
	p.mu.Unlock()
	if p.ReadSourceFunc == nil {
		return boardsync.SourceRead{}, boardsync.ErrUnsupported
	}
	return p.ReadSourceFunc(ctx, key, target, opts)
}

// JoinSource implements boardsync.SourceJoiner. Without a function it behaves
// like an adapter that can't join: ErrUnsupported.
func (p *Provider) JoinSource(ctx context.Context, key boardsync.Key, target string) (boardsync.SourceJoin, error) {
	p.mu.Lock()
	p.sourceJoins = append(p.sourceJoins, SourceJoinCall{Key: key, Target: target})
	p.mu.Unlock()
	if p.JoinSourceFunc == nil {
		return boardsync.SourceJoin{}, boardsync.ErrUnsupported
	}
	return p.JoinSourceFunc(ctx, key, target)
}

// SourceReads returns recorded source reads.
func (p *Provider) SourceReads() []SourceReadCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]SourceReadCall(nil), p.sourceReads...)
}

// SourceJoins returns recorded joins.
func (p *Provider) SourceJoins() []SourceJoinCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]SourceJoinCall(nil), p.sourceJoins...)
}

// Receives returns recorded webhooks.
func (p *Provider) Receives() []ReceiveCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]ReceiveCall(nil), p.receives...)
}

// Resolves returns recorded channel lookups.
func (p *Provider) Resolves() []ResolveCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]ResolveCall(nil), p.resolves...)
}

// Targets returns recorded target resolutions.
func (p *Provider) Targets() []TargetCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]TargetCall(nil), p.targets...)
}

// Posts returns recorded posts.
func (p *Provider) Posts() []PostCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]PostCall(nil), p.posts...)
}

// Edits returns recorded edits.
func (p *Provider) Edits() []EditCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]EditCall(nil), p.edits...)
}

// Deletes returns recorded deletes.
func (p *Provider) Deletes() []DeleteCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]DeleteCall(nil), p.deletes...)
}

// Reactions returns recorded reactions.
func (p *Provider) Reactions() []ReactCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]ReactCall(nil), p.reactions...)
}

// Labels returns recorded labels-only changes.
func (p *Provider) LabelsCalls() []LabelsCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]LabelsCall(nil), p.labels...)
}

// Uploads returns recorded file uploads.
func (p *Provider) Uploads() []UploadCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]UploadCall(nil), p.uploads...)
}

// Typings returns recorded typing statuses.
func (p *Provider) Typings() []TypingCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]TypingCall(nil), p.typings...)
}

// Permalinks returns recorded permalink lookups.
func (p *Provider) Permalinks() []PermalinkCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]PermalinkCall(nil), p.permalink...)
}

// Host is a programmable boardsync.Host. Imports are idempotent by EventID:
// the second import of the same event returns the first result.
type Host struct {
	mu         sync.Mutex
	imports    []boardsync.ImportRequest
	records    []boardsync.BoardEvent
	byEvent    map[string]boardsync.ImportResult
	nextID     uint64
	ImportFunc func(ctx context.Context, req boardsync.ImportRequest) (boardsync.ImportResult, error)
	RecordFunc func(ctx context.Context, event boardsync.BoardEvent) error
}

// NewHost returns an empty fake host.
func NewHost() *Host {
	return &Host{byEvent: make(map[string]boardsync.ImportResult), nextID: 800}
}

// Import implements boardsync.Host.
func (h *Host) Import(ctx context.Context, req boardsync.ImportRequest) (boardsync.ImportResult, error) {
	h.mu.Lock()
	h.imports = append(h.imports, req)
	if result, ok := h.byEvent[req.EventID]; ok {
		h.mu.Unlock()
		return result, nil
	}
	h.mu.Unlock()
	if h.ImportFunc != nil {
		result, err := h.ImportFunc(ctx, req)
		if err == nil {
			h.mu.Lock()
			h.byEvent[req.EventID] = result
			h.mu.Unlock()
		}
		return result, err
	}
	h.mu.Lock()
	h.nextID++
	result := boardsync.ImportResult{Root: h.nextID, Message: h.nextID}
	if req.Channel != "" {
		result.CreatedRoot = true
	}
	h.byEvent[req.EventID] = result
	h.mu.Unlock()
	return result, nil
}

// Record implements boardsync.Host.
func (h *Host) Record(ctx context.Context, event boardsync.BoardEvent) error {
	h.mu.Lock()
	h.records = append(h.records, event)
	h.mu.Unlock()
	if h.RecordFunc != nil {
		return h.RecordFunc(ctx, event)
	}
	return nil
}

// Imports returns recorded imports, including retries.
func (h *Host) Imports() []boardsync.ImportRequest {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]boardsync.ImportRequest(nil), h.imports...)
}

// Records returns recorded board events.
func (h *Host) Records() []boardsync.BoardEvent {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]boardsync.BoardEvent(nil), h.records...)
}

// Gate is a static test Gate: channels in Mapped are mapped, threads in Synced
// are synced. It deliberately ignores tenant and key; production code must
// build a gate per (tenant, key) instead of sharing one.
type Gate struct {
	Mapped map[string]bool
	Synced map[string]bool
	Err    error
}

// NewGate returns a gate over the given channel and thread IDs.
func NewGate(mapped, synced []string) *Gate {
	gate := &Gate{Mapped: make(map[string]bool), Synced: make(map[string]bool)}
	for _, channel := range mapped {
		gate.Mapped[channel] = true
	}
	for _, thread := range synced {
		gate.Synced[thread] = true
	}
	return gate
}

// Routes implements boardsync.Gate.
func (g *Gate) Routes(_ context.Context, _ string, _ boardsync.Key, channel string) (bool, error) {
	if g.Err != nil {
		return false, g.Err
	}
	return g.Mapped[channel], nil
}

// ThreadSynced implements boardsync.Gate.
func (g *Gate) ThreadSynced(_ context.Context, _ string, _ boardsync.Key, thread string) (bool, error) {
	if g.Err != nil {
		return false, g.Err
	}
	return g.Synced[thread], nil
}

// ErrReceive is a convenient error for a ReceiveFunc.
var ErrReceive = errors.New("fake: receive failed")

// Now is a fixed clock for tests that need one.
func Now() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) }

// Interface checks.
var (
	_ boardsync.Provider     = (*Provider)(nil)
	_ boardsync.Editor       = (*Provider)(nil)
	_ boardsync.Deleter      = (*Provider)(nil)
	_ boardsync.Reactor      = (*Provider)(nil)
	_ boardsync.Permalinker  = (*Provider)(nil)
	_ boardsync.RootFetcher  = (*Provider)(nil)
	_ boardsync.Linker       = (*Provider)(nil)
	_ boardsync.FileFetcher  = (*Provider)(nil)
	_ boardsync.FileUploader = (*Provider)(nil)
	_ boardsync.Typist       = (*Provider)(nil)
)
