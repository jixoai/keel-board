package sync

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

// SyncFunc is what one sync entry does with every event, in both directions.
// It calls Import for an inbound event, or Deliver and Start for an outbound
// one; a function that calls nothing does nothing. A returned error is
// retried (inbound on the thread's lane, outbound by the outbox).
type SyncFunc func(ctx context.Context, call *Call) error

// Call is one event offered to a SyncFunc. Import, Deliver, and Start run at
// most once per call; a call used after the function returns answers
// ErrCallFinished.
type Call struct {
	Tenant    string
	Key       Key
	Direction Direction
	// Event is the normalized platform event for an inbound call.
	Event *Event
	// Board is the board event for an outbound call.
	Board *BoardEvent
	// Message is the board message an outbound event is about, as it is now.
	Message *Message
	// Target is where an outbound message goes. nil means a new root that has
	// no platform target yet; only Start can give it one.
	Target *ResolvedTarget
	// Channel is the platform channel a channel-mapping helper offers a new
	// root. Empty when no helper supplied one.
	Channel string
	// Renderer, when set by a Render wrapper, replaces the default rendering.
	Renderer Renderer
	// Copy is the native platform message ID for an edit, delete, or
	// reaction, when the call has one.
	Copy string

	ops   callOps
	state *callState
}

// callState is a Call's mutable part. Call is copied when a helper adds a
// renderer or offers a root to another channel, so the lock-bearing state is
// a pointer both copies share.
type callState struct {
	closed atomic.Bool

	importOnce sync.Once
	importRes  ImportResult
	importErr  error

	deliverOnce sync.Once
	deliverRes  Receipt
	deliverErr  error

	startOnce sync.Once
	startRes  ThreadRef
	startErr  error

	permalinkOnce sync.Once
	permalink     string
	permalinkErr  error
}

// newCall builds a Call with its state.
func newCall(call *Call) *Call {
	call.state = &callState{}
	return call
}

var errUninitialized = errors.New("sync: call was not made by a Runner")

type callOps interface {
	Import(ctx context.Context, req ImportRequest) (ImportResult, error)
	Deliver(ctx context.Context, req DeliverRequest) (Receipt, error)
	Start(ctx context.Context, req StartRequest) (ThreadRef, error)
	ResolveChannel(ctx context.Context, key Key, name string) (string, error)
	Permalink(ctx context.Context, key Key, event Event) (string, error)
}

// DeliverRequest is what Deliver hands to the board side.
type DeliverRequest struct {
	Tenant   string
	Key      Key
	Event    BoardEvent
	Message  Message
	Target   ResolvedTarget
	Copy     string
	Renderer Renderer
}

// StartRequest is what Start hands to the board side.
type StartRequest struct {
	Tenant  string
	Key     Key
	Message Message
	Channel string
}

// Import applies the default import to an inbound event. channel places a new
// root; without one, only a thread the board already syncs can receive it. A
// second call answers what the first did.
func (c *Call) Import(ctx context.Context, channel string, opts ImportOptions) (ImportResult, error) {
	if c.state == nil {
		return ImportResult{}, errUninitialized
	}
	if c.state.closed.Load() {
		return ImportResult{}, ErrCallFinished
	}
	if c.Direction != DirectionIn || c.Event == nil {
		return ImportResult{}, fmt.Errorf("%w: Import is only for an inbound event", ErrBadRequest)
	}
	c.state.importOnce.Do(func() {
		c.state.importRes, c.state.importErr = c.ops.Import(ctx, ImportRequest{
			Tenant:    c.Tenant,
			Key:       c.Key,
			EventID:   c.Event.ID,
			Channel:   channel,
			Event:     *c.Event,
			Overrides: opts,
		})
	})
	return c.state.importRes, c.state.importErr
}

// Deliver sends an outbound event to its target. A new root has no target, so
// Deliver sends nothing; Start gives it one first. A second call answers what
// the first did.
func (c *Call) Deliver(ctx context.Context, opts DeliverOptions) (Receipt, error) {
	if c.state == nil {
		return Receipt{}, errUninitialized
	}
	if c.state.closed.Load() {
		return Receipt{}, ErrCallFinished
	}
	if c.Direction != DirectionOut || c.Board == nil || c.Message == nil {
		return Receipt{}, fmt.Errorf("%w: Deliver is only for an outbound event", ErrBadRequest)
	}
	c.state.deliverOnce.Do(func() {
		if c.Target == nil {
			return
		}
		renderer := opts.Renderer
		if renderer == nil {
			renderer = c.Renderer
		}
		c.state.deliverRes, c.state.deliverErr = c.ops.Deliver(ctx, DeliverRequest{
			Tenant:   c.Tenant,
			Key:      c.Key,
			Event:    *c.Board,
			Message:  *c.Message,
			Target:   *c.Target,
			Copy:     c.Copy,
			Renderer: renderer,
		})
	})
	return c.state.deliverRes, c.state.deliverErr
}

// Start syncs the message's thread with a platform channel. It never starts a
// thread in the channel the message came from.
func (c *Call) Start(ctx context.Context, channel string) (ThreadRef, error) {
	if c.state == nil {
		return ThreadRef{}, errUninitialized
	}
	if c.state.closed.Load() {
		return ThreadRef{}, ErrCallFinished
	}
	if c.Direction != DirectionOut || c.Message == nil {
		return ThreadRef{}, fmt.Errorf("%w: Start is only for an outbound event", ErrBadRequest)
	}
	c.state.startOnce.Do(func() {
		c.state.startRes, c.state.startErr = c.ops.Start(ctx, StartRequest{
			Tenant: c.Tenant, Key: c.Key, Message: *c.Message, Channel: channel,
		})
	})
	return c.state.startRes, c.state.startErr
}

// ResolveChannel turns a platform channel's name into its ID.
func (c *Call) ResolveChannel(ctx context.Context, name string) (string, error) {
	if c.state == nil {
		return "", errUninitialized
	}
	if c.state.closed.Load() {
		return "", ErrCallFinished
	}
	return c.ops.ResolveChannel(ctx, c.Key, name)
}

// Permalink returns the platform's link to the inbound event's message. It is
// fetched once per call. Empty means the platform has no link for it.
func (c *Call) Permalink(ctx context.Context) (string, error) {
	if c.state == nil {
		return "", errUninitialized
	}
	if c.state.closed.Load() {
		return "", ErrCallFinished
	}
	if c.Direction != DirectionIn || c.Event == nil {
		return "", fmt.Errorf("%w: Permalink is only for an inbound event", ErrBadRequest)
	}
	c.state.permalinkOnce.Do(func() {
		c.state.permalink, c.state.permalinkErr = c.ops.Permalink(ctx, c.Key, *c.Event)
	})
	return c.state.permalink, c.state.permalinkErr
}

// finish closes the call, so a function that kept a reference can't use it
// after returning.
func (c *Call) finish() {
	if c.state != nil {
		c.state.closed.Store(true)
	}
}

// Host is the board service seam. A keel server implements it over the board
// log; tests use sync/fake.Host.
type Host interface {
	// Import applies one inbound import. It must be idempotent by EventID and
	// call order: a retry that writes in the same order returns the same IDs.
	Import(ctx context.Context, req ImportRequest) (ImportResult, error)
	// Record appends a board event sync produces: a receipt or thread.synced.
	// It must be idempotent for one (message, target) pair: a retried
	// message.synced may repeat after a copy was saved, and it must preserve
	// Files/FilesSet rather than dropping a file receipt. The sync-server Host
	// implements this; fake.Host records calls for tests.
	Record(ctx context.Context, event BoardEvent) error
}

// Runner invokes sync functions with the calls they made bound to a Host and a
// Provider. It is the executable seam between the board service and adapters;
// the durable inbox, outbox, and subscription belong to the server.
type Runner struct {
	host Host
}

// NewRunner returns a runner over host.
func NewRunner(host Host) *Runner { return &Runner{host: host} }

// Inbound offers one platform event to entry's sync function and waits for the
// function's import. The event must already be validated and stored in the
// inbox; the caller acks it after this returns.
func (r *Runner) Inbound(ctx context.Context, entry Entry, event Event) error {
	if err := entry.Validate(); err != nil {
		return err
	}
	if err := event.Validate(); err != nil {
		return err
	}
	if event.Key != entry.Key {
		return fmt.Errorf("%w: event key %q isn't entry %q", ErrBadRequest, event.Key, entry.Key)
	}
	call := newCall(&Call{
		Tenant:    entry.Tenant,
		Key:       entry.Key,
		Direction: DirectionIn,
		Event:     &event,
		ops:       &inboundOps{host: r.host, provider: entry.Provider},
	})
	defer call.finish()
	return entry.Sync(ctx, call)
}

// Outbound offers one board event to entry's sync function for one target.
// target nil is a new root: the function may call Start. copy is the native
// platform message ID for an edit, delete, or reaction.
func (r *Runner) Outbound(ctx context.Context, entry Entry, event BoardEvent, message Message, target *ResolvedTarget, copy string) (Receipt, error) {
	return r.OutboundWithOptions(ctx, entry, event, message, target, copy, OutboundOptions{})
}

// OutboundWithOptions is Outbound with a renderer and a copy sink. The worker
// uses the sink so a post's copy is durable before Host.Record; a receipt
// failure then retries without posting to the platform again.
func (r *Runner) OutboundWithOptions(ctx context.Context, entry Entry, event BoardEvent, message Message, target *ResolvedTarget, copy string, opts OutboundOptions) (Receipt, error) {
	if err := entry.Validate(); err != nil {
		return Receipt{}, err
	}
	if event.Tenant == "" {
		event.Tenant = entry.Tenant
	}
	if event.Tenant != entry.Tenant {
		return Receipt{}, fmt.Errorf("%w: event tenant %q isn't entry tenant %q", ErrBadRequest, event.Tenant, entry.Tenant)
	}
	if event.Message == nil {
		event.Message = &message
	}
	if target != nil {
		if err := target.Validate(); err != nil {
			return Receipt{}, err
		}
		if target.Key != entry.Key {
			return Receipt{}, fmt.Errorf("%w: target key %q isn't entry %q", ErrBadRequest, target.Key, entry.Key)
		}
	}
	call := newCall(&Call{
		Tenant:    entry.Tenant,
		Key:       entry.Key,
		Direction: DirectionOut,
		Board:     &event,
		Message:   &message,
		Target:    target,
		Copy:      copy,
		Renderer:  opts.Renderer,
		ops: &outboundOps{
			host: r.host, provider: entry.Provider, copySink: opts.CopySink,
		},
	})
	defer call.finish()
	if err := entry.Sync(ctx, call); err != nil {
		return Receipt{}, err
	}
	if call.state != nil && call.state.deliverErr != nil {
		return call.state.deliverRes, call.state.deliverErr
	}
	if call.state == nil {
		return Receipt{}, nil
	}
	return call.state.deliverRes, nil
}

// OfferRoot offers a new board root to entry's sync function for one platform
// channel, with no target yet: the function may call Start. It is how a new
// root reaches each platform that can start threads in a mapped channel.
func (r *Runner) OfferRoot(ctx context.Context, entry Entry, event BoardEvent, message Message, platformChannel string) error {
	if err := entry.Validate(); err != nil {
		return err
	}
	if event.Tenant == "" {
		event.Tenant = entry.Tenant
	}
	if event.Tenant != entry.Tenant {
		return fmt.Errorf("%w: event tenant %q isn't entry tenant %q", ErrBadRequest, event.Tenant, entry.Tenant)
	}
	if event.Message == nil {
		event.Message = &message
	}
	call := newCall(&Call{
		Tenant:    entry.Tenant,
		Key:       entry.Key,
		Direction: DirectionOut,
		Board:     &event,
		Message:   &message,
		Channel:   platformChannel,
		ops:       &outboundOps{host: r.host, provider: entry.Provider},
	})
	defer call.finish()
	return entry.Sync(ctx, call)
}

type inboundOps struct {
	host     Host
	provider Provider
}

func (o *inboundOps) Import(ctx context.Context, req ImportRequest) (ImportResult, error) {
	if o.host == nil {
		return ImportResult{}, errors.New("sync: inbound call has no host")
	}
	return o.host.Import(ctx, req)
}

func (o *inboundOps) Deliver(context.Context, DeliverRequest) (Receipt, error) {
	return Receipt{}, fmt.Errorf("%w: an inbound call can't deliver", ErrBadRequest)
}

func (o *inboundOps) Start(context.Context, StartRequest) (ThreadRef, error) {
	return ThreadRef{}, fmt.Errorf("%w: an inbound call can't start a thread", ErrBadRequest)
}

func (o *inboundOps) ResolveChannel(ctx context.Context, key Key, name string) (string, error) {
	return o.provider.ResolveChannel(ctx, key, name)
}

func (o *inboundOps) Permalink(ctx context.Context, key Key, event Event) (string, error) {
	linker, ok := o.provider.(Permalinker)
	if !ok {
		return "", nil
	}
	return linker.Permalink(ctx, key, event)
}

type outboundOps struct {
	host     Host
	provider Provider
	copySink CopySink
}

func (o *outboundOps) Import(context.Context, ImportRequest) (ImportResult, error) {
	return ImportResult{}, fmt.Errorf("%w: an outbound call can't import", ErrBadRequest)
}

func (o *outboundOps) Deliver(ctx context.Context, req DeliverRequest) (Receipt, error) {
	if err := req.Target.Validate(); err != nil {
		return Receipt{}, err
	}
	if req.Target.Key != req.Key {
		return Receipt{}, fmt.Errorf("%w: target key %q isn't call key %q", ErrBadRequest, req.Target.Key, req.Key)
	}
	msg := OutboundMessage{
		Markdown:     req.Message.Text,
		Sender:       senderOf(req.Message),
		External:     req.Message.From != "" && req.Message.From != "board",
		BoardMessage: req.Message.ID,
		BoardThread:  req.Message.Thread,
		Labels:       req.Message.Labels,
	}
	if req.Renderer != nil {
		rendered, err := req.Renderer(RenderInput{
			Message: req.Message,
			Author:  Author{ID: req.Message.Actor, Name: req.Message.ActorName},
			Label:   req.Message.ActorName,
			Target:  req.Target,
		})
		if err != nil {
			return Receipt{}, err
		}
		msg.Markdown = rendered
	} else if req.Event.Type != BoardReactionAdded && req.Event.Type != BoardReactionRemove {
		rendered, err := DefaultRender(RenderInput{
			Message: req.Message,
			Author:  Author{ID: req.Message.Actor, Name: req.Message.ActorName},
			Label:   req.Message.ActorName,
			Target:  req.Target,
		})
		if err != nil {
			return Receipt{}, err
		}
		msg.Markdown = rendered
	}

	var receipt Receipt
	var err error
	switch req.Event.Type {
	case BoardMessagePosted:
		var posted Posted
		posted, err = o.provider.Post(ctx, req.Key, req.Target, msg)
		if err == nil {
			err = posted.Validate()
		}
		if err == nil {
			receipt = Receipt{Target: req.Target, Thread: postedThread(req.Target, posted.Thread), Message: posted.Message, Files: posted.Files, FilesSet: posted.Files != nil}
		}
	case BoardMessageEdited:
		editor, ok := o.provider.(Editor)
		if !ok {
			return Receipt{}, fmt.Errorf("%w: %s can't edit a message", ErrUnsupported, o.provider.Platform())
		}
		if req.Copy == "" {
			return Receipt{}, fmt.Errorf("%w: an edit needs the copy's platform message ID", ErrBadRequest)
		}
		err = editor.Edit(ctx, req.Key, targetRef(req.Target), req.Copy, msg)
		receipt = Receipt{Target: req.Target, Thread: targetRef(req.Target), Message: req.Copy}
	case BoardMessageDeleted:
		deleter, ok := o.provider.(Deleter)
		if !ok {
			return Receipt{}, fmt.Errorf("%w: %s can't delete a message", ErrUnsupported, o.provider.Platform())
		}
		if req.Copy == "" {
			return Receipt{}, fmt.Errorf("%w: a delete needs the copy's platform message ID", ErrBadRequest)
		}
		err = deleter.Delete(ctx, req.Key, targetRef(req.Target), req.Copy)
		receipt = Receipt{Target: req.Target, Thread: targetRef(req.Target), Message: req.Copy}
	case BoardReactionAdded, BoardReactionRemove:
		reactor, ok := o.provider.(Reactor)
		if !ok {
			return Receipt{}, fmt.Errorf("%w: %s can't react", ErrUnsupported, o.provider.Platform())
		}
		if req.Copy == "" || req.Event.Emoji == "" {
			return Receipt{}, fmt.Errorf("%w: a reaction needs a message and an emoji", ErrBadRequest)
		}
		err = reactor.React(ctx, req.Key, targetRef(req.Target), req.Copy, req.Event.Emoji, req.Event.Type == BoardReactionAdded)
		receipt = Receipt{Target: req.Target, Thread: targetRef(req.Target), Message: req.Copy}
	case BoardLabelsChanged:
		// A provider that mirrors labels (Linear's issue state/properties) is
		// told even when no renderer would change the text. Others only edit a
		// copy when a renderer reads labels.
		if req.Renderer == nil {
			if labeler, ok := o.provider.(Labeler); ok {
				err = labeler.Labels(ctx, req.Key, targetRef(req.Target), req.Message.Labels)
				receipt = Receipt{Target: req.Target, Thread: targetRef(req.Target), Message: req.Copy}
				break
			}
			return Receipt{}, nil
		}
		editor, ok := o.provider.(Editor)
		if !ok {
			return Receipt{}, fmt.Errorf("%w: %s can't edit a message", ErrUnsupported, o.provider.Platform())
		}
		if req.Copy == "" {
			return Receipt{}, fmt.Errorf("%w: a label change needs the copy's platform message ID", ErrBadRequest)
		}
		err = editor.Edit(ctx, req.Key, targetRef(req.Target), req.Copy, msg)
		receipt = Receipt{Target: req.Target, Thread: targetRef(req.Target), Message: req.Copy}
	case BoardThreadSynced:
		if req.Target.Kind != TargetChannel {
			return Receipt{}, nil
		}
		var posted Posted
		posted, err = o.provider.Post(ctx, req.Key, req.Target, msg)
		if err == nil {
			err = posted.Validate()
		}
		if err == nil {
			receipt = Receipt{Target: req.Target, Thread: postedThread(req.Target, posted.Thread), Message: posted.Message, Files: posted.Files, FilesSet: posted.Files != nil}
		}
	default:
		return Receipt{}, fmt.Errorf("%w: sync can't deliver %q", ErrUnsupported, req.Event.Type)
	}
	if err != nil {
		return receipt, err
	}
	// Only a post creates a copy; edits, deletes, and reactions are
	// best-effort changes to one and record no receipt (upstream semantics).
	if req.Event.Type == BoardMessagePosted || req.Event.Type == BoardThreadSynced {
		// The copy is durable before the receipt, so a receipt failure can be
		// retried without posting to the platform a second time.
		if o.copySink != nil && receipt.Message != "" {
			if err := o.copySink(ctx, CopyRequest{
				Tenant: req.Tenant, Message: req.Message, Target: req.Target, Receipt: receipt,
			}); err != nil {
				return receipt, err
			}
		}
		if o.host != nil {
			record := BoardEvent{
				Tenant:          req.Tenant,
				Type:            BoardMessageSynced,
				Thread:          req.Message.Thread,
				Message:         &req.Message,
				External:        &receipt.Thread,
				ExternalMessage: receipt.Message,
				Files:           receipt.Files,
				FilesSet:        receipt.FilesSet,
			}
			if err := o.host.Record(ctx, record); err != nil {
				return receipt, err
			}
		}
	}
	return receipt, nil
}

func (o *outboundOps) Start(ctx context.Context, req StartRequest) (ThreadRef, error) {
	if !o.provider.Capabilities().StartThreads {
		return ThreadRef{}, fmt.Errorf("%w: %s can't start threads", ErrUnsupported, o.provider.Platform())
	}
	target, err := o.provider.ResolveTarget(ctx, req.Key, req.Channel)
	if err != nil {
		return ThreadRef{}, err
	}
	if err := target.Validate(); err != nil {
		return ThreadRef{}, err
	}
	if target.Key != req.Key {
		return ThreadRef{}, fmt.Errorf("%w: resolved target belongs to %q, not %q", ErrBadRequest, target.Key, req.Key)
	}
	if target.Kind != TargetChannel {
		return ThreadRef{}, fmt.Errorf("%w: Start needs a platform channel, got a thread", ErrBadRequest)
	}
	ref := target.Ref()
	if req.Message.Origin != nil && req.Message.Origin.Channel != "" && req.Message.Origin.Channel == ref.ChannelID() {
		return ThreadRef{}, fmt.Errorf("%w: a thread never starts in the channel it came from", ErrBadRequest)
	}
	if req.Message.NoSync {
		return ThreadRef{}, nil
	}
	if o.host == nil {
		return ThreadRef{}, errors.New("sync: outbound call has no host")
	}
	if err := o.host.Record(ctx, BoardEvent{
		Tenant: req.Tenant,
		Type:   BoardThreadSynced,
		Thread: req.Message.Thread,
		Target: &ref,
		Cause:  "config",
	}); err != nil {
		return ThreadRef{}, err
	}
	return ref, nil
}

func (o *outboundOps) ResolveChannel(ctx context.Context, key Key, name string) (string, error) {
	return o.provider.ResolveChannel(ctx, key, name)
}

func (o *outboundOps) Permalink(context.Context, Key, Event) (string, error) {
	return "", fmt.Errorf("%w: an outbound call has no permalink", ErrBadRequest)
}

func targetRef(target ResolvedTarget) ThreadRef { return target.Ref() }

func postedThread(target ResolvedTarget, nativeThread string) ThreadRef {
	return ThreadRef{
		Key:       target.Key,
		Platform:  target.Platform,
		Channel:   target.Channel,
		Thread:    nativeThread,
		Subthread: target.Subthread,
	}
}

func senderOf(msg Message) *Sender {
	if !msg.Verified || (msg.ActorName == "" && msg.Actor == "") {
		return nil
	}
	name := msg.ActorName
	if name == "" {
		name = msg.Actor
	}
	return &Sender{Name: name}
}
