package automation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/protocol"
)

// Action is a code or plugin action a deployment registered. Its name is what
// a watcher declares; nothing resolves it from the network.
type Action interface {
	Name() string
	Run(ctx context.Context, call *ActionCall) error
}

// ActionFunc adapts a function to Action.
type ActionFunc struct {
	ActionName string
	Fn         func(ctx context.Context, call *ActionCall) error
}

// Name implements Action.
func (a ActionFunc) Name() string { return a.ActionName }

// Run implements Action.
func (a ActionFunc) Run(ctx context.Context, call *ActionCall) error { return a.Fn(ctx, call) }

// Registry is the deployment's set of action implementations. A worker refuses
// a delivery whose declared action isn't registered; a management API that has
// a registry refuses to create or update a watcher naming an unknown action.
type Registry struct {
	mu      sync.RWMutex
	actions map[string]Action
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{actions: make(map[string]Action)}
}

// NewRegistryWith returns a registry holding actions.
func NewRegistryWith(actions ...Action) (*Registry, error) {
	registry := NewRegistry()
	for _, action := range actions {
		if err := registry.Register(action); err != nil {
			return nil, err
		}
	}
	return registry, nil
}

// Register adds an action. Registering the same name twice is a deployment
// bug, never a silent replacement.
func (r *Registry) Register(action Action) error {
	if action == nil {
		return errors.New("automation: nil action")
	}
	if err := (ActionSpec{Name: action.Name()}).Validate(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.actions[action.Name()]; exists {
		return fmt.Errorf("automation: action %q is registered twice", action.Name())
	}
	r.actions[action.Name()] = action
	return nil
}

// Get returns a registered action.
func (r *Registry) Get(name string) (Action, bool) {
	if r == nil {
		return nil, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	action, ok := r.actions[name]
	return action, ok
}

// Names returns the registered names, sorted.
func (r *Registry) Names() []string {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.actions))
	for name := range r.actions {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// BotActor is the actor a watcher acts as. An empty Bot falls back to
// local:bot:<watcher ID>, so a deployment can rely on a stable identity per
// watcher without declaring one.
func BotActor(watcher Watcher) protocol.Actor {
	id := watcher.Bot
	if id == "" {
		id = "local:bot:" + watcher.ID
	}
	principal := id
	var profile *string
	if index := strings.LastIndex(id, "+"); index > 0 && protocol.ValidProfile(id[index+1:]) {
		principal = id[:index]
		value := id[index+1:]
		profile = &value
	}
	kind := protocol.ActorPerson
	if profile != nil {
		kind = protocol.ActorAgent
	}
	return protocol.Actor{
		ID: id, Principal: principal, Profile: profile, Kind: kind,
		Handle: handleOfActor(id), Name: handleOfActor(id),
	}
}

func handleOfActor(id string) string {
	value := id
	if index := strings.LastIndex(value, "+"); index >= 0 {
		value = value[:index]
	}
	if index := strings.LastIndex(value, ":"); index >= 0 {
		value = value[index+1:]
	}
	if value == "" {
		return id
	}
	return value
}

// ActorFromID reconstructs enough of a stored actor for a permission check. A
// schedule stores its creator's full actor ID; its principal is the part before
// a profile suffix.
func ActorFromID(id string) protocol.Actor {
	principal := id
	var profile *string
	if index := strings.LastIndex(id, "+"); index > 0 && protocol.ValidProfile(id[index+1:]) {
		principal = id[:index]
		value := id[index+1:]
		profile = &value
	}
	kind := protocol.ActorPerson
	if profile != nil {
		kind = protocol.ActorAgent
	}
	return protocol.Actor{
		ID: id, Principal: principal, Profile: profile, Kind: kind,
		Handle: handleOfActor(id), Name: handleOfActor(id),
	}
}

// logVia converts a protocol Via to the log's record type.
func logVia(via *protocol.Via) *log.Via {
	if via == nil {
		return nil
	}
	out := &log.Via{Name: via.Name}
	if via.Environment != nil {
		environment := *via.Environment
		out.Environment = &environment
	}
	return out
}

// ScheduleActor reconstructs the creator identity a schedule stored, including
// the kind and Via a permission policy may read.
func ScheduleActor(schedule Schedule) protocol.Actor {
	actor := ActorFromID(schedule.Creator)
	if schedule.CreatorKind != "" {
		actor.Kind = protocol.ActorKind(schedule.CreatorKind)
	}
	if schedule.CreatorVia != nil {
		via := deepCopy(*schedule.CreatorVia)
		actor.Via = &via
	}
	return actor
}

// ActionCall is one action run: the triggering event, the watcher that matched
// it, and the only way the action may write to the board. A write goes through
// Post or Reply, which check the bot's post permission in the target channel;
// an action that never calls them has no board side effect.
type ActionCall struct {
	tenant    string
	watcher   Watcher
	event     log.Event
	message   *log.Message
	bot       protocol.Actor
	board     Board
	authorize Authorizer

	posts int
}

// Tenant returns the board an action runs on.
func (c *ActionCall) Tenant() string { return c.tenant }

// Watcher returns a detached copy of the watcher declaration.
func (c *ActionCall) Watcher() Watcher { return cloneWatcher(c.watcher) }

// Event returns a detached copy of the triggering event, including its typed
// payload. Mutating it never changes the delivery or the board.
func (c *ActionCall) Event() log.Event { return deepCopy(c.event) }

// Message returns a detached copy of the event's message, or nil when the
// event concerns none.
func (c *ActionCall) Message() *log.Message {
	if c.message == nil {
		return nil
	}
	cloned := deepCopy(*c.message)
	return &cloned
}

// Bot returns the actor the watcher's writes are made as.
func (c *ActionCall) Bot() protocol.Actor { return deepCopy(c.bot) }

// Channel returns the channel the triggering event concerns, or "" when it
// concerns none.
func (c *ActionCall) Channel() string {
	if c.message != nil {
		return c.message.Channel
	}
	switch payload := c.event.Payload.(type) {
	case *log.MessagePostedPayload:
		return payload.Channel
	case *log.ChannelCreatedPayload:
		return payload.Channel
	}
	return ""
}

// Post writes a message to channel as the bot, with the watcher's event and
// call order as an idempotency key. A retried delivery that makes the same
// call lands the same message instead of posting twice.
func (c *ActionCall) Post(ctx context.Context, channel, text string, labels map[string]string) (uint64, error) {
	return c.write(ctx, channel, nil, nil, text, labels)
}

// Reply writes a message in target's thread as the bot. Target may be a root
// or a reply. A missing or deleted target is a permanent failure.
func (c *ActionCall) Reply(ctx context.Context, target uint64, text string, labels map[string]string) (uint64, error) {
	var (
		channel string
		rootID  uint64
		replyTo *uint64
		found   bool
	)
	err := c.board.ReadContext(ctx, c.tenant, func(snapshot *log.Snapshot) error {
		message := snapshot.Message(target)
		if message == nil || message.Deleted {
			return nil
		}
		found = true
		channel = message.Channel
		rootID = message.ID
		if message.Re != nil {
			rootID = *message.Re
			replied := message.ID
			replyTo = &replied
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, Permanent(protocol.NotFound(protocol.CodeMessageNotFound, fmt.Sprintf("message not found: #%d", target)))
	}
	return c.write(ctx, channel, &rootID, replyTo, text, labels)
}

func (c *ActionCall) write(ctx context.Context, channel string, re, replyTo *uint64, text string, labels map[string]string) (uint64, error) {
	if c.board == nil {
		return 0, errors.New("automation: action call has no board")
	}
	if err := protocol.ValidateChannel(channel); err != nil {
		return 0, err
	}
	if err := protocol.ValidateText(text); err != nil {
		return 0, err
	}
	if err := protocol.ValidateNewLabels(labels); err != nil {
		return 0, err
	}
	if labels == nil {
		labels = map[string]string{}
	}
	if c.authorize != nil {
		if err := c.authorize.Authorize(ctx, c.tenant, c.bot, channel, protocol.PermissionPost); err != nil {
			if IsAccessDenied(err) {
				return 0, Permanent(fmt.Errorf("watcher bot may not post in %s: %w", channel, err))
			}
			return 0, fmt.Errorf("watcher bot authorization is unavailable: %w", err)
		}
		if len(labels) > 0 {
			if err := c.authorize.Authorize(ctx, c.tenant, c.bot, channel, protocol.PermissionLabel); err != nil {
				if IsAccessDenied(err) {
					return 0, Permanent(fmt.Errorf("watcher bot may not label in %s: %w", channel, err))
				}
				return 0, fmt.Errorf("watcher bot label authorization is unavailable: %w", err)
			}
		}
	}
	readable := true
	if c.authorize != nil {
		switch err := c.authorize.Authorize(ctx, c.tenant, c.bot, channel, protocol.PermissionRead); {
		case err == nil:
			readable = true
		case IsAccessDenied(err):
			readable = false
		default:
			return 0, fmt.Errorf("watcher bot read authorization is unavailable: %w", err)
		}
	}
	key := fmt.Sprintf("watch:%s:%d:%d", c.watcher.ID, c.event.Seq, c.posts)
	c.posts++
	digest := actionDigest(channel, re, replyTo, text, labels)
	var id uint64
	err := c.board.Update(ctx, c.tenant, log.UpdateOptions{
		Idempotency: &log.Idempotency{Key: key, Digest: digest},
	}, func(tx *log.Tx) error {
		if replay, ok := tx.Replay(); ok {
			if len(replay.MessageIDs) == 0 {
				return protocol.NewError(protocol.CodeDuplicateID, "that watcher event already posted a different message")
			}
			id = replay.MessageIDs[0]
			return nil
		}
		offset := 0
		if tx.Actor(c.bot.ID) == nil {
			tx.Emit(log.EventActorSeen, &log.ActorSeenPayload{
				Actor: c.bot.ID, Kind: string(c.bot.Kind), Handle: c.bot.Handle,
				Name: c.bot.Name, Via: logVia(c.bot.Via), Platform: "local",
			})
			offset++
		}
		id = tx.NextID(offset)
		thread := log.ThreadKey(id)
		if re != nil {
			thread = log.ThreadKey(*re)
		}
		tx.Emit(log.EventMessagePosted, &log.MessagePostedPayload{
			ID: id, Channel: channel, Text: text, Re: re, ReplyTo: replyTo,
			Thread: thread, Labels: labels, By: c.bot.Actored(), Blind: !readable,
		})
		tx.RecordResult(id)
		return nil
	})
	if err != nil {
		if isDuplicateID(err) {
			return 0, Permanent(err)
		}
		return 0, err
	}
	return id, nil
}

func actionDigest(channel string, re, replyTo *uint64, text string, labels map[string]string) string {
	raw, _ := json.Marshal(struct {
		Channel string            `json:"channel"`
		Re      *uint64           `json:"re,omitempty"`
		ReplyTo *uint64           `json:"replyTo,omitempty"`
		Text    string            `json:"text"`
		Labels  map[string]string `json:"labels"`
	}{Channel: channel, Re: re, ReplyTo: replyTo, Text: text, Labels: labels})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// PostAction is the built-in board-write action: it posts a fixed, bounded
// text from its deployment parameters to the event's channel as the watcher's
// bot. It never reads a URL from event data or parameters.
type PostAction struct{}

// Name implements Action.
func (PostAction) Name() string { return "post" }

// Run implements Action.
func (PostAction) Run(ctx context.Context, call *ActionCall) error {
	watcher := call.Watcher()
	if len(watcher.Action.Params) == 0 {
		return Permanent(protocol.Invalid("action.params.text: the post action needs text"))
	}
	var params struct {
		Text string `json:"text"`
	}
	decoder := json.NewDecoder(strings.NewReader(string(watcher.Action.Params)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&params); err != nil {
		return Permanent(protocol.Invalid("action.params: the post action takes {text}"))
	}
	if strings.TrimSpace(params.Text) == "" {
		return Permanent(protocol.Invalid("action.params.text: the post action needs text"))
	}
	channel := call.Channel()
	if channel == "" {
		return Permanent(protocol.Invalid("the event names no channel the post action can write to"))
	}
	_, err := call.Post(ctx, channel, params.Text, nil)
	return err
}

// Now returns the time an action should use for its own logic. It exists so a
// test action is deterministic; production code can use time.Now directly.
func Now() time.Time { return time.Now().UTC() }
