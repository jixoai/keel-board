package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/jixoai/keel-board/automation"
	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/protocol"
)

// AutomationOptions configure the watcher and schedule management API. The
// durable store is shared with a deployment's workers; Actions is optional but
// recommended, so creating a watcher naming an unknown action is refused.
type AutomationOptions struct {
	Store   automation.Store
	Actions *automation.Registry
	// Bots is the deployment's allowlist of bot actors a watcher may act as.
	// Empty allows any syntactically valid actor (development).
	Bots   []string
	Logger *slog.Logger
}

// automationService is one server's automation management wiring.
type automationService struct {
	store   automation.Store
	actions *automation.Registry
	bots    []string
	logger  *slog.Logger
}

func newAutomationService(opts AutomationOptions) (*automationService, error) {
	if opts.Store == nil {
		return nil, errors.New("automation needs a store")
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &automationService{
		store: opts.Store, actions: opts.Actions,
		bots: append([]string(nil), opts.Bots...), logger: opts.Logger,
	}, nil
}

// permitAutomationManage gates the management surface on manage at *.
func (req *request) permitAutomationManage(s *Server) *protocol.Error {
	return req.permit(s, "*", protocol.PermissionManage)
}

// permitAutomationChannel gates creating automation on a concrete channel.
// A subtree declaration is managed at *; every event it later matches is still
// checked against the bot's own channel relation at execution time.
func (req *request) permitAutomationChannel(s *Server, channel string) *protocol.Error {
	if strings.HasSuffix(channel, "/") {
		return nil
	}
	return req.permit(s, channel, protocol.PermissionPost)
}

// validateWatcherBot checks a watcher's bot against the deployment allowlist.
func (s *Server) validateWatcherBot(watcher automation.Watcher) *protocol.Error {
	if !automation.BotAllowed(automation.BotActor(watcher).ID, s.automation.bots) {
		return protocol.Invalidf("bot: %q isn't an allowlisted bot", automation.BotActor(watcher).ID)
	}
	return nil
}

// validateAction checks an action's shape and, when the process has a registry,
// that the name is registered.
func (s *Server) validateAction(action automation.ActionSpec) *protocol.Error {
	if err := action.Validate(); err != nil {
		return asProtocol(err)
	}
	if s.automation.actions != nil {
		if _, ok := s.automation.actions.Get(action.Name); !ok {
			return protocol.Invalidf("action.name: %q is not registered on this deployment", action.Name)
		}
	}
	return nil
}

// actionRequest is the wire form of a watcher action. It exists because
// ActionSpec.Params is json:"-" so a stored watcher can never leak deployment
// parameters; only this request type accepts params.
type actionRequest struct {
	Name   string          `json:"name"`
	Params json.RawMessage `json:"params,omitempty"`
}

func (a actionRequest) spec() automation.ActionSpec {
	return automation.ActionSpec{Name: a.Name, Params: a.Params}
}

// watcherRequest is a create or partial update. Pointers distinguish an omitted
// field from a zero value, so an update can't silently clear the bot or the
// filter.
type watcherRequest struct {
	ID      string             `json:"id,omitempty"`
	Channel *string            `json:"channel,omitempty"`
	Filter  *automation.Filter `json:"filter,omitempty"`
	Action  *actionRequest     `json:"action,omitempty"`
	Bot     *string            `json:"bot,omitempty"`
}

// watcherView is the management DTO. It names the action but never returns its
// parameters: a parameter may hold a credential, and the API's job is to
// enable, disable, and list, not to echo deployment secrets.
type watcherView struct {
	ID             string            `json:"id"`
	Channel        string            `json:"channel"`
	Filter         automation.Filter `json:"filter"`
	Action         string            `json:"action"`
	Bot            string            `json:"bot,omitempty"`
	Enabled        bool              `json:"enabled"`
	Cursor         string            `json:"cursor,omitempty"`
	Dead           int               `json:"dead"`
	LastError      string            `json:"lastError,omitempty"`
	LastDeliveryAt string            `json:"lastDeliveryAt,omitempty"`
	CreatedAt      string            `json:"createdAt"`
	UpdatedAt      string            `json:"updatedAt"`
}

func watcherViewOf(watcher automation.Watcher) watcherView {
	view := watcherView{
		ID: watcher.ID, Channel: watcher.Channel, Filter: watcher.Filter,
		Action: watcher.Action.Name, Bot: watcher.Bot, Enabled: watcher.Enabled,
		CreatedAt: protocol.FormatTime(watcher.CreatedAt),
		UpdatedAt: protocol.FormatTime(watcher.UpdatedAt),
	}
	if watcher.Cursor != 0 {
		view.Cursor = protocol.FormatRef(watcher.Cursor)
	}
	if view.Filter.Types == nil {
		view.Filter.Types = []string{}
	}
	if view.Filter.Labels == nil {
		view.Filter.Labels = map[string]string{}
	}
	return view
}

// watcherHealthView adds the store's optional delivery facts. A store without
// StatsReader leaves dead=0 and the optional fields absent, which is honest:
// the API doesn't invent stats it can't read.
func (s *Server) watcherHealthView(ctx context.Context, tenant string, watcher automation.Watcher) watcherView {
	view := watcherViewOf(watcher)
	reader, ok := s.automation.store.(automation.StatsReader)
	if !ok {
		return view
	}
	stats, err := reader.WatcherStats(ctx, tenant, watcher.ID)
	if err != nil {
		s.logger.Warn("watcher stats failed; omitting delivery facts",
			"tenant", tenant, "watcher", watcher.ID, "error", err)
		return view
	}
	view.Dead = stats.Dead
	view.LastError = stats.LastError
	if !stats.LastDeliveryAt.IsZero() {
		view.LastDeliveryAt = protocol.FormatTime(stats.LastDeliveryAt)
	}
	return view
}

// deliveryView is a delivery without the event's content or the lease
// credential.
type deliveryView struct {
	WatcherID string  `json:"watcherId"`
	Event     string  `json:"event"`
	State     string  `json:"state"`
	Attempts  int     `json:"attempts"`
	NextAt    string  `json:"nextAt"`
	LastError string  `json:"lastError,omitempty"`
	CreatedAt string  `json:"createdAt"`
	UpdatedAt string  `json:"updatedAt"`
	Completed *string `json:"completedAt,omitempty"`
}

func deliveryViewOf(delivery automation.Delivery) deliveryView {
	return deliveryView{
		WatcherID: delivery.WatcherID, Event: protocol.FormatRef(delivery.EventSeq),
		State: delivery.State, Attempts: delivery.Attempts,
		NextAt: protocol.FormatTime(delivery.NextAt), LastError: delivery.LastError,
		CreatedAt: protocol.FormatTime(delivery.CreatedAt),
		UpdatedAt: protocol.FormatTime(delivery.UpdatedAt),
		Completed: protocol.FormatTimePtr(delivery.Completed),
	}
}

// scheduleRequest is a create. Channel and Re are alternatives; exactly one is
// required. Text is the body the schedule sends and is never returned.
type scheduleRequest struct {
	Channel        string            `json:"channel,omitempty"`
	Re             string            `json:"re,omitempty"`
	Text           string            `json:"text"`
	Labels         map[string]string `json:"labels,omitempty"`
	NextRunAt      string            `json:"nextRunAt,omitempty"`
	EverySeconds   *int              `json:"everySeconds,omitempty"`
	UntilAt        string            `json:"untilAt,omitempty"`
	Times          *int              `json:"times,omitempty"`
	IdempotencyKey string            `json:"idempotencyKey,omitempty"`
}

// scheduleUpdate is a partial update; Channel and Re are immutable because
// changing a plan's target would need a new permission decision.
type scheduleUpdate struct {
	Text         *string            `json:"text,omitempty"`
	Labels       *map[string]string `json:"labels,omitempty"`
	NextRunAt    *string            `json:"nextRunAt,omitempty"`
	EverySeconds *int               `json:"everySeconds,omitempty"`
	UntilAt      *string            `json:"untilAt,omitempty"`
	Times        *int               `json:"times,omitempty"`
	// IfUpdatedAt is the version the caller read; a mismatch is a 409 and no
	// write. It is the scheduleView's updatedAt.
	IfUpdatedAt *string `json:"ifUpdatedAt,omitempty"`
}

// scheduleView is the management DTO. It omits Text on purpose: a schedule's
// body may be sensitive, and listing plans must not leak it.
type scheduleView struct {
	ID           string            `json:"id"`
	Channel      string            `json:"channel"`
	Re           *string           `json:"re,omitempty"`
	Labels       map[string]string `json:"labels"`
	Creator      string            `json:"creator"`
	NextRunAt    string            `json:"nextRunAt"`
	EverySeconds int               `json:"everySeconds,omitempty"`
	UntilAt      *string           `json:"untilAt,omitempty"`
	Times        int               `json:"times,omitempty"`
	Sent         int               `json:"sent"`
	Status       string            `json:"status"`
	Attempts     int               `json:"attempts"`
	LastError    string            `json:"lastError,omitempty"`
	CancelReason string            `json:"cancelReason,omitempty"`
	CreatedAt    string            `json:"createdAt"`
	UpdatedAt    string            `json:"updatedAt"`
	Completed    *string           `json:"completedAt,omitempty"`
}

func scheduleViewOf(schedule automation.Schedule) scheduleView {
	view := scheduleView{
		ID: schedule.ID, Channel: schedule.Channel, Creator: schedule.Creator,
		NextRunAt:    protocol.FormatTime(schedule.NextRunAt),
		EverySeconds: schedule.EverySeconds, Times: schedule.Times,
		Sent: schedule.Sent, Status: schedule.Status, Attempts: schedule.Attempts,
		LastError: schedule.LastError, CancelReason: schedule.CancelReason,
		CreatedAt: protocol.FormatTime(schedule.CreatedAt),
		UpdatedAt: protocol.FormatTime(schedule.UpdatedAt),
		Completed: protocol.FormatTimePtr(schedule.Completed),
	}
	if schedule.Re != nil {
		ref := protocol.FormatRef(*schedule.Re)
		view.Re = &ref
	}
	if schedule.UntilAt != nil {
		at := protocol.FormatTime(*schedule.UntilAt)
		view.UntilAt = &at
	}
	view.Labels = schedule.Labels
	if view.Labels == nil {
		view.Labels = map[string]string{}
	}
	return view
}

// handleWatchers answers GET /watchers.
func (s *Server) handleWatchers(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	if perr := req.permitAutomationManage(s); perr != nil {
		writeError(w, perr)
		return
	}
	watchers, err := s.automation.store.Watchers(req.ctx, req.tenant)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	views := make([]watcherView, 0, len(watchers))
	for _, watcher := range watchers {
		views = append(views, s.watcherHealthView(req.ctx, req.tenant, watcher))
	}
	writeJSON(w, http.StatusOK, map[string]any{"watchers": views})
}

// handleWatcher answers GET /watchers/{id}.
func (s *Server) handleWatcher(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	if perr := req.permitAutomationManage(s); perr != nil {
		writeError(w, perr)
		return
	}
	watcher, found, err := s.automation.store.Watcher(req.ctx, req.tenant, strings.TrimSpace(r.PathValue("id")))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if !found {
		writeError(w, protocol.NotFound(protocol.CodeNotFound, "no such watcher"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"watcher": s.watcherHealthView(req.ctx, req.tenant, watcher)})
}

// handleCreateWatcher answers POST /watchers.
func (s *Server) handleCreateWatcher(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	if perr := req.permitAutomationManage(s); perr != nil {
		writeError(w, perr)
		return
	}
	var body watcherRequest
	if perr := decodeBody(r, &body, true, false); perr != nil {
		writeError(w, perr)
		return
	}
	if body.Channel == nil {
		writeError(w, protocol.Invalid("channel: a watcher names a channel or subtree"))
		return
	}
	channel := strings.TrimSpace(*body.Channel)
	if err := protocol.ValidateChannelOrSubtree(channel); err != nil {
		writeError(w, err)
		return
	}
	if perr := req.permitAutomationChannel(s, channel); perr != nil {
		writeError(w, perr)
		return
	}
	if body.Action == nil {
		writeError(w, protocol.Invalid("action: a watcher names a registered action"))
		return
	}
	action := body.Action.spec()
	if perr := s.validateAction(action); perr != nil {
		writeError(w, perr)
		return
	}
	filter := automation.Filter{}
	if body.Filter != nil {
		filter = *body.Filter
	}
	bot := ""
	if body.Bot != nil {
		bot = strings.TrimSpace(*body.Bot)
	}
	id := strings.TrimSpace(body.ID)
	if id == "" {
		generated, err := automation.NewWatcherID()
		if err != nil {
			s.fail(w, r, err)
			return
		}
		id = generated
	}
	head, err := s.board.Head(req.tenant)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	watcher := automation.Watcher{
		Tenant: req.tenant, ID: id, Channel: channel, Filter: filter,
		Action: action, Bot: bot, Enabled: true, Cursor: head, CursorSet: true,
	}
	if perr := s.validateWatcherBot(watcher); perr != nil {
		writeError(w, perr)
		return
	}
	watcher, err = s.automation.store.CreateWatcher(req.ctx, watcher)
	if err != nil {
		s.failAutomation(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"watcher": watcherViewOf(watcher)})
}

// handleUpdateWatcher answers POST /watchers/{id}.
func (s *Server) handleUpdateWatcher(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	if perr := req.permitAutomationManage(s); perr != nil {
		writeError(w, perr)
		return
	}
	var body watcherRequest
	if perr := decodeBody(r, &body, true, false); perr != nil {
		writeError(w, perr)
		return
	}
	id := strings.TrimSpace(r.PathValue("id"))
	if body.ID != "" && strings.TrimSpace(body.ID) != id {
		writeError(w, protocol.Invalid("id: a watcher's ID is in its path and can't change"))
		return
	}
	watcher, found, err := s.automation.store.Watcher(req.ctx, req.tenant, id)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if !found {
		writeError(w, protocol.NotFound(protocol.CodeNotFound, "no such watcher"))
		return
	}
	if body.Channel != nil {
		channel := strings.TrimSpace(*body.Channel)
		if err := protocol.ValidateChannelOrSubtree(channel); err != nil {
			writeError(w, err)
			return
		}
		if perr := req.permitAutomationChannel(s, channel); perr != nil {
			writeError(w, perr)
			return
		}
		watcher.Channel = channel
	}
	if body.Filter != nil {
		watcher.Filter = *body.Filter
	}
	if body.Action != nil {
		action := body.Action.spec()
		if perr := s.validateAction(action); perr != nil {
			writeError(w, perr)
			return
		}
		watcher.Action = action
	}
	if body.Bot != nil {
		watcher.Bot = strings.TrimSpace(*body.Bot)
	}
	if perr := s.validateWatcherBot(watcher); perr != nil {
		writeError(w, perr)
		return
	}
	updated, err := s.automation.store.UpdateWatcher(req.ctx, watcher)
	if err != nil {
		s.failAutomation(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"watcher": watcherViewOf(updated)})
}

// handleDeleteWatcher answers DELETE /watchers/{id}.
func (s *Server) handleDeleteWatcher(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	if perr := req.permitAutomationManage(s); perr != nil {
		writeError(w, perr)
		return
	}
	if err := s.automation.store.DeleteWatcher(req.ctx, req.tenant, strings.TrimSpace(r.PathValue("id"))); err != nil {
		s.failAutomation(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deleted": true})
}

// handleEnableWatcher answers POST /watchers/{id}/enable. Enabling advances the
// cursor to the current head, so a long-disabled watcher acts on future events
// rather than replaying a backlog.
func (s *Server) handleEnableWatcher(w http.ResponseWriter, r *http.Request) {
	s.setWatcherEnabled(w, r, true)
}

// handleDisableWatcher answers POST /watchers/{id}/disable.
func (s *Server) handleDisableWatcher(w http.ResponseWriter, r *http.Request) {
	s.setWatcherEnabled(w, r, false)
}

func (s *Server) setWatcherEnabled(w http.ResponseWriter, r *http.Request, enabled bool) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	if perr := req.permitAutomationManage(s); perr != nil {
		writeError(w, perr)
		return
	}
	id := strings.TrimSpace(r.PathValue("id"))
	var cursor *uint64
	if enabled {
		head, err := s.board.Head(req.tenant)
		if err != nil {
			s.fail(w, r, err)
			return
		}
		cursor = &head
	}
	watcher, err := s.automation.store.SetWatcherEnabled(req.ctx, req.tenant, id, enabled, cursor)
	if err != nil {
		s.failAutomation(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"watcher": s.watcherHealthView(req.ctx, req.tenant, watcher)})
}

// handleWatcherDeliveries answers GET /watchers/{id}/deliveries.
func (s *Server) handleWatcherDeliveries(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	if perr := req.permitAutomationManage(s); perr != nil {
		writeError(w, perr)
		return
	}
	id := strings.TrimSpace(r.PathValue("id"))
	if _, found, err := s.automation.store.Watcher(req.ctx, req.tenant, id); err != nil {
		s.fail(w, r, err)
		return
	} else if !found {
		writeError(w, protocol.NotFound(protocol.CodeNotFound, "no such watcher"))
		return
	}
	state, hasState := queryString(r, "state")
	if hasState && state != "all" && state != "dead" {
		writeError(w, protocol.Invalid("state: use dead or all"))
		return
	}
	limit, perr := queryInt(r, "limit", 100, 1, 500)
	if perr != nil {
		writeError(w, perr)
		return
	}
	var (
		deliveries []automation.Delivery
		err        error
	)
	if state == "dead" {
		deliveries, err = s.automation.store.DeadDeliveries(req.ctx, req.tenant, id, limit)
	} else {
		deliveries, err = s.automation.store.Deliveries(req.ctx, req.tenant, id, limit)
	}
	if err != nil {
		s.fail(w, r, err)
		return
	}
	views := make([]deliveryView, 0, len(deliveries))
	for _, delivery := range deliveries {
		views = append(views, deliveryViewOf(delivery))
	}
	depth, err := s.automation.store.DeliveryDepth(req.ctx, req.tenant, id)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deliveries": views, "depth": depth})
}

// handleRetryDelivery answers POST /watchers/{id}/deliveries/{seq}/retry.
func (s *Server) handleRetryDelivery(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	if perr := req.permitAutomationManage(s); perr != nil {
		writeError(w, perr)
		return
	}
	id := strings.TrimSpace(r.PathValue("id"))
	if _, found, err := s.automation.store.Watcher(req.ctx, req.tenant, id); err != nil {
		s.fail(w, r, err)
		return
	} else if !found {
		writeError(w, protocol.NotFound(protocol.CodeNotFound, "no such watcher"))
		return
	}
	seq, valid := protocol.ParseRef(strings.TrimSpace(r.PathValue("seq")))
	if !valid || seq == 0 {
		writeError(w, protocol.NotFound(protocol.CodeNotFound, "no such delivery"))
		return
	}
	if err := s.automation.store.RetryDelivery(req.ctx, req.tenant, id, seq); err != nil {
		s.failAutomation(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"retried": true})
}

// handleClearDeliveries answers POST /watchers/{id}/deliveries/clear.
func (s *Server) handleClearDeliveries(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	if perr := req.permitAutomationManage(s); perr != nil {
		writeError(w, perr)
		return
	}
	cleared, err := s.automation.store.ClearDeliveries(req.ctx, req.tenant, strings.TrimSpace(r.PathValue("id")))
	if err != nil {
		s.failAutomation(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"cleared": cleared})
}

// handleSchedules answers GET /schedules.
func (s *Server) handleSchedules(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	if perr := req.permitAutomationManage(s); perr != nil {
		writeError(w, perr)
		return
	}
	schedules, err := s.automation.store.Schedules(req.ctx, req.tenant)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	views := make([]scheduleView, 0, len(schedules))
	for _, schedule := range schedules {
		views = append(views, scheduleViewOf(schedule))
	}
	writeJSON(w, http.StatusOK, map[string]any{"schedules": views})
}

// handleSchedule answers GET /schedules/{id}.
func (s *Server) handleSchedule(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	if perr := req.permitAutomationManage(s); perr != nil {
		writeError(w, perr)
		return
	}
	schedule, found, err := s.automation.store.Schedule(req.ctx, req.tenant, strings.TrimSpace(r.PathValue("id")))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if !found {
		writeError(w, protocol.NotFound(protocol.CodeNotFound, "no such schedule"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"schedule": scheduleViewOf(schedule)})
}

// handleCreateSchedule answers POST /schedules.
func (s *Server) handleCreateSchedule(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	if perr := req.permitAutomationManage(s); perr != nil {
		writeError(w, perr)
		return
	}
	var body scheduleRequest
	if perr := decodeBody(r, &body, true, false); perr != nil {
		writeError(w, perr)
		return
	}
	hasChannel := strings.TrimSpace(body.Channel) != ""
	hasRe := strings.TrimSpace(body.Re) != ""
	if hasChannel == hasRe {
		writeError(w, protocol.Invalid("channel/re: a schedule names a channel or a message to reply to, not both"))
		return
	}
	channel := strings.TrimSpace(body.Channel)
	var re *uint64
	if hasRe {
		id, valid := protocol.ParseRef(strings.TrimSpace(body.Re))
		if !valid || id == 0 {
			writeError(w, protocol.NotFound(protocol.CodeMessageNotFound, fmt.Sprintf("message not found: #%s", body.Re)))
			return
		}
		var target *log.Message
		err := s.board.Read(req.tenant, func(snapshot *log.Snapshot) error {
			target = snapshot.Message(id)
			return nil
		})
		if err != nil {
			s.fail(w, r, err)
			return
		}
		if target == nil || target.Deleted || !req.can(s, target.Channel, protocol.PermissionRead) {
			writeError(w, protocol.NotFound(protocol.CodeMessageNotFound, fmt.Sprintf("message not found: #%s", body.Re)))
			return
		}
		channel = target.Channel
		re = &id
	} else if err := protocol.ValidateChannel(channel); err != nil {
		writeError(w, err)
		return
	}
	if perr := req.permitAutomationChannel(s, channel); perr != nil {
		writeError(w, perr)
		return
	}
	if len(body.Labels) > 0 {
		if perr := req.permit(s, channel, protocol.PermissionLabel); perr != nil {
			writeError(w, perr)
			return
		}
	}
	nextRunAt := s.now().UTC()
	if strings.TrimSpace(body.NextRunAt) != "" {
		parsed, err := protocol.ParseTime(strings.TrimSpace(body.NextRunAt))
		if err != nil {
			writeError(w, protocol.Invalid("nextRunAt: "+err.Error()))
			return
		}
		nextRunAt = parsed.UTC()
	}
	var untilAt *time.Time
	if strings.TrimSpace(body.UntilAt) != "" {
		parsed, err := protocol.ParseTime(strings.TrimSpace(body.UntilAt))
		if err != nil {
			writeError(w, protocol.Invalid("untilAt: "+err.Error()))
			return
		}
		untilAt = &parsed
	}
	everySeconds := 0
	if body.EverySeconds != nil {
		everySeconds = *body.EverySeconds
	}
	times := 0
	if body.Times != nil {
		times = *body.Times
	}
	labels := body.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	creationKey := strings.TrimSpace(body.IdempotencyKey)
	creationDigest := ""
	nextRunKey := ""
	if strings.TrimSpace(body.NextRunAt) != "" {
		nextRunKey = protocol.FormatTime(nextRunAt)
	}
	if creationKey != "" {
		creationDigest = scheduleRequestDigest(channel, re, body.Text, labels, req.actor,
			nextRunKey, everySeconds, untilAt, times)
	}
	schedule, err := s.automation.store.CreateSchedule(req.ctx, automation.Schedule{
		Tenant: req.tenant, Channel: channel, Re: re, Text: body.Text,
		Labels: labels, Creator: req.actor.ID,
		CreatorKind: string(req.actor.Kind), CreatorVia: req.actor.Via,
		NextRunAt: nextRunAt, EverySeconds: everySeconds, UntilAt: untilAt, Times: times,
		CreationKey: creationKey, CreationDigest: creationDigest,
	})
	if err != nil {
		s.failAutomation(w, r, err)
		return
	}
	status := http.StatusCreated
	if schedule.Replayed {
		status = http.StatusOK
	}
	writeJSON(w, status, map[string]any{
		"schedule": scheduleViewOf(schedule), "replayed": schedule.Replayed,
	})
}

// handleUpdateSchedule answers POST /schedules/{id}.
func (s *Server) handleUpdateSchedule(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	if perr := req.permitAutomationManage(s); perr != nil {
		writeError(w, perr)
		return
	}
	var body scheduleUpdate
	if perr := decodeBody(r, &body, true, false); perr != nil {
		writeError(w, perr)
		return
	}
	id := strings.TrimSpace(r.PathValue("id"))
	schedule, found, err := s.automation.store.Schedule(req.ctx, req.tenant, id)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if !found {
		writeError(w, protocol.NotFound(protocol.CodeNotFound, "no such schedule"))
		return
	}
	if schedule.Status == automation.ScheduleDone || schedule.Status == automation.ScheduleCanceled {
		writeError(w, protocol.Invalid("status: the schedule is finished; create a new one"))
		return
	}
	if body.Text != nil {
		schedule.Text = *body.Text
	}
	if body.Labels != nil {
		schedule.Labels = *body.Labels
	}
	if body.NextRunAt != nil {
		parsed, err := protocol.ParseTime(strings.TrimSpace(*body.NextRunAt))
		if err != nil {
			writeError(w, protocol.Invalid("nextRunAt: "+err.Error()))
			return
		}
		schedule.NextRunAt = parsed.UTC()
	}
	if body.EverySeconds != nil {
		schedule.EverySeconds = *body.EverySeconds
	}
	if body.UntilAt != nil {
		if strings.TrimSpace(*body.UntilAt) == "" {
			schedule.UntilAt = nil
		} else {
			parsed, err := protocol.ParseTime(strings.TrimSpace(*body.UntilAt))
			if err != nil {
				writeError(w, protocol.Invalid("untilAt: "+err.Error()))
				return
			}
			parsed = parsed.UTC()
			schedule.UntilAt = &parsed
		}
	}
	if body.Times != nil {
		schedule.Times = *body.Times
	}
	if body.IfUpdatedAt != nil {
		parsed, err := protocol.ParseTime(strings.TrimSpace(*body.IfUpdatedAt))
		if err != nil {
			writeError(w, protocol.Invalid("ifUpdatedAt: "+err.Error()))
			return
		}
		schedule.ExpectedUpdatedAt = &parsed
	}
	updated, err := s.automation.store.UpdateSchedule(req.ctx, schedule)
	if err != nil {
		s.failAutomation(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"schedule": scheduleViewOf(updated)})
}

// handleDeleteSchedule answers DELETE /schedules/{id}.
func (s *Server) handleDeleteSchedule(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	if perr := req.permitAutomationManage(s); perr != nil {
		writeError(w, perr)
		return
	}
	if err := s.automation.store.DeleteSchedule(req.ctx, req.tenant, strings.TrimSpace(r.PathValue("id"))); err != nil {
		s.failAutomation(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deleted": true})
}

// handleEnableSchedule answers POST /schedules/{id}/enable.
func (s *Server) handleEnableSchedule(w http.ResponseWriter, r *http.Request) {
	s.setScheduleEnabled(w, r, true)
}

// handleDisableSchedule answers POST /schedules/{id}/disable.
func (s *Server) handleDisableSchedule(w http.ResponseWriter, r *http.Request) {
	s.setScheduleEnabled(w, r, false)
}

func (s *Server) setScheduleEnabled(w http.ResponseWriter, r *http.Request, enabled bool) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	if perr := req.permitAutomationManage(s); perr != nil {
		writeError(w, perr)
		return
	}
	schedule, err := s.automation.store.SetScheduleEnabled(req.ctx, req.tenant,
		strings.TrimSpace(r.PathValue("id")), enabled, s.now().UTC())
	if err != nil {
		s.failAutomation(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"schedule": scheduleViewOf(schedule)})
}

// handleRetrySchedule answers POST /schedules/{id}/retry.
func (s *Server) handleRetrySchedule(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	if perr := req.permitAutomationManage(s); perr != nil {
		writeError(w, perr)
		return
	}
	if err := s.automation.store.RetrySchedule(req.ctx, req.tenant, strings.TrimSpace(r.PathValue("id"))); err != nil {
		s.failAutomation(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"retried": true})
}

// handleClearSchedule answers POST /schedules/{id}/clear: it removes a dead
// plan. A live or finished plan stays.
func (s *Server) handleClearSchedule(w http.ResponseWriter, r *http.Request) {
	req, ok := s.resolve(w, r)
	if !ok {
		return
	}
	if perr := req.permitAutomationManage(s); perr != nil {
		writeError(w, perr)
		return
	}
	if err := s.automation.store.ClearSchedule(req.ctx, req.tenant, strings.TrimSpace(r.PathValue("id"))); err != nil {
		s.failAutomation(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"cleared": true})
}

// failAutomation maps a store outcome to the board's error shape.
func (s *Server) failAutomation(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, automation.ErrNotFound):
		writeError(w, protocol.NotFound(protocol.CodeNotFound, "no such automation row"))
		return
	case errors.Is(err, automation.ErrExists):
		writeError(w, protocol.Conflict(protocol.CodeDuplicateID, "that automation ID or idempotency key is already used"))
		return
	case errors.Is(err, automation.ErrNotDead):
		writeError(w, protocol.NotFound(protocol.CodeNotFound, "no dead-lettered row with that id"))
		return
	case errors.Is(err, automation.ErrLeased):
		writeError(w, protocol.Conflict(protocol.CodeConflict, "the automation row is running; try again"))
		return
	case errors.Is(err, automation.ErrVersionConflict):
		writeError(w, protocol.Conflict(protocol.CodeConflict, "the automation row changed; read it again"))
		return
	case errors.Is(err, automation.ErrScheduleNotPending):
		writeError(w, protocol.Invalid("the schedule is finished or canceled"))
		return
	case errors.Is(err, automation.ErrWatcherDisabled):
		writeError(w, protocol.Invalid("the watcher is disabled"))
		return
	default:
		s.fail(w, r, err)
	}
}

// scheduleRequestDigest is the identity of a POST /schedules request. The same
// key and the same normalized content answers the first plan; a different
// digest is a 409, never a second schedule.
func scheduleRequestDigest(channel string, re *uint64, text string, labels map[string]string,
	actor protocol.Actor, nextRunKey string, every int, until *time.Time, times int) string {
	raw, _ := json.Marshal(struct {
		Channel     string            `json:"channel"`
		Re          *uint64           `json:"re,omitempty"`
		Text        string            `json:"text"`
		Labels      map[string]string `json:"labels"`
		Creator     string            `json:"creator"`
		CreatorKind string            `json:"creatorKind,omitempty"`
		CreatorVia  *protocol.Via     `json:"creatorVia,omitempty"`
		NextRunAt   string            `json:"nextRunAt,omitempty"`
		Every       int               `json:"every,omitempty"`
		Until       *string           `json:"until,omitempty"`
		Times       int               `json:"times,omitempty"`
	}{
		Channel: channel, Re: re, Text: text, Labels: labels,
		Creator: actor.ID, CreatorKind: string(actor.Kind), CreatorVia: actor.Via,
		NextRunAt: nextRunKey, Every: every,
		Until: protocol.FormatTimePtr(until), Times: times,
	})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
