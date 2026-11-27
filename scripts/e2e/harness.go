package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/jixoai/keel-board/blob"
	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/protocol"
	"github.com/jixoai/keel-board/server"
	"github.com/jixoai/keel-board/store"
	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/github"
	"github.com/jixoai/keel-board/sync/linear"
	"github.com/jixoai/keel-board/sync/slack"
	"github.com/jixoai/keel-board/syncstore"
)

const harnessTenant = "e2e"

// harness is the harness-hosted board server: the real server.New, the real
// durable syncstore, the real adapters, and the real workers, over PostgreSQL.
// It mirrors cmd/board's serve wiring (buildSyncOptions/startSyncWorkers); the
// only difference is that adapter HTTP clients point at the local TLS fake.
type harness struct {
	dsn    string
	schema string
	tenant string

	fake       *fakePlatform
	board      *log.Board
	boardStore *store.Postgres
	syncStore  *syncstore.Postgres
	server     *server.Server
	http       *http.Server
	listener   net.Listener
	baseURL    string
	registry   *boardsync.Registry
	adapters   map[boardsync.Key]boardsync.Provider

	cancel context.CancelFunc
	wg     sync.WaitGroup
	logBuf *syncBuffer
	logger *slog.Logger
}

// newHarness returns an unstarted harness over schema and the fake platform.
func newHarness(fake *fakePlatform, dsn, schema string) *harness {
	buffer := &syncBuffer{}
	return &harness{
		dsn: dsn, schema: schema, tenant: harnessTenant, fake: fake,
		logBuf: buffer,
		logger: slog.New(slog.NewTextHandler(buffer, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}
}

// Start opens PostgreSQL, builds the server, and starts its workers.
func (h *harness) Start(ctx context.Context) error {
	boardStore, err := store.OpenPostgres(ctx, store.PostgresOptions{DSN: h.dsn, SearchPath: h.schema})
	if err != nil {
		return fmt.Errorf("open board store: %w", err)
	}
	h.boardStore = boardStore
	h.board = log.New(log.Options{Store: boardStore, Freshness: -1})

	syncStore, err := syncstore.OpenPostgres(ctx, syncstore.PostgresOptions{
		DSN: h.dsn, SearchPath: h.schema,
		Config: syncstore.Config{
			InboundRetry:  boardsync.RetryPolicy{MaxAttempts: 5, BaseDelay: 30 * time.Millisecond, MaxDelay: 120 * time.Millisecond},
			OutboundRetry: boardsync.RetryPolicy{MaxAttempts: 3, BaseDelay: 30 * time.Millisecond, MaxDelay: 120 * time.Millisecond},
			LeaseTTL:      2 * time.Second,
		},
	})
	if err != nil {
		boardStore.Close()
		return fmt.Errorf("open syncstore: %w", err)
	}
	h.syncStore = syncStore

	registry, adapters, err := h.buildEntries(ctx)
	if err != nil {
		syncStore.Close()
		boardStore.Close()
		return err
	}
	h.registry = registry
	h.adapters = adapters

	api := server.New(server.Options{
		Board:  h.board,
		Logger: h.logger,
		Blobs:  blob.NewMemory(),
		Tenant: func(*http.Request) (string, error) { return h.tenant, nil },
		Sync: &server.SyncOptions{
			Store: syncStore, Registry: registry,
			Tenant: func(*http.Request) (string, error) { return h.tenant, nil },
			Owned: []server.SyncKey{
				{Tenant: h.tenant, Key: "slack"},
				{Tenant: h.tenant, Key: "github"},
				{Tenant: h.tenant, Key: "linear"},
			},
			StartChannels: map[server.SyncKey][]string{
				{Tenant: h.tenant, Key: "slack"}: {"C_SUPPORT"},
			},
			FileHosts: []string{"files.slack.com"},
			Logger:    h.logger,
		},
	})
	h.server = api
	if err := api.PrepareSync(ctx); err != nil {
		syncStore.Close()
		boardStore.Close()
		return fmt.Errorf("prepare sync: %w", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		syncStore.Close()
		boardStore.Close()
		return fmt.Errorf("listen: %w", err)
	}
	h.listener = listener
	h.baseURL = "http://" + listener.Addr().String()
	h.http = &http.Server{Handler: api, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = h.http.Serve(listener) }()

	workerCtx, cancel := context.WithCancel(ctx)
	h.cancel = cancel
	h.startWorkers(workerCtx)
	h.logger.Info("harness server started", "addr", h.baseURL, "schema", h.schema)
	return nil
}

// buildEntries constructs the adapters exactly as cmd/board does, but with the
// fake platform's HTTP client.
func (h *harness) buildEntries(ctx context.Context) (*boardsync.Registry, map[boardsync.Key]boardsync.Provider, error) {
	slackProvider, err := slack.New(slack.Config{
		Key: "slack", SigningSecret: "slack-e2e-signing", BotToken: "xoxb-e2e",
		BotUserID: "UBOT", BotID: "BBOT", HTTPClient: h.fake.HTTPClient(),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("slack adapter: %w", err)
	}
	githubProvider, err := github.New(github.Config{
		Key: "github", WebhookSecret: "github-e2e-secret", Token: "ghp_e2e",
		BotUserID: 7000, Repos: []string{"acme/www"}, WriteRepos: []string{"acme/www"},
		Doer: h.fake.HTTPClient(),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("github adapter: %w", err)
	}
	linearProvider, err := linear.New(linear.Config{
		Key: "linear", WebhookSecret: "linear-e2e-secret", APIToken: "lin_api_e2e",
		TeamKeys: []string{"ENG"}, BotUserID: "bot-linear", HTTPClient: h.fake.HTTPClient(),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("linear adapter: %w", err)
	}

	slackFn, slackRoutes := boardsync.Channels(map[string]boardsync.SyncFunc{
		"C_SUPPORT":  boardsync.Mirror("support"),
		"C0BACKFILL": boardsync.Mirror("backfill"),
		"C0FUTURE":   boardsync.Mirror("future"),
	})
	githubFn, githubRoutes := boardsync.Channels(map[string]boardsync.SyncFunc{
		"acme/www:pulls":  boardsync.Mirror("github"),
		"acme/www:issues": boardsync.Mirror("github"),
	})
	linearFn, linearRoutes := boardsync.Channels(map[string]boardsync.SyncFunc{
		"ENG": boardsync.Mirror("linear"),
	})
	entries := []boardsync.Entry{
		{Tenant: h.tenant, Key: "slack", Platform: "slack", Provider: slackProvider, Sync: slackFn, Routes: slackRoutes},
		{Tenant: h.tenant, Key: "github", Platform: "github", Provider: githubProvider, Sync: githubFn, Routes: githubRoutes},
		{Tenant: h.tenant, Key: "linear", Platform: "linear", Provider: linearProvider, Sync: linearFn, Routes: linearRoutes},
	}
	registry := boardsync.NewRegistry()
	for _, entry := range entries {
		if err := registry.Add(entry); err != nil {
			return nil, nil, err
		}
		if _, err := h.syncStore.UpsertEntry(ctx, syncstore.Entry{
			Tenant: h.tenant, Key: entry.Key, Platform: entry.Platform, Enabled: true,
		}); err != nil {
			return nil, nil, fmt.Errorf("upsert entry %s: %w", entry.Key, err)
		}
	}
	return registry, map[boardsync.Key]boardsync.Provider{
		"slack": slackProvider, "github": githubProvider, "linear": linearProvider,
	}, nil
}

// startWorkers runs one inbound and one outbound worker per key and the board
// subscription, mirroring cmd/board/startSyncWorkers.
func (h *harness) startWorkers(ctx context.Context) {
	runner := h.server.SyncRunner()
	host := h.server.SyncHost()
	for _, key := range []boardsync.Key{"slack", "github", "linear"} {
		inbound, err := syncstore.NewInboundWorker(syncstore.InboundWorkerConfig{
			Store: h.syncStore, Registry: h.registry, Runner: runner,
			Tenant: h.tenant, Key: key, Owner: "e2e-inbound-" + string(key),
			PollInterval: 10 * time.Millisecond, Defer: 50 * time.Millisecond, Logger: h.logger,
		})
		if err != nil {
			h.logger.Error("inbound worker", "key", key, "error", err)
			continue
		}
		outbound, err := syncstore.NewOutboundWorker(syncstore.OutboundWorkerConfig{
			Store: h.syncStore, Registry: h.registry, Runner: runner, Host: host,
			Tenant: h.tenant, Keys: []boardsync.Key{key}, Owner: "e2e-outbound-" + string(key),
			PollInterval: 10 * time.Millisecond, Defer: 50 * time.Millisecond, Logger: h.logger,
		})
		if err != nil {
			h.logger.Error("outbound worker", "key", key, "error", err)
			continue
		}
		h.wg.Add(1)
		go func() { defer h.wg.Done(); _ = inbound.Run(ctx) }()
		h.wg.Add(1)
		go func() { defer h.wg.Done(); _ = outbound.Run(ctx) }()
	}
	h.wg.Add(1)
	go func() { defer h.wg.Done(); _ = h.server.RunSync(ctx) }()
}

// Stop cancels workers, shuts the HTTP server, and closes both stores, leaving
// only PostgreSQL rows behind — exactly what a process restart leaves.
func (h *harness) Stop() {
	if h == nil {
		return
	}
	if h.cancel != nil {
		h.cancel()
		h.cancel = nil
	}
	done := make(chan struct{})
	go func() { h.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		h.logger.Warn("harness workers did not stop in time")
	}
	if h.http != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = h.http.Shutdown(ctx)
		cancel()
		h.http = nil
	}
	if h.listener != nil {
		_ = h.listener.Close()
		h.listener = nil
	}
	if h.syncStore != nil {
		h.syncStore.Close()
		h.syncStore = nil
	}
	if h.boardStore != nil {
		h.boardStore.Close()
		h.boardStore = nil
	}
	h.board = nil
	h.server = nil
	h.registry = nil
	h.adapters = nil
}

// Logs returns the captured server/worker logs.
func (h *harness) Logs() string { return h.logBuf.String() }

// webhook posts one signed platform webhook to the harness server.
func (h *harness) webhook(key string, headers map[string]string, body []byte) (int, []byte, error) {
	request, err := http.NewRequest(http.MethodPost, h.baseURL+"/webhooks/"+key, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	response, err := h.httpClient().Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	return response.StatusCode, data, err
}

// httpClient never uses a proxy: a CI proxy must not intercept fake traffic.
func (h *harness) httpClient() *http.Client {
	return &http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: 20 * time.Second}
}

// API calls the board API on the harness server.
func (h *harness) API(method, path string, body any) (int, []byte, error) {
	return h.APIAs("local:user:dev", method, path, body)
}

// APIAs calls the board API with an explicit development actor.
func (h *harness) APIAs(actor, method, path string, body any) (int, []byte, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequest(method, h.baseURL+server.APIPrefix+path, reader)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if actor != "" {
		request.Header.Set("X-Board-Actor", actor)
	}
	response, err := h.httpClient().Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
	return response.StatusCode, data, err
}

// ---------------------------------------------------------------- schema ----

// createSchema makes a per-run schema and returns a cleanup that drops it.
func createSchema(ctx context.Context, dsn, schema string) (func(), error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect for schema: %w", err)
	}
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		_ = admin.Close(ctx)
		return nil, fmt.Errorf("create schema %s: %w", schema, err)
	}
	cleanup := func() {
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer dropCancel()
		if _, err := admin.Exec(dropCtx, "DROP SCHEMA "+quoted+" CASCADE"); err != nil {
			fmt.Printf("warning: drop schema %s: %v\n", schema, err)
		}
		_ = admin.Close(dropCtx)
	}
	return cleanup, nil
}

// schemaDSN adds a search_path to a URL DSN for the real board serve binary.
func schemaDSN(dsn, schema string) (string, error) {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("KEEL_BOARD_TEST_DSN must be a postgres URL: %w", err)
	}
	values := parsed.Query()
	values.Set("search_path", schema)
	parsed.RawQuery = values.Encode()
	return parsed.String(), nil
}

// redactDSN keeps host/database for the report, never a password or user.
func redactDSN(dsn string) string {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return "(unparseable DSN)"
	}
	user := ""
	if parsed.User != nil {
		user = "***@"
	}
	query := ""
	if parsed.RawQuery != "" {
		query = "?" + parsed.RawQuery
	}
	return parsed.Scheme + "://" + user + parsed.Host + parsed.Path + query
}

// ------------------------------------------------------------- sync views ----

type syncStatusResponse struct {
	Keys []struct {
		Key   string `json:"key"`
		Owned bool   `json:"owned"`
		Inbox struct {
			Pending int `json:"pending"`
			Leased  int `json:"leased"`
			Done    int `json:"done"`
			Dead    int `json:"dead"`
		} `json:"inbox"`
	} `json:"keys"`
	Outbox struct {
		Pending int `json:"pending"`
		Leased  int `json:"leased"`
		Done    int `json:"done"`
		Dead    int `json:"dead"`
	} `json:"outbox"`
}

type syncInboxResponse struct {
	Dead []struct {
		ID       string `json:"id"`
		Key      string `json:"key"`
		Thread   string `json:"thread"`
		Attempts int    `json:"attempts"`
	} `json:"dead"`
}

type syncFailureResponse struct {
	Failures []struct {
		Delivery string `json:"delivery"`
		Message  string `json:"message"`
		Target   string `json:"target"`
		Attempts int    `json:"attempts"`
		Error    string `json:"error"`
	} `json:"failures"`
}

type syncThreadResponse struct {
	Threads []struct {
		BoardThread string `json:"boardThread"`
		Key         string `json:"key"`
		Platform    string `json:"platform"`
		Channel     string `json:"channel"`
		Thread      string `json:"thread"`
		Subthread   string `json:"subthread"`
		Kind        string `json:"kind"`
		Target      string `json:"target"`
	} `json:"threads"`
}

type syncAttachResponse struct {
	ID          string `json:"id"`
	BoardThread string `json:"boardThread"`
	Target      string `json:"target"`
	Kind        string `json:"kind"`
}

type threadResponse struct {
	Message protocol.MessageView   `json:"message"`
	Replies []protocol.MessageView `json:"replies"`
}

// ------------------------------------------------------------- utilities ----

// syncBuffer is a goroutine-safe log sink.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(data)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// waitFor polls until fn returns nil or the timeout expires.
func waitFor(timeout time.Duration, fn func() error) error {
	deadline := time.Now().Add(timeout)
	var last error
	for {
		if err := fn(); err == nil {
			return nil
		} else {
			last = err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out after %s: %w", timeout, last)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// decodeJSON decodes one response body.
func decodeJSON[T any](data []byte) (T, error) {
	var value T
	if err := json.Unmarshal(data, &value); err != nil {
		return value, fmt.Errorf("decode JSON: %w (body: %s)", err, clip(string(data), 800))
	}
	return value, nil
}

// appendBoardReply writes a reply directly through the log, simulating another
// process writing while the harness server is stopped.
func appendBoardReply(ctx context.Context, dsn, schema string, rootID uint64, text string) (uint64, error) {
	events, err := store.OpenPostgres(ctx, store.PostgresOptions{DSN: dsn, SearchPath: schema})
	if err != nil {
		return 0, err
	}
	defer events.Close()
	board := log.New(log.Options{Store: events, Freshness: -1})
	var id uint64
	err = board.Update(ctx, harnessTenant, log.UpdateOptions{}, func(tx *log.Tx) error {
		id = tx.NextID(0)
		tx.Emit(log.EventMessagePosted, &log.MessagePostedPayload{
			ID: id, Channel: "support", Text: text,
			Re: uint64Pointer(rootID), Thread: log.ThreadKey(rootID),
			Labels: map[string]string{},
			By:     protocol.Author{Actor: "local:user:ops", Principal: "local:user:ops"},
		})
		return nil
	})
	if err != nil {
		return 0, err
	}
	return id, nil
}

func uint64Pointer(value uint64) *uint64 { return &value }

// mustSample renders a small JSON view for the report.
func mustSample(value any) string {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Sprint(value)
	}
	return clip(string(encoded), 4000)
}

// trimBody makes a compact single-line body for step logs.
func trimBody(text string) string {
	return clip(strings.Join(strings.Fields(text), " "), 500)
}
