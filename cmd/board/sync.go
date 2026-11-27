package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jixoai/keel-board/server"
	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/chatsdk"
	"github.com/jixoai/keel-board/sync/github"
	"github.com/jixoai/keel-board/sync/linear"
	"github.com/jixoai/keel-board/sync/slack"
	"github.com/jixoai/keel-board/syncstore"
)

// syncFlags are the `board serve` flags that enable sync. A process needs an
// explicit key list: an empty list serves no sync and mounts no webhook.
type syncFlags struct {
	tenant string
	dsn    string
	schema string
	keys   []string
	// mirrors is "key=platformChannel:boardChannel"; starts is
	// "key=platformChannel".
	mirrors []string
	starts  []string
	// fileHosts is the inbound file and link URL host allowlist.
	fileHosts []string
}

// registerSyncFlags adds the sync flags to a serve flag set.
func registerSyncFlags(flags *flag.FlagSet) *syncFlags {
	cfg := &syncFlags{}
	flags.StringVar(&cfg.tenant, "sync-tenant", "default", "the board tenant sync serves")
	flags.StringVar(&cfg.dsn, "sync-dsn", "", "PostgreSQL DSN for sync state; empty uses --dsn, or memory when --dsn is empty")
	flags.StringVar(&cfg.schema, "sync-schema", "", "schema for the sync tables; empty uses the database default")
	flags.Func("sync-key", "a connection key and platform, key=platform (repeatable)", func(value string) error {
		cfg.keys = append(cfg.keys, value)
		return nil
	})
	flags.Func("sync-mirror", "a platform channel mirrored into a board channel, key=platformChannel:boardChannel (repeatable)", func(value string) error {
		cfg.mirrors = append(cfg.mirrors, value)
		return nil
	})
	flags.Func("sync-start", "a platform channel a new board root may start a thread in, key=platformChannel (repeatable; defaults to the mirror channels)", func(value string) error {
		cfg.starts = append(cfg.starts, value)
		return nil
	})
	flags.Func("sync-file-host", "a host an inbound file or link URL may name, for example files.slack.com (repeatable; empty refuses every URL)", func(value string) error {
		cfg.fileHosts = append(cfg.fileHosts, value)
		return nil
	})
	return cfg
}

// enabled reports whether any sync connection was configured.
func (f *syncFlags) enabled() bool { return len(f.keys) > 0 }

// keyConfig is one parsed connection.
type keyConfig struct {
	key      boardsync.Key
	platform string
	// mirrors maps a native platform channel to the board channel it mirrors.
	mirrors map[string]string
	starts  []string
}

// parseSync returns the configured connections, keyed by sync key. The flags
// are explicit: a misspelled key or an entry without a mirror is an error, not
// a quietly disabled connection.
func (f *syncFlags) parseSync() (map[boardsync.Key]*keyConfig, error) {
	configs := make(map[boardsync.Key]*keyConfig, len(f.keys))
	for _, raw := range f.keys {
		key, platform, found := strings.Cut(strings.TrimSpace(raw), "=")
		if !found || strings.TrimSpace(platform) == "" {
			return nil, fmt.Errorf("sync-key %q: use key=platform", raw)
		}
		key = strings.TrimSpace(key)
		platform = strings.TrimSpace(platform)
		parsed := boardsync.Key(key)
		if !parsed.Valid() {
			return nil, fmt.Errorf("sync-key %q: %q isn't a sync key", raw, key)
		}
		if _, duplicate := configs[parsed]; duplicate {
			return nil, fmt.Errorf("sync-key %q is configured twice", key)
		}
		configs[parsed] = &keyConfig{key: parsed, platform: platform, mirrors: map[string]string{}}
	}
	for _, raw := range f.mirrors {
		key, rest, found := strings.Cut(strings.TrimSpace(raw), "=")
		if !found {
			return nil, fmt.Errorf("sync-mirror %q: use key=platformChannel:boardChannel", raw)
		}
		config, ok := configs[boardsync.Key(strings.TrimSpace(key))]
		if !ok {
			return nil, fmt.Errorf("sync-mirror %q names an unconfigured key", raw)
		}
		channel, board, found := strings.Cut(strings.TrimSpace(rest), ":")
		if !found || strings.TrimSpace(channel) == "" || strings.TrimSpace(board) == "" {
			return nil, fmt.Errorf("sync-mirror %q: use key=platformChannel:boardChannel", raw)
		}
		channel, board = strings.TrimSpace(channel), strings.TrimSpace(board)
		if existing, duplicate := config.mirrors[channel]; duplicate && existing != board {
			return nil, fmt.Errorf("sync-mirror %q: %s already mirrors %s", raw, channel, existing)
		}
		config.mirrors[channel] = board
	}
	for _, raw := range f.starts {
		key, channel, found := strings.Cut(strings.TrimSpace(raw), "=")
		if !found || strings.TrimSpace(channel) == "" {
			return nil, fmt.Errorf("sync-start %q: use key=platformChannel", raw)
		}
		config, ok := configs[boardsync.Key(strings.TrimSpace(key))]
		if !ok {
			return nil, fmt.Errorf("sync-start %q names an unconfigured key", raw)
		}
		config.starts = append(config.starts, strings.TrimSpace(channel))
	}
	for _, config := range configs {
		if len(config.mirrors) == 0 {
			return nil, fmt.Errorf("sync key %q needs at least one --sync-mirror", config.key)
		}
		if len(config.starts) == 0 {
			for channel := range config.mirrors {
				config.starts = append(config.starts, channel)
			}
		}
	}
	return configs, nil
}

// envName is the environment variable for one connection's setting.
func envName(key boardsync.Key, setting string) string {
	upper := strings.ToUpper(strings.ReplaceAll(string(key), "-", "_"))
	return "KEEL_SYNC_" + upper + "_" + setting
}

// secretFromEnv reads a required secret, naming the variable but never a value.
func secretFromEnv(key boardsync.Key, setting string) (boardsync.Secret, error) {
	name := envName(key, setting)
	secret, err := boardsync.SecretFromEnv(os.Getenv, name)
	if err != nil {
		return "", fmt.Errorf("sync key %q: %s", key, name)
	}
	return secret, nil
}

// optionalSecretFromEnv reads a secret that may be empty (a read-only app).
func optionalSecretFromEnv(key boardsync.Key, setting string) boardsync.Secret {
	return boardsync.Secret(os.Getenv(envName(key, setting)))
}

// buildProvider constructs the platform adapter for one connection from its
// documented environment variables. Credentials never come from flags (which
// are visible in a process list) or from the board log.
func buildProvider(config *keyConfig) (boardsync.Provider, error) {
	switch config.platform {
	case "slack":
		if err := requireIdentity(config.key, "BOT_USER_ID", "BOT_ID"); err != nil {
			return nil, err
		}
		signing, err := secretFromEnv(config.key, "SIGNING_SECRET")
		if err != nil {
			return nil, err
		}
		return slack.New(slack.Config{
			Key:           config.key,
			SigningSecret: signing,
			BotToken:      optionalSecretFromEnv(config.key, "BOT_TOKEN"),
			BotUserID:     os.Getenv(envName(config.key, "BOT_USER_ID")),
			BotID:         os.Getenv(envName(config.key, "BOT_ID")),
		})
	case "github":
		webhook, err := secretFromEnv(config.key, "WEBHOOK_SECRET")
		if err != nil {
			return nil, err
		}
		botID, _ := strconv.ParseInt(os.Getenv(envName(config.key, "BOT_USER_ID")), 10, 64)
		return github.New(github.Config{
			Key:           config.key,
			WebhookSecret: webhook,
			Token:         optionalSecretFromEnv(config.key, "TOKEN"),
			BotUserID:     botID,
			BotLogin:      os.Getenv(envName(config.key, "BOT_LOGIN")),
			Repos:         splitList(os.Getenv(envName(config.key, "REPOS"))),
			WriteRepos:    splitList(os.Getenv(envName(config.key, "WRITE_REPOS"))),
		})
	case "linear":
		if err := requireIdentity(config.key, "BOT_USER_ID"); err != nil {
			return nil, err
		}
		webhook, err := secretFromEnv(config.key, "WEBHOOK_SECRET")
		if err != nil {
			return nil, err
		}
		return linear.New(linear.Config{
			Key:           config.key,
			WebhookSecret: webhook,
			APIToken:      optionalSecretFromEnv(config.key, "TOKEN"),
			BotUserID:     os.Getenv(envName(config.key, "BOT_USER_ID")),
			TeamKeys:      splitList(os.Getenv(envName(config.key, "TEAMS"))),
		})
	case "chatsdk":
		return buildChatSDKProvider(config)
	default:
		return nil, fmt.Errorf("sync key %q: platform %q has no built-in adapter (slack, github, linear, chatsdk)", config.key, config.platform)
	}
}

// buildChatSDKProvider builds a signed-HTTP bridge entry. The sidecar's
// platform name, URL, shared secret, and own author IDs all come from the
// environment; the entry is registered under the sidecar's real platform so
// IDs, targets, and the bridge route agree.
func buildChatSDKProvider(config *keyConfig) (boardsync.Provider, error) {
	platform := boardsync.Platform(strings.TrimSpace(os.Getenv(envName(config.key, "PLATFORM"))))
	if strings.TrimSpace(string(platform)) == "" {
		return nil, fmt.Errorf("sync key %q: chatsdk needs %s (the sidecar's platform name)",
			config.key, envName(config.key, "PLATFORM"))
	}
	selfIDs := splitList(os.Getenv(envName(config.key, "SELF_IDS")))
	if len(selfIDs) == 0 {
		return nil, fmt.Errorf("sync key %q: chatsdk needs %s so its own posts are not imported",
			config.key, envName(config.key, "SELF_IDS"))
	}
	secret, err := secretFromEnv(config.key, "BRIDGE_SECRET")
	if err != nil {
		return nil, err
	}
	client, err := chatsdk.NewClient(chatsdk.ClientConfig{
		BaseURL:  os.Getenv(envName(config.key, "BRIDGE_URL")),
		Secret:   secret,
		Key:      config.key,
		Platform: platform,
		SelfIDs:  selfIDs,
	})
	if err != nil {
		return nil, fmt.Errorf("sync key %q: %w", config.key, err)
	}
	caps := boardsync.Capabilities{StartThreads: true, Edits: true, Deletes: true, Reactions: true}
	remote, err := chatsdk.NewRemoteAdapter(client, platform, caps)
	if err != nil {
		return nil, fmt.Errorf("sync key %q: %w", config.key, err)
	}
	return chatsdk.New(chatsdk.Config{Key: config.key, Adapter: remote, SelfIDs: selfIDs, Caps: caps})
}

// bridgeSecretFor returns the per-entry HMAC secret for a chatsdk connection.
func bridgeSecretFor(config *keyConfig) (boardsync.Secret, bool) {
	if config.platform != "chatsdk" {
		return "", false
	}
	return optionalSecretFromEnv(config.key, "BRIDGE_SECRET"), true
}

// requireIdentity refuses a connection that can't recognize its own posts.
// Slack and Linear carry no marker the platform hides, so without a bot ID an
// echo is inevitable.
func requireIdentity(key boardsync.Key, settings ...string) error {
	for _, setting := range settings {
		if strings.TrimSpace(os.Getenv(envName(key, setting))) != "" {
			return nil
		}
	}
	return fmt.Errorf("sync key %q needs one of %s so its own posts are not imported",
		key, strings.Join(prefixEnv(key, settings), " or "))
}

// prefixEnv names the settings' environment variables for an error message.
func prefixEnv(key boardsync.Key, settings []string) []string {
	names := make([]string, len(settings))
	for i, setting := range settings {
		names[i] = envName(key, setting)
	}
	return names
}

// splitList splits a comma-separated environment setting.
func splitList(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

// buildSyncOptions parses the flags, builds the adapters and entries, and
// upserts the tenant's connection records. The registry and store are the
// process's; the caller starts the workers.
func buildSyncOptions(ctx context.Context, flags *syncFlags, boardTenant string) (*serverSyncRuntime, error) {
	if !flags.enabled() {
		return nil, nil
	}
	configs, err := flags.parseSync()
	if err != nil {
		return nil, err
	}
	dsn := strings.TrimSpace(flags.dsn)
	if dsn == "" {
		dsn = strings.TrimSpace(os.Getenv("KEEL_SYNC_DSN"))
	}
	var store syncstore.Store
	var closeStore func()
	if dsn == "" {
		memory, err := syncstore.NewMemory(syncstore.Config{})
		if err != nil {
			return nil, err
		}
		store, closeStore = memory, func() {}
	} else {
		postgres, err := syncstore.OpenPostgres(ctx, syncstore.PostgresOptions{
			DSN: dsn, SearchPath: strings.TrimSpace(flags.schema),
		})
		if err != nil {
			return nil, err
		}
		store, closeStore = postgres, postgres.Close
	}
	tenant := strings.TrimSpace(flags.tenant)
	if tenant == "" {
		tenant = boardTenant
	}
	registry := boardsync.NewRegistry()
	startChannels := make(map[server.SyncKey][]string, len(configs))
	bridgeSecrets := make(map[boardsync.Key]boardsync.Secret)
	owned := make([]server.SyncKey, 0, len(configs))
	for _, config := range configs {
		provider, err := buildProvider(config)
		if err != nil {
			closeStore()
			return nil, err
		}
		platform := provider.Platform()
		routes := make(map[string]boardsync.SyncFunc, len(config.mirrors))
		for channel, board := range config.mirrors {
			routes[channel] = boardsync.Mirror(board)
		}
		fn, route := boardsync.Channels(routes)
		if err := registry.Add(boardsync.Entry{
			Tenant: tenant, Key: config.key, Platform: platform,
			Provider: provider, Sync: fn, Routes: route,
		}); err != nil {
			closeStore()
			return nil, err
		}
		if _, err := store.UpsertEntry(ctx, syncstore.Entry{
			Tenant: tenant, Key: config.key, Platform: platform, Enabled: true,
		}); err != nil {
			closeStore()
			return nil, err
		}
		if secret, ok := bridgeSecretFor(config); ok {
			bridgeSecrets[config.key] = secret
		}
		startChannels[server.SyncKey{Tenant: tenant, Key: config.key}] = append([]string(nil), config.starts...)
		owned = append(owned, server.SyncKey{Tenant: tenant, Key: config.key})
	}
	return &serverSyncRuntime{
		store: store, closeStore: closeStore, registry: registry,
		tenant: tenant, owned: owned, startChannels: startChannels,
		bridgeSecrets: bridgeSecrets,
		fileHosts:     append([]string(nil), flags.fileHosts...),
	}, nil
}

// serverSyncRuntime is what the serve process starts and stops.
type serverSyncRuntime struct {
	store         syncstore.Store
	closeStore    func()
	registry      *boardsync.Registry
	tenant        string
	owned         []server.SyncKey
	startChannels map[server.SyncKey][]string
	bridgeSecrets map[boardsync.Key]boardsync.Secret
	fileHosts     []string
}

// startSyncWorkers runs one inbound and one outbound worker per owned key and
// the board subscription. The returned stop function cancels them, waits, and
// closes the store.
func startSyncWorkers(ctx context.Context, logger *slog.Logger, runtime *serverSyncRuntime, srv *server.Server) (stop func(), err error) {
	if runtime == nil {
		return func() {}, nil
	}
	if err := srv.PrepareSync(ctx); err != nil {
		runtime.closeStore()
		return nil, err
	}
	runner, ok := srv.SyncRunnerFor()
	if !ok {
		runtime.closeStore()
		return nil, errors.New("board: sync is configured but the server has no runner")
	}
	host := srv.SyncHost()
	workerCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	cleanup := func() {
		cancel()
		wg.Wait()
		runtime.closeStore()
	}
	start := func(name string, run func(context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := run(workerCtx); err != nil && workerCtx.Err() == nil {
				logger.Error("sync worker stopped", "worker", name, "error", syncstore.ErrorCode(err))
			}
		}()
	}
	for _, owned := range runtime.owned {
		inbound, err := syncstore.NewInboundWorker(syncstore.InboundWorkerConfig{
			Store: runtime.store, Registry: runtime.registry, Runner: runner,
			Tenant: owned.Tenant, Key: owned.Key, Owner: "board-inbound-" + string(owned.Key),
			Logger: logger,
		})
		if err != nil {
			cleanup()
			return nil, err
		}
		outbound, err := syncstore.NewOutboundWorker(syncstore.OutboundWorkerConfig{
			Store: runtime.store, Registry: runtime.registry, Runner: runner, Host: host,
			Tenant: owned.Tenant, Keys: []boardsync.Key{owned.Key},
			Files: boardsync.FileLoader(host.LoadFiles), Owner: "board-outbound-" + string(owned.Key), Logger: logger,
		})
		if err != nil {
			cleanup()
			return nil, err
		}
		start("inbound/"+string(owned.Key), inbound.Run)
		start("outbound/"+string(owned.Key), outbound.Run)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := srv.RunSync(workerCtx); err != nil && workerCtx.Err() == nil {
			logger.Error("sync subscription stopped", "error", syncstore.ErrorCode(err))
		}
	}()
	var stopOnce sync.Once
	stop = func() {
		stopOnce.Do(func() {
			cancel()
			done := make(chan struct{})
			go func() {
				wg.Wait()
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				logger.Warn("sync workers did not stop in time")
			}
			runtime.closeStore()
		})
	}
	return stop, nil
}
