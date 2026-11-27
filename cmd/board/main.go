// Command board runs a phase-1 Keel board: the message, channel, task, claim,
// needs, label and event-feed core over HTTP.
//
//	board serve --addr 127.0.0.1:8790
//	board serve --dsn 'postgres://user:pass@host/db?sslmode=disable'
//
// Without --dsn the log lives in memory and is lost when the process exits.
// With --dsn it lives in PostgreSQL, and a restart replays it. Processes
// sharing a DSN see each other's writes within --freshness (one second by
// default): a board rechecks the store's head at that interval, not per
// request, and a write that loses a race catches up and decides again once.
//
// The development server trusts its caller headers (see server.DevActor) and
// allows every permission. It is for curl smoke tests and local work, never
// for a shared deployment; a deployment embeds server.New with its own
// Authorizer and actor verifier.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jixoai/keel-board/blob"
	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/server"
	"github.com/jixoai/keel-board/store"
	boardsync "github.com/jixoai/keel-board/sync"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "serve":
		if err := serve(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "board:", err)
			os.Exit(1)
		}
	case "rebuild":
		if err := rebuild(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "board:", err)
			os.Exit(1)
		}
	case "version":
		fmt.Println(server.Version)
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "board: unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `board: the Keel board server

Usage:
  board serve [--addr 127.0.0.1:8790] [--dsn postgres://...] [--freshness 1s] [--blobs <dir>] [--verbose]
              [--sync-tenant <tenant>] [--sync-dsn <dsn>] [--sync-schema <schema>]
              [--sync-key key=platform] [--sync-mirror key=platformChannel:boardChannel]
              [--sync-start key=platformChannel]
              [--automation-tenant <tenant>] [--automation-dsn <dsn>] [--automation-schema <schema>]
              [--automation-poll 1s] [--automation-lease 5m]
  board rebuild --dsn postgres://... [--tenant <tenant>]...
  board version

board rebuild replays each tenant's durable log into a cold projection and
prints its digest; a running server rebuilds through
POST /v1/board/admin/projection/rebuild. A rebuild never writes or deletes an
event.

With --blobs, attachment bytes and their records live under that directory;
without it they live in memory and are gone when the process ends.

Watchers and schedules are always served. Their state uses --automation-dsn,
else KEEL_AUTOMATION_DSN, else --dsn, else memory (development only). The
built-in post action writes a fixed message as the watcher's bot; other actions
are registered by an embedding deployment, never fetched from a URL.

Sync is disabled unless at least one --sync-key is given. Each key's
credentials come from KEEL_SYNC_<KEY>_* environment variables (SIGNING_SECRET or
WEBHOOK_SECRET, BOT_TOKEN/TOKEN, BOT_USER_ID, and the platform's allowlists);
for a chatsdk key (--sync-key key=chatsdk) use BRIDGE_URL, BRIDGE_SECRET,
PLATFORM, and SELF_IDS. Secrets never come from flags or the board log. One
process must own each key.
`)
}

// openStore opens the log's store: PostgreSQL when a DSN is given, memory
// otherwise. closeStore is never nil.
func openStore(ctx context.Context, dsn string) (events store.EventStore, closeStore func(), name string, err error) {
	if strings.TrimSpace(dsn) == "" {
		return store.NewMemory(), func() {}, "memory", nil
	}
	postgres, err := store.Open(ctx, dsn)
	if err != nil {
		return nil, nil, "", err
	}
	return postgres, postgres.Close, "postgres", nil
}

// openBlobs returns the attachment store a serve process uses: memory by
// default, files under dir when one is given.
func openBlobs(dir string) (blob.Store, error) {
	if dir == "" {
		return blob.NewMemory(), nil
	}
	files, err := blob.NewFS(dir)
	if err != nil {
		return nil, fmt.Errorf("blobs: %w", err)
	}
	return files, nil
}

// devHandler is the development server's handler over the board and blob store
// the serve process opened. It is what `board serve` runs, and what the smoke
// test drives.
func devHandler(logger *slog.Logger, board *log.Board, blobs blob.Store) http.Handler {
	return devServer(logger, board, blobs, nil)
}

// devServer is devHandler with explicit sync and automation surfaces; serve
// uses it.
func devServer(logger *slog.Logger, board *log.Board, blobs blob.Store, syncOptions *server.SyncOptions) *server.Server {
	return devServerWithAutomation(logger, board, blobs, syncOptions, nil)
}

func devServerWithAutomation(logger *slog.Logger, board *log.Board, blobs blob.Store, syncOptions *server.SyncOptions, automationOptions *server.AutomationOptions) *server.Server {
	return server.New(server.Options{
		Board: board, Logger: logger, Blobs: blobs,
		Sync: syncOptions, Automation: automationOptions,
	})
}

// serve runs the HTTP server until it's interrupted.
func serve(args []string) error {
	flags := flag.NewFlagSet("board serve", flag.ExitOnError)
	addr := flags.String("addr", "127.0.0.1:8790", "the address to listen on")
	dsn := flags.String("dsn", "", "PostgreSQL DSN for the board's log; empty keeps it in memory")
	freshness := flags.Duration("freshness", time.Second, "how stale a shared PostgreSQL cache may be; negative disables the check")
	blobsDir := flags.String("blobs", "", "directory for attachment files (default: memory)")
	verbose := flags.Bool("verbose", false, "log every request")
	syncCfg := registerSyncFlags(flags)
	automationCfg := registerAutomationFlags(flags)
	if err := flags.Parse(args); err != nil {
		return err
	}

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	events, closeStore, storeName, err := openStore(ctx, *dsn)
	if err != nil {
		return err
	}
	defer closeStore()

	blobs, err := openBlobs(*blobsDir)
	if err != nil {
		return err
	}

	board := log.New(log.Options{Store: events, Freshness: *freshness})
	var syncOptions *server.SyncOptions
	var syncRuntime *serverSyncRuntime
	if syncCfg.enabled() {
		if strings.TrimSpace(syncCfg.dsn) == "" && strings.TrimSpace(*dsn) != "" {
			syncCfg.dsn = *dsn
		}
		syncRuntime, err = buildSyncOptions(ctx, syncCfg, "default")
		if err != nil {
			return err
		}
		syncOptions = &server.SyncOptions{
			Store: syncRuntime.store, Registry: syncRuntime.registry,
			Tenant:        func(*http.Request) (string, error) { return syncRuntime.tenant, nil },
			Owned:         syncRuntime.owned,
			StartChannels: syncRuntime.startChannels,
			BridgeSecret: func(tenant string, key boardsync.Key) (boardsync.Secret, bool) {
				if tenant != syncRuntime.tenant {
					return "", false
				}
				secret, ok := syncRuntime.bridgeSecrets[key]
				return secret, ok
			},
			Logger: logger,
		}
	}
	automationRuntime, err := buildAutomation(ctx, automationCfg, *dsn)
	if err != nil {
		if syncRuntime != nil {
			syncRuntime.closeStore()
		}
		return err
	}
	api := devServerWithAutomation(logger, board, blobs, syncOptions, automationRuntime.options(logger))
	handler := http.Handler(api)
	stopSync, err := startSyncWorkers(ctx, logger, syncRuntime, api)
	if err != nil {
		automationRuntime.closeStore()
		return err
	}
	defer stopSync()
	stopAutomation, err := startAutomationWorkers(ctx, logger, automationRuntime, board, server.AllowAll{})
	if err != nil {
		stopSync()
		return err
	}
	defer stopAutomation()

	if *verbose {
		handler = requestLogger(logger, handler)
	}
	httpServer := &http.Server{
		Addr:              *addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		// No WriteTimeout: the event feed is a long-lived SSE stream.
	}

	serverErr := make(chan error, 1)
	go func() {
		logger.Info("board serving", "addr", "http://"+*addr+server.APIPrefix, "store", storeName)
		fmt.Printf("board listening on http://%s%s (store: %s)\n", *addr, server.APIPrefix, storeName)
		err := httpServer.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
		close(serverErr)
	}()

	select {
	case err := <-serverErr:
		return err
	case <-ctx.Done():
	}

	logger.Info("board shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return httpServer.Shutdown(shutdownCtx)
}

// requestLogger logs one line per request, after it finishes.
func requestLogger(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		next.ServeHTTP(w, r)
		logger.Debug("request",
			"method", r.Method,
			"path", strings.TrimSpace(r.URL.Path),
			"query", r.URL.RawQuery,
			"tenant", r.Header.Get("X-Board-Tenant"),
			"took", time.Since(started).String(),
		)
	})
}
