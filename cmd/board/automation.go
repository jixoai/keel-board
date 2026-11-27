package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jixoai/keel-board/automation"
	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/server"
)

// automationFlags are the `board serve` settings for watchers and schedules.
// Automation is on whenever the process serves a board: without a dedicated
// DSN it uses the board's DSN, and without either it keeps declarations in
// memory (development only).
type automationFlags struct {
	tenant string
	dsn    string
	schema string
	poll   time.Duration
	lease  time.Duration
}

// registerAutomationFlags adds the automation flags to a serve flag set.
func registerAutomationFlags(flags *flag.FlagSet) *automationFlags {
	cfg := &automationFlags{}
	flags.StringVar(&cfg.tenant, "automation-tenant", "default", "the board tenant watchers and schedules serve")
	flags.StringVar(&cfg.dsn, "automation-dsn", "", "PostgreSQL DSN for automation state; empty uses KEEL_AUTOMATION_DSN, --dsn, or memory")
	flags.StringVar(&cfg.schema, "automation-schema", "", "schema for the automation tables; empty uses the database default")
	flags.DurationVar(&cfg.poll, "automation-poll", time.Second, "how long an idle automation worker waits between passes")
	flags.DurationVar(&cfg.lease, "automation-lease", time.Minute, "how long a worker holds a delivery or schedule lease")
	return cfg
}

// automationRuntime is what the serve process starts and stops.
type automationRuntime struct {
	store      automation.Store
	closeStore func()
	registry   *automation.Registry
	tenant     string
	poll       time.Duration
}

// options returns the management API's configuration.
func (r *automationRuntime) options(logger *slog.Logger) *server.AutomationOptions {
	return &server.AutomationOptions{Store: r.store, Actions: r.registry, Logger: logger}
}

// buildAutomation opens the automation store and registers the built-in
// actions. The post action is a deployment-declared board write; no action
// fetches a URL.
func buildAutomation(ctx context.Context, flags *automationFlags, boardDSN string) (*automationRuntime, error) {
	dsn := strings.TrimSpace(flags.dsn)
	if dsn == "" {
		dsn = strings.TrimSpace(os.Getenv("KEEL_AUTOMATION_DSN"))
	}
	if dsn == "" {
		dsn = strings.TrimSpace(boardDSN)
	}
	var (
		store      automation.Store
		closeStore func()
	)
	if dsn == "" {
		memory, err := automation.NewMemory(automation.Config{})
		if err != nil {
			return nil, err
		}
		store, closeStore = memory, func() {}
	} else {
		postgres, err := automation.OpenPostgres(ctx, automation.PostgresOptions{
			DSN: dsn, SearchPath: strings.TrimSpace(flags.schema),
			Config: automation.Config{LeaseTTL: flags.lease},
		})
		if err != nil {
			return nil, err
		}
		store, closeStore = postgres, postgres.Close
	}
	registry, err := automation.NewRegistryWith(automation.PostAction{})
	if err != nil {
		closeStore()
		return nil, err
	}
	tenant := strings.TrimSpace(flags.tenant)
	if tenant == "" {
		tenant = "default"
	}
	poll := flags.poll
	if poll <= 0 {
		poll = time.Second
	}
	return &automationRuntime{
		store: store, closeStore: closeStore, registry: registry,
		tenant: tenant, poll: poll,
	}, nil
}

// startAutomationWorkers runs one watcher worker and one schedule worker. The
// returned stop function cancels them, waits, and closes the store; it must
// run after the HTTP server has stopped accepting requests.
func startAutomationWorkers(ctx context.Context, logger *slog.Logger, runtime *automationRuntime, board *log.Board, authorize automation.Authorizer) (stop func(), err error) {
	if runtime == nil {
		return func() {}, nil
	}
	watchers, err := automation.NewWatcherWorker(automation.WatcherWorkerConfig{
		Store: runtime.store, Board: board, Actions: runtime.registry,
		Authorize: authorize, Tenant: runtime.tenant, Owner: "board-watcher",
		PollInterval: runtime.poll, Logger: logger,
	})
	if err != nil {
		runtime.closeStore()
		return nil, err
	}
	schedules, err := automation.NewScheduleWorker(automation.ScheduleWorkerConfig{
		Store: runtime.store, Board: board, Authorize: authorize,
		Tenant: runtime.tenant, Owner: "board-schedule",
		PollInterval: runtime.poll, Logger: logger,
	})
	if err != nil {
		runtime.closeStore()
		return nil, err
	}
	workerCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	start := func(name string, run func(context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := run(workerCtx); err != nil && workerCtx.Err() == nil {
				logger.Error("automation worker stopped", "worker", name, "error", automation.ErrorCode(err))
			}
		}()
	}
	start("watchers", watchers.Run)
	start("schedules", schedules.Run)
	stop = func() {
		cancel()
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			logger.Warn("automation workers did not stop in time")
		}
		runtime.closeStore()
	}
	return stop, nil
}
