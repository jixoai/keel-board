// Command board runs a phase-1 Keel board: the message, channel, task, claim,
// needs, label and event-feed core over HTTP.
//
//	board serve --addr 127.0.0.1:8790
//	board serve --dsn 'postgres://user:pass@host/db?sslmode=disable'
//
// Without --dsn the log lives in memory and is lost when the process exits.
// With --dsn it lives in PostgreSQL, and a restart replays it.
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

	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/server"
	"github.com/jixoai/keel-board/store"
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
	fmt.Fprint(os.Stderr, `board: the Keel board server (phase 1)

Usage:
  board serve [--addr 127.0.0.1:8790] [--dsn postgres://...] [--verbose]
  board version
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

// serve runs the HTTP server until it's interrupted.
func serve(args []string) error {
	flags := flag.NewFlagSet("board serve", flag.ExitOnError)
	addr := flags.String("addr", "127.0.0.1:8790", "the address to listen on")
	dsn := flags.String("dsn", "", "PostgreSQL DSN for the board's log; empty keeps it in memory")
	verbose := flags.Bool("verbose", false, "log every request")
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

	board := log.New(log.Options{Store: events})
	api := server.New(server.Options{Board: board, Logger: logger})
	handler := http.Handler(api)
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
