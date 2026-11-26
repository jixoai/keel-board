// Command board runs a phase-1 Keel board: the message, channel, task, claim,
// needs, label and event-feed core over HTTP, with an in-memory log.
//
//	board serve --addr 127.0.0.1:8790
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
  board serve [--addr 127.0.0.1:8790] [--verbose]
  board version
`)
}

// serve runs the HTTP server until it's interrupted.
func serve(args []string) error {
	flags := flag.NewFlagSet("board serve", flag.ExitOnError)
	addr := flags.String("addr", "127.0.0.1:8790", "the address to listen on")
	verbose := flags.Bool("verbose", false, "log every request")
	if err := flags.Parse(args); err != nil {
		return err
	}

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	board := log.New(log.Options{})
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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serverErr := make(chan error, 1)
	go func() {
		logger.Info("board serving", "addr", "http://"+*addr+server.APIPrefix)
		fmt.Printf("board listening on http://%s%s\n", *addr, server.APIPrefix)
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
