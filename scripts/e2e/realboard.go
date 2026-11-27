package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"
)

// realBoard runs the actual `board serve` binary built from ./cmd/board. It
// proves the production command boots over PostgreSQL, serves the documented
// API, mounts sync, rejects unsigned webhooks, and resumes after a process
// restart. Full mirroring cannot run through this binary because cmd/board has
// no seam for a platform endpoint (and the harness must not modify product
// code); those scenarios use the harness-hosted server instead.
type realBoard struct {
	bin    string
	dsn    string
	schema string
	addr   string
	cmd    *exec.Cmd
	logs   *syncBuffer
	done   chan error
	base   string
}

// buildBoardBinary builds the production command into outDir.
func buildBoardBinary(ctx context.Context, outDir string) (string, error) {
	root, err := findRepoRoot()
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return "", err
	}
	bin := filepath.Join(outDir, "board")
	command := exec.CommandContext(ctx, "go", "build", "-trimpath", "-o", bin, "./cmd/board")
	command.Dir = root
	output, err := command.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("go build ./cmd/board: %w\n%s", err, output)
	}
	return bin, nil
}

// findRepoRoot walks up from the harness source and the working directory
// looking for this module's go.mod. No machine-specific path is hardcoded.
func findRepoRoot() (string, error) {
	var starts []string
	if cwd, err := os.Getwd(); err == nil {
		starts = append(starts, cwd)
	}
	if _, file, _, ok := runtime.Caller(0); ok {
		starts = append(starts, filepath.Dir(file))
	}
	for _, start := range starts {
		dir := start
		for {
			candidate := filepath.Join(dir, "go.mod")
			if data, err := os.ReadFile(candidate); err == nil {
				if strings.Contains(string(data), "module github.com/jixoai/keel-board") {
					return dir, nil
				}
			}
			parent := filepath.Dir(dir)
			if parent == dir {
				break
			}
			dir = parent
		}
	}
	return "", fmt.Errorf("cannot find the keel-board module root (run the harness from the repository)")
}

// newRealBoard prepares a process handle; Start launches it.
func newRealBoard(bin, dsn, schema string) *realBoard {
	return &realBoard{bin: bin, dsn: dsn, schema: schema, logs: &syncBuffer{}}
}

// Start launches board serve on a free loopback port and waits for discovery.
func (r *realBoard) Start(ctx context.Context) error {
	port, err := freePort()
	if err != nil {
		return err
	}
	dsn, err := schemaDSN(r.dsn, r.schema)
	if err != nil {
		return err
	}
	r.addr = fmt.Sprintf("127.0.0.1:%d", port)
	r.base = "http://" + r.addr
	command := exec.Command(r.bin, "serve",
		"--addr", r.addr,
		"--dsn", dsn,
		"--sync-dsn", dsn,
		"--sync-key", "slack=slack",
		"--sync-mirror", "slack=C_SUPPORT:support",
	)
	command.Env = append(os.Environ(),
		"KEEL_SYNC_SLACK_SIGNING_SECRET=slack-e2e-signing",
		"KEEL_SYNC_SLACK_BOT_USER_ID=UBOT",
		"KEEL_SYNC_SLACK_BOT_ID=BBOT",
	)
	command.Stdout = r.logs
	command.Stderr = r.logs
	if err := command.Start(); err != nil {
		return fmt.Errorf("start board serve: %w", err)
	}
	r.cmd = command
	r.done = make(chan error, 1)
	go func() { r.done <- command.Wait() }()
	err = waitFor(25*time.Second, func() error {
		select {
		case err := <-r.done:
			r.done <- err
			return fmt.Errorf("board serve exited early: %v\n%s", err, r.Logs())
		default:
		}
		status, _, requestErr := r.Request("", http.MethodGet, "", nil)
		if requestErr != nil {
			return requestErr
		}
		if status != http.StatusOK {
			return fmt.Errorf("discovery status %d", status)
		}
		return nil
	})
	if err != nil {
		_ = r.Stop()
		return fmt.Errorf("board serve didn't become ready: %w", err)
	}
	return nil
}

// Stop sends SIGTERM, waits for a clean exit, and turns a nonzero exit into an
// error. It is safe to call twice.
func (r *realBoard) Stop() error {
	if r == nil || r.cmd == nil || r.cmd.Process == nil {
		return nil
	}
	_ = r.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case err := <-r.done:
		r.cmd = nil
		if err != nil {
			return fmt.Errorf("board serve exited: %w\n%s", err, r.Logs())
		}
		return nil
	case <-time.After(15 * time.Second):
		_ = r.cmd.Process.Kill()
		<-r.done
		r.cmd = nil
		return fmt.Errorf("board serve didn't stop within 15s\n%s", r.Logs())
	}
}

// BaseURL is the board server's base URL.
func (r *realBoard) BaseURL() string { return r.base }

// Logs is everything the process wrote.
func (r *realBoard) Logs() string { return r.logs.String() }

// Request calls one board API path on the real serve process.
func (r *realBoard) Request(actor, method, path string, body any) (int, []byte, error) {
	return boardHTTPRequest(r.base, actor, method, path, body)
}

// boardHTTPRequest is the shared JSON client for both board servers.
func boardHTTPRequest(baseURL, actor, method, path string, body any) (int, []byte, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequest(method, baseURL+"/v1/board"+path, reader)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if actor != "" {
		request.Header.Set("X-Board-Actor", actor)
	}
	client := &http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: 20 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
	return response.StatusCode, data, err
}

// freePort reserves and releases a loopback port for a subprocess.
func freePort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
}
