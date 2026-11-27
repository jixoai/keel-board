package slack_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/slack"
)

func TestFetchFileBoundsTheBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("this body is longer than the limit"))
	}))
	defer server.Close()
	p := newProvider(t, server.URL, &http.Client{Transport: rewriteTransport{target: server.URL}}, func(c *slack.Config) {
		c.MaxFileBytes = 8
	})
	// The declaration is under the limit; only the body is too large.
	_, err := p.FetchFile(context.Background(), "slack", boardsync.File{
		Name: "a.txt", Size: 4, URL: "https://files.slack.com/files-pri/T-F/a.txt",
	})
	if !errors.Is(err, slack.ErrFileTooLarge) {
		t.Fatalf("FetchFile = %v", err)
	}
}

func TestFetchFileTimesOut(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(time.Second):
		}
	}))
	defer server.Close()
	client := &http.Client{
		Transport: rewriteTransport{target: server.URL},
		Timeout:   20 * time.Millisecond,
	}
	p := newProvider(t, server.URL, client, nil)
	start := time.Now()
	_, err := p.FetchFile(context.Background(), "slack", boardsync.File{
		URL: "https://files.slack.com/files-pri/T-F/a.txt",
	})
	if err == nil {
		t.Fatal("FetchFile succeeded after its timeout")
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("FetchFile waited %s", elapsed)
	}
}

func TestFetchFileChecksEveryRedirectHop(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch requests.Add(1) {
		case 1:
			http.Redirect(w, r, "https://files.slack.com/files-pri/T-F/next", http.StatusFound)
		default:
			// The second hop leaves Slack; the first was allowed.
			http.Redirect(w, r, "http://169.254.169.254/latest/meta-data/", http.StatusFound)
		}
	}))
	defer server.Close()
	p := newProvider(t, server.URL, &http.Client{Transport: rewriteTransport{target: server.URL}}, nil)
	_, err := p.FetchFile(context.Background(), "slack", boardsync.File{
		URL: "https://files.slack.com/files-pri/T-F/a.txt",
	})
	if err == nil {
		t.Fatal("FetchFile followed a redirect off Slack's domain")
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("requests = %d, want the allowed hop then a refusal", got)
	}
}
