package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jixoai/keel-board/blob"
	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/protocol"
	"github.com/jixoai/keel-board/server"
	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/syncstore"
)

type fileFetcherFunc func(ctx context.Context, key boardsync.Key, file boardsync.File) ([]byte, error)

func (f fileFetcherFunc) FetchFile(ctx context.Context, key boardsync.Key, file boardsync.File) ([]byte, error) {
	return f(ctx, key, file)
}

func newFetchHost(t *testing.T, mutate func(*server.SyncHostOptions)) (*log.Board, *blob.Memory, *server.SyncHost) {
	t.Helper()
	blobs := blob.NewMemory()
	board, host := newFetchHostOn(t, blobs, mutate)
	return board, blobs, host
}

func newFetchHostOn(t *testing.T, blobs blob.Store, mutate func(*server.SyncHostOptions)) (*log.Board, *server.SyncHost) {
	t.Helper()
	board := log.New(log.Options{})
	store, err := syncstore.NewMemory(syncstore.Config{})
	if err != nil {
		t.Fatal(err)
	}
	opts := server.SyncHostOptions{
		Blobs: blobs, FileHosts: []string{"files.example.test"}, MaxFileBytes: 1 << 20,
		FileFetchTimeout: time.Second,
	}
	if mutate != nil {
		mutate(&opts)
	}
	return board, server.NewSyncHost(board, store, opts)
}

// countingBlob counts Put calls so a retry can prove it didn't write bytes or
// metadata twice.
type countingBlob struct {
	blob.Store
	mu   sync.Mutex
	puts int
}

func (c *countingBlob) Put(ctx context.Context, key string, data []byte, contentType string) error {
	c.mu.Lock()
	c.puts++
	c.mu.Unlock()
	return c.Store.Put(ctx, key, data, contentType)
}

func (c *countingBlob) PutCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.puts
}

func fileEvent(id string, files ...boardsync.File) boardsync.Event {
	event := syncEvent(id, "C0123:1727.9")
	event.Text = "hello"
	event.Files = files
	return event
}

func TestSyncHostFetchesFilesIntoBlob(t *testing.T) {
	board, _, host := newFetchHost(t, nil)
	ctx := t.Context()
	fetches := 0
	fetcher := fileFetcherFunc(func(_ context.Context, _ boardsync.Key, file boardsync.File) ([]byte, error) {
		fetches++
		return []byte("hello"), nil
	})
	event := fileEvent("f1", boardsync.File{
		Name: "a.txt", Type: "file", MIMEType: "text/plain", Size: 5,
		URL: "https://files.example.test/a",
	})
	req := boardsync.ImportRequest{
		Tenant: "t1", Key: "slack", EventID: "f1", Channel: "support", Event: event, Fetcher: fetcher,
	}
	first, err := host.Import(ctx, req)
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	var ref boardsync.AttachmentRef
	var text string
	if err := board.Read("t1", func(snapshot *log.Snapshot) error {
		message := snapshot.Message(first.Message)
		if message == nil {
			t.Fatal("message missing")
		}
		text = message.Text
		if len(message.Attachments) != 1 {
			t.Fatalf("attachments = %+v", message.Attachments)
		}
		attachment := message.Attachments[0]
		if len(attachment.ID) != 22 || attachment.Name != "a.txt" || attachment.Size != 5 || attachment.Type != "text/plain" {
			t.Fatalf("attachment = %+v", attachment)
		}
		ref = boardsync.AttachmentRef{ID: attachment.ID, Name: attachment.Name, Type: attachment.Type, Size: attachment.Size}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(text, "📎") {
		t.Fatalf("a fetched file left a note: %q", text)
	}
	loaded, err := host.LoadFiles(ctx, "t1", boardsync.Message{Attachments: []boardsync.AttachmentRef{ref}})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(loaded.Files) != 1 || string(loaded.Files[0].Data) != "hello" || len(loaded.Notes) != 0 {
		t.Fatalf("loaded = %+v", loaded)
	}

	// A retry finds the same attachment instead of writing a second copy.
	second, err := host.Import(ctx, req)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if second != first {
		t.Fatalf("retry = %+v, first %+v", second, first)
	}
	if err := board.Read("t1", func(snapshot *log.Snapshot) error {
		message := snapshot.Message(second.Message)
		if len(message.Attachments) != 1 || message.Attachments[0].ID != ref.ID {
			t.Fatalf("retry attachment = %+v", message.Attachments)
		}
		if n := len(snapshot.MessagesInChannel("support")); n != 1 {
			t.Fatalf("messages = %d", n)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if fetches != 1 {
		// The retry finds the root it created and skips the fetch; the
		// deterministic ID also makes a fetch before a failed write safe.
		t.Fatalf("fetches = %d", fetches)
	}
}

func TestSyncHostWithoutAFetcherKeepsAPlaceholder(t *testing.T) {
	board, _, host := newFetchHost(t, nil)
	event := fileEvent("nf1", boardsync.File{
		Name: "a.txt", Size: 4, MIMEType: "text/plain", URL: "https://files.example.test/a",
	})
	result, err := host.Import(t.Context(), boardsync.ImportRequest{
		Tenant: "t1", Key: "slack", EventID: "nf1", Channel: "support", Event: event,
	})
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	if err := board.Read("t1", func(snapshot *log.Snapshot) error {
		message := snapshot.Message(result.Message)
		if len(message.Attachments) != 1 || len(message.Attachments[0].ID) != 22 {
			t.Fatalf("attachments = %+v", message.Attachments)
		}
		if strings.Contains(message.Text, "📎") {
			t.Fatalf("a placeholder added a fetch note: %q", message.Text)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSyncHostFileFetchFailuresBecomeNotes(t *testing.T) {
	board, _, host := newFetchHost(t, nil)
	event := fileEvent("f2",
		boardsync.File{Name: "secret.txt", Size: 4, URL: "https://files.example.test/secret"},
		boardsync.File{Name: "broken.bin", Size: 4, URL: "https://files.example.test/broken"},
	)
	fetcher := fileFetcherFunc(func(_ context.Context, _ boardsync.Key, file boardsync.File) ([]byte, error) {
		if strings.Contains(file.URL, "secret") {
			return nil, boardsync.ErrUnauthorized
		}
		return nil, boardsync.ErrRateLimited
	})
	result, err := host.Import(t.Context(), boardsync.ImportRequest{
		Tenant: "t1", Key: "slack", EventID: "f2", Channel: "support", Event: event, Fetcher: fetcher,
	})
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	if err := board.Read("t1", func(snapshot *log.Snapshot) error {
		message := snapshot.Message(result.Message)
		if message == nil {
			t.Fatal("message lost")
		}
		if !strings.Contains(message.Text, "hello") {
			t.Fatalf("original text lost: %q", message.Text)
		}
		for _, want := range []string{"secret.txt", "not authorized", "broken.bin", "rate limiting"} {
			if !strings.Contains(message.Text, want) {
				t.Fatalf("note missing %q: %q", want, message.Text)
			}
		}
		if len(message.Attachments) != 2 {
			t.Fatalf("placeholder attachments = %+v", message.Attachments)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	loaded, err := host.LoadFiles(t.Context(), "t1", boardsync.Message{Attachments: []boardsync.AttachmentRef{
		{ID: "0123456789abcdefghijkl", Name: "secret.txt", Size: 4},
	}})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(loaded.Files) != 0 || len(loaded.Notes) != 1 || !strings.Contains(loaded.Notes[0], "no longer on this board") {
		t.Fatalf("placeholder load = %+v", loaded)
	}
}

func TestSyncHostFileFetchTimeoutAndSize(t *testing.T) {
	board, _, host := newFetchHost(t, func(opts *server.SyncHostOptions) {
		opts.MaxFileBytes = 10
		opts.FileFetchTimeout = 30 * time.Millisecond
	})
	fetch := 0
	fetcher := fileFetcherFunc(func(ctx context.Context, _ boardsync.Key, file boardsync.File) ([]byte, error) {
		fetch++
		if strings.Contains(file.URL, "slow") {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		if strings.Contains(file.URL, "big") {
			return []byte(strings.Repeat("x", 20)), nil
		}
		return []byte("ok"), nil
	})
	event := fileEvent("f3",
		boardsync.File{Name: "declared.bin", Size: 20, URL: "https://files.example.test/declared"},
		boardsync.File{Name: "slow.bin", Size: 4, URL: "https://files.example.test/slow"},
		boardsync.File{Name: "big.bin", Size: 2, URL: "https://files.example.test/big"},
	)
	start := time.Now()
	result, err := host.Import(t.Context(), boardsync.ImportRequest{
		Tenant: "t1", Key: "slack", EventID: "f3", Channel: "support", Event: event, Fetcher: fetcher,
	})
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("import blocked for %s", elapsed)
	}
	if fetch != 2 {
		t.Fatalf("fetches = %d, want the slow and big files only", fetch)
	}
	if err := board.Read("t1", func(snapshot *log.Snapshot) error {
		message := snapshot.Message(result.Message)
		for _, want := range []string{"declared.bin", "10 B file limit", "slow.bin", "timed out", "big.bin"} {
			if !strings.Contains(message.Text, want) {
				t.Fatalf("note missing %q: %q", want, message.Text)
			}
		}
		if len(message.Attachments) != 3 {
			t.Fatalf("attachments = %+v", message.Attachments)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSyncHostFileCountLimit(t *testing.T) {
	board, _, host := newFetchHost(t, func(opts *server.SyncHostOptions) { opts.MaxFiles = 1 })
	event := fileEvent("f4",
		boardsync.File{Name: "one.txt", Size: 1, URL: "https://files.example.test/one"},
		boardsync.File{Name: "two.txt", Size: 1, URL: "https://files.example.test/two"},
	)
	fetches := 0
	fetcher := fileFetcherFunc(func(context.Context, boardsync.Key, boardsync.File) ([]byte, error) {
		fetches++
		return []byte("x"), nil
	})
	result, err := host.Import(t.Context(), boardsync.ImportRequest{
		Tenant: "t1", Key: "slack", EventID: "f4", Channel: "support", Event: event, Fetcher: fetcher,
	})
	if err != nil {
		t.Fatal(err)
	}
	if fetches != 1 {
		t.Fatalf("fetches = %d", fetches)
	}
	if err := board.Read("t1", func(snapshot *log.Snapshot) error {
		message := snapshot.Message(result.Message)
		if len(message.Attachments) != 1 || message.Attachments[0].Name != "one.txt" {
			t.Fatalf("attachments = %+v", message.Attachments)
		}
		if !strings.Contains(message.Text, "two.txt") || !strings.Contains(message.Text, "at most 1 files") {
			t.Fatalf("count note = %q", message.Text)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSyncHostRefusesUnsafeFileURLWithoutFetching(t *testing.T) {
	board, _, host := newFetchHost(t, func(opts *server.SyncHostOptions) {
		// Even an explicitly listed IP is refused: an allowlist names
		// services, not cloud metadata endpoints.
		opts.FileHosts = append(opts.FileHosts, "127.0.0.1")
	})
	fetches := 0
	fetcher := fileFetcherFunc(func(context.Context, boardsync.Key, boardsync.File) ([]byte, error) {
		fetches++
		return nil, nil
	})
	event := fileEvent("f5",
		boardsync.File{Name: "meta.txt", Size: 1, URL: "http://169.254.169.254/latest/meta-data/"},
		boardsync.File{Name: "foreign.txt", Size: 1, URL: "https://evil.test/a"},
		boardsync.File{Name: "loopback.txt", Size: 1, URL: "https://127.0.0.1/a"},
		boardsync.File{Name: "missing.txt", Size: 1},
	)
	result, err := host.Import(t.Context(), boardsync.ImportRequest{
		Tenant: "t1", Key: "slack", EventID: "f5", Channel: "support", Event: event, Fetcher: fetcher,
	})
	if err != nil {
		t.Fatal(err)
	}
	if fetches != 0 {
		t.Fatalf("fetches = %d, want none", fetches)
	}
	if err := board.Read("t1", func(snapshot *log.Snapshot) error {
		message := snapshot.Message(result.Message)
		for _, want := range []string{"file hosts", "loopback.txt", "missing.txt", "download URL isn't available"} {
			if !strings.Contains(message.Text, want) {
				t.Fatalf("unsafe note missing %q: %q", want, message.Text)
			}
		}
		if len(message.Attachments) != 4 {
			t.Fatalf("metadata placeholders = %+v", message.Attachments)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSyncHostLinksKeepSafeSnapshotAndNeverFetch(t *testing.T) {
	board, _, host := newFetchHost(t, nil)
	event := fileEvent("l1")
	event.Text = "linking"
	event.Links = []boardsync.Link{
		{
			URL: "https://files.example.test/archives/C1/p1", Message: "1727.1",
			Available: true,
			Snapshot:  &boardsync.LinkSnapshot{Author: "Ada", Text: "the deploy is green"},
		},
		{URL: "https://evil.test/x", Message: "2"},
		{URL: "http://files.example.test/y"},
	}
	result, err := host.Import(t.Context(), boardsync.ImportRequest{
		Tenant: "t1", Key: "slack", EventID: "l1", Channel: "support", Event: event,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := board.Read("t1", func(snapshot *log.Snapshot) error {
		message := snapshot.Message(result.Message)
		if len(message.Links) != 3 {
			t.Fatalf("links = %+v", message.Links)
		}
		first := linkAt(t, message.Links, 0)
		if first.URL != "https://files.example.test/archives/C1/p1" || first.Snapshot == nil {
			t.Fatalf("safe link = %+v", first)
		}
		if !strings.Contains(*first.Snapshot, "Ada") {
			t.Fatalf("snapshot = %q", *first.Snapshot)
		}
		for index := 1; index < 3; index++ {
			link := linkAt(t, message.Links, index)
			if link.URL != "" || link.Unavailable == "" {
				t.Fatalf("unsafe link kept a URL: %+v", link)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSyncHostLoadFilesBoundsAndMissing(t *testing.T) {
	_, _, host := newFetchHost(t, func(opts *server.SyncHostOptions) { opts.MaxFileBytes = 10 })
	loaded, err := host.LoadFiles(t.Context(), "t1", boardsync.Message{Attachments: []boardsync.AttachmentRef{
		{ID: "0123456789abcdefghijkl", Name: "tiny.bin", Size: 4},
		{ID: "0123456789abcdefghijkm", Name: "huge.bin", Size: 20},
		{ID: "bad", Name: "bad.bin", Size: 1},
	}})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(loaded.Files) != 0 || len(loaded.Notes) != 3 {
		t.Fatalf("loaded = %+v", loaded)
	}
	for _, note := range loaded.Notes {
		if !strings.Contains(note, "📎") {
			t.Fatalf("note = %q", note)
		}
	}
}

func TestSyncDeliversBoardAttachmentBytes(t *testing.T) {
	h := newSyncHarness(t, syncHarnessOptions{startWorkers: false})
	h.provider.Caps.UploadsFiles = true
	h.startWorkers(t)

	attachment := h.uploadAttachment(t, "log.txt", "text/plain", []byte("hello bytes"))
	h.postMessage(map[string]any{"channel": "support", "text": "with a file", "attachments": []string{attachment.ID}})
	h.waitPosts(1)

	posts := h.provider.Posts()
	if len(posts) != 1 || len(posts[0].Msg.Files) != 1 {
		t.Fatalf("post = %+v", posts)
	}
	if string(posts[0].Msg.Files[0].Data) != "hello bytes" || posts[0].Msg.Files[0].Filename != "log.txt" {
		t.Fatalf("post file = %+v", posts[0].Msg.Files[0])
	}
	deadline := time.Now().Add(5 * time.Second)
	for len(h.provider.Uploads()) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	uploads := h.provider.Uploads()
	if len(uploads) != 1 || len(uploads[0].Files) != 1 || string(uploads[0].Files[0].Data) != "hello bytes" {
		t.Fatalf("uploads = %+v", uploads)
	}
}

func TestSyncAttachmentNotesOnAPlatformWithoutUpload(t *testing.T) {
	h := newSyncHarness(t, syncHarnessOptions{startWorkers: false})
	h.provider.Caps.UploadsFiles = false
	h.startWorkers(t)

	attachment := h.uploadAttachment(t, "log.txt", "text/plain", []byte("hello bytes"))
	h.postMessage(map[string]any{"channel": "support", "text": "with a file", "attachments": []string{attachment.ID}})
	h.waitPosts(1)
	posts := h.provider.Posts()
	if len(posts) != 1 || len(posts[0].Msg.Files) != 0 {
		t.Fatalf("post = %+v", posts)
	}
	if !strings.Contains(posts[0].Msg.Markdown, "log.txt") || !strings.Contains(posts[0].Msg.Markdown, "carry no files") {
		t.Fatalf("markdown = %q", posts[0].Msg.Markdown)
	}
	if len(h.provider.Uploads()) != 0 {
		t.Fatalf("uploads = %+v", h.provider.Uploads())
	}
}

func TestSyncHostFetchesFilesIntoFSBlob(t *testing.T) {
	files, err := blob.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	board := log.New(log.Options{})
	store, err := syncstore.NewMemory(syncstore.Config{})
	if err != nil {
		t.Fatal(err)
	}
	host := server.NewSyncHost(board, store, server.SyncHostOptions{
		Blobs: files, FileHosts: []string{"files.example.test"},
	})
	fetcher := fileFetcherFunc(func(context.Context, boardsync.Key, boardsync.File) ([]byte, error) {
		return []byte("fs bytes"), nil
	})
	event := fileEvent("fs1", boardsync.File{
		Name: "a.txt", Size: 8, MIMEType: "text/plain", URL: "https://files.example.test/a",
	})
	result, err := host.Import(t.Context(), boardsync.ImportRequest{
		Tenant: "t1", Key: "slack", EventID: "fs1", Channel: "support", Event: event, Fetcher: fetcher,
	})
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	var ref boardsync.AttachmentRef
	if err := board.Read("t1", func(snapshot *log.Snapshot) error {
		message := snapshot.Message(result.Message)
		if len(message.Attachments) != 1 {
			t.Fatalf("attachments = %+v", message.Attachments)
		}
		ref = boardsync.AttachmentRef{ID: message.Attachments[0].ID, Name: message.Attachments[0].Name}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	loaded, err := host.LoadFiles(t.Context(), "t1", boardsync.Message{Attachments: []boardsync.AttachmentRef{ref}})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(loaded.Files) != 1 || string(loaded.Files[0].Data) != "fs bytes" {
		t.Fatalf("loaded = %+v", loaded)
	}
}

func TestSyncHostCapsFilesAtProtocolMax(t *testing.T) {
	board, _, host := newFetchHost(t, func(opts *server.SyncHostOptions) { opts.MaxFiles = 32 })
	files := make([]boardsync.File, 0, 12)
	for index := 0; index < 12; index++ {
		files = append(files, boardsync.File{
			Name: fmt.Sprintf("f%02d.bin", index), Size: 1,
			URL: fmt.Sprintf("https://files.example.test/%d", index),
		})
	}
	event := fileEvent("max1", files...)
	fetcher := fileFetcherFunc(func(context.Context, boardsync.Key, boardsync.File) ([]byte, error) {
		return []byte("x"), nil
	})
	result, err := host.Import(t.Context(), boardsync.ImportRequest{
		Tenant: "t1", Key: "slack", EventID: "max1", Channel: "support", Event: event, Fetcher: fetcher,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := board.Read("t1", func(snapshot *log.Snapshot) error {
		message := snapshot.Message(result.Message)
		if len(message.Attachments) != protocol.AttachmentsPerMessageMax {
			t.Fatalf("attachments = %d, want %d", len(message.Attachments), protocol.AttachmentsPerMessageMax)
		}
		if !strings.Contains(message.Text, "at most 10 files") {
			t.Fatalf("cap note missing: %q", message.Text)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSyncHostAttachmentIDsAreDeterministicAndDistinct(t *testing.T) {
	counting := &countingBlob{Store: blob.NewMemory()}
	board, host := newFetchHostOn(t, counting, nil)
	event := fileEvent("ids1",
		boardsync.File{Name: "same.bin", Size: 1, URL: "https://files.example.test/same"},
		boardsync.File{Name: "same.bin", Size: 1, URL: "https://files.example.test/same"},
	)
	fetcher := fileFetcherFunc(func(context.Context, boardsync.Key, boardsync.File) ([]byte, error) {
		return []byte("x"), nil
	})
	req := boardsync.ImportRequest{Tenant: "t1", Key: "slack", EventID: "ids1", Channel: "support", Event: event, Fetcher: fetcher}
	result, err := host.Import(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	if err := board.Read("t1", func(snapshot *log.Snapshot) error {
		for _, attachment := range snapshot.Message(result.Message).Attachments {
			ids = append(ids, attachment.ID)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || ids[0] == ids[1] {
		t.Fatalf("ids = %v, want two distinct deterministic IDs", ids)
	}
	if _, err := host.Import(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	if got := counting.PutCount(); got != 4 {
		// Two files, each one data object and one record; the retry added none.
		t.Fatalf("blob puts = %d, want 4", got)
	}
}

func TestSyncHostReplyRetryDoesNotDuplicateBlob(t *testing.T) {
	counting := &countingBlob{Store: blob.NewMemory()}
	board, host := newFetchHostOn(t, counting, nil)
	ctx := t.Context()
	rootEvent := syncEvent("root", "C0123:T9")
	if _, err := host.Import(ctx, boardsync.ImportRequest{
		Tenant: "t1", Key: "slack", EventID: "root", Channel: "support", Event: rootEvent,
	}); err != nil {
		t.Fatal(err)
	}
	reply := syncEvent("reply", "C0123:T9")
	reply.IsRoot, reply.Message, reply.Text = false, "reply", "with a file"
	reply.Files = []boardsync.File{{Name: "a.bin", Size: 1, URL: "https://files.example.test/a"}}
	fetcher := fileFetcherFunc(func(context.Context, boardsync.Key, boardsync.File) ([]byte, error) {
		return []byte("x"), nil
	})
	req := boardsync.ImportRequest{Tenant: "t1", Key: "slack", EventID: "reply", Event: reply, Fetcher: fetcher}
	if _, err := host.Import(ctx, req); err != nil {
		t.Fatal(err)
	}
	if _, err := host.Import(ctx, req); err != nil {
		t.Fatal(err)
	}
	if got := counting.PutCount(); got != 2 {
		// One data object + one record; the retry found both.
		t.Fatalf("blob puts = %d, want 2", got)
	}
	if err := board.Read("t1", func(snapshot *log.Snapshot) error {
		messages := snapshot.MessagesInChannel("support")
		if len(messages) != 2 {
			t.Fatalf("messages = %d, want root + one reply", len(messages))
		}
		for _, message := range messages {
			if strings.Contains(message.Text, "with a file") {
				if len(message.Attachments) != 1 {
					t.Fatalf("reply attachments = %+v", message.Attachments)
				}
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSyncHostLinkSnapshotIsBounded(t *testing.T) {
	board, _, host := newFetchHost(t, nil)
	event := fileEvent("link-big")
	event.Text = "big link"
	event.Links = []boardsync.Link{{
		URL:      "https://files.example.test/archives/C1/p1",
		Snapshot: &boardsync.LinkSnapshot{Author: "Ada", Text: strings.Repeat("x", 40<<10)},
	}}
	result, err := host.Import(t.Context(), boardsync.ImportRequest{
		Tenant: "t1", Key: "slack", EventID: "link-big", Channel: "support", Event: event,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := board.Read("t1", func(snapshot *log.Snapshot) error {
		link := linkAt(t, snapshot.Message(result.Message).Links, 0)
		if link.URL == "" {
			t.Fatalf("safe URL was dropped with the snapshot: %+v", link)
		}
		if link.Snapshot != nil {
			t.Fatalf("oversized snapshot stored (%d bytes)", len(*link.Snapshot))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func linkAt(t *testing.T, links []any, index int) protocol.LinkView {
	t.Helper()
	if index >= len(links) {
		t.Fatalf("link %d missing from %+v", index, links)
	}
	switch link := links[index].(type) {
	case protocol.LinkView:
		return link
	case *protocol.LinkView:
		return *link
	default:
		t.Fatalf("link %d has type %T", index, links[index])
		return protocol.LinkView{}
	}
}

func (h *syncHarness) uploadAttachment(t *testing.T, name, contentType string, data []byte) protocol.Attachment {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, h.http.URL+server.APIPrefix+"/attachments", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", contentType)
	request.Header.Set("X-Board-Filename", url.PathEscape(name))
	response, err := h.http.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var decoded protocol.AttachmentResponse
	if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil || response.StatusCode != http.StatusCreated {
		t.Fatalf("upload = %d %v", response.StatusCode, err)
	}
	return decoded.Attachment
}
