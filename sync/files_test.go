package sync_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/fake"
)

func fileMessage() boardsync.Message {
	return boardsync.Message{
		ID: 7, Thread: 1, Channel: "C0123", Text: "**Ada**: see attached", Actor: "local:user:ada",
		Attachments: []boardsync.AttachmentRef{{ID: "0123456789abcdefghijkl", Name: "log.txt", Type: "text/plain", Size: 5, MIMEType: "text/plain"}},
	}
}

func postTarget() boardsync.ResolvedTarget {
	return boardsync.ResolvedTarget{
		Key: "slack", Platform: "slack", Channel: "C0123", Thread: "C0123:1", Kind: boardsync.TargetThread,
	}
}

func TestOutboundFilesLoadIntoPost(t *testing.T) {
	provider := fake.New("slack", boardsync.Capabilities{UploadsFiles: true, Edits: true})
	provider.PostFunc = func(_ context.Context, _ boardsync.Key, _ boardsync.ResolvedTarget, msg boardsync.OutboundMessage) (boardsync.Posted, error) {
		if len(msg.Files) != 1 || string(msg.Files[0].Data) != "hello" || msg.Files[0].Filename != "log.txt" {
			t.Fatalf("post files = %+v", msg.Files)
		}
		return boardsync.Posted{Thread: "C0123:1", Message: "m1", Files: []string{"F1"}}, nil
	}
	entry := boardsync.Entry{Tenant: "t1", Key: "slack", Platform: "slack", Provider: provider, Sync: boardsync.Mirror("support")}
	host := fake.NewHost()
	runner := boardsync.NewRunner(host)
	loader := func(_ context.Context, tenant string, message boardsync.Message) (boardsync.FileLoadResult, error) {
		if tenant != "t1" || message.ID != 7 {
			t.Fatalf("loader saw %q, %d", tenant, message.ID)
		}
		return boardsync.FileLoadResult{Files: []boardsync.OutboundFile{{Data: []byte("hello"), Filename: "log.txt", MIMEType: "text/plain"}}}, nil
	}
	receipt, err := runner.OutboundWithOptions(t.Context(), entry, boardsync.BoardEvent{
		Tenant: "t1", Type: boardsync.BoardMessagePosted, Thread: 1,
	}, fileMessage(), targetPtr(postTarget()), "", boardsync.OutboundOptions{Files: loader})
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if !receipt.FilesSet || len(receipt.Files) != 1 || receipt.Files[0] != "F1" {
		t.Fatalf("receipt = %+v", receipt)
	}
	records := host.Records()
	if len(records) != 1 || !records[0].FilesSet || len(records[0].Files) != 1 {
		t.Fatalf("record = %+v", records)
	}
}

func TestOutboundFileUploaderFallback(t *testing.T) {
	provider := fake.New("slack", boardsync.Capabilities{UploadsFiles: true})
	provider.PostFunc = func(context.Context, boardsync.Key, boardsync.ResolvedTarget, boardsync.OutboundMessage) (boardsync.Posted, error) {
		return boardsync.Posted{Thread: "C0123:1", Message: "m1"}, nil
	}
	provider.UploadFilesFunc = func(_ context.Context, key boardsync.Key, thread boardsync.ThreadRef, files []boardsync.OutboundFile) ([]string, error) {
		if key != "slack" || thread.Thread != "C0123:1" || len(files) != 1 {
			t.Fatalf("upload saw %q %+v %d", key, thread, len(files))
		}
		return []string{"F9"}, nil
	}
	entry := boardsync.Entry{Tenant: "t1", Key: "slack", Platform: "slack", Provider: provider, Sync: boardsync.Mirror("support")}
	runner := boardsync.NewRunner(fake.NewHost())
	loader := func(context.Context, string, boardsync.Message) (boardsync.FileLoadResult, error) {
		return boardsync.FileLoadResult{Files: []boardsync.OutboundFile{{Data: []byte("x"), Filename: "a.bin"}}}, nil
	}
	receipt, err := runner.OutboundWithOptions(t.Context(), entry, boardsync.BoardEvent{
		Tenant: "t1", Type: boardsync.BoardMessagePosted, Thread: 1,
	}, fileMessage(), targetPtr(postTarget()), "", boardsync.OutboundOptions{Files: loader})
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if !receipt.FilesSet || len(receipt.Files) != 1 || receipt.Files[0] != "F9" {
		t.Fatalf("receipt = %+v", receipt)
	}
	if len(provider.Uploads()) != 1 {
		t.Fatalf("uploads = %+v", provider.Uploads())
	}
}

func TestOutboundFileUploadFailureBecomesNote(t *testing.T) {
	provider := fake.New("slack", boardsync.Capabilities{UploadsFiles: true, Edits: true})
	provider.PostFunc = func(context.Context, boardsync.Key, boardsync.ResolvedTarget, boardsync.OutboundMessage) (boardsync.Posted, error) {
		return boardsync.Posted{Thread: "C0123:1", Message: "m1"}, nil
	}
	provider.UploadFilesFunc = func(context.Context, boardsync.Key, boardsync.ThreadRef, []boardsync.OutboundFile) ([]string, error) {
		return nil, boardsync.ErrRateLimited
	}
	entry := boardsync.Entry{Tenant: "t1", Key: "slack", Platform: "slack", Provider: provider, Sync: boardsync.Mirror("support")}
	runner := boardsync.NewRunner(fake.NewHost())
	loader := func(context.Context, string, boardsync.Message) (boardsync.FileLoadResult, error) {
		return boardsync.FileLoadResult{Files: []boardsync.OutboundFile{{Data: []byte("x"), Filename: "a.bin"}}}, nil
	}
	receipt, err := runner.OutboundWithOptions(t.Context(), entry, boardsync.BoardEvent{
		Tenant: "t1", Type: boardsync.BoardMessagePosted, Thread: 1,
	}, fileMessage(), targetPtr(postTarget()), "", boardsync.OutboundOptions{Files: loader})
	if err != nil {
		t.Fatalf("upload failure lost the message: %v", err)
	}
	if !receipt.FilesSet || len(receipt.Files) != 0 {
		t.Fatalf("receipt = %+v", receipt)
	}
	edits := provider.Edits()
	if len(edits) != 1 {
		t.Fatalf("edits = %+v", edits)
	}
	if !strings.Contains(edits[0].Msg.Markdown, "a.bin") || !strings.Contains(edits[0].Msg.Markdown, "rate limited") {
		t.Fatalf("note edit = %q", edits[0].Msg.Markdown)
	}
	if len(edits[0].Msg.Files) != 0 {
		t.Fatalf("note edit carried files: %+v", edits[0].Msg.Files)
	}
}

func TestOutboundFilesOnPlatformWithoutUpload(t *testing.T) {
	provider := fake.New("linear", boardsync.Capabilities{})
	provider.PostFunc = func(_ context.Context, _ boardsync.Key, _ boardsync.ResolvedTarget, msg boardsync.OutboundMessage) (boardsync.Posted, error) {
		if len(msg.Files) != 0 {
			t.Fatalf("linear got files: %+v", msg.Files)
		}
		if !strings.Contains(msg.Markdown, "log.txt") || !strings.Contains(msg.Markdown, "carry no files") {
			t.Fatalf("note missing: %q", msg.Markdown)
		}
		return boardsync.Posted{Thread: "linear:ENG:1", Message: "c1"}, nil
	}
	entry := boardsync.Entry{Tenant: "t1", Key: "linear", Platform: "linear", Provider: provider, Sync: boardsync.Mirror("support")}
	runner := boardsync.NewRunner(fake.NewHost())
	called := false
	loader := func(context.Context, string, boardsync.Message) (boardsync.FileLoadResult, error) {
		called = true
		return boardsync.FileLoadResult{}, nil
	}
	target := boardsync.ResolvedTarget{Key: "linear", Platform: "linear", Channel: "ENG", Thread: "ENG:1", Kind: boardsync.TargetThread}
	receipt, err := runner.OutboundWithOptions(t.Context(), entry, boardsync.BoardEvent{
		Tenant: "t1", Type: boardsync.BoardMessagePosted, Thread: 1,
	}, fileMessage(), &target, "", boardsync.OutboundOptions{Files: loader})
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if called {
		t.Fatal("loader was called for a platform that can't upload")
	}
	if !receipt.FilesSet || len(receipt.Files) != 0 {
		t.Fatalf("receipt = %+v", receipt)
	}
}

func TestOutboundFileLoaderErrorBecomesNote(t *testing.T) {
	provider := fake.New("slack", boardsync.Capabilities{UploadsFiles: true})
	provider.PostFunc = func(_ context.Context, _ boardsync.Key, _ boardsync.ResolvedTarget, msg boardsync.OutboundMessage) (boardsync.Posted, error) {
		if !strings.Contains(msg.Markdown, "couldn't read") {
			t.Fatalf("note missing: %q", msg.Markdown)
		}
		return boardsync.Posted{Thread: "C0123:1", Message: "m1", Files: []string{}}, nil
	}
	entry := boardsync.Entry{Tenant: "t1", Key: "slack", Platform: "slack", Provider: provider, Sync: boardsync.Mirror("support")}
	runner := boardsync.NewRunner(fake.NewHost())
	loader := func(context.Context, string, boardsync.Message) (boardsync.FileLoadResult, error) {
		return boardsync.FileLoadResult{}, errors.New("store down")
	}
	if _, err := runner.OutboundWithOptions(t.Context(), entry, boardsync.BoardEvent{
		Tenant: "t1", Type: boardsync.BoardMessagePosted, Thread: 1,
	}, fileMessage(), targetPtr(postTarget()), "", boardsync.OutboundOptions{Files: loader}); err != nil {
		t.Fatalf("deliver: %v", err)
	}
}

func TestRunnerTyping(t *testing.T) {
	provider := fake.New("slack", boardsync.Capabilities{Typing: true})
	entry := boardsync.Entry{Tenant: "t1", Key: "slack", Platform: "slack", Provider: provider, Sync: boardsync.Mirror("support")}
	thread := boardsync.ThreadRef{Key: "slack", Platform: "slack", Channel: "C0123", Thread: "C0123:1"}
	if err := boardsync.NewRunner(fake.NewHost()).Typing(t.Context(), entry, thread, "", "writing"); err != nil {
		t.Fatalf("Typing: %v", err)
	}
	calls := provider.Typings()
	if len(calls) != 1 || calls[0].Status != "writing" || calls[0].Thread.Thread != "C0123:1" {
		t.Fatalf("typing calls = %+v", calls)
	}

	unsupported := fake.New("github", boardsync.Capabilities{})
	other := boardsync.Entry{Tenant: "t1", Key: "github", Platform: "github", Provider: unsupported, Sync: boardsync.Mirror("support")}
	if err := boardsync.NewRunner(fake.NewHost()).Typing(t.Context(), other, boardsync.ThreadRef{Key: "github", Channel: "acme/www:1"}, "", ""); !errors.Is(err, boardsync.ErrUnsupported) {
		t.Fatalf("unsupported Typing = %v", err)
	}
	if err := boardsync.NewRunner(fake.NewHost()).Typing(t.Context(), entry, boardsync.ThreadRef{Key: "other", Channel: "C0123"}, "", ""); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("foreign-key Typing = %v", err)
	}
}

func TestFileNoteIsInert(t *testing.T) {
	note := boardsync.FileNote("<@U1> [click](https://evil.test) & `x`", 2048, "too large")
	for _, forbidden := range []string{"<", ">", "[", "]", "`", "&"} {
		if strings.Contains(note, forbidden) {
			t.Fatalf("note %q kept %q", note, forbidden)
		}
	}
	if !strings.Contains(note, "2.0 KB") || !strings.Contains(note, "too large") {
		t.Fatalf("note = %q", note)
	}
}

func TestFileURLSafe(t *testing.T) {
	hosts := map[string]struct{}{"files.example.test": {}}
	for _, raw := range []string{
		"https://files.example.test/a",
		"https://files.example.test:443/a",
	} {
		if !boardsync.FileURLSafe(raw, hosts) {
			t.Fatalf("%q refused", raw)
		}
	}
	for _, raw := range []string{
		"http://files.example.test/a",
		"https://files.example.test:8443/a",
		"https://user:pass@files.example.test/a",
		"https://127.0.0.1/a",
		"https://[::1]/a",
		"https://evil.test/a",
	} {
		if boardsync.FileURLSafe(raw, hosts) {
			t.Fatalf("%q accepted", raw)
		}
	}
	if boardsync.FileURLSafe("https://files.example.test/a", nil) {
		t.Fatal("an empty allowlist accepted a URL")
	}
}

func targetPtr(target boardsync.ResolvedTarget) *boardsync.ResolvedTarget { return &target }
