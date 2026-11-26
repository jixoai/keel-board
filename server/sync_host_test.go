package server_test

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/protocol"
	"github.com/jixoai/keel-board/server"
	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/syncstore"
)

func newSyncHost(t *testing.T) (*log.Board, *syncstore.Memory, *server.SyncHost) {
	t.Helper()
	board := log.New(log.Options{})
	store, err := syncstore.NewMemory(syncstore.Config{})
	if err != nil {
		t.Fatalf("new memory store: %v", err)
	}
	host := server.NewSyncHost(board, store, server.SyncHostOptions{
		FileHosts: []string{"files.example.test"},
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	return board, store, host
}

func syncEvent(id, thread string) boardsync.Event {
	return boardsync.Event{
		ID: id, Key: "slack", Platform: "slack",
		Kind: boardsync.KindMessage, Type: boardsync.TypeMessage,
		Channel: "C0123", Thread: thread, Message: id, IsRoot: true,
		Text:   "hello " + id,
		Author: boardsync.Author{ID: "slack:user:U1", Handle: "ada", Name: "Ada"},
	}
}

func TestSyncHostImportRootAndReplyIsIdempotent(t *testing.T) {
	board, store, host := newSyncHost(t)
	ctx := t.Context()
	rootEvent := syncEvent("m1", "C0123:1727.9")
	req := boardsync.ImportRequest{Tenant: "t1", Key: "slack", EventID: rootEvent.ID, Channel: "support", Event: rootEvent}
	first, err := host.Import(ctx, req)
	if err != nil {
		t.Fatalf("import root: %v", err)
	}
	if !first.CreatedRoot || first.Root == 0 || first.Message != first.Root {
		t.Fatalf("root result = %+v", first)
	}
	second, err := host.Import(ctx, req)
	if err != nil {
		t.Fatalf("retry root: %v", err)
	}
	if second != first {
		t.Fatalf("retry = %+v, first %+v", second, first)
	}
	var messages int
	if err := board.Read("t1", func(snapshot *log.Snapshot) error {
		messages = len(snapshot.MessagesInChannel("support"))
		message := snapshot.Message(first.Root)
		if message == nil {
			t.Fatalf("root message missing")
		}
		if message.Text != "hello m1" || message.From == nil || *message.From != "slack" {
			t.Fatalf("root = %+v", message)
		}
		if message.Origin == nil || message.Origin.Thread != "slack:C0123:1727.9" || message.Origin.Message != "m1" {
			t.Fatalf("origin = %+v", message.Origin)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if messages != 1 {
		t.Fatalf("messages = %d, want one after retry", messages)
	}
	thread, found, err := store.ThreadByTarget(ctx, "t1", "slack", boardsync.ResolvedTarget{
		Key: "slack", Platform: "slack", Channel: "C0123", Thread: "C0123:1727.9", Kind: boardsync.TargetThread,
	})
	if err != nil || !found || thread.BoardThread != first.Root {
		t.Fatalf("thread mapping = %+v, %v, %v", thread, found, err)
	}

	reply := syncEvent("m2", "C0123:1727.9")
	reply.IsRoot = false
	reply.Message = "m2"
	reply.Text = "a reply"
	result, err := host.Import(ctx, boardsync.ImportRequest{Tenant: "t1", Key: "slack", EventID: "m2", Event: reply})
	if err != nil {
		t.Fatalf("import reply: %v", err)
	}
	if result.Root != first.Root || result.Message == first.Root || result.CreatedRoot {
		t.Fatalf("reply result = %+v", result)
	}
	if err := board.Read("t1", func(snapshot *log.Snapshot) error {
		message := snapshot.Message(result.Message)
		if message == nil || message.Re == nil || *message.Re != first.Root {
			t.Fatalf("reply = %+v", message)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.Copy(ctx, "t1", "slack", boardsync.ResolvedTarget{
		Key: "slack", Platform: "slack", Channel: "C0123", Thread: "C0123:1727.9", Kind: boardsync.TargetThread,
	}, result.Message); err != nil || !found {
		t.Fatalf("reply copy = %v, %v", found, err)
	}
}

func TestSyncHostImportsSubthreadIntoThread(t *testing.T) {
	board, _, host := newSyncHost(t)
	ctx := t.Context()
	root := syncEvent("m1", "acme/www:123")
	root.Platform = "github"
	root.Channel = "github:acme/www:pulls"
	root.Key = "github"
	first, err := host.Import(ctx, boardsync.ImportRequest{Tenant: "t1", Key: "github", EventID: "m1", Channel: "prs", Event: root})
	if err != nil {
		t.Fatalf("import root: %v", err)
	}
	reply := syncEvent("c1", "acme/www:123")
	reply.Key, reply.Platform, reply.Channel = "github", "github", "github:acme/www:pulls"
	reply.Subthread = "acme/www:123:rc:345"
	reply.Message, reply.IsRoot, reply.Text = "c1", false, "review note"
	result, err := host.Import(ctx, boardsync.ImportRequest{Tenant: "t1", Key: "github", EventID: "c1", Event: reply})
	if err != nil {
		t.Fatalf("import subthread: %v", err)
	}
	if result.Root != first.Root {
		t.Fatalf("subthread root = %d, want %d", result.Root, first.Root)
	}
	if err := board.Read("t1", func(snapshot *log.Snapshot) error {
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSyncHostEchoSuppressionAndSafeInput(t *testing.T) {
	board, _, host := newSyncHost(t)
	ctx := t.Context()
	own := syncEvent("m1", "C0123:T1")
	own.Text = "hi\n\n" + boardsync.Marker{Key: "slack", Message: 9, Thread: "C0123:T1"}.String()
	result, err := host.Import(ctx, boardsync.ImportRequest{Tenant: "t1", Key: "slack", EventID: own.ID, Channel: "support", Event: own})
	if err != nil || result != (boardsync.ImportResult{}) {
		t.Fatalf("own marker import = %+v, %v", result, err)
	}
	if err := board.Read("t1", func(snapshot *log.Snapshot) error {
		if len(snapshot.MessagesInChannel("support")) != 0 {
			t.Fatal("own marker was imported")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	foreign := syncEvent("m2", "C0123:T2")
	foreign.Text = "visible\n\n" + boardsync.Marker{Key: "other", Message: 1, Thread: "C0123:T2"}.String()
	foreign.Files = []boardsync.File{
		{Name: "safe.txt", Type: "file", MIMEType: "text/plain", Size: 10, URL: "https://files.example.test/a", FetchMetadata: map[string]string{"token": "secret"}},
		{Name: "unsafe.txt", Size: 10, URL: "http://evil.test/a"},
		{Name: "huge.bin", Size: 1 << 40, URL: "https://files.example.test/big"},
	}
	foreign.Links = []boardsync.Link{
		{URL: "https://files.example.test/thread", Message: "1"},
		{URL: "https://evil.test/x", Message: "2"},
		{URL: "http://files.example.test/y"},
	}
	result, err = host.Import(ctx, boardsync.ImportRequest{Tenant: "t1", Key: "slack", EventID: foreign.ID, Channel: "support", Event: foreign})
	if err != nil {
		t.Fatalf("import foreign marker: %v", err)
	}
	if err := board.Read("t1", func(snapshot *log.Snapshot) error {
		message := snapshot.Message(result.Message)
		if message == nil {
			t.Fatal("message missing")
		}
		if strings.Contains(message.Text, "keel-board") {
			t.Fatalf("marker leaked: %q", message.Text)
		}
		if len(message.Attachments) != 1 || message.Attachments[0].Name != "safe.txt" {
			t.Fatalf("attachments = %+v", message.Attachments)
		}
		links, _ := json.Marshal(message.Links)
		if strings.Contains(string(links), "evil.test") || strings.Contains(string(links), "http://") {
			t.Fatalf("unsafe link stored: %s", links)
		}
		if !strings.Contains(string(links), "files.example.test") {
			t.Fatalf("safe link missing: %s", links)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSyncHostEditsDeletesReactions(t *testing.T) {
	board, _, host := newSyncHost(t)
	ctx := t.Context()
	root := syncEvent("m1", "C0123:T1")
	imported, err := host.Import(ctx, boardsync.ImportRequest{Tenant: "t1", Key: "slack", EventID: "m1", Channel: "support", Event: root})
	if err != nil {
		t.Fatal(err)
	}
	edit := root
	edit.ID, edit.Kind, edit.Type, edit.Text = "m1-edited", boardsync.KindEdited, boardsync.TypeMessageEdited, "edited"
	if _, err := host.Import(ctx, boardsync.ImportRequest{Tenant: "t1", Key: "slack", EventID: edit.ID, Event: edit}); err != nil {
		t.Fatalf("edit: %v", err)
	}
	reaction := root
	reaction.ID, reaction.Kind, reaction.Type = "r1", boardsync.KindReaction, boardsync.TypeReactionAdded
	reaction.Emoji, reaction.Added = "eyes", true
	if _, err := host.Import(ctx, boardsync.ImportRequest{Tenant: "t1", Key: "slack", EventID: "r1", Event: reaction}); err != nil {
		t.Fatalf("reaction: %v", err)
	}
	if err := board.Read("t1", func(snapshot *log.Snapshot) error {
		message := snapshot.Message(imported.Message)
		if message.Text != "edited" {
			t.Fatalf("edited text = %q", message.Text)
		}
		actors := message.Reactions["eyes"]
		if len(actors) != 1 || actors[0] != "slack:user:U1" {
			t.Fatalf("reactions = %+v", message.Reactions)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	remove := reaction
	remove.ID, remove.Type, remove.Added = "r2", boardsync.TypeReactionRemove, false
	if _, err := host.Import(ctx, boardsync.ImportRequest{Tenant: "t1", Key: "slack", EventID: "r2", Event: remove}); err != nil {
		t.Fatalf("reaction remove: %v", err)
	}
	del := root
	del.ID, del.Kind, del.Type = "m1-deleted", boardsync.KindDeleted, boardsync.TypeMessageDeleted
	if _, err := host.Import(ctx, boardsync.ImportRequest{Tenant: "t1", Key: "slack", EventID: del.ID, Event: del}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := board.Read("t1", func(snapshot *log.Snapshot) error {
		message := snapshot.Message(imported.Message)
		if !message.Deleted || len(message.Reactions) != 0 {
			t.Fatalf("deleted message = %+v", message)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSyncHostRecordThreadAndBackfill(t *testing.T) {
	board, store, host := newSyncHost(t)
	ctx := t.Context()
	// A board-authored root and reply: a channel attach posts the root and
	// backfills the reply.
	var rootID, replyID uint64
	if err := board.Update(ctx, "t1", log.UpdateOptions{}, func(tx *log.Tx) error {
		rootID = tx.NextID(0)
		tx.Emit(log.EventMessagePosted, &log.MessagePostedPayload{
			ID: rootID, Channel: "support", Text: "board root",
			Thread: log.ThreadKey(rootID), Labels: map[string]string{},
			By: protocol.Author{Actor: "local:user:ada", Principal: "local:user:ada"},
		})
		replyID = tx.NextID(1)
		tx.Emit(log.EventMessagePosted, &log.MessagePostedPayload{
			ID: replyID, Channel: "support", Text: "board reply",
			Re: uint64Ptr(rootID), Thread: log.ThreadKey(rootID),
			Labels: map[string]string{},
			By:     protocol.Author{Actor: "local:user:ada", Principal: "local:user:ada"},
		})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	target := boardsync.ThreadRef{Key: "slack", Platform: "slack", Channel: "C0999"}
	if err := host.Record(ctx, boardsync.BoardEvent{
		Tenant: "t1", Type: boardsync.BoardThreadSynced, Thread: rootID, Target: &target, Cause: "attach",
	}); err != nil {
		t.Fatalf("record thread.synced: %v", err)
	}
	if _, found, err := store.ThreadByTarget(ctx, "t1", "slack", boardsync.ResolvedTarget{
		Key: "slack", Platform: "slack", Channel: "C0999", Kind: boardsync.TargetChannel,
	}); err != nil || !found {
		t.Fatalf("channel mapping = %v, %v", found, err)
	}
	jobs, err := store.LeaseOutbox(ctx, "t1", []boardsync.Key{"slack"}, 10, "test")
	if err != nil {
		t.Fatalf("lease outbox: %v", err)
	}
	if len(jobs) != 1 || jobs[0].Event.Type != boardsync.BoardThreadSynced {
		t.Fatalf("root job = %+v", jobs)
	}
	if jobs[0].Event.Target == nil || jobs[0].Event.Target.Thread != "" {
		t.Fatalf("root job target = %+v", jobs[0].Event.Target)
	}
	// The root lands on the platform; the receipt starts the backfill.
	external := boardsync.ThreadRef{Key: "slack", Platform: "slack", Channel: "C0999", Thread: "C0999:1"}
	if _, err := store.SaveCopy(ctx, "t1", syncstore.Copy{
		BoardMessage: rootID, BoardThread: rootID, Key: "slack",
		Target:         boardsync.ResolvedTarget{Key: "slack", Platform: "slack", Channel: "C0999", Kind: boardsync.TargetChannel},
		ExternalThread: "C0999:1", ExternalMessage: "root-1", Files: []string{"f1"}, FilesSet: true,
	}); err != nil {
		t.Fatalf("save copy: %v", err)
	}
	receipt := boardsync.BoardEvent{
		Tenant: "t1", Type: boardsync.BoardMessageSynced, Thread: rootID,
		Message:  &boardsync.Message{ID: rootID, Thread: rootID, Channel: "support"},
		External: &external, ExternalMessage: "root-1",
	}
	if err := host.Record(ctx, receipt); err != nil {
		t.Fatalf("record receipt: %v", err)
	}
	receipt.FilesSet = false
	if err := host.Record(ctx, receipt); err != nil {
		t.Fatalf("retry receipt: %v", err)
	}
	if err := board.Read("t1", func(snapshot *log.Snapshot) error {
		message := snapshot.Message(rootID)
		if message.Sync == nil || len(message.Sync.Copies) != 1 {
			t.Fatalf("sync copies = %+v", message.Sync)
		}
		copied := message.Sync.Copies[0]
		if !copied.FilesSet || len(copied.Files) != 1 || copied.Files[0] != "f1" {
			t.Fatalf("copy files = %+v", copied)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.ThreadByTarget(ctx, "t1", "slack", boardsync.ResolvedTarget{
		Key: "slack", Platform: "slack", Channel: "C0999", Thread: "C0999:1", Kind: boardsync.TargetThread,
	}); err != nil || !found {
		t.Fatalf("concrete thread mapping = %v, %v", found, err)
	}
	// Lease the backfilled reply. The lane is ordered, so the earlier root job
	// is still leased; release it first.
	for _, job := range jobs {
		if _, err := store.AckOutbox(ctx, "t1", job.ID, job.Lease, nil, nil); err != nil {
			t.Fatalf("ack root: %v", err)
		}
	}
	backfilled, err := store.LeaseOutbox(ctx, "t1", []boardsync.Key{"slack"}, 10, "test-2")
	if err != nil {
		t.Fatalf("lease backfill: %v", err)
	}
	if len(backfilled) != 1 || backfilled[0].Message.ID != replyID {
		t.Fatalf("backfill = %+v", backfilled)
	}
	if backfilled[0].Target.Kind != boardsync.TargetThread || backfilled[0].Target.Thread != "C0999:1" {
		t.Fatalf("backfill target = %+v", backfilled[0].Target)
	}
}

func TestSyncHostReceiptClearsAnEarlierFailure(t *testing.T) {
	board, _, host := newSyncHost(t)
	ctx := t.Context()
	root := hBoardRoot(t, board, "t1", "support", "root")
	external := boardsync.ThreadRef{Key: "slack", Platform: "slack", Channel: "C1", Thread: "C1:1"}
	message := &boardsync.Message{ID: root, Thread: root, Channel: "support"}
	if err := host.Record(ctx, boardsync.BoardEvent{
		Tenant: "t1", Type: boardsync.BoardMessageSyncFailed, Thread: root,
		Message: message, External: &external, Error: "not_in_channel",
	}); err != nil {
		t.Fatal(err)
	}
	if err := host.Record(ctx, boardsync.BoardEvent{
		Tenant: "t1", Type: boardsync.BoardMessageSynced, Thread: root,
		Message: message, External: &external, ExternalMessage: "m1",
	}); err != nil {
		t.Fatal(err)
	}
	if err := board.Read("t1", func(snapshot *log.Snapshot) error {
		state := snapshot.Message(root).Sync
		if state == nil || len(state.Undelivered) != 0 {
			t.Fatalf("undelivered after success = %+v", state)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSyncHostReactionWaitsForCopy(t *testing.T) {
	_, _, host := newSyncHost(t)
	ctx := t.Context()
	root := syncEvent("m1", "C0123:T1")
	reaction := root
	reaction.ID, reaction.Kind, reaction.Type = "r1", boardsync.KindReaction, boardsync.TypeReactionAdded
	reaction.Emoji, reaction.Added = "eyes", true
	if _, err := host.Import(ctx, boardsync.ImportRequest{Tenant: "t1", Key: "slack", EventID: "r1", Event: reaction}); err == nil {
		t.Fatal("a reaction before its copy should be retried")
	}
	if _, err := host.Import(ctx, boardsync.ImportRequest{Tenant: "t1", Key: "slack", EventID: "m1", Channel: "support", Event: root}); err != nil {
		t.Fatalf("import root: %v", err)
	}
	if _, err := host.Import(ctx, boardsync.ImportRequest{Tenant: "t1", Key: "slack", EventID: "r1", Event: reaction}); err != nil {
		t.Fatalf("reaction after copy: %v", err)
	}
}

func TestSyncHostRecordsFailure(t *testing.T) {
	board, _, host := newSyncHost(t)
	ctx := t.Context()
	root := syncEvent("m1", "C0123:T1")
	imported, err := host.Import(ctx, boardsync.ImportRequest{Tenant: "t1", Key: "slack", EventID: "m1", Channel: "support", Event: root})
	if err != nil {
		t.Fatal(err)
	}
	external := boardsync.ThreadRef{Key: "slack", Platform: "slack", Channel: "C0123", Thread: "C0123:T1"}
	message := &boardsync.Message{ID: imported.Root, Thread: imported.Root, Channel: "support"}
	if err := host.Record(ctx, boardsync.BoardEvent{
		Tenant: "t1", Type: boardsync.BoardMessageSyncFailed, Thread: imported.Root,
		Message: message, External: &external, Error: "not_in_channel",
	}); err != nil {
		t.Fatalf("record failure: %v", err)
	}
	if err := board.Read("t1", func(snapshot *log.Snapshot) error {
		got := snapshot.Message(imported.Root).Sync
		if got == nil || got.Undelivered["slack:C0123:T1"] != "not_in_channel" {
			t.Fatalf("undelivered = %+v", got)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func uint64Ptr(value uint64) *uint64 { return &value }

func TestSyncHostReceiptIDigestIgnoresFiles(t *testing.T) {
	board, _, host := newSyncHost(t)
	ctx := t.Context()
	root := hBoardRoot(t, board, "t1", "support", "root")
	external := boardsync.ThreadRef{Key: "slack", Platform: "slack", Channel: "C1", Thread: "C1:1"}
	message := &boardsync.Message{ID: root, Thread: root, Channel: "support"}
	if err := host.Record(ctx, boardsync.BoardEvent{
		Tenant: "t1", Type: boardsync.BoardMessageSynced, Thread: root,
		Message: message, External: &external, ExternalMessage: "m1",
		Files: []string{"f1"}, FilesSet: true,
	}); err != nil {
		t.Fatalf("first receipt: %v", err)
	}
	// The retry has no files; the digest is the same fact, so it replays and
	// keeps what landed.
	if err := host.Record(ctx, boardsync.BoardEvent{
		Tenant: "t1", Type: boardsync.BoardMessageSynced, Thread: root,
		Message: message, External: &external, ExternalMessage: "m1",
	}); err != nil {
		t.Fatalf("retry receipt: %v", err)
	}
	if err := host.Record(ctx, boardsync.BoardEvent{
		Tenant: "t1", Type: boardsync.BoardMessageFilesSynced, Thread: root,
		Message: message, External: &external, Files: []string{"f1", "f2"}, FilesSet: true,
	}); err != nil {
		t.Fatalf("files receipt: %v", err)
	}
	if err := board.Read("t1", func(snapshot *log.Snapshot) error {
		state := snapshot.Message(root).Sync
		if state == nil || len(state.Copies) != 1 {
			t.Fatalf("copies = %+v", state)
		}
		if !state.Copies[0].FilesSet || len(state.Copies[0].Files) != 2 {
			t.Fatalf("files = %+v", state.Copies[0])
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func hBoardRoot(t *testing.T, board *log.Board, tenant, channel, text string) uint64 {
	t.Helper()
	var id uint64
	if err := board.Update(t.Context(), tenant, log.UpdateOptions{}, func(tx *log.Tx) error {
		id = tx.NextID(0)
		tx.Emit(log.EventMessagePosted, &log.MessagePostedPayload{
			ID: id, Channel: channel, Text: text, Thread: log.ThreadKey(id),
			Labels: map[string]string{},
			By:     protocol.Author{Actor: "local:user:ada", Principal: "local:user:ada"},
		})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return id
}

func TestSyncHostThreadReceiptIgnoresCause(t *testing.T) {
	board, store, host := newSyncHost(t)
	ctx := t.Context()
	root := hBoardRoot(t, board, "t1", "support", "root")
	target := boardsync.ThreadRef{Key: "slack", Platform: "slack", Channel: "C1"}
	for _, cause := range []string{"config", "attach"} {
		if err := host.Record(ctx, boardsync.BoardEvent{
			Tenant: "t1", Type: boardsync.BoardThreadSynced, Thread: root, Target: &target, Cause: cause,
		}); err != nil {
			t.Fatalf("record %s: %v", cause, err)
		}
	}
	depth, err := store.OutboxDepth(ctx, "t1")
	if err != nil || depth.Pending != 1 {
		t.Fatalf("outbox depth = %+v, %v", depth, err)
	}
}

func TestSyncHostConcurrentRootsCreateOne(t *testing.T) {
	board, _, host := newSyncHost(t)
	ctx := t.Context()
	const events = 8
	var (
		wg      sync.WaitGroup
		start   = make(chan struct{})
		mu      sync.Mutex
		results []boardsync.ImportResult
		errs    []error
	)
	for i := 0; i < events; i++ {
		event := syncEvent(fmt.Sprintf("m%d", i), "C0123:1727.9")
		event.Text = fmt.Sprintf("message %d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			result, err := host.Import(ctx, boardsync.ImportRequest{
				Tenant: "t1", Key: "slack", EventID: event.ID, Channel: "support", Event: event,
			})
			mu.Lock()
			results = append(results, result)
			if err != nil {
				errs = append(errs, err)
			}
			mu.Unlock()
		}()
	}
	close(start)
	wg.Wait()
	if len(errs) != 0 {
		t.Fatalf("import errors = %v", errs)
	}
	roots := 0
	for _, result := range results {
		if result.CreatedRoot {
			roots++
		}
	}
	if roots != 1 {
		t.Fatalf("created roots = %d, want one: %+v", roots, results)
	}
	if err := board.Read("t1", func(snapshot *log.Snapshot) error {
		messages := snapshot.MessagesInChannel("support")
		if len(messages) != events {
			t.Fatalf("messages = %d, want %d", len(messages), events)
		}
		root := messages[0]
		if root.Re != nil {
			t.Fatalf("first message is a reply: %+v", root)
		}
		for _, message := range messages[1:] {
			if message.Re == nil || *message.Re != root.ID {
				t.Fatalf("message %d isn't a reply to the root: %+v", message.ID, message)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSyncHostLifecycleRetitledKeepsBodyAndLink(t *testing.T) {
	board, _, host := newSyncHost(t)
	ctx := t.Context()
	root := syncEvent("m1", "acme/www:12")
	root.Key, root.Platform, root.Channel = "github", "github", "github:acme/www:pulls"
	imported, err := host.Import(ctx, boardsync.ImportRequest{Tenant: "t1", Key: "github", EventID: "m1", Channel: "prs", Event: root})
	if err != nil {
		t.Fatalf("import root: %v", err)
	}
	base := boardsync.Event{
		ID: "life1", Key: "github", Platform: "github",
		Kind: boardsync.KindLifecycle, Type: "pr.edited",
		Channel: "github:acme/www:pulls", Thread: "acme/www:12", Message: "12",
		Author: boardsync.Author{ID: "github:user:1", Handle: "ada", Name: "Ada"},
		Lifecycle: &boardsync.Lifecycle{
			Type: "pr.edited", Retitled: true,
			Title: "new title", Body: "new body",
			URL: "https://files.example.test/pr/12",
		},
	}
	if _, err := host.Import(ctx, boardsync.ImportRequest{Tenant: "t1", Key: "github", EventID: "life1", Event: base}); err != nil {
		t.Fatalf("retitled lifecycle: %v", err)
	}
	if err := board.Read("t1", func(snapshot *log.Snapshot) error {
		message := snapshot.Message(imported.Root)
		if message.Text != "new title\n\nnew body" {
			t.Fatalf("text = %q", message.Text)
		}
		if len(message.Links) != 1 {
			t.Fatalf("mapped lifecycle URL link = %+v", message.Links)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// A lifecycle that opens a root keeps its subject URL as a board link.
	opened := base
	opened.ID, opened.Thread, opened.Lifecycle = "life-open", "acme/www:99", &boardsync.Lifecycle{
		Type: "pr.opened", CreateRoot: true, Title: "opened", URL: "https://files.example.test/pr/99",
	}
	result, err := host.Import(ctx, boardsync.ImportRequest{Tenant: "t1", Key: "github", EventID: "life-open", Channel: "prs", Event: opened})
	if err != nil {
		t.Fatalf("create-root lifecycle: %v", err)
	}
	if err := board.Read("t1", func(snapshot *log.Snapshot) error {
		message := snapshot.Message(result.Root)
		if message == nil || len(message.Links) == 0 {
			t.Fatalf("created root link = %+v", message)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// A body-only edit carries no title; it must still replace the text.
	bodyOnly := base
	bodyOnly.ID, bodyOnly.Type = "life2", "pr.body"
	bodyOnly.Lifecycle = &boardsync.Lifecycle{Type: "pr.body", Retitled: true, Body: "body only"}
	if _, err := host.Import(ctx, boardsync.ImportRequest{Tenant: "t1", Key: "github", EventID: "life2", Event: bodyOnly}); err != nil {
		t.Fatalf("body-only lifecycle: %v", err)
	}
	if err := board.Read("t1", func(snapshot *log.Snapshot) error {
		if text := snapshot.Message(imported.Root).Text; text != "body only" {
			t.Fatalf("body-only text = %q", text)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSyncHostImportEditDeleteFollowsTaskRules(t *testing.T) {
	board, _, host := newSyncHost(t)
	ctx := t.Context()
	root := syncEvent("m1", "C0123:T1")
	imported, err := host.Import(ctx, boardsync.ImportRequest{Tenant: "t1", Key: "slack", EventID: "m1", Channel: "support", Event: root})
	if err != nil {
		t.Fatal(err)
	}
	if err := board.Update(ctx, "t1", log.UpdateOptions{}, func(tx *log.Tx) error {
		tx.Emit(log.EventTaskCreated, &log.TaskCreatedPayload{
			ID: imported.Root, Thread: log.ThreadKey(imported.Root),
			By: protocol.Author{Actor: "local:user:ada", Principal: "local:user:ada"},
		})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	edit := root
	edit.ID, edit.Kind, edit.Type, edit.Text = "m1-edit", boardsync.KindEdited, boardsync.TypeMessageEdited, "different"
	if _, err := host.Import(ctx, boardsync.ImportRequest{Tenant: "t1", Key: "slack", EventID: edit.ID, Event: edit}); err != nil {
		t.Fatalf("edit: %v", err)
	}
	if err := board.Read("t1", func(snapshot *log.Snapshot) error {
		task := snapshot.Message(imported.Root).Task
		if task == nil || task.Blocked == nil || task.Blocked.Kind == nil || *task.Blocked.Kind != protocol.EditedHoldKind {
			t.Fatalf("task after import edit = %+v", task)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	del := root
	del.ID, del.Kind, del.Type = "m1-del", boardsync.KindDeleted, boardsync.TypeMessageDeleted
	if _, err := host.Import(ctx, boardsync.ImportRequest{Tenant: "t1", Key: "slack", EventID: del.ID, Event: del}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := board.Read("t1", func(snapshot *log.Snapshot) error {
		task := snapshot.Message(imported.Root).Task
		if task == nil || task.State != "canceled" {
			t.Fatalf("task after import delete = %+v", task)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
