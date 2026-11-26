package sync_test

import (
	"context"
	"errors"
	"testing"

	boardsync "github.com/jixoai/keel-board/sync"
	"github.com/jixoai/keel-board/sync/fake"
)

func testEntry(t *testing.T, syncFn boardsync.SyncFunc) (boardsync.Entry, *fake.Provider, *fake.Host) {
	t.Helper()
	provider := fake.New("slack", boardsync.Capabilities{StartThreads: true, Edits: true, Deletes: true, Reactions: true})
	return boardsync.Entry{Tenant: "t1", Key: "slack", Platform: "slack", Provider: provider, Sync: syncFn}, provider, fake.NewHost()
}

func TestEntryValidateChecksPlatform(t *testing.T) {
	provider := fake.New("slack", boardsync.Capabilities{})
	entry := boardsync.Entry{
		Tenant: "t1", Key: "slack", Platform: "github", Provider: provider, Sync: boardsync.Noop,
	}
	if err := entry.Validate(); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("platform mismatch = %v, want ErrBadRequest", err)
	}
	entry.Platform = "slack"
	if err := entry.Validate(); err != nil {
		t.Fatalf("matching platform = %v", err)
	}
}

func TestRunnerOutboundRefusesForeignTarget(t *testing.T) {
	entry, provider, _ := testEntry(t, func(ctx context.Context, call *boardsync.Call) error {
		_, err := call.Deliver(ctx, boardsync.DeliverOptions{})
		return err
	})
	message := boardsync.Message{ID: 812, Thread: 812, Channel: "support", Text: "hello"}
	target := boardsync.ResolvedTarget{
		Key: "github", Platform: "github", Channel: "acme/www:pulls",
		Thread: "acme/www:123", Kind: boardsync.TargetThread,
	}
	if _, err := boardsync.NewRunner(fake.NewHost()).Outbound(context.Background(), entry,
		boardsync.BoardEvent{Type: boardsync.BoardMessagePosted, Thread: 812},
		message, &target, ""); !errors.Is(err, boardsync.ErrBadRequest) {
		t.Fatalf("foreign target = %v, want ErrBadRequest", err)
	}
	if posts := provider.Posts(); len(posts) != 0 {
		t.Fatalf("foreign target reached the provider: %+v", posts)
	}
}

func TestRunnerInboundImportsOnce(t *testing.T) {
	var call *boardsync.Call
	entry, provider, host := testEntry(t, func(ctx context.Context, c *boardsync.Call) error {
		call = c
		first, err := c.Import(ctx, "support", boardsync.ImportOptions{})
		if err != nil {
			return err
		}
		second, err := c.Import(ctx, "elsewhere", boardsync.ImportOptions{})
		if err != nil {
			return err
		}
		if first != second {
			t.Fatalf("second import = %+v, first %+v", second, first)
		}
		return nil
	})
	runner := boardsync.NewRunner(host)
	event := inboxEvent("m1", "C0123:T1")
	event.Thread = "C0123:1727.9"
	if err := runner.Inbound(context.Background(), entry, event); err != nil {
		t.Fatalf("Inbound: %v", err)
	}
	imports := host.Imports()
	if len(imports) != 1 || imports[0].Channel != "support" || imports[0].EventID != "m1" {
		t.Fatalf("imports = %+v", imports)
	}
	if len(provider.Receives()) != 0 {
		t.Fatal("inbound import called the provider")
	}
	if _, err := call.Import(context.Background(), "", boardsync.ImportOptions{}); !errors.Is(err, boardsync.ErrCallFinished) {
		t.Fatalf("post-finish import = %v", err)
	}
}

func TestRunnerOutboundDeliversOnceAndReceipts(t *testing.T) {
	entry, provider, host := testEntry(t, func(ctx context.Context, c *boardsync.Call) error {
		if _, err := c.Deliver(ctx, boardsync.DeliverOptions{}); err != nil {
			return err
		}
		_, err := c.Deliver(ctx, boardsync.DeliverOptions{})
		return err
	})
	runner := boardsync.NewRunner(host)
	message := boardsync.Message{
		ID: 812, Thread: 812, Channel: "support", Text: "ship it",
		Actor: "slack:user:U1", ActorName: "Ada", Verified: true,
	}
	target := boardsync.ResolvedTarget{
		Key: "slack", Platform: "slack", Channel: "C0123", Thread: "C0123:1727.9", Kind: boardsync.TargetThread,
	}
	receipt, err := runner.Outbound(context.Background(), entry,
		boardsync.BoardEvent{Type: boardsync.BoardMessagePosted, Thread: 812},
		message, &target, "C0123:1727.9")
	if err != nil {
		t.Fatalf("Outbound: %v", err)
	}
	if receipt.Message != "m1" || receipt.Thread.External() != "slack:C0123:1727.9" {
		t.Fatalf("receipt = %+v", receipt)
	}
	posts := provider.Posts()
	if len(posts) != 1 {
		t.Fatalf("posts = %d, want one", len(posts))
	}
	if posts[0].Msg.Markdown != "**Ada**: ship it" || posts[0].Msg.Sender == nil || posts[0].Msg.Sender.Name != "Ada" {
		t.Fatalf("outbound message = %+v", posts[0].Msg)
	}
	records := host.Records()
	if len(records) != 1 || records[0].Type != boardsync.BoardMessageSynced {
		t.Fatalf("records = %+v", records)
	}
}

func TestRunnerMirrorStartsThread(t *testing.T) {
	entry, provider, host := testEntry(t, boardsync.Mirror("support"))
	provider.ResolveTargetFunc = func(_ context.Context, key boardsync.Key, target string) (boardsync.ResolvedTarget, error) {
		return boardsync.ResolvedTarget{Key: key, Platform: "slack", Channel: target, Kind: boardsync.TargetChannel}, nil
	}
	runner := boardsync.NewRunner(host)
	message := boardsync.Message{ID: 9, Thread: 9, Channel: "support", Text: "hello", Actor: "local:user:ada"}
	if err := runner.OfferRoot(context.Background(), entry,
		boardsync.BoardEvent{Type: boardsync.BoardMessagePosted, Thread: 9}, message, "C0123"); err != nil {
		t.Fatalf("OfferRoot: %v", err)
	}
	records := host.Records()
	if len(records) != 1 || records[0].Type != boardsync.BoardThreadSynced || records[0].Target == nil {
		t.Fatalf("records = %+v", records)
	}
	if records[0].Target.External() != "slack:C0123" {
		t.Fatalf("thread.synced target = %q", records[0].Target.External())
	}
	if len(provider.Posts()) != 0 {
		t.Fatal("Start posted a root through the provider; the backfill is the server's")
	}
}

func TestRunnerStartRunsOnce(t *testing.T) {
	resolves := 0
	entry, provider, host := testEntry(t, func(ctx context.Context, call *boardsync.Call) error {
		first, err := call.Start(ctx, "C0123")
		if err != nil {
			return err
		}
		second, err := call.Start(ctx, "C0123")
		if err != nil {
			return err
		}
		if first != second {
			t.Fatalf("second Start = %+v, first %+v", second, first)
		}
		return nil
	})
	provider.ResolveTargetFunc = func(_ context.Context, key boardsync.Key, target string) (boardsync.ResolvedTarget, error) {
		resolves++
		return boardsync.ResolvedTarget{Key: key, Platform: "slack", Channel: target, Kind: boardsync.TargetChannel}, nil
	}
	runner := boardsync.NewRunner(host)
	message := boardsync.Message{ID: 9, Thread: 9, Channel: "support", Text: "hello", Actor: "local:user:ada"}
	if err := runner.OfferRoot(context.Background(), entry,
		boardsync.BoardEvent{Type: boardsync.BoardMessagePosted, Thread: 9}, message, "C0123"); err != nil {
		t.Fatalf("OfferRoot: %v", err)
	}
	if resolves != 1 {
		t.Fatalf("Start resolved %d times, want once", resolves)
	}
	if records := host.Records(); len(records) != 1 || records[0].Type != boardsync.BoardThreadSynced {
		t.Fatalf("thread.synced receipts = %+v", records)
	}
}

func TestRunnerStartRefusesOriginChannel(t *testing.T) {
	entry, provider, host := testEntry(t, boardsync.Mirror("support"))
	provider.ResolveTargetFunc = func(_ context.Context, key boardsync.Key, target string) (boardsync.ResolvedTarget, error) {
		return boardsync.ResolvedTarget{Key: key, Platform: "slack", Channel: target, Kind: boardsync.TargetChannel}, nil
	}
	runner := boardsync.NewRunner(host)
	message := boardsync.Message{
		ID: 9, Thread: 9, Channel: "support", Text: "hello",
		Origin: &boardsync.Origin{Channel: "slack:C0123", Thread: "slack:C0123:1", Message: "1"},
	}
	if err := runner.OfferRoot(context.Background(), entry,
		boardsync.BoardEvent{Type: boardsync.BoardMessagePosted, Thread: 9}, message, "C0123"); err == nil {
		t.Fatal("Start should refuse the channel the message came from")
	}
	if len(host.Records()) != 0 {
		t.Fatal("nothing should be recorded")
	}
}

func TestRunnerStartRefusedWithoutCapability(t *testing.T) {
	provider := fake.New("github", boardsync.Capabilities{StartThreads: false})
	entry := boardsync.Entry{Tenant: "t1", Key: "github", Platform: "github", Provider: provider, Sync: boardsync.Mirror("support")}
	runner := boardsync.NewRunner(fake.NewHost())
	message := boardsync.Message{ID: 9, Thread: 9, Channel: "support", Text: "hello"}
	if err := runner.OfferRoot(context.Background(), entry,
		boardsync.BoardEvent{Type: boardsync.BoardMessagePosted, Thread: 9}, message, "acme/www"); err == nil {
		t.Fatal("Start should respect the provider's capability")
	}
}

func TestRunnerEditsAndReactions(t *testing.T) {
	entry, provider, _ := testEntry(t, func(ctx context.Context, c *boardsync.Call) error {
		_, err := c.Deliver(ctx, boardsync.DeliverOptions{})
		return err
	})
	runner := boardsync.NewRunner(fake.NewHost())
	target := boardsync.ResolvedTarget{
		Key: "slack", Platform: "slack", Channel: "C0123", Thread: "C0123:1727.9", Kind: boardsync.TargetThread,
	}
	message := boardsync.Message{ID: 812, Thread: 812, Channel: "support", Text: "edited"}
	if _, err := runner.Outbound(context.Background(), entry,
		boardsync.BoardEvent{Type: boardsync.BoardMessageEdited, Thread: 812}, message, &target, "1727.9"); err != nil {
		t.Fatal(err)
	}
	if edits := provider.Edits(); len(edits) != 1 || edits[0].Message != "1727.9" {
		t.Fatalf("edits = %+v", edits)
	}
	if _, err := runner.Outbound(context.Background(), entry,
		boardsync.BoardEvent{Type: boardsync.BoardReactionAdded, Thread: 812, Emoji: "thumbsup"}, message, &target, "1727.9"); err != nil {
		t.Fatal(err)
	}
	if reactions := provider.Reactions(); len(reactions) != 1 || !reactions[0].Added || reactions[0].Emoji != "thumbsup" {
		t.Fatalf("reactions = %+v", reactions)
	}
}

func TestRegistryHandleWebhookGateAndInbox(t *testing.T) {
	provider := fake.New("slack", boardsync.Capabilities{})
	provider.ReceiveFunc = func(context.Context, boardsync.Key, *boardsync.WebhookRequest) (boardsync.Intake, error) {
		return boardsync.Intake{
			Response: boardsync.OK(),
			Records: []boardsync.Record{
				{Dedupe: "d1", Event: inboxEvent("m1", "C0123:T1")},
				{Dedupe: "d2", Event: func() boardsync.Event {
					event := inboxEvent("m2", "C0999:T9")
					event.Channel = "C0999"
					event.Thread = "C0999:T9"
					return event
				}()},
			},
		}, nil
	}
	registry := boardsync.NewRegistry()
	if err := registry.Add(boardsync.Entry{Tenant: "t1", Key: "slack", Platform: "slack", Provider: provider, Sync: boardsync.Noop}); err != nil {
		t.Fatal(err)
	}
	inbox := boardsync.NewMemoryInbox(boardsync.DefaultRetry, 0)
	gate := fake.NewGate([]string{"C0123"}, []string{"slack:C0999:T9"})
	response, err := registry.HandleWebhook(context.Background(), "t1", "slack", &boardsync.WebhookRequest{
		Method: "POST", Path: "/webhooks/slack", Header: map[string][]string{"Content-Type": {"application/json"}}, Body: []byte("{}"),
	}, gate, inbox)
	if err != nil || response.Status != 200 {
		t.Fatalf("HandleWebhook = %+v, %v", response, err)
	}
	if pending := inbox.Pending("slack"); pending != 2 {
		t.Fatalf("inbox pending = %d, want both records kept", pending)
	}
}

func TestRegistryDropsUnmappedUnsynced(t *testing.T) {
	provider := fake.New("slack", boardsync.Capabilities{})
	provider.ReceiveFunc = func(context.Context, boardsync.Key, *boardsync.WebhookRequest) (boardsync.Intake, error) {
		event := inboxEvent("m1", "C0999:T9")
		event.Channel = "C0999"
		return boardsync.Intake{Response: boardsync.OK(), Records: []boardsync.Record{{Dedupe: "d1", Event: event}}}, nil
	}
	registry := boardsync.NewRegistry()
	if err := registry.Add(boardsync.Entry{Tenant: "t1", Key: "slack", Platform: "slack", Provider: provider, Sync: boardsync.Noop}); err != nil {
		t.Fatal(err)
	}
	inbox := boardsync.NewMemoryInbox(boardsync.DefaultRetry, 0)
	response, err := registry.HandleWebhook(context.Background(), "t1", "slack", &boardsync.WebhookRequest{
		Method: "POST", Path: "/webhooks/slack", Header: map[string][]string{"Content-Type": {"application/json"}}, Body: []byte("{}"),
	}, fake.NewGate([]string{"C0123"}, nil), inbox)
	if err != nil || response.Status != 200 {
		t.Fatalf("HandleWebhook = %+v, %v", response, err)
	}
	if inbox.Pending("slack") != 0 {
		t.Fatal("an unmapped, unsynced event should be dropped")
	}
}

func TestRegistryFailures(t *testing.T) {
	registry := boardsync.NewRegistry()
	if _, err := registry.HandleWebhook(context.Background(), "t1", "missing", &boardsync.WebhookRequest{Method: "POST", Header: map[string][]string{}}, nil, nil); !errors.Is(err, boardsync.ErrNotFound) {
		t.Fatalf("missing entry = %v", err)
	}
	provider := fake.New("slack", boardsync.Capabilities{})
	registry = boardsync.NewRegistry()
	if err := registry.Add(boardsync.Entry{Tenant: "t1", Key: "slack", Platform: "slack", Provider: provider, Sync: boardsync.Noop}); err != nil {
		t.Fatal(err)
	}
	provider.ReceiveFunc = func(context.Context, boardsync.Key, *boardsync.WebhookRequest) (boardsync.Intake, error) {
		return boardsync.Intake{}, boardsync.ErrUnauthorized
	}
	response, err := registry.HandleWebhook(context.Background(), "t1", "slack", &boardsync.WebhookRequest{
		Method: "POST", Header: map[string][]string{"X": {"y"}},
	}, nil, nil)
	if !errors.Is(err, boardsync.ErrUnauthorized) || response.Status != 401 {
		t.Fatalf("unauthorized = %+v, %v", response, err)
	}
	provider.ReceiveFunc = func(context.Context, boardsync.Key, *boardsync.WebhookRequest) (boardsync.Intake, error) {
		return boardsync.Intake{Records: []boardsync.Record{{Dedupe: "d1", Event: boardsync.Event{}}}}, nil
	}
	response, err = registry.HandleWebhook(context.Background(), "t1", "slack", &boardsync.WebhookRequest{
		Method: "POST", Header: map[string][]string{"X": {"y"}},
	}, nil, nil)
	if err == nil || response.Status != 400 {
		t.Fatalf("malformed record = %+v, %v", response, err)
	}
	provider.ReceiveFunc = func(context.Context, boardsync.Key, *boardsync.WebhookRequest) (boardsync.Intake, error) {
		return boardsync.Intake{Records: []boardsync.Record{{Dedupe: "d1", Event: inboxEvent("m1", "C0123:T1")}}}, nil
	}
	gateErr := errors.New("listing failed")
	response, err = registry.HandleWebhook(context.Background(), "t1", "slack", &boardsync.WebhookRequest{
		Method: "POST", Header: map[string][]string{"X": {"y"}},
	}, &fake.Gate{Err: gateErr}, nil)
	if !errors.Is(err, gateErr) || response.Status != 500 {
		t.Fatalf("gate error = %+v, %v", response, err)
	}
}
