package server_test

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/protocol"
	"github.com/jixoai/keel-board/server"
)

func (h *harness) threads(actor, query string) protocol.ThreadsPage {
	h.t.Helper()
	status, data := h.request(http.MethodGet, "/threads"+query, nil, actor, "", nil)
	h.expect(status, data, http.StatusOK)
	return decode[protocol.ThreadsPage](h.t, data)
}

func TestThreadsListAndPaging(t *testing.T) {
	h := newHarness(t)
	const alice = "local:user:alice"
	const bob = "local:user:bob"

	a := h.postMessage(alice, "dev", "deploy alpha")
	b := h.postMessage(bob, "dev", "@alice please review")
	status, data := h.request(http.MethodPost, "/messages", map[string]any{"re": a.ID, "text": "first reply"}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	aReply1 := decode[postBody](t, data).Message
	c := h.postMessage(alice, "ops", "ops only")
	status, data = h.request(http.MethodPost, "/messages", map[string]any{"re": a.ID, "text": "second reply"}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	aReply2 := decode[postBody](t, data).Message
	status, data = h.deleteMessage(alice, aReply2.ID, nil)
	h.expect(status, data, http.StatusOK)
	// A label on the root is a filter, not activity.
	status, data = h.request(http.MethodPost, "/messages/"+a.ID+"/labels", map[string]any{"set": map[string]string{"status": "open"}}, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	status, data = h.request(http.MethodPost, "/messages/"+aReply1.ID+"/labels", map[string]any{"set": map[string]string{"kind": "reply"}}, alice, "", nil)
	h.expect(status, data, http.StatusOK)

	t.Run("a page is threads by latest activity", func(t *testing.T) {
		page := h.threads(alice, "?channel=dev")
		if len(page.Threads) != 2 || page.Threads[0].Message.ID != b.ID || page.Threads[1].Message.ID != a.ID {
			t.Fatalf("unexpected page: %+v", page)
		}
		if page.Threads[1].Replies != 1 || page.Threads[1].Last == nil || page.Threads[1].Last.ID != aReply1.ID {
			t.Fatalf("the deleted reply doesn't count, and the last live one is the summary: %+v", page.Threads[1])
		}
		if page.Threads[1].Message.Text != "deploy alpha" || page.Threads[1].Message.Thread != a.ID {
			t.Fatalf("unexpected root: %+v", page.Threads[1].Message)
		}
		if page.Cursor == "" || page.Older != nil {
			t.Fatalf("a short page has no older cursor: %+v", page)
		}
	})

	t.Run("older pages backward", func(t *testing.T) {
		first := h.threads(alice, "?channel=dev&limit=1")
		if len(first.Threads) != 1 || first.Threads[0].Message.ID != a.ID || first.Older == nil {
			t.Fatalf("unexpected first page: %+v", first)
		}
		second := h.threads(alice, "?channel=dev&limit=1&before="+url.QueryEscape(*first.Older))
		if len(second.Threads) != 1 || second.Threads[0].Message.ID != b.ID {
			t.Fatalf("unexpected second page: %+v", second)
		}
		if second.Older != nil {
			t.Fatalf("the last page has no older: %+v", second)
		}
	})

	t.Run("after reads forward", func(t *testing.T) {
		page := h.threads(alice, "?after="+b.ID)
		if len(page.Threads) != 2 || page.Threads[0].Message.ID != a.ID || page.Threads[1].Message.ID != c.ID {
			t.Fatalf("unexpected forward page: %+v", page)
		}
		if page.Older != nil {
			t.Fatalf("a forward page has no older: %+v", page)
		}
	})

	filters := []struct {
		name  string
		query string
		want  []string
	}{
		{"channel subtree", "?channel=dev/", []string{b.ID, a.ID}},
		{"root or reply ref", "?re=" + aReply1.ID, []string{a.ID}},
		{"root ref", "?re=" + b.ID, []string{b.ID}},
		{"by handle", "?channel=dev&by=bob", []string{b.ID}},
		{"by me", "?channel=dev&by=me", []string{a.ID}},
		{"mentions handle", "?channel=dev&mentions=alice", []string{b.ID}},
		{"label", "?label=status%3Dopen", []string{a.ID}},
		{"root label and reply text", "?channel=dev&label=status%3Dopen&text=first", []string{a.ID}},
		{"root text and reply label", "?channel=dev&text=deploy&label=kind%3Dreply", []string{}},
		{"text", "?text=review", []string{b.ID}},
		{"from the board", "?channel=dev&from=board", []string{b.ID, a.ID}},
		{"from nowhere", "?channel=dev&from=slack", []string{}},
	}
	for _, filter := range filters {
		t.Run(filter.name, func(t *testing.T) {
			page := h.threads(alice, filter.query)
			if len(page.Threads) != len(filter.want) {
				t.Fatalf("got %+v, want %d threads", page, len(filter.want))
			}
			for at, id := range filter.want {
				if page.Threads[at].Message.ID != id {
					t.Fatalf("thread %d is %s, want %s", at, page.Threads[at].Message.ID, id)
				}
			}
		})
	}

	t.Run("invalid cursors", func(t *testing.T) {
		cases := []struct {
			query string
			code  protocol.Code
		}{
			{"?before=nope", protocol.CodeInvalidCursor},
			{"?before=1.2&after=1", protocol.CodeInvalidRequest},
			{"?re=nope", protocol.CodeInvalidCursor},
			{"?text=", protocol.CodeInvalidRequest},
		}
		for _, test := range cases {
			status, data := h.request(http.MethodGet, "/threads"+test.query, nil, alice, "", nil)
			h.expect(status, data, http.StatusBadRequest)
			if code := errorCode(t, data); code != test.code {
				t.Fatalf("%s: expected %s, got %s", test.query, test.code, code)
			}
		}
	})

	t.Run("a deleted root is listed as a tombstone", func(t *testing.T) {
		gone := h.postMessage(alice, "gone", "gone")
		status, data := h.deleteMessage(alice, gone.ID, nil)
		h.expect(status, data, http.StatusOK)
		page := h.threads(alice, "?channel=gone")
		if len(page.Threads) != 1 || !page.Threads[0].Message.Deleted || page.Threads[0].Message.Text != "" {
			t.Fatalf("unexpected tombstone thread: %+v", page)
		}
		if page.Threads[0].Replies != 0 || page.Threads[0].Last != nil {
			t.Fatalf("a tombstone root has no live replies: %+v", page.Threads[0])
		}
	})

	t.Run("channel counts count posts and threads", func(t *testing.T) {
		status, data := h.request(http.MethodGet, "/channels", nil, alice, "", nil)
		h.expect(status, data, http.StatusOK)
		channels := decode[protocol.ChannelsPage](t, data)
		found := map[string]protocol.ChannelView{}
		for _, channel := range channels.Channels {
			found[channel.Path] = channel
		}
		if dev := found["dev"]; dev.Threads != 2 || dev.Messages != 4 {
			t.Fatalf("unexpected dev counts: %+v", dev)
		}
		if ops := found["ops"]; ops.Threads != 1 || ops.Messages != 1 {
			t.Fatalf("unexpected ops counts: %+v", ops)
		}
	})
}

func TestActorsAndActorSnapshots(t *testing.T) {
	const alice = "local:user:alice"
	const bob = "local:user:bob"
	const carol = "local:user:carol"
	policy := server.AuthorizerFunc(func(_ context.Context, _ string, actor protocol.Actor, channel string, permission protocol.Permission) error {
		if actor.ID == bob && channel == "secret" && permission == protocol.PermissionRead {
			return protocol.Forbidden("no secret")
		}
		return nil
	})
	h := newHarness(t, server.Options{Authorize: policy})

	message := h.postMessage(alice, "work", "visible")
	status, data := h.request(http.MethodPost, "/messages/"+message.ID+"/reactions", map[string]any{"emoji": "eyes"}, bob, "", nil)
	h.expect(status, data, http.StatusOK)
	h.postMessage(carol, "secret", "carol's secret")
	// Alice edits and deletes to leave actor records on both events.
	edited := h.postMessage(alice, "work", "edit me")
	status, data = h.edit(alice, edited.ID, "edited", nil)
	h.expect(status, data, http.StatusOK)
	status, data = h.deleteMessage(alice, edited.ID, nil)
	h.expect(status, data, http.StatusOK)
	// A task records who made it.
	status, data = h.request(http.MethodPost, "/tasks", map[string]any{"channel": "work", "text": "task"}, alice, "", nil)
	h.expect(status, data, http.StatusCreated)
	task := decode[taskBody](t, data).Task
	if task.Task == nil || task.Task.Author == nil || task.Task.Author.Handle != "alice" {
		t.Fatalf("a task keeps its actor snapshot: %+v", task.Task)
	}

	status, data = h.request(http.MethodGet, "/actors", nil, bob, "", nil)
	h.expect(status, data, http.StatusOK)
	page := decode[protocol.ActorsPage](t, data)
	ids := make(map[string]protocol.ActorWithID)
	for _, actor := range page.Actors {
		ids[actor.ID] = actor
	}
	if _, ok := ids[alice]; !ok {
		t.Fatalf("alice wrote where bob reads: %+v", page)
	}
	if _, ok := ids[bob]; !ok {
		t.Fatalf("bob reacted where bob reads: %+v", page)
	}
	if _, ok := ids[carol]; ok {
		t.Fatalf("carol wrote only where bob can't read: %+v", page)
	}
	if ids[alice].Handle != "alice" || ids[alice].Kind != protocol.ActorPerson {
		t.Fatalf("unexpected actor view: %+v", ids[alice])
	}
	if page.Cursor == "" {
		t.Fatalf("expected a cursor: %+v", page)
	}

	// Another reader sees carol.
	status, data = h.request(http.MethodGet, "/actors", nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	seen := map[string]bool{}
	for _, actor := range decode[protocol.ActorsPage](t, data).Actors {
		seen[actor.ID] = true
	}
	if !seen[carol] {
		t.Fatalf("alice reads secret and should see carol")
	}

	// `id` names only the actors asked for.
	status, data = h.request(http.MethodGet, "/actors?id="+url.QueryEscape(bob)+"&id="+url.QueryEscape("local:user:nobody"), nil, alice, "", nil)
	h.expect(status, data, http.StatusOK)
	named := decode[protocol.ActorsPage](t, data).Actors
	if len(named) != 1 || named[0].ID != bob {
		t.Fatalf("unexpected named actors: %+v", named)
	}
	// Too many names are refused.
	values := make([]string, protocol.ActorsByIdMax+1)
	for at := range values {
		values[at] = "id=" + url.QueryEscape("local:user:x")
	}
	status, data = h.request(http.MethodGet, "/actors?"+strings.Join(values, "&"), nil, alice, "", nil)
	h.expect(status, data, http.StatusBadRequest)

	// Edit, delete, and reaction events carry who did them.
	head, err := h.board.Head("default")
	if err != nil {
		t.Fatal(err)
	}
	events, err := h.board.History("default", 1, head)
	if err != nil {
		t.Fatal(err)
	}
	seenTypes := map[log.EventType]string{}
	for _, event := range events {
		switch payload := event.Payload.(type) {
		case *log.MessageEditedPayload:
			seenTypes[event.Type] = payload.By.Actor
		case *log.MessageDeletedPayload:
			seenTypes[event.Type] = payload.By.Actor
		case *log.ReactionPayload:
			seenTypes[event.Type] = payload.By.Actor
		}
	}
	for _, eventType := range []log.EventType{log.EventMessageEdited, log.EventMessageDeleted, log.EventReactionAdded} {
		if seenTypes[eventType] != alice && seenTypes[eventType] != bob {
			t.Fatalf("%s carries no actor: %+v", eventType, seenTypes)
		}
	}
}

func TestThreadsByPersonAndAgent(t *testing.T) {
	h := newHarness(t)
	const alice = "local:user:alice"
	const agent = "local:user:alice+claude"

	h.postMessage(alice, "work", "person wrote this")
	status, data := h.request(http.MethodPost, "/messages", map[string]any{
		"channel": "work", "text": "agent wrote this @alice/claude",
	}, "", "", map[string]string{"X-Board-Actor": agent})
	h.expect(status, data, http.StatusCreated)

	byProfile := h.threads(alice, "?channel=work&by="+url.QueryEscape("alice/claude"))
	if len(byProfile.Threads) != 1 || byProfile.Threads[0].Message.Text != "agent wrote this @alice/claude" {
		t.Fatalf("a profile handle should match only the profile: %+v", byProfile)
	}
	byID := h.threads(alice, "?channel=work&by="+url.QueryEscape(agent))
	if len(byID.Threads) != 1 || byID.Threads[0].Message.ID != byProfile.Threads[0].Message.ID {
		t.Fatalf("a profile actor ID should match its thread: %+v", byID)
	}
	// A person's handle still covers that person's profiles.
	byPerson := h.threads(alice, "?channel=work&by=alice")
	if len(byPerson.Threads) != 2 {
		t.Fatalf("a person handle should cover the person and its agent: %+v", byPerson)
	}
	// A mention of a profile matches the person prefix.
	prefixed := h.threads(alice, "?channel=work&mentions=alice")
	if len(prefixed.Threads) != 1 || prefixed.Threads[0].Message.ID != byProfile.Threads[0].Message.ID {
		t.Fatalf("mentions=alice should match @alice/claude: %+v", prefixed)
	}
}

func TestThreadsMentionPrefixes(t *testing.T) {
	h := newHarness(t)
	const alice = "local:user:alice"

	actorPost := h.postMessage(alice, "work", "id @[local:user:alice+claude]")
	handlePost := h.postMessage(alice, "work", "handle @alice/claude")

	tests := []struct {
		name  string
		query string
		want  []string
	}{
		{"an actor ID matches its + profile", "?mentions=" + url.QueryEscape("local:user:alice"), []string{actorPost.ID}},
		{"an actor ID matches exactly", "?mentions=" + url.QueryEscape("local:user:alice+claude"), []string{actorPost.ID}},
		{"a handle matches its / profile", "?mentions=alice", []string{handlePost.ID}},
		{"a handle matches exactly", "?mentions=" + url.QueryEscape("alice/claude"), []string{handlePost.ID}},
		{"me is exact only", "?mentions=me", []string{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			page := h.threads(alice, "?channel=work&"+strings.TrimPrefix(test.query, "?"))
			if len(page.Threads) != len(test.want) {
				t.Fatalf("got %+v, want %v", page, test.want)
			}
			for at, id := range test.want {
				if page.Threads[at].Message.ID != id {
					t.Fatalf("thread %d is %s, want %s", at, page.Threads[at].Message.ID, id)
				}
			}
		})
	}
}
