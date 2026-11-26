package access

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/protocol"
	"github.com/jixoai/keel-board/server"
)

func TestEmptyUpdateNeedsReachAndNeverRewrites(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	service := NewGrantService(store)
	service.BoardTeam = "team-new"
	mustCreateGrant(t, store, "t", Grant{
		Principals: []Principal{UserPrincipal("alice")},
		Can:        Can{"*": All()},
	})
	target := mustCreateGrant(t, store, "t", Grant{
		Principals: []Principal{{
			Kind:      PrincipalProject,
			ProjectID: "prj_1",
			TeamID:    "team-original",
		}},
		Can:    Can{"*": Only(PermissionRead)},
		Capped: true,
	})

	unchanged := func(label string) {
		t.Helper()
		grants, err := store.List(ctx, "t")
		if err != nil {
			t.Fatalf("%s list: %v", label, err)
		}
		after := grantByID(t, grants, target.ID)
		if after.Principals[0].TeamID != "team-original" || !after.Capped {
			t.Fatalf("%s rewrote the grant: %+v", label, after)
		}
	}

	// A caller with no grants can't use an empty update to rewrite anything.
	_, err := service.Update(ctx, "t", target.ID, Caller{Principal: UserPrincipal("bob")}, GrantUpdate{}, false)
	if err == nil || errorCode(err) != protocol.CodeForbidden {
		t.Fatalf("unprivileged empty update = %v, want forbidden", err)
	}
	unchanged("unprivileged")

	// A scoped owner still has to reach the grant.
	scope := Can{"*": Only(PermissionRead)}
	_, err = service.Update(ctx, "t", target.ID, Caller{Principal: UserPrincipal("alice"), Owner: true, Scope: &scope}, GrantUpdate{}, false)
	if err == nil || errorCode(err) != protocol.CodeForbidden {
		t.Fatalf("scoped owner empty update = %v, want forbidden", err)
	}
	unchanged("scoped owner")

	// An authorized no-op returns the grant as it is.
	old, err := service.Update(ctx, "t", target.ID, Caller{Principal: UserPrincipal("alice")}, GrantUpdate{}, false)
	if err != nil {
		t.Fatalf("authorized empty update: %v", err)
	}
	if old.Principals[0].TeamID != "team-original" || !old.Capped {
		t.Fatalf("authorized empty update rewrote: %+v", old)
	}
	unchanged("authorized")
}

func TestScopeTextAndScopeIntersect(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	mustCreateGrant(t, store, "default", Grant{
		Principals: []Principal{UserPrincipal("local:user:alice")},
		Can:        Can{"*": All()},
	})
	alice := aliceActor()
	read := Can{"*": Only(PermissionRead)}

	// A text scope that says all can't widen the host's read-only scope.
	both := &Authorizer{
		Store: store,
		Scope: func(context.Context, string, protocol.Actor) *Can { return &read },
		ScopeText: func(context.Context, string, protocol.Actor) (string, bool) {
			return "*=all", true
		},
	}
	if err := both.Authorize(ctx, "default", alice, "dev", PermissionRead); err != nil {
		t.Fatalf("intersection read: %v", err)
	}
	expectAccessError(t, both.Authorize(ctx, "default", alice, "dev", PermissionPost), protocol.CodeForbidden, 403)

	// A text hook that returns false leaves the host scope alone.
	fallback := &Authorizer{
		Store: store,
		Scope: func(context.Context, string, protocol.Actor) *Can { return &read },
		ScopeText: func(context.Context, string, protocol.Actor) (string, bool) {
			return "", false
		},
	}
	if err := fallback.Authorize(ctx, "default", alice, "dev", PermissionRead); err != nil {
		t.Fatalf("fallback read: %v", err)
	}
	expectAccessError(t, fallback.Authorize(ctx, "default", alice, "dev", PermissionPost), protocol.CodeForbidden, 403)

	// Two real scopes intersect channel by channel.
	devRead := Can{"dev/": Only(PermissionRead)}
	text := &Authorizer{
		Store: store,
		Scope: func(context.Context, string, protocol.Actor) *Can { return &devRead },
		ScopeText: func(context.Context, string, protocol.Actor) (string, bool) {
			return "dev/=read,post", true
		},
	}
	if err := text.Authorize(ctx, "default", alice, "dev/ci", PermissionRead); err != nil {
		t.Fatalf("intersected dev read: %v", err)
	}
	expectAccessError(t, text.Authorize(ctx, "default", alice, "dev/ci", PermissionPost), protocol.CodeForbidden, 403)
	expectAccessError(t, text.Authorize(ctx, "default", alice, "other", PermissionRead), protocol.CodeNotFound, 404)

	// Without a host scope, the text scope is the only scope.
	textOnly := &Authorizer{
		Store: store,
		ScopeText: func(context.Context, string, protocol.Actor) (string, bool) {
			return "*=all", true
		},
	}
	if err := textOnly.Authorize(ctx, "default", alice, "dev", PermissionPost); err != nil {
		t.Fatalf("text-only all: %v", err)
	}
}

func TestWritePathsHideGrantDetailsFromNonListers(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	service := NewGrantService(store)
	mustCreateGrant(t, store, "t", Grant{
		Principals: []Principal{UserPrincipal("alice")},
		Can:        Can{"*": All()},
	})
	target := mustCreateGrant(t, store, "t", Grant{
		Principals: []Principal{UserPrincipal("bob")},
		Can:        Can{"security/": All()},
	})
	mustCreateGrant(t, store, "t", Grant{
		Principals: []Principal{UserPrincipal("carol")},
		Can:        Can{"security/": All()},
	})
	mustCreateGrant(t, store, "t", Grant{
		Principals: []Principal{UserPrincipal("dave")},
		Can:        Can{"*": All()},
	})
	mustCreateGrant(t, store, "t", Grant{
		Principals: []Principal{UserPrincipal("dave")},
		Can:        Can{"security/": Only(PermissionRead)},
	})
	admin := Caller{Principal: UserPrincipal("alice")}
	carol := Caller{Principal: UserPrincipal("carol")}
	dave := Caller{Principal: UserPrincipal("dave")}
	erin := Caller{Principal: UserPrincipal("erin")}

	wantMessage := forbiddenListing().Message
	cases := []struct {
		name   string
		caller Caller
		id     string
		update GrantUpdate
	}{
		{name: "missing", caller: erin, id: "missing"},
		{name: "unreachable", caller: erin, id: target.ID},
		{name: "member missing", caller: carol, id: target.ID, update: GrantUpdate{Remove: []Principal{UserPrincipal("nobody")}}},
		{name: "last principal", caller: carol, id: target.ID, update: GrantUpdate{Remove: []Principal{UserPrincipal("bob")}}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			_, err := service.Update(ctx, "t", test.id, test.caller, test.update, false)
			if err == nil {
				t.Fatal("update should be refused")
			}
			boardErr, ok := protocol.AsError(err)
			if !ok || boardErr.Code != protocol.CodeForbidden || boardErr.Status != 403 {
				t.Fatalf("error = %v, want one 403", err)
			}
			if boardErr.Message != wantMessage || strings.Contains(boardErr.Message, "security") {
				t.Fatalf("error leaks a pattern: %q", boardErr.Message)
			}
		})
	}

	// A lister gets the real 404 and the 400 detail.
	if _, err := service.Update(ctx, "t", "missing", admin, GrantUpdate{}, false); !errors.Is(err, ErrGrantNotFound) {
		t.Fatalf("lister missing = %v, want ErrGrantNotFound", err)
	}
	if _, err := service.Update(ctx, "t", target.ID, admin, GrantUpdate{Remove: []Principal{UserPrincipal("nobody")}}, false); errorCode(err) != CodeNotInGrant {
		t.Fatalf("lister not in grant = %v, want not_in_grant", err)
	}
	if _, err := service.Update(ctx, "t", target.ID, admin, GrantUpdate{Remove: []Principal{UserPrincipal("bob")}}, false); errorCode(err) != CodeGrantEmpty {
		t.Fatalf("lister last principal = %v, want grant_empty", err)
	}

	// A lister may still be unreachable at the grant's own pattern, and sees
	// the pattern because listing already proved it can see the board.
	_, err := service.Update(ctx, "t", target.ID, dave, GrantUpdate{Remove: []Principal{UserPrincipal("nobody")}}, false)
	if err == nil || errorCode(err) != protocol.CodeForbidden || !strings.Contains(err.Error(), "security/") {
		t.Fatalf("lister unreachable = %v, want a 403 naming security/", err)
	}

	// Revoke hides the same distinction from a non-lister.
	if err := service.Revoke(ctx, "t", "missing", erin, false); errorCode(err) != protocol.CodeForbidden {
		t.Fatalf("non-lister revoke missing = %v, want forbidden", err)
	}
	if err := service.Revoke(ctx, "t", target.ID, erin, false); errorCode(err) != protocol.CodeForbidden {
		t.Fatalf("non-lister revoke unreachable = %v, want forbidden", err)
	}
	if err := service.Revoke(ctx, "t", "missing", admin, false); !errors.Is(err, ErrGrantNotFound) {
		t.Fatalf("lister revoke missing = %v, want ErrGrantNotFound", err)
	}
}

type failingStore struct {
	err error
}

func (s failingStore) List(context.Context, string) ([]Grant, error) { return nil, s.err }
func (s failingStore) Create(context.Context, string, Grant) (Grant, error) {
	return Grant{}, s.err
}
func (s failingStore) Update(context.Context, string, string, Grant) (Grant, error) {
	return Grant{}, s.err
}
func (s failingStore) Revoke(context.Context, string, string) error { return s.err }

func TestServerPreflightPropagatesScopeAndStoreErrors(t *testing.T) {
	board := log.New(log.Options{})
	newServer := func(authorizer server.Authorizer) *httptest.Server {
		t.Helper()
		httpServer := httptest.NewServer(server.New(server.Options{
			Board:     board,
			Authorize: authorizer,
			Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		}))
		t.Cleanup(httpServer.Close)
		return httpServer
	}
	get := func(httpServer *httptest.Server, path string) (int, []byte) {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, httpServer.URL+server.APIPrefix+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("X-Board-Actor", "local:user:alice")
		response, err := httpServer.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		data, _ := io.ReadAll(response.Body)
		return response.StatusCode, data
	}

	badScope := &Authorizer{
		Store: NewMemoryStore(),
		ScopeText: func(context.Context, string, protocol.Actor) (string, bool) {
			return "dev/=nope", true
		},
	}
	scopeServer := newServer(badScope)
	for _, path := range []string{"/channels", "/tasks"} {
		status, data := get(scopeServer, path)
		if status != http.StatusBadRequest || protocolErrorCode(t, data) != CodeInvalidScope {
			t.Fatalf("%s bad scope = %d: %s", path, status, data)
		}
	}

	tenantChanger := &Authorizer{
		Store: NewMemoryStore(),
		Tenant: func(context.Context, string, protocol.Actor) string {
			return "another-tenant"
		},
	}
	tenantServer := newServer(tenantChanger)
	status, data := get(tenantServer, "/channels")
	if status != http.StatusInternalServerError {
		t.Fatalf("tenant hook change = %d: %s", status, data)
	}

	broken := newServer(&Authorizer{Store: failingStore{err: errors.New("grants down")}})
	status, data = get(broken, "/channels")
	if status != http.StatusInternalServerError {
		t.Fatalf("store failure = %d: %s", status, data)
	}

	canceledAuth := &Authorizer{Store: failingStore{err: context.Canceled}}
	canceled := newServer(canceledAuth)
	status, data = get(canceled, "/channels")
	if status != 499 {
		t.Fatalf("canceled = %d: %s", status, data)
	}
	if _, err := canceledAuth.Explain(context.Background(), "default", aliceActor(), "dev"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Authorizer lost context.Canceled: %v", err)
	}

	deadlineAuth := &Authorizer{Store: failingStore{err: context.DeadlineExceeded}}
	deadline := newServer(deadlineAuth)
	status, data = get(deadline, "/channels")
	if status != http.StatusServiceUnavailable {
		t.Fatalf("deadline = %d: %s", status, data)
	}
	if _, err := deadlineAuth.Explain(context.Background(), "default", aliceActor(), "dev"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Authorizer lost context.DeadlineExceeded: %v", err)
	}
}

func TestAdmissionUsesTheBeforeCaller(t *testing.T) {
	alice := UserPrincipal("alice")
	before := []Grant{
		{ID: "g1", Tenant: "t", Principals: []Principal{alice}, Can: Can{"*": All()}},
		{ID: "g2", Tenant: "t", Principals: []Principal{alice}, Can: Can{"secret/": Only(PermissionRead)}},
		{ID: "g3", Tenant: "t", Principals: []Principal{EveryonePrincipal()}, Can: Can{"secret/": Only(PermissionPost)}},
	}
	changed := Grant{
		ID:         "g4",
		Tenant:     "t",
		Principals: []Principal{UserPrincipal("bob")},
		Can:        Can{"dev/": All()},
	}
	// The after set drops g2. The after caller would gain post at secret/
	// (falling back to *), but admission judges the caller before the change
	// and sees only read there.
	after := []Grant{before[0], before[2], changed}
	refusal := CheckGrantChange(GrantChange{
		Tenant:    "t",
		Caller:    Caller{Principal: alice},
		Before:    before,
		After:     after,
		Changed:   &changed,
		LockMeOut: true,
	})
	if refusal == nil || refusal.Reason != RefusalAdmission || refusal.Pattern != "secret/" || refusal.Permission != PermissionPost {
		t.Fatalf("refusal = %+v, want admission at secret/ for post", refusal)
	}
}

func TestScopedOwnerStillPassesAdmission(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	service := NewGrantService(store)
	mustCreateGrant(t, store, "t", Grant{
		Principals: []Principal{UserPrincipal("owner")},
		Can:        Can{"*": All()},
	})
	mustCreateGrant(t, store, "t", Grant{
		Principals: []Principal{UserPrincipal("owner")},
		Can:        Can{"secret/": Only(PermissionRead)},
	})
	mustCreateGrant(t, store, "t", Grant{
		Principals: []Principal{EveryonePrincipal()},
		Can:        Can{"secret/": Only(PermissionPost)},
	})
	scope := Can{"*": All()}
	_, err := service.Create(ctx, "t", Caller{Principal: UserPrincipal("owner"), Owner: true, Scope: &scope}, Grant{
		Principals: []Principal{UserPrincipal("bob")},
		Can:        Can{"dev/": All()},
	}, false)
	var refusal *Refusal
	if !errors.As(err, &refusal) || refusal.Reason != RefusalAdmission || refusal.Pattern != "secret/" {
		t.Fatalf("scoped owner create = %v, want admission at secret/", err)
	}
}

func TestUpdatePropagatesTeamFuncErrors(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	service := NewGrantService(store)
	target := mustCreateGrant(t, store, "t", Grant{
		Principals: []Principal{UserPrincipal("bob")},
		Can:        Can{"dev/": Only(PermissionRead)},
	})
	service.TeamFunc = func(context.Context, string) (string, error) {
		return "", errors.New("team source down")
	}
	can := Can{"dev/": Only(PermissionRead)}
	_, err := service.Update(ctx, "t", target.ID, Caller{Principal: UserPrincipal("owner"), Owner: true}, GrantUpdate{Can: &can}, false)
	if err == nil || !strings.Contains(err.Error(), "team source down") {
		t.Fatalf("update TeamFunc error = %v", err)
	}
}

func TestEmailShapedUserMatchesEndToEnd(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	mustCreateGrant(t, store, "default", Grant{
		Principals: []Principal{{Kind: PrincipalUser, UserID: "local:user:Alice@Example.com"}},
		Can:        Can{"work": Only(PermissionRead)},
	})
	authorizer := &Authorizer{Store: store}
	actor := protocol.Actor{Principal: "local:user:Alice@Example.com", Kind: protocol.ActorPerson}
	if err := authorizer.Authorize(ctx, "default", actor, "work", PermissionRead); err != nil {
		t.Fatalf("email-shaped actor: %v", err)
	}
}

func TestAuthorizerScopeTextErrorsStillBadRequest(t *testing.T) {
	ctx := context.Background()
	authorizer := &Authorizer{
		Store: NewMemoryStore(),
		ScopeText: func(context.Context, string, protocol.Actor) (string, bool) {
			return "dev/=nope", true
		},
	}
	expectAccessError(t, authorizer.ValidateRequest(ctx, "default", aliceActor()), CodeInvalidScope, 400)
}
