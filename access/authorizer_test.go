package access

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jixoai/keel-board/log"
	"github.com/jixoai/keel-board/protocol"
	"github.com/jixoai/keel-board/server"
)

func aliceActor() protocol.Actor {
	return protocol.Actor{
		ID:        "local:user:alice",
		Principal: "local:user:alice",
		Kind:      protocol.ActorPerson,
		Handle:    "alice",
	}
}

func bobActor() protocol.Actor {
	return protocol.Actor{
		ID:        "local:user:bob",
		Principal: "local:user:bob",
		Kind:      protocol.ActorPerson,
		Handle:    "bob",
	}
}

func expectAccessError(t *testing.T, err error, code protocol.Code, status int) {
	t.Helper()
	boardErr, ok := protocol.AsError(err)
	if !ok {
		t.Fatalf("error %v isn't a board error", err)
	}
	if boardErr.Code != code || boardErr.Status != status {
		t.Fatalf("error = %+v, want %s/%d", boardErr, code, status)
	}
}

func TestAuthorizerDenialSemantics(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	mustCreateGrant(t, store, "default", Grant{
		Principals: []Principal{UserPrincipal("local:user:alice")},
		Can:        Can{"work": Only(PermissionPost)},
	})
	mustCreateGrant(t, store, "default", Grant{
		Principals: []Principal{UserPrincipal("local:user:alice")},
		Can:        Can{"public": Only(PermissionRead)},
	})
	authorizer := &Authorizer{Store: store}
	alice := aliceActor()

	// A channel the caller can't read is 404, even when it asks to write.
	expectAccessError(t, authorizer.Authorize(ctx, "default", alice, "work", PermissionRead), protocol.CodeNotFound, 404)
	expectAccessError(t, authorizer.Authorize(ctx, "default", alice, "secret", PermissionRead), protocol.CodeNotFound, 404)
	expectAccessError(t, authorizer.Authorize(ctx, "default", alice, "secret", PermissionPost), protocol.CodeNotFound, 404)

	// Write-only is a real policy: posting to a channel you can't read works.
	if err := authorizer.Authorize(ctx, "default", alice, "work", PermissionPost); err != nil {
		t.Fatalf("write-only post: %v", err)
	}

	// A refused write you can read is 403.
	if err := authorizer.Authorize(ctx, "default", alice, "public", PermissionRead); err != nil {
		t.Fatalf("read public: %v", err)
	}
	expectAccessError(t, authorizer.Authorize(ctx, "default", alice, "public", PermissionPost), protocol.CodeForbidden, 403)

	// A caller nothing names is 404, and never gains everyone's permissions.
	expectAccessError(t, authorizer.Authorize(ctx, "default", bobActor(), "public", PermissionRead), protocol.CodeNotFound, 404)
	if authorizer.Can(ctx, "default", bobActor(), "public", PermissionRead) {
		t.Fatal("bob should have no access")
	}
	if !authorizer.Can(ctx, "default", alice, "public", PermissionRead) {
		t.Fatal("alice should read public")
	}

	explanation, err := authorizer.Explain(ctx, "default", alice, "public")
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	if explanation.Pattern != "public" || explanation.Level != LevelPrincipal || !explanation.Allows(PermissionRead) {
		t.Fatalf("explanation = %+v", explanation)
	}
}

func TestAuthorizerRevealUnreadable(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	mustCreateGrant(t, store, "default", Grant{
		Principals: []Principal{UserPrincipal("local:user:alice")},
		Can:        Can{"work": Only(PermissionPost)},
	})
	authorizer := &Authorizer{Store: store, RevealUnreadable: true}
	alice := aliceActor()

	expectAccessError(t, authorizer.Authorize(ctx, "default", alice, "work", PermissionRead), protocol.CodeForbidden, 403)
	expectAccessError(t, authorizer.Authorize(ctx, "default", alice, "secret", PermissionRead), protocol.CodeForbidden, 403)
	expectAccessError(t, authorizer.Authorize(ctx, "default", bobActor(), "work", PermissionRead), protocol.CodeForbidden, 403)
	if err := authorizer.Authorize(ctx, "default", alice, "work", PermissionPost); err != nil {
		t.Fatalf("write-only post: %v", err)
	}
}

func TestAuthorizerTenantIsExplicit(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	mustCreateGrant(t, store, "tenant-a", Grant{
		Principals: []Principal{UserPrincipal("local:user:alice")},
		Can:        Can{"work": All()},
	})
	mustCreateGrant(t, store, "tenant-b", Grant{
		Principals: []Principal{UserPrincipal("local:user:bob")},
		Can:        Can{"work": All()},
	})
	authorizer := &Authorizer{Store: store}
	alice := aliceActor()
	bob := bobActor()

	if err := authorizer.Authorize(ctx, "tenant-a", alice, "work", PermissionRead); err != nil {
		t.Fatalf("alice in tenant-a: %v", err)
	}
	expectAccessError(t, authorizer.Authorize(ctx, "tenant-b", alice, "work", PermissionRead), protocol.CodeNotFound, 404)
	if err := authorizer.Authorize(ctx, "tenant-b", bob, "work", PermissionRead); err != nil {
		t.Fatalf("bob in tenant-b: %v", err)
	}
	expectAccessError(t, authorizer.Authorize(ctx, "tenant-a", bob, "work", PermissionRead), protocol.CodeNotFound, 404)

	// A Tenant hook may confirm the server's tenant but may not replace it.
	confirming := &Authorizer{Store: store, Tenant: func(_ context.Context, tenant string, _ protocol.Actor) string {
		return tenant
	}}
	if err := confirming.Authorize(ctx, "tenant-a", alice, "work", PermissionRead); err != nil {
		t.Fatalf("confirming tenant hook: %v", err)
	}
	different := &Authorizer{Store: store, Tenant: func(_ context.Context, _ string, _ protocol.Actor) string {
		return "tenant-b"
	}}
	expectAccessError(t, different.Authorize(ctx, "tenant-a", alice, "work", PermissionRead), protocol.CodeInternal, 500)
	expectAccessError(t, different.ValidateRequest(ctx, "tenant-a", alice), protocol.CodeInternal, 500)
}

func TestActorPrincipal(t *testing.T) {
	user, err := ActorPrincipal(protocol.Actor{Principal: "local:user:alice", Kind: protocol.ActorPerson})
	if err != nil || user.Kind != PrincipalUser || user.UserID != "local:user:alice" {
		t.Fatalf("person = %+v, %v", user, err)
	}
	agent, err := ActorPrincipal(protocol.Actor{Principal: "local:user:alice", Kind: protocol.ActorAgent})
	if err != nil || agent.Kind != PrincipalUser {
		t.Fatalf("agent = %+v, %v", agent, err)
	}
	environment := "preview"
	deployment, err := ActorPrincipal(protocol.Actor{
		Principal: "vercel:project:prj_1",
		Kind:      protocol.ActorDeployment,
		Via:       &protocol.Via{Environment: &environment},
	})
	if err != nil || deployment.Kind != PrincipalProject || deployment.ProjectID != "vercel:project:prj_1" || deployment.Environment != "preview" {
		t.Fatalf("deployment = %+v, %v", deployment, err)
	}
	production, err := ActorPrincipal(protocol.Actor{Principal: "vercel:project:prj_1", Kind: protocol.ActorDeployment})
	if err != nil || production.Environment != "production" {
		t.Fatalf("deployment default = %+v, %v", production, err)
	}
	workflow, err := ActorPrincipal(protocol.Actor{Principal: "acme/web", Kind: protocol.ActorWorkflow})
	if err != nil || workflow.Kind != PrincipalGitHub || workflow.Repository != "acme/web" {
		t.Fatalf("workflow = %+v, %v", workflow, err)
	}
	if _, err := ActorPrincipal(protocol.Actor{Kind: protocol.ActorPerson}); err == nil {
		t.Fatal("an actor without a principal should fail")
	}
}

func TestAuthorizerCustomPrincipalHook(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	mustCreateGrant(t, store, "default", Grant{
		Principals: []Principal{{
			Kind:   PrincipalOIDC,
			Issuer: "https://gitlab.com",
			Claims: map[string][]string{"sub": {"project_path:acme/api:*"}},
		}},
		Can: Can{"ci/": Only(PermissionPost)},
	})
	authorizer := &Authorizer{
		Store: store,
		Principal: func(_ context.Context, _ string, actor protocol.Actor) (Principal, error) {
			return OIDCPrincipal("https://gitlab.com", map[string][]string{"sub": {actor.Principal}}), nil
		},
	}
	actor := protocol.Actor{Principal: "project_path:acme/api:job-1", Kind: protocol.ActorExternal}
	if err := authorizer.Authorize(ctx, "default", actor, "ci", PermissionPost); err != nil {
		t.Fatalf("oidc hook: %v", err)
	}
	wrong := protocol.Actor{Principal: "project_path:acme/api-evil:job-1", Kind: protocol.ActorExternal}
	expectAccessError(t, authorizer.Authorize(ctx, "default", wrong, "ci", PermissionPost), protocol.CodeNotFound, 404)
}

func TestAuthorizerTeamHookFillsCallerTeam(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	mustCreateGrant(t, store, "default", Grant{
		Principals: []Principal{{
			Kind:         PrincipalProject,
			ProjectID:    "vercel:project:prj_1",
			TeamID:       "team-a",
			Environments: []string{"production"},
		}},
		Can: Can{"work": All()},
	})
	actor := protocol.Actor{Principal: "vercel:project:prj_1", Kind: protocol.ActorDeployment}

	withoutTeam := &Authorizer{Store: store}
	expectAccessError(t, withoutTeam.Authorize(ctx, "default", actor, "work", PermissionRead), protocol.CodeNotFound, 404)

	withTeam := &Authorizer{Store: store, Team: func(_ context.Context, _ string, _ Principal) string {
		return "team-a"
	}}
	if err := withTeam.Authorize(ctx, "default", actor, "work", PermissionRead); err != nil {
		t.Fatalf("team hook: %v", err)
	}
	if !withTeam.Can(ctx, "default", actor, "work", PermissionManage) {
		t.Fatal("the team hook should fill the caller's team")
	}
}

func TestAuthorizerInvalidActorAndMissingStore(t *testing.T) {
	authorizer := &Authorizer{Store: NewMemoryStore()}
	expectAccessError(t, authorizer.Authorize(context.Background(), "default", protocol.Actor{}, "dev", PermissionRead), protocol.CodeUnauthorized, 401)
	empty := &Authorizer{}
	expectAccessError(t, empty.Authorize(context.Background(), "default", aliceActor(), "dev", PermissionRead), protocol.CodeInternal, 500)
}

func TestAuthorizerScopeText(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	mustCreateGrant(t, store, "default", Grant{
		Principals: []Principal{UserPrincipal("local:user:alice")},
		Can:        Can{"*": All()},
	})
	authorizer := &Authorizer{
		Store: store,
		ScopeText: func(_ context.Context, _ string, _ protocol.Actor) (string, bool) {
			return "dev/=read", true
		},
	}
	alice := aliceActor()
	if err := authorizer.Authorize(ctx, "default", alice, "dev", PermissionRead); err != nil {
		t.Fatalf("scoped read: %v", err)
	}
	expectAccessError(t, authorizer.Authorize(ctx, "default", alice, "dev", PermissionPost), protocol.CodeForbidden, 403)
	expectAccessError(t, authorizer.Authorize(ctx, "default", alice, "other", PermissionRead), protocol.CodeNotFound, 404)

	bad := &Authorizer{
		Store: store,
		ScopeText: func(_ context.Context, _ string, _ protocol.Actor) (string, bool) {
			return "dev/=nope", true
		},
	}
	expectAccessError(t, bad.Authorize(ctx, "default", alice, "dev", PermissionRead), CodeInvalidScope, 400)

	long := &Authorizer{
		Store: store,
		ScopeText: func(_ context.Context, _ string, _ protocol.Actor) (string, bool) {
			return string(bytes.Repeat([]byte("x"), MaxCanText+1)), true
		},
	}
	expectAccessError(t, long.Authorize(ctx, "default", alice, "dev", PermissionRead), CodeInvalidScope, 400)
}

func TestAuthorizerThroughServerTenantHeader(t *testing.T) {
	store := NewMemoryStore()
	mustCreateGrant(t, store, "default", Grant{
		Principals: []Principal{UserPrincipal("local:user:alice")},
		Can:        Can{"public": Only(PermissionRead), "work": Only(PermissionPost)},
	})
	mustCreateGrant(t, store, "tenant-b", Grant{
		Principals: []Principal{UserPrincipal("local:user:alice")},
		Can:        Can{"public": Only(PermissionRead)},
	})
	board := log.New(log.Options{})
	api := server.New(server.Options{
		Board:     board,
		Authorize: &Authorizer{Store: store},
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	httpServer := httptest.NewServer(api)
	t.Cleanup(httpServer.Close)

	request := func(method, path string, body any, tenant string) (int, []byte) {
		t.Helper()
		var reader io.Reader
		if body != nil {
			encoded, err := json.Marshal(body)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			reader = bytes.NewReader(encoded)
		}
		req, err := http.NewRequest(method, httpServer.URL+server.APIPrefix+path, reader)
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		req.Header.Set("X-Board-Actor", "local:user:alice")
		if tenant != "" {
			req.Header.Set("X-Board-Tenant", tenant)
		}
		if body != nil {
			req.Header.Set("content-type", "application/json")
		}
		response, err := httpServer.Client().Do(req)
		if err != nil {
			t.Fatalf("do: %v", err)
		}
		defer response.Body.Close()
		data, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		return response.StatusCode, data
	}

	status, data := request(http.MethodGet, "/messages?channel=public", nil, "")
	if status != http.StatusOK {
		t.Fatalf("read public = %d: %s", status, data)
	}
	// The default tenant's grant must not authorize tenant-b.
	status, data = request(http.MethodGet, "/messages?channel=public", nil, "tenant-b")
	if status != http.StatusOK {
		t.Fatalf("tenant-b has its own grant, got %d: %s", status, data)
	}
	// A tenant with no grants gets nothing, not the default tenant's access.
	status, data = request(http.MethodGet, "/messages?channel=public", nil, "tenant-c")
	if status != http.StatusNotFound {
		t.Fatalf("tenant-c leaked the default grant, got %d: %s", status, data)
	}
	status, data = request(http.MethodGet, "/messages?channel=secret", nil, "")
	if status != http.StatusNotFound {
		t.Fatalf("read secret = %d: %s", status, data)
	}
	status, data = request(http.MethodPost, "/messages", map[string]any{"channel": "public", "text": "hi"}, "")
	if status != http.StatusForbidden {
		t.Fatalf("post public = %d: %s", status, data)
	}
	status, data = request(http.MethodPost, "/messages", map[string]any{"channel": "work", "text": "blind"}, "")
	if status != http.StatusCreated {
		t.Fatalf("write-only post = %d: %s", status, data)
	}
	status, data = request(http.MethodGet, "/messages?channel=work", nil, "")
	if status != http.StatusNotFound {
		t.Fatalf("read work = %d: %s", status, data)
	}
	// A channel tenant-b doesn't grant is refused even though default does.
	status, data = request(http.MethodPost, "/messages", map[string]any{"channel": "work", "text": "blind"}, "tenant-b")
	if status != http.StatusNotFound {
		t.Fatalf("tenant-b write-only post = %d: %s", status, data)
	}
}

func TestServerAsProtocolRecognizesRefusalAndCanceled(t *testing.T) {
	board := log.New(log.Options{})
	lockout := server.AuthorizerFunc(func(context.Context, string, protocol.Actor, string, protocol.Permission) error {
		return &Refusal{Reason: RefusalLockout, Pattern: "*", Permission: PermissionManage}
	})
	httpServer := httptest.NewServer(server.New(server.Options{
		Board:     board,
		Authorize: lockout,
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}))
	t.Cleanup(httpServer.Close)
	request := func(body any) (int, []byte) {
		t.Helper()
		encoded, _ := json.Marshal(body)
		req, err := http.NewRequest(http.MethodPost, httpServer.URL+server.APIPrefix+"/messages", bytes.NewReader(encoded))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("content-type", "application/json")
		req.Header.Set("X-Board-Actor", "local:user:alice")
		response, err := httpServer.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		data, _ := io.ReadAll(response.Body)
		return response.StatusCode, data
	}
	status, data := request(map[string]any{"channel": "work", "text": "hi"})
	if status != http.StatusConflict {
		t.Fatalf("lockout status = %d: %s", status, data)
	}
	if code := protocolErrorCode(t, data); code != CodeGrantLockout {
		t.Fatalf("lockout code = %s: %s", code, data)
	}

	canceled := server.AuthorizerFunc(func(context.Context, string, protocol.Actor, string, protocol.Permission) error {
		return context.Canceled
	})
	httpServer2 := httptest.NewServer(server.New(server.Options{
		Board:     board,
		Authorize: canceled,
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}))
	t.Cleanup(httpServer2.Close)
	req, _ := http.NewRequest(http.MethodPost, httpServer2.URL+server.APIPrefix+"/messages", bytes.NewReader([]byte(`{"channel":"work","text":"hi"}`)))
	req.Header.Set("content-type", "application/json")
	req.Header.Set("X-Board-Actor", "local:user:alice")
	response, err := httpServer2.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 499 {
		t.Fatalf("canceled status = %d", response.StatusCode)
	}
}

func protocolErrorCode(t *testing.T, data []byte) protocol.Code {
	t.Helper()
	var body protocol.ErrorBody
	if err := json.Unmarshal(data, &body); err != nil {
		t.Fatalf("decode error: %v: %s", err, data)
	}
	return body.Error.Code
}

func TestAuthorizerMemberTeamAndCapHooks(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	mustCreateGrant(t, store, "default", Grant{
		Principals: []Principal{AllUsersPrincipal()},
		Can:        Can{"*": Only(PermissionRead)},
	})
	mustCreateGrant(t, store, "default", Grant{
		Principals: []Principal{AllProjectsPrincipal("production")},
		Can:        Can{"dev/": All()},
	})
	mustCreateGrant(t, store, "default", Grant{
		Principals: []Principal{UserPrincipal("local:user:capped")},
		Can:        Can{"*": All()},
	})
	user := aliceActor()

	member := &Authorizer{Store: store, Member: func(_ context.Context, tenant string, _ Principal) bool {
		return tenant == "default"
	}}
	if err := member.Authorize(ctx, "default", user, "dev", PermissionRead); err != nil {
		t.Fatalf("member read: %v", err)
	}
	outsider := &Authorizer{Store: store}
	expectAccessError(t, outsider.Authorize(ctx, "default", user, "dev", PermissionRead), protocol.CodeNotFound, 404)

	project := protocol.Actor{Principal: "vercel:project:prj_1", Kind: protocol.ActorDeployment}
	teamProject := &Authorizer{Store: store, TeamProject: func(_ context.Context, _ string, _ Principal) bool {
		return true
	}}
	if err := teamProject.Authorize(ctx, "default", project, "dev", PermissionManage); err != nil {
		t.Fatalf("team project: %v", err)
	}
	expectAccessError(t, outsider.Authorize(ctx, "default", project, "dev", PermissionRead), protocol.CodeNotFound, 404)

	cappedActor := protocol.Actor{Principal: "local:user:capped", Kind: protocol.ActorPerson}
	capped := &Authorizer{Store: store, Cap: func(_ context.Context, _ string, _ Principal) []Permission {
		return []Permission{PermissionRead}
	}}
	if err := capped.Authorize(ctx, "default", cappedActor, "dev", PermissionRead); err != nil {
		t.Fatalf("capped read: %v", err)
	}
	expectAccessError(t, capped.Authorize(ctx, "default", cappedActor, "dev", PermissionPost), protocol.CodeForbidden, 403)
}
