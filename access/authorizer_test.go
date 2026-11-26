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
	expectAccessError(t, authorizer.Authorize(ctx, alice, "work", PermissionRead), protocol.CodeNotFound, 404)
	expectAccessError(t, authorizer.Authorize(ctx, alice, "secret", PermissionRead), protocol.CodeNotFound, 404)
	expectAccessError(t, authorizer.Authorize(ctx, alice, "secret", PermissionPost), protocol.CodeNotFound, 404)

	// Write-only is a real policy: posting to a channel you can't read works.
	if err := authorizer.Authorize(ctx, alice, "work", PermissionPost); err != nil {
		t.Fatalf("write-only post: %v", err)
	}

	// A refused write you can read is 403.
	if err := authorizer.Authorize(ctx, alice, "public", PermissionRead); err != nil {
		t.Fatalf("read public: %v", err)
	}
	expectAccessError(t, authorizer.Authorize(ctx, alice, "public", PermissionPost), protocol.CodeForbidden, 403)

	// A caller nothing names is 404, and never gains everyone's permissions.
	expectAccessError(t, authorizer.Authorize(ctx, bobActor(), "public", PermissionRead), protocol.CodeNotFound, 404)
	if authorizer.Can(ctx, bobActor(), "public", PermissionRead) {
		t.Fatal("bob should have no access")
	}
	if !authorizer.Can(ctx, alice, "public", PermissionRead) {
		t.Fatal("alice should read public")
	}

	explanation, err := authorizer.Explain(ctx, alice, "public")
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

	expectAccessError(t, authorizer.Authorize(ctx, alice, "work", PermissionRead), protocol.CodeForbidden, 403)
	expectAccessError(t, authorizer.Authorize(ctx, alice, "secret", PermissionRead), protocol.CodeForbidden, 403)
	expectAccessError(t, authorizer.Authorize(ctx, bobActor(), "work", PermissionRead), protocol.CodeForbidden, 403)
	if err := authorizer.Authorize(ctx, alice, "work", PermissionPost); err != nil {
		t.Fatalf("write-only post: %v", err)
	}
}

func TestAuthorizerTenantContextAndHook(t *testing.T) {
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

	if err := authorizer.Authorize(WithTenant(ctx, "tenant-a"), alice, "work", PermissionRead); err != nil {
		t.Fatalf("alice in tenant-a: %v", err)
	}
	expectAccessError(t, authorizer.Authorize(WithTenant(ctx, "tenant-b"), alice, "work", PermissionRead), protocol.CodeNotFound, 404)
	if err := authorizer.Authorize(WithTenant(ctx, "tenant-b"), bob, "work", PermissionRead); err != nil {
		t.Fatalf("bob in tenant-b: %v", err)
	}
	expectAccessError(t, authorizer.Authorize(WithTenant(ctx, "tenant-a"), bob, "work", PermissionRead), protocol.CodeNotFound, 404)

	// A Tenant hook decides the tenant when there is one.
	byHandle := &Authorizer{Store: store, Tenant: func(_ context.Context, actor protocol.Actor) string {
		return actor.Handle
	}}
	tenantAActor := alice
	tenantAActor.Handle = "tenant-a"
	if err := byHandle.Authorize(ctx, tenantAActor, "work", PermissionRead); err != nil {
		t.Fatalf("hook tenant-a: %v", err)
	}
	tenantBActor := alice
	tenantBActor.Handle = "tenant-b"
	expectAccessError(t, byHandle.Authorize(ctx, tenantBActor, "work", PermissionRead), protocol.CodeNotFound, 404)
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
		Principal: func(actor protocol.Actor) (Principal, error) {
			return OIDCPrincipal("https://gitlab.com", map[string][]string{"sub": {actor.Principal}}), nil
		},
	}
	actor := protocol.Actor{Principal: "project_path:acme/api:job-1", Kind: protocol.ActorExternal}
	if err := authorizer.Authorize(ctx, actor, "ci", PermissionPost); err != nil {
		t.Fatalf("oidc hook: %v", err)
	}
	wrong := protocol.Actor{Principal: "project_path:acme/api-evil:job-1", Kind: protocol.ActorExternal}
	expectAccessError(t, authorizer.Authorize(ctx, wrong, "ci", PermissionPost), protocol.CodeNotFound, 404)
}

func TestAuthorizerInvalidActorAndMissingStore(t *testing.T) {
	authorizer := &Authorizer{Store: NewMemoryStore()}
	expectAccessError(t, authorizer.Authorize(context.Background(), protocol.Actor{}, "dev", PermissionRead), protocol.CodeUnauthorized, 401)
	empty := &Authorizer{}
	expectAccessError(t, empty.Authorize(context.Background(), aliceActor(), "dev", PermissionRead), protocol.CodeInternal, 500)
}

func TestAuthorizerThroughServer(t *testing.T) {
	store := NewMemoryStore()
	mustCreateGrant(t, store, "default", Grant{
		Principals: []Principal{UserPrincipal("local:user:alice")},
		Can:        Can{"public": Only(PermissionRead), "work": Only(PermissionPost)},
	})
	board := log.New(log.Options{})
	api := server.New(server.Options{
		Board:     board,
		Authorize: &Authorizer{Store: store},
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	httpServer := httptest.NewServer(api)
	t.Cleanup(httpServer.Close)

	request := func(method, path string, body any) (int, []byte) {
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

	status, data := request(http.MethodGet, "/messages?channel=public", nil)
	if status != http.StatusOK {
		t.Fatalf("read public = %d: %s", status, data)
	}
	status, data = request(http.MethodGet, "/messages?channel=secret", nil)
	if status != http.StatusNotFound {
		t.Fatalf("read secret = %d: %s", status, data)
	}
	status, data = request(http.MethodPost, "/messages", map[string]any{"channel": "public", "text": "hi"})
	if status != http.StatusForbidden {
		t.Fatalf("post public = %d: %s", status, data)
	}
	status, data = request(http.MethodPost, "/messages", map[string]any{"channel": "work", "text": "blind"})
	if status != http.StatusCreated {
		t.Fatalf("write-only post = %d: %s", status, data)
	}
	status, data = request(http.MethodGet, "/messages?channel=work", nil)
	if status != http.StatusNotFound {
		t.Fatalf("read work = %d: %s", status, data)
	}
}
