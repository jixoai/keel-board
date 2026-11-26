package access

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/jixoai/keel-board/protocol"
)

func mustCreateGrant(t *testing.T, store GrantStore, tenant string, grant Grant) Grant {
	t.Helper()
	created, err := store.Create(context.Background(), tenant, grant)
	if err != nil {
		t.Fatalf("create grant: %v", err)
	}
	return created
}

func grantByID(t *testing.T, grants []Grant, id string) Grant {
	t.Helper()
	for _, grant := range grants {
		if grant.ID == id {
			return grant
		}
	}
	t.Fatalf("no grant %q in %+v", id, grants)
	return Grant{}
}

func TestMemoryStoreCRUD(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	created := mustCreateGrant(t, store, "t", Grant{
		Principals: []Principal{UserPrincipal("alice")},
		Can:        Can{"*": Only(PermissionRead)},
	})
	if created.ID != "g1" || created.Tenant != "t" {
		t.Fatalf("created = %+v", created)
	}
	if created.CreatedAt.IsZero() || created.UpdatedAt.IsZero() {
		t.Fatalf("timestamps weren't set: %+v", created)
	}

	explicit := mustCreateGrant(t, store, "t", Grant{
		ID:         "custom",
		Principals: []Principal{UserPrincipal("bob")},
		Can:        Can{"dev/": All()},
	})
	if explicit.ID != "custom" {
		t.Fatalf("explicit ID = %q", explicit.ID)
	}

	list, err := store.List(ctx, "t")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("list = %+v", list)
	}
	// A returned grant is a copy: mutating it changes nothing in the store.
	for index := range list {
		if list[index].ID == "g1" {
			list[index].Can["*"] = All()
		}
	}
	again, err := store.List(ctx, "t")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if stored := grantByID(t, again, "g1"); stored.Can["*"].All {
		t.Fatalf("the store's grant was mutated through List: %+v", stored)
	}

	dev := Can{"dev/": Only(PermissionPost)}
	updated, err := store.Update(ctx, "t", "g1", Grant{
		Principals: []Principal{UserPrincipal("bob")},
		Can:        dev,
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if len(updated.Principals) != 1 || updated.Principals[0].UserID != "bob" {
		t.Fatalf("principals = %+v", updated.Principals)
	}
	if !reflect.DeepEqual(updated.Can, dev) {
		t.Fatalf("can = %+v, want %+v", updated.Can, dev)
	}
	if updated.ID != "g1" || updated.Tenant != "t" || updated.CreatedAt.IsZero() {
		t.Fatalf("identity or timestamps lost: %+v", updated)
	}

	if err := store.Revoke(ctx, "t", "g1"); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if err := store.Revoke(ctx, "t", "g1"); !errors.Is(err, ErrGrantNotFound) {
		t.Fatalf("second revoke = %v, want ErrGrantNotFound", err)
	}
	if _, err := store.Update(ctx, "t", "missing", Grant{Principals: []Principal{UserPrincipal("x")}, Can: Can{"*": All()}}); !errors.Is(err, ErrGrantNotFound) {
		t.Fatalf("update missing = %v", err)
	}
	if _, err := store.Create(ctx, "t", Grant{ID: "custom", Principals: []Principal{UserPrincipal("x")}, Can: Can{"*": All()}}); !errors.Is(err, ErrGrantExists) {
		t.Fatalf("duplicate create = %v", err)
	}
}

func TestMemoryStoreTenantIsolation(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	created := mustCreateGrant(t, store, "a", Grant{
		Principals: []Principal{UserPrincipal("alice")},
		Can:        Can{"*": All()},
	})

	other, err := store.List(ctx, "b")
	if err != nil || len(other) != 0 {
		t.Fatalf("tenant b list = %+v, %v", other, err)
	}
	if _, err := store.Update(ctx, "b", created.ID, Grant{Principals: []Principal{UserPrincipal("x")}, Can: Can{"*": All()}}); !errors.Is(err, ErrGrantNotFound) {
		t.Fatalf("tenant b update = %v, want ErrGrantNotFound", err)
	}
	if err := store.Revoke(ctx, "b", created.ID); !errors.Is(err, ErrGrantNotFound) {
		t.Fatalf("tenant b revoke = %v, want ErrGrantNotFound", err)
	}
	if _, err := store.Create(ctx, "a", Grant{
		Tenant:     "b",
		Principals: []Principal{UserPrincipal("alice")},
		Can:        Can{"*": All()},
	}); !errors.Is(err, ErrTenantMismatch) {
		t.Fatalf("cross-tenant create = %v, want ErrTenantMismatch", err)
	}

	// The same ID may exist in two tenants; each keeps its own.
	mustCreateGrant(t, store, "b", Grant{
		ID:         created.ID,
		Principals: []Principal{UserPrincipal("bob")},
		Can:        Can{"*": Only(PermissionRead)},
	})
	a, _ := store.List(ctx, "a")
	b, _ := store.List(ctx, "b")
	if len(a) != 1 || a[0].Principals[0].UserID != "alice" {
		t.Fatalf("tenant a = %+v", a)
	}
	if len(b) != 1 || b[0].Principals[0].UserID != "bob" {
		t.Fatalf("tenant b = %+v", b)
	}
}

func TestNormalizeGrant(t *testing.T) {
	grant, err := NormalizeGrant(Grant{
		Principals: []Principal{
			{Kind: PrincipalUser, Email: "Alice@Example.com"},
			{Kind: PrincipalProject, ProjectID: "prj_1"},
			{Kind: PrincipalGitHub, Repository: "acme/web", Workflow: "ci.yml", Branch: "refs/heads/main"},
		},
		Can: Can{"*": Only(PermissionRead, PermissionRead)},
	})
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if grant.Principals[0].UserID != "alice@example.com" || grant.Principals[0].Email != "alice@example.com" {
		t.Fatalf("user = %+v", grant.Principals[0])
	}
	if !reflect.DeepEqual(grant.Principals[1].Environments, []string{"production"}) {
		t.Fatalf("project environments = %v", grant.Principals[1].Environments)
	}
	if grant.Principals[2].Workflow != ".github/workflows/ci.yml" || grant.Principals[2].Branch != "main" {
		t.Fatalf("github = %+v", grant.Principals[2])
	}
	if !reflect.DeepEqual(grant.Can["*"].Permissions, []Permission{PermissionRead}) {
		t.Fatalf("can = %+v", grant.Can)
	}

	invalid := []Grant{
		{Principals: []Principal{{Kind: PrincipalUser, UserID: "a", Email: "a@example.com"}}, Can: Can{"*": All()}},
		{Principals: []Principal{{Kind: PrincipalProject}}, Can: Can{"*": All()}},
		{Principals: []Principal{{Kind: PrincipalOIDC, Issuer: "https://issuer", Claims: map[string][]string{"ref": {"main"}}}}, Can: Can{"*": All()}},
		{Principals: []Principal{UserPrincipal("alice")}, Can: Can{"a/*": All()}},
		{Principals: []Principal{UserPrincipal("alice")}, Can: Can{"*": Only(Permission("nope"))}},
		{Principals: []Principal{UserPrincipal("alice")}, Can: Can{}},
	}
	for index, grant := range invalid {
		if _, err := NormalizeGrant(grant); err == nil {
			t.Fatalf("invalid grant %d normalized", index)
		}
	}

	oidc, err := NormalizeGrant(Grant{
		Principals: []Principal{{
			Kind:   PrincipalOIDC,
			Issuer: "https://gitlab.com",
			Claims: map[string][]string{"sub": {"project_path:acme/api:*"}, "project_id": {"42", "42"}},
		}},
		Can: Can{"ci/": Only(PermissionPost)},
	})
	if err != nil {
		t.Fatalf("oidc: %v", err)
	}
	if !reflect.DeepEqual(oidc.Principals[0].Claims["project_id"], []string{"42"}) {
		t.Fatalf("claims = %+v", oidc.Principals[0].Claims)
	}
}

func TestApplyUpdateAddsAndRemoves(t *testing.T) {
	old := Grant{
		ID:     "g1",
		Tenant: "t",
		Principals: []Principal{
			{Kind: PrincipalProject, ProjectID: "prj_1", Environments: []string{"production"}},
			{Kind: PrincipalGitHub, Repository: "acme/web", Workflow: ".github/workflows/ci.yml", Branch: "main"},
		},
		Can: Can{"*": Only(PermissionRead)},
	}
	next, err := ApplyUpdate(old, GrantUpdate{
		Remove: []Principal{{Kind: PrincipalGitHub, Repository: "acme/web"}},
		Add:    []Principal{{Kind: PrincipalProject, ProjectID: "prj_1", Environments: []string{"preview"}}},
	})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if len(next.Principals) != 1 {
		t.Fatalf("principals = %+v", next.Principals)
	}
	if !reflect.DeepEqual(next.Principals[0].Environments, []string{"production", "preview"}) {
		t.Fatalf("environments = %v", next.Principals[0].Environments)
	}

	// An update that names no can keeps the old one.
	kept, err := ApplyUpdate(old, GrantUpdate{Add: []Principal{UserPrincipal("alice")}})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if !reflect.DeepEqual(kept.Can, old.Can) {
		t.Fatalf("can changed: %+v", kept.Can)
	}

	// Removing every principal is refused as an empty grant; removing one the
	// grant doesn't name is refused as not_in_grant.
	if _, err := ApplyUpdate(Grant{Principals: []Principal{UserPrincipal("alice")}, Can: Can{"*": All()}}, GrantUpdate{
		Remove: []Principal{UserPrincipal("alice")},
	}); err == nil || errorCode(err) != CodeGrantEmpty {
		t.Fatalf("remove all = %v, want grant_empty", err)
	}
	if _, err := ApplyUpdate(old, GrantUpdate{
		Remove: []Principal{UserPrincipal("nobody")},
	}); err == nil || errorCode(err) != CodeNotInGrant {
		t.Fatalf("remove nobody = %v, want not_in_grant", err)
	}
}

func TestGrantServiceLockoutCreate(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	service := NewGrantService(store)
	seed := mustCreateGrant(t, store, "t", Grant{
		Principals: []Principal{UserPrincipal("alice")},
		Can:        Can{"*": All()},
	})
	caller := Caller{Principal: UserPrincipal("alice")}
	lock := Grant{
		Principals: []Principal{EveryonePrincipal()},
		Can:        Can{"security/": None()},
	}

	_, err := service.Create(ctx, "t", caller, lock, false)
	var refusal *Refusal
	if !errors.As(err, &refusal) {
		t.Fatalf("create lock = %v, want a refusal", err)
	}
	if refusal.Reason != RefusalLockout || refusal.Pattern != "security/" {
		t.Fatalf("refusal = %+v", refusal)
	}
	protocolError := refusal.ProtocolError()
	if protocolError.Status != 409 || protocolError.Code != CodeGrantLockout {
		t.Fatalf("protocol error = %+v", protocolError)
	}

	created, err := service.Create(ctx, "t", caller, lock, true)
	if err != nil {
		t.Fatalf("create with lockMeOut: %v", err)
	}
	if created.ID == "" || created.Tenant != "t" {
		t.Fatalf("created = %+v", created)
	}

	grants, err := store.List(ctx, "t")
	if err != nil || len(grants) != 2 {
		t.Fatalf("grants = %+v, %v", grants, err)
	}
	policy := Policy{Tenant: "t", Grants: grants}
	hidden := policy.Explain(UserPrincipal("alice"), "security/incidents")
	if hidden.Pattern != "security/" || len(hidden.Permissions) != 0 || hidden.Allows(PermissionManage) {
		t.Fatalf("the lock should close security/: %+v", hidden)
	}
	outside := policy.Explain(UserPrincipal("alice"), "other")
	if !outside.Allows(PermissionManage) {
		t.Fatalf("the * grant should still manage other channels: %+v", outside)
	}
	if seed.ID == "" {
		t.Fatal("seed lost")
	}
}

func TestGrantServiceLockoutRespectsAGrantKeepingCallerIn(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	service := NewGrantService(store)
	mustCreateGrant(t, store, "t", Grant{
		Principals: []Principal{UserPrincipal("alice")},
		Can:        Can{"*": All()},
	})
	mustCreateGrant(t, store, "t", Grant{
		Principals: []Principal{UserPrincipal("alice")},
		Can:        Can{"security/": All()},
	})
	caller := Caller{Principal: UserPrincipal("alice")}

	if _, err := service.Create(ctx, "t", caller, Grant{
		Principals: []Principal{EveryonePrincipal()},
		Can:        Can{"security/": None()},
	}, false); err != nil {
		t.Fatalf("a grant naming alice at security/ should keep her in: %v", err)
	}
	grants, _ := store.List(ctx, "t")
	explanation := (Policy{Tenant: "t", Grants: grants}).Explain(UserPrincipal("alice"), "security/incidents")
	if explanation.Pattern != "security/" || !explanation.Allows(PermissionManage) {
		t.Fatalf("alice should still manage security/: %+v", explanation)
	}
}

func TestGrantServiceLockoutUpdate(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	service := NewGrantService(store)
	mustCreateGrant(t, store, "t", Grant{
		Principals: []Principal{UserPrincipal("alice")},
		Can:        Can{"*": All()},
	})
	security := mustCreateGrant(t, store, "t", Grant{
		Principals: []Principal{UserPrincipal("alice")},
		Can:        Can{"security/": All()},
	})
	caller := Caller{Principal: UserPrincipal("alice")}

	readOnly := Can{"security/": Only(PermissionRead)}
	_, err := service.Update(ctx, "t", security.ID, caller, GrantUpdate{Can: &readOnly}, false)
	var refusal *Refusal
	if !errors.As(err, &refusal) || refusal.Reason != RefusalLockout || refusal.Pattern != "security/" {
		t.Fatalf("update = %v, want a lockout refusal", err)
	}
	if _, err := service.Update(ctx, "t", security.ID, caller, GrantUpdate{Can: &readOnly}, true); err != nil {
		t.Fatalf("update with lockMeOut: %v", err)
	}
	grants, _ := store.List(ctx, "t")
	explanation := (Policy{Tenant: "t", Grants: grants}).Explain(UserPrincipal("alice"), "security/incidents")
	if explanation.Allows(PermissionManage) || !explanation.Allows(PermissionRead) {
		t.Fatalf("alice should read but not manage security/: %+v", explanation)
	}
	if !(Policy{Tenant: "t", Grants: grants}).Can(UserPrincipal("alice"), "other", PermissionManage) {
		t.Fatal("the * grant should still manage other channels")
	}
}

func TestGrantServiceUpdateAddingAPrincipalIsNotALockout(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	service := NewGrantService(store)
	mustCreateGrant(t, store, "t", Grant{
		Principals: []Principal{UserPrincipal("alice")},
		Can:        Can{"*": All()},
	})
	security := mustCreateGrant(t, store, "t", Grant{
		Principals: []Principal{UserPrincipal("alice")},
		Can:        Can{"security/": All()},
	})
	caller := Caller{Principal: UserPrincipal("alice")}
	updated, err := service.Update(ctx, "t", security.ID, caller, GrantUpdate{
		Add: []Principal{UserPrincipal("bob")},
	}, false)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if len(updated.Principals) != 2 {
		t.Fatalf("principals = %+v", updated.Principals)
	}
}

func TestGrantServiceLockoutRevoke(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	service := NewGrantService(store)
	only := mustCreateGrant(t, store, "t", Grant{
		Principals: []Principal{UserPrincipal("alice")},
		Can:        Can{"security/": All()},
	})
	caller := Caller{Principal: UserPrincipal("alice")}

	err := service.Revoke(ctx, "t", only.ID, caller, false)
	var refusal *Refusal
	if !errors.As(err, &refusal) || refusal.Reason != RefusalLockout || refusal.Pattern != "security/" {
		t.Fatalf("revoke = %v, want a lockout refusal", err)
	}
	if err := service.Revoke(ctx, "t", only.ID, caller, true); err != nil {
		t.Fatalf("revoke with lockMeOut: %v", err)
	}
	grants, _ := store.List(ctx, "t")
	if len(grants) != 0 {
		t.Fatalf("grants = %+v", grants)
	}

	// A second grant that keeps manage elsewhere makes the revoke no lockout.
	store = NewMemoryStore()
	service = NewGrantService(store)
	mustCreateGrant(t, store, "t", Grant{
		Principals: []Principal{UserPrincipal("alice")},
		Can:        Can{"*": All()},
	})
	security := mustCreateGrant(t, store, "t", Grant{
		Principals: []Principal{UserPrincipal("alice")},
		Can:        Can{"security/": All()},
	})
	if err := service.Revoke(ctx, "t", security.ID, caller, false); err != nil {
		t.Fatalf("revoke should fall back to the * grant: %v", err)
	}
}

func TestGrantServiceReachRefusal(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	service := NewGrantService(store)
	mustCreateGrant(t, store, "t", Grant{
		Principals: []Principal{UserPrincipal("alice")},
		Can:        Can{"*": All()},
	})
	alice := Caller{Principal: UserPrincipal("alice")}
	bob := Caller{Principal: UserPrincipal("bob")}

	_, err := service.Create(ctx, "t", bob, Grant{
		Principals: []Principal{UserPrincipal("bob")},
		Can:        Can{"dev/": Only(PermissionRead)},
	}, false)
	var refusal *Refusal
	if !errors.As(err, &refusal) || refusal.Reason != RefusalReach || refusal.Pattern != "dev/" {
		t.Fatalf("bob create = %v, want a reach refusal", err)
	}
	if protocolError := refusal.ProtocolError(); protocolError.Status != 403 || protocolError.Code != protocol.CodeForbidden {
		t.Fatalf("protocol error = %+v", protocolError)
	}

	if _, err := service.Create(ctx, "t", alice, Grant{
		Principals: []Principal{UserPrincipal("bob")},
		Can:        Can{"dev/": Only(PermissionRead)},
	}, false); err != nil {
		t.Fatalf("alice create: %v", err)
	}
	grants, _ := store.List(ctx, "t")
	if !(Policy{Tenant: "t", Grants: grants}).Can(UserPrincipal("bob"), "dev/ci", PermissionRead) {
		t.Fatal("bob should read dev/")
	}
}

func TestGrantServiceOwnerBypassesTheChecks(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	service := NewGrantService(store)
	owner := Caller{Principal: UserPrincipal("owner"), Owner: true}
	if _, err := service.Create(ctx, "t", owner, Grant{
		Principals: []Principal{UserPrincipal("alice")},
		Can:        Can{"*": All()},
	}, false); err != nil {
		t.Fatalf("owner create: %v", err)
	}
	grants, _ := store.List(ctx, "t")
	if !(Policy{Tenant: "t", Grants: grants}).Can(UserPrincipal("alice"), "dev", PermissionManage) {
		t.Fatal("alice should have access")
	}
}

func TestGrantServiceUpdateMissing(t *testing.T) {
	store := NewMemoryStore()
	service := NewGrantService(store)
	ctx := context.Background()
	caller := Caller{Principal: UserPrincipal("alice"), Owner: true}
	if _, err := service.Update(ctx, "t", "none", caller, GrantUpdate{}, false); !errors.Is(err, ErrGrantNotFound) {
		t.Fatalf("update missing = %v", err)
	}
	if err := service.Revoke(ctx, "t", "none", caller, false); !errors.Is(err, ErrGrantNotFound) {
		t.Fatalf("revoke missing = %v", err)
	}
}

func TestGrantServiceReachChecksOwnLimitsUnderThePattern(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	service := NewGrantService(store)
	mustCreateGrant(t, store, "t", Grant{
		Principals: []Principal{UserPrincipal("alice")},
		Can:        Can{"*": All()},
	})
	mustCreateGrant(t, store, "t", Grant{
		Principals: []Principal{UserPrincipal("alice")},
		Can:        Can{"security/": Only(PermissionRead)},
	})
	caller := Caller{Principal: UserPrincipal("alice")}

	// Alice may manage at *, but her own security/ grant narrows her there.
	// She can't grant a pattern that reaches past where she may manage, even
	// with lockMeOut.
	_, err := service.Create(ctx, "t", caller, Grant{
		Principals: []Principal{UserPrincipal("bob")},
		Can:        Can{"*": All()},
	}, true)
	var refusal *Refusal
	if !errors.As(err, &refusal) || refusal.Reason != RefusalReach || refusal.Pattern != "security/" {
		t.Fatalf("create * = %v, want a reach refusal at security/", err)
	}

	// A pattern she manages throughout still works.
	if _, err := service.Create(ctx, "t", caller, Grant{
		Principals: []Principal{UserPrincipal("bob")},
		Can:        Can{"dev/": All()},
	}, false); err != nil {
		t.Fatalf("create dev/: %v", err)
	}
}

func errorCode(err error) protocol.Code {
	if boardErr := AsProtocolError(err); boardErr != nil {
		return boardErr.Code
	}
	return ""
}
