package access

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jixoai/keel-board/protocol"
)

func TestGrantServiceForcesBoardTeamAndMigratesIt(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	service := NewGrantService(store)
	service.BoardTeam = "team-a"
	owner := Caller{Principal: UserPrincipal("owner"), Owner: true}

	created, err := service.Create(ctx, "t", owner, Grant{
		Principals: []Principal{{
			Kind:         PrincipalProject,
			ProjectID:    "prj_1",
			Environments: []string{"production"},
			TeamID:       "client-team",
		}},
		Can: Can{"*": Only(PermissionRead)},
	}, false)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if got := created.Principals[0].TeamID; got != "team-a" {
		t.Fatalf("client TeamID survived: %q", got)
	}

	// Adding a project through an update forces the board's team too.
	updated, err := service.Update(ctx, "t", created.ID, owner, GrantUpdate{
		Add: []Principal{{
			Kind:      PrincipalProject,
			ProjectID: "prj_2",
			TeamID:    "another-client-team",
		}},
	}, false)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	for _, principal := range updated.Principals {
		if principal.TeamID != "team-a" {
			t.Fatalf("update kept a client team: %+v", principal)
		}
	}

	// A move to another team migrates the project principals the service
	// writes, so the old team's tokens no longer match.
	service.BoardTeam = "team-b"
	can := Can{"dev/": Only(PermissionRead)}
	if _, err := service.Update(ctx, "t", created.ID, owner, GrantUpdate{Can: &can}, false); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	grants, err := store.List(ctx, "t")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	policy := Policy{Tenant: "t", Grants: grants}
	oldTeam := Principal{Kind: PrincipalProject, ProjectID: "prj_1", Environment: "production", TeamID: "team-a"}
	newTeam := Principal{Kind: PrincipalProject, ProjectID: "prj_1", Environment: "production", TeamID: "team-b"}
	noTeam := Principal{Kind: PrincipalProject, ProjectID: "prj_1", Environment: "production"}
	if policy.Can(oldTeam, "dev", PermissionRead) {
		t.Fatal("the old team should no longer match after migration")
	}
	if !policy.Can(newTeam, "dev", PermissionRead) {
		t.Fatal("the new team should match after migration")
	}
	if policy.Can(noTeam, "dev", PermissionRead) {
		t.Fatal("a caller with no team must not match a team-scoped grant")
	}

	// TeamFunc resolves the team per tenant and wins over BoardTeam.
	service.TeamFunc = func(_ context.Context, tenant string) (string, error) {
		return "team-" + tenant, nil
	}
	can = Can{"preview/": Only(PermissionRead)}
	if _, err := service.Update(ctx, "t", created.ID, owner, GrantUpdate{Can: &can}, false); err != nil {
		t.Fatalf("team func update: %v", err)
	}
	grants, _ = store.List(ctx, "t")
	for _, grant := range grants {
		for _, principal := range grant.Principals {
			if principal.Kind == PrincipalProject && principal.TeamID != "team-t" {
				t.Fatalf("TeamFunc didn't win: %+v", principal)
			}
		}
	}

	service.TeamFunc = func(context.Context, string) (string, error) {
		return "", errors.New("team source down")
	}
	if _, err := service.Create(ctx, "t", owner, Grant{
		Principals: []Principal{{Kind: PrincipalProject, ProjectID: "prj_3"}},
		Can:        Can{"*": Only(PermissionRead)},
	}, false); err == nil {
		t.Fatal("a TeamFunc error should fail the write")
	}
}

func TestCappedCallerCannotGrantManage(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	service := NewGrantService(store)
	mustCreateGrant(t, store, "t", Grant{
		Principals: []Principal{UserPrincipal("alice")},
		Can:        Can{"*": All()},
	})
	caller := Caller{Principal: UserPrincipal("alice"), Cap: []Permission{PermissionRead}}
	_, err := service.Create(ctx, "t", caller, Grant{
		Principals: []Principal{UserPrincipal("bob")},
		Can:        Can{"dev/": All()},
	}, false)
	var refusal *Refusal
	if !errors.As(err, &refusal) || refusal.Reason != RefusalReach {
		t.Fatalf("capped caller create = %v, want a reach refusal", err)
	}
}

func TestOwnerWithScopeOrCapStillRespectsReachAndAdmission(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	service := NewGrantService(store)
	mustCreateGrant(t, store, "t", Grant{
		Principals: []Principal{UserPrincipal("owner")},
		Can:        Can{"*": All()},
	})
	grant := Grant{
		Principals: []Principal{UserPrincipal("bob")},
		Can:        Can{"dev/": All()},
	}

	scope := Can{"*": Only(PermissionRead)}
	scoped := Caller{Principal: UserPrincipal("owner"), Owner: true, Scope: &scope}
	_, err := service.Create(ctx, "t", scoped, grant, false)
	var refusal *Refusal
	if !errors.As(err, &refusal) || refusal.Reason != RefusalReach {
		t.Fatalf("scoped owner create = %v, want a reach refusal", err)
	}

	capped := Caller{Principal: UserPrincipal("owner"), Owner: true, Cap: []Permission{PermissionRead}}
	_, err = service.Create(ctx, "t", capped, grant, false)
	if !errors.As(err, &refusal) || refusal.Reason != RefusalReach {
		t.Fatalf("capped owner create = %v, want a reach refusal", err)
	}

	// An owner is still spared the lockout, even behind a token.
	all := Can{"*": All()}
	unrestricted := Caller{Principal: UserPrincipal("owner"), Owner: true, Scope: &all}
	if _, err := service.Create(ctx, "t", unrestricted, Grant{
		Principals: []Principal{EveryonePrincipal()},
		Can:        Can{"security/": None()},
	}, false); err != nil {
		t.Fatalf("an owner's lockout should still be waived: %v", err)
	}
}

func TestGrantServiceListAndExplainGates(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	service := NewGrantService(store)
	mustCreateGrant(t, store, "t", Grant{
		Principals: []Principal{UserPrincipal("alice")},
		Can:        Can{"*": All()},
	})
	mustCreateGrant(t, store, "t", Grant{
		Principals: []Principal{UserPrincipal("bob")},
		Can:        Can{"dev/": Only(PermissionRead)},
	})
	mustCreateGrant(t, store, "t", Grant{
		Principals: []Principal{UserPrincipal("carol")},
		Can:        Can{"security/": All()},
	})
	alice := Caller{Principal: UserPrincipal("alice")}
	bob := Caller{Principal: UserPrincipal("bob")}
	carol := Caller{Principal: UserPrincipal("carol")}

	if grants, err := service.List(ctx, "t", alice); err != nil || len(grants) != 3 {
		t.Fatalf("alice list = %+v, %v", grants, err)
	}
	if _, err := service.List(ctx, "t", bob); err == nil || errorCode(err) != protocol.CodeForbidden {
		t.Fatalf("bob list = %v, want forbidden", err)
	}
	// Manage at a subtree isn't manage at *, so carol can't list.
	if _, err := service.List(ctx, "t", carol); err == nil || errorCode(err) != protocol.CodeForbidden {
		t.Fatalf("carol list = %v, want forbidden", err)
	}
	if grants, err := service.List(ctx, "t", Caller{Principal: UserPrincipal("owner"), Owner: true}); err != nil || len(grants) != 3 {
		t.Fatalf("owner list = %+v, %v", grants, err)
	}
	readOnly := Can{"*": Only(PermissionRead)}
	if _, err := service.List(ctx, "t", Caller{Principal: UserPrincipal("owner"), Owner: true, Scope: &readOnly}); err == nil || errorCode(err) != protocol.CodeForbidden {
		t.Fatalf("scoped owner list = %v, want forbidden", err)
	}

	// A caller that can't list sees only its own permissions, no pattern or
	// grant, for a channel it can read.
	explanation, err := service.Explain(ctx, "t", bob, "dev/ci")
	if err != nil {
		t.Fatalf("bob explain: %v", err)
	}
	if explanation.Pattern != "" || explanation.Level != "" || len(explanation.Over) != 0 || len(explanation.Grants) != 0 {
		t.Fatalf("bob's explanation leaked a pattern or grant: %+v", explanation)
	}
	if !explanation.Admitted || !explanation.Allows(PermissionRead) {
		t.Fatalf("bob's own permissions should remain: %+v", explanation)
	}
	if _, err := service.Explain(ctx, "t", bob, "other"); err == nil || errorCode(err) != protocol.CodeForbidden {
		t.Fatalf("bob explain unreachable = %v, want a uniform forbidden", err)
	}
	full, err := service.Explain(ctx, "t", alice, "dev/ci")
	if err != nil {
		t.Fatalf("alice explain: %v", err)
	}
	if full.Pattern == "" || len(full.Grants) == 0 {
		t.Fatalf("alice should see the pattern and grants: %+v", full)
	}

	// Explaining someone else takes lists; otherwise it's 403 without a
	// pattern, whether or not the target exists.
	if _, err := service.ExplainFor(ctx, "t", bob, carol, "dev"); err == nil || errorCode(err) != protocol.CodeForbidden {
		t.Fatalf("bob explainFor = %v, want forbidden", err)
	}
	unknown := Caller{Principal: UserPrincipal("nobody")}
	if _, err := service.ExplainFor(ctx, "t", alice, unknown, "dev"); err == nil || errorCode(err) != protocol.CodeForbidden {
		t.Fatalf("unknown target = %v, want a uniform forbidden", err)
	}
	target, err := service.ExplainFor(ctx, "t", alice, bob, "dev/ci")
	if err != nil {
		t.Fatalf("alice explainFor bob: %v", err)
	}
	if target.Pattern != "dev/" || !target.Allows(PermissionRead) {
		t.Fatalf("target explanation = %+v", target)
	}
}

func TestGrantServiceAdmissionRefusesBeforeTheChange(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	service := NewGrantService(store)
	mustCreateGrant(t, store, "t", Grant{
		Principals: []Principal{UserPrincipal("alice")},
		Can:        Can{"*": All()},
	})
	mustCreateGrant(t, store, "t", Grant{
		Principals: []Principal{UserPrincipal("alice")},
		Can:        Can{"secret/": Only(PermissionRead)},
	})
	mustCreateGrant(t, store, "t", Grant{
		Principals: []Principal{EveryonePrincipal()},
		Can:        Can{"secret/": Only(PermissionPost)},
	})
	alice := Caller{Principal: UserPrincipal("alice")}

	_, err := service.Create(ctx, "t", alice, Grant{
		Principals: []Principal{UserPrincipal("bob")},
		Can:        Can{"dev/": All()},
	}, false)
	var refusal *Refusal
	if !errors.As(err, &refusal) || refusal.Reason != RefusalAdmission {
		t.Fatalf("create = %v, want an admission refusal", err)
	}
	if refusal.Pattern != "secret/" || refusal.Permission != PermissionPost {
		t.Fatalf("refusal = %+v", refusal)
	}
}

func TestGrantServiceRemovalErrorsAndGitHubIdentity(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	service := NewGrantService(store)
	owner := Caller{Principal: UserPrincipal("owner"), Owner: true}
	created, err := service.Create(ctx, "t", owner, Grant{
		Principals: []Principal{
			{Kind: PrincipalGitHub, RepositoryID: "123", Repository: "acme/web"},
			UserPrincipal("alice"),
		},
		Can: Can{"*": All()},
	}, false)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// A removal by name still matches when the grant kept both.
	if _, err := service.Update(ctx, "t", created.ID, owner, GrantUpdate{
		Remove: []Principal{{Kind: PrincipalGitHub, Repository: "acme/web"}},
	}, false); err != nil {
		t.Fatalf("remove by name: %v", err)
	}
	// A removal that names a repository ID must match the ID, not the name.
	if _, err := service.Update(ctx, "t", created.ID, owner, GrantUpdate{
		Remove: []Principal{{Kind: PrincipalGitHub, RepositoryID: "999", Repository: "acme/web"}},
	}, false); err == nil || errorCode(err) != CodeNotInGrant {
		t.Fatalf("remove wrong ID = %v, want not_in_grant", err)
	}
	if _, err := service.Update(ctx, "t", created.ID, owner, GrantUpdate{
		Remove: []Principal{UserPrincipal("nobody")},
	}, false); err == nil || errorCode(err) != CodeNotInGrant {
		t.Fatalf("remove nobody = %v, want not_in_grant", err)
	}
	if _, err := service.Update(ctx, "t", created.ID, owner, GrantUpdate{
		Remove: []Principal{UserPrincipal("alice")},
	}, false); err == nil || errorCode(err) != CodeGrantEmpty {
		t.Fatalf("remove last = %v, want grant_empty", err)
	}
}

func TestParseCanBoundsAndLargeScopeNarrows(t *testing.T) {
	if _, err := ParseCan(strings.Repeat("x", MaxCanText+1)); err == nil {
		t.Fatal("a can longer than MaxCanText should fail")
	}
	var long strings.Builder
	for index := 0; index < MaxCanPatterns+1; index++ {
		if index > 0 {
			long.WriteByte(';')
		}
		fmt.Fprintf(&long, "ch%d/=read", index)
	}
	if _, err := ParseCan(long.String()); err == nil {
		t.Fatal("a header naming more than MaxCanPatterns should fail")
	}

	// A programmatic scope over the limit still narrows channel by channel;
	// it is not silently denied.
	scope := make(Can, MaxCanPatterns+2)
	for index := 0; index < MaxCanPatterns+1; index++ {
		scope[fmt.Sprintf("ch%d/", index)] = None()
	}
	scope["dev/"] = Only(PermissionRead)
	alice := UserPrincipal("alice")
	policy := Policy{Tenant: "t", Scope: &scope, Grants: []Grant{{
		Tenant:     "t",
		Principals: []Principal{alice},
		Can:        Can{"*": All()},
	}}}
	explanation := policy.Explain(alice, "dev/ci")
	if !explanation.Allows(PermissionRead) || explanation.Allows(PermissionPost) {
		t.Fatalf("large scope = %+v", explanation)
	}
}

func TestNormalizePrincipalUserIdentity(t *testing.T) {
	principal, err := NormalizePrincipal(Principal{Kind: PrincipalUser, UserID: "Alice@Example.com"})
	if err != nil || principal.UserID != "alice@example.com" {
		t.Fatalf("email-shaped userId = %+v, %v", principal, err)
	}
	if _, err := NormalizePrincipal(Principal{Kind: PrincipalUser, UserID: "alice", Email: "alice@example.com"}); err == nil {
		t.Fatal("a user with both userId and email should fail")
	}
	principal, err = NormalizePrincipal(Principal{Kind: PrincipalUser, Email: "Alice@Example.com"})
	if err != nil || principal.UserID != "alice@example.com" {
		t.Fatalf("email-only user = %+v, %v", principal, err)
	}
}

func TestGrantCappedIsRecomputedFromCallerCap(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	service := NewGrantService(store)
	mustCreateGrant(t, store, "t", Grant{
		Principals: []Principal{UserPrincipal("alice")},
		Can:        Can{"*": All()},
	})
	alice := Caller{Principal: UserPrincipal("alice")}

	// A client-written Capped is not trusted without a caller cap.
	uncapped, err := service.Create(ctx, "t", alice, Grant{
		Principals: []Principal{UserPrincipal("bob")},
		Can:        Can{"dev/": Only(PermissionRead)},
		Capped:     true,
	}, false)
	if err != nil {
		t.Fatalf("create uncapped: %v", err)
	}
	if uncapped.Capped {
		t.Fatal("a client-written capped flag survived")
	}

	// A caller whose role caps people writes a grant marked capped.
	cappedCaller := Caller{Principal: UserPrincipal("alice"), Cap: []Permission{PermissionManage}}
	capped, err := service.Create(ctx, "t", cappedCaller, Grant{
		Principals: []Principal{UserPrincipal("carol")},
		Can:        Can{"dev/": Only(PermissionRead)},
		Capped:     false,
	}, false)
	if err != nil {
		t.Fatalf("create capped: %v", err)
	}
	if !capped.Capped {
		t.Fatal("a user grant written by a capped caller should be marked capped")
	}

	// A project grant isn't capped by a person's role.
	projected, err := service.Create(ctx, "t", cappedCaller, Grant{
		Principals: []Principal{{Kind: PrincipalProject, ProjectID: "prj_1"}},
		Can:        Can{"dev/": Only(PermissionRead)},
	}, false)
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	if projected.Capped {
		t.Fatal("a project grant isn't capped by a person's role")
	}

	// An update recomputes it from the updating caller too.
	can := Can{"dev/": Only(PermissionRead)}
	recapped, err := service.Update(ctx, "t", uncapped.ID, cappedCaller, GrantUpdate{Can: &can}, false)
	if err != nil {
		t.Fatalf("update capped: %v", err)
	}
	if !recapped.Capped {
		t.Fatal("an update by a capped caller should mark the grant capped")
	}
	uncappedAgain, err := service.Update(ctx, "t", uncapped.ID, alice, GrantUpdate{Can: &can}, false)
	if err != nil {
		t.Fatalf("update uncapped: %v", err)
	}
	if uncappedAgain.Capped {
		t.Fatal("an update by an uncapped caller should clear the flag")
	}
}

type blockingStore struct {
	GrantStore
	active    int32
	maxActive int32
	sleep     time.Duration
}

func (s *blockingStore) List(ctx context.Context, tenant string) ([]Grant, error) {
	active := atomic.AddInt32(&s.active, 1)
	for {
		max := atomic.LoadInt32(&s.maxActive)
		if active <= max || atomic.CompareAndSwapInt32(&s.maxActive, max, active) {
			break
		}
	}
	time.Sleep(s.sleep)
	defer atomic.AddInt32(&s.active, -1)
	return s.GrantStore.List(ctx, tenant)
}

func TestGrantServiceSerializesConcurrentWrites(t *testing.T) {
	ctx := context.Background()
	store := &blockingStore{GrantStore: NewMemoryStore(), sleep: 2 * time.Millisecond}
	service := NewGrantService(store)
	owner := Caller{Principal: UserPrincipal("owner"), Owner: true}

	var wait sync.WaitGroup
	errs := make(chan error, 8)
	for index := 0; index < 8; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			_, err := service.Create(ctx, "t", owner, Grant{
				Principals: []Principal{UserPrincipal(fmt.Sprintf("user-%d", index))},
				Can:        Can{"dev/": Only(PermissionRead)},
			}, false)
			errs <- err
		}(index)
	}
	wait.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent create: %v", err)
		}
	}
	if got := atomic.LoadInt32(&store.maxActive); got != 1 {
		t.Fatalf("GrantService didn't serialize reads, max active = %d", got)
	}
	grants, err := store.List(ctx, "t")
	if err != nil || len(grants) != 8 {
		t.Fatalf("grants = %d, %v", len(grants), err)
	}
	seen := make(map[string]struct{}, len(grants))
	for _, grant := range grants {
		if _, ok := seen[grant.ID]; ok {
			t.Fatalf("duplicate grant ID %q", grant.ID)
		}
		seen[grant.ID] = struct{}{}
	}
	if len(seen) != 8 {
		t.Fatalf("unique IDs = %d", len(seen))
	}
}
