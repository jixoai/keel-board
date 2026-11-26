package access

import (
	"errors"
	"reflect"
	"testing"
)

func explanationPermissions(t *testing.T, explanation Explanation, want ...Permission) {
	t.Helper()
	if !explanation.Admitted {
		t.Fatalf("caller wasn't admitted: %+v", explanation)
	}
	if len(explanation.Permissions) != len(want) {
		t.Fatalf("permissions = %v, want %v (explanation %+v)", explanation.Permissions, want, explanation)
	}
	for index := range want {
		if explanation.Permissions[index] != want[index] {
			t.Fatalf("permissions = %v, want %v (explanation %+v)", explanation.Permissions, want, explanation)
		}
	}
}

func TestPatternPriorityExactThenSubtreeThenAll(t *testing.T) {
	alice := UserPrincipal("alice")
	policy := Policy{
		Tenant: "t",
		Grants: []Grant{
			{ID: "g1", Tenant: "t", Principals: []Principal{EveryonePrincipal()}, Can: Can{"*": All()}},
			{ID: "g2", Tenant: "t", Principals: []Principal{alice}, Can: Can{"dev/": Only(PermissionRead)}},
			{ID: "g3", Tenant: "t", Principals: []Principal{alice}, Can: Can{"dev/ci": Only(PermissionPost)}},
			{ID: "g4", Tenant: "t", Principals: []Principal{alice}, Can: Can{"dev/ci/x": Only(PermissionLabel)}},
			{ID: "g5", Tenant: "t", Principals: []Principal{alice}, Can: Can{"dev/ci/": Only(PermissionSync)}},
		},
	}

	// An exact channel beats the longest subtree and everything above it.
	exact := policy.Explain(alice, "dev/ci/x")
	if exact.Pattern != "dev/ci/x" || exact.Level != LevelPrincipal {
		t.Fatalf("dev/ci/x = %+v", exact)
	}
	explanationPermissions(t, exact, PermissionRead, PermissionLabel)
	if !reflect.DeepEqual(exact.Over, []string{"dev/ci/", "dev/", "*"}) {
		t.Fatalf("over = %v", exact.Over)
	}
	if policy.Can(alice, "dev/ci/x", PermissionPost) {
		t.Fatal("the exact channel should win over the subtree's sync and the all grant")
	}

	// An exact channel that is the root of a subtree still wins there.
	root := policy.Explain(alice, "dev/ci")
	if root.Pattern != "dev/ci" {
		t.Fatalf("dev/ci pattern = %q", root.Pattern)
	}
	explanationPermissions(t, root, PermissionPost)
	if !reflect.DeepEqual(root.Over, []string{"dev/ci/", "dev/", "*"}) {
		t.Fatalf("dev/ci over = %v", root.Over)
	}

	// A subtree covers its root and its children.
	for _, channel := range []string{"dev", "dev/other"} {
		explanation := policy.Explain(alice, channel)
		if explanation.Pattern != "dev/" {
			t.Fatalf("%s pattern = %q, want dev/", channel, explanation.Pattern)
		}
		explanationPermissions(t, explanation, PermissionRead)
	}
	subtree := policy.Explain(alice, "dev/ci/y")
	if subtree.Pattern != "dev/ci/" {
		t.Fatalf("dev/ci/y pattern = %q", subtree.Pattern)
	}
	explanationPermissions(t, subtree, PermissionRead, PermissionSync)

	// `*` covers channels no more specific pattern names, including future ones.
	everywhere := policy.Explain(alice, "new-channel")
	if everywhere.Pattern != "*" || everywhere.Level != LevelEveryone {
		t.Fatalf("new-channel = %+v", everywhere)
	}
	explanationPermissions(t, everywhere, Permissions...)
	if len(everywhere.Over) != 0 {
		t.Fatalf("new-channel over = %v", everywhere.Over)
	}
}

func TestLongestSubtreeAndUnion(t *testing.T) {
	alice := UserPrincipal("alice")
	policy := Policy{
		Tenant: "t",
		Grants: []Grant{
			{Tenant: "t", Principals: []Principal{alice}, Can: Can{"a/": Only(PermissionRead)}},
			{Tenant: "t", Principals: []Principal{alice}, Can: Can{"a/b/": Only(PermissionPost)}},
			{Tenant: "t", Principals: []Principal{alice}, Can: Can{"a/b/": Only(PermissionReact)}},
		},
	}
	deep := policy.Explain(alice, "a/b/c")
	if deep.Pattern != "a/b/" {
		t.Fatalf("pattern = %q, want a/b/", deep.Pattern)
	}
	// The two grants at a/b/ add up; react implies read, post implies nothing.
	explanationPermissions(t, deep, PermissionRead, PermissionPost, PermissionReact)
	if !reflect.DeepEqual(deep.Over, []string{"a/"}) {
		t.Fatalf("over = %v", deep.Over)
	}
	if policy.Can(alice, "a/b/c", PermissionLabel) {
		t.Fatal("label wasn't granted")
	}
	if !policy.Can(alice, "a/b/c", PermissionRead) {
		t.Fatal("react should imply read")
	}

	for _, channel := range []string{"a", "a/c"} {
		if got := policy.Explain(alice, channel).Pattern; got != "a/" {
			t.Fatalf("%s pattern = %q", channel, got)
		}
	}
	if got := policy.Explain(alice, "a/b").Pattern; got != "a/b/" {
		t.Fatalf("a/b pattern = %q", got)
	}
}

func TestClosestPrincipalWinsAtSamePattern(t *testing.T) {
	alice := UserPrincipal("alice")
	policy := Policy{
		Tenant: "t",
		Grants: []Grant{
			{Tenant: "t", Principals: []Principal{EveryonePrincipal()}, Can: Can{"*": All()}},
			{ID: "mine", Tenant: "t", Principals: []Principal{alice}, Can: Can{"*": Only(PermissionRead)}},
		},
	}
	explanation := policy.Explain(alice, "dev")
	if explanation.Pattern != "*" || explanation.Level != LevelPrincipal {
		t.Fatalf("explanation = %+v", explanation)
	}
	explanationPermissions(t, explanation, PermissionRead)
	if len(explanation.Grants) != 1 || explanation.Grants[0].ID != "mine" {
		t.Fatalf("the closest grant should be the one that decides: %+v", explanation.Grants)
	}

	// A member is admitted by the team and gets only what everyone gives.
	member := Policy{
		Tenant: "t",
		Member: true,
		Grants: []Grant{
			{Tenant: "t", Principals: []Principal{EveryonePrincipal()}, Can: Can{"*": Only(PermissionRead)}},
		},
	}
	memberExplanation := member.Explain(alice, "dev")
	if !memberExplanation.Admitted || memberExplanation.Level != LevelEveryone {
		t.Fatalf("member explanation = %+v", memberExplanation)
	}
	explanationPermissions(t, memberExplanation, PermissionRead)

	// Without membership or a grant, everyone names no one.
	outsider := member
	outsider.Member = false
	if explanation := outsider.Explain(alice, "dev"); explanation.Admitted || len(explanation.Permissions) != 0 {
		t.Fatalf("everyone let an outsider in: %+v", explanation)
	}
}

func TestPermissionImplications(t *testing.T) {
	alice := UserPrincipal("alice")
	cases := []struct {
		name string
		give CanAt
		want []Permission
		deny Permission
	}{
		{name: "write only", give: Only(PermissionPost), want: []Permission{PermissionPost}, deny: PermissionRead},
		{name: "react implies read", give: Only(PermissionReact), want: []Permission{PermissionRead, PermissionReact}},
		{name: "label implies read", give: Only(PermissionLabel), want: []Permission{PermissionRead, PermissionLabel}},
		{name: "manage implies everything", give: Only(PermissionManage), want: Permissions},
		{name: "none", give: None(), want: []Permission{}, deny: PermissionRead},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			policy := Policy{Tenant: "t", Grants: []Grant{
				{Tenant: "t", Principals: []Principal{alice}, Can: Can{"*": test.give}},
			}}
			explanationPermissions(t, policy.Explain(alice, "dev"), test.want...)
			for _, permission := range test.want {
				if !policy.Can(alice, "dev", permission) {
					t.Fatalf("want %s", permission)
				}
			}
			if test.deny != "" && policy.Can(alice, "dev", test.deny) {
				t.Fatalf("%s should be denied", test.deny)
			}
		})
	}
}

func TestCapAndScopeNarrow(t *testing.T) {
	alice := UserPrincipal("alice")
	all := Policy{Tenant: "t", Grants: []Grant{
		{Tenant: "t", Principals: []Principal{alice}, Can: Can{"*": All()}},
	}}

	capped := all
	capped.Cap = []Permission{PermissionRead, PermissionReact}
	explanation := capped.Explain(alice, "dev")
	explanationPermissions(t, explanation, PermissionRead, PermissionReact)
	if !explanation.Capped {
		t.Fatal("the cap should report that it took something away")
	}
	if capped.Can(alice, "dev", PermissionPost) {
		t.Fatal("the cap should remove post")
	}

	noAccess := all
	noAccess.Cap = []Permission{}
	explanation = noAccess.Explain(alice, "dev")
	explanationPermissions(t, explanation)
	if !explanation.Capped {
		t.Fatal("an empty cap should report that it took everything away")
	}

	scope := Can{"dev/": Only(PermissionRead)}
	scoped := all
	scoped.Scope = &scope
	explanation = scoped.Explain(alice, "dev/ci")
	explanationPermissions(t, explanation, PermissionRead)
	if explanation.Capped {
		t.Fatal("a token scope is not a cap")
	}
	if scoped.Can(alice, "dev/ci", PermissionPost) {
		t.Fatal("the scope should remove post")
	}
	explanation = scoped.Explain(alice, "other")
	explanationPermissions(t, explanation)
	if scoped.Can(alice, "other", PermissionRead) {
		t.Fatal("a scope that names only dev/ gives nothing elsewhere")
	}

	postOnly := Can{"*": Only(PermissionPost)}
	scoped.Scope = &postOnly
	explanation = scoped.Explain(alice, "other")
	explanationPermissions(t, explanation, PermissionPost)
}

func TestExplanationWithoutWinningPattern(t *testing.T) {
	alice := UserPrincipal("alice")
	policy := Policy{Tenant: "t", Grants: []Grant{
		{Tenant: "t", Principals: []Principal{alice}, Can: Can{"security/": Only(PermissionRead)}},
	}}
	explanation := policy.Explain(alice, "dev")
	if !explanation.Admitted {
		t.Fatal("alice is named by a grant, so she is admitted")
	}
	if explanation.Pattern != "" || explanation.Level != "" || len(explanation.Permissions) != 0 {
		t.Fatalf("dev should have no deciding pattern: %+v", explanation)
	}
	if policy.Can(alice, "dev", PermissionRead) {
		t.Fatal("no pattern covers dev")
	}
	security := policy.Explain(alice, "security/incidents")
	if security.Pattern != "security/" {
		t.Fatalf("security pattern = %q", security.Pattern)
	}
	explanationPermissions(t, security, PermissionRead)
	if len(security.Over) != 0 {
		t.Fatalf("security over = %v", security.Over)
	}
}

func TestUnknownPrincipalKinds(t *testing.T) {
	unknownCaller := Principal{Kind: PrincipalKind("robot"), UserID: "alice"}
	policy := Policy{Tenant: "t", Grants: []Grant{
		{Tenant: "t", Principals: []Principal{EveryonePrincipal()}, Can: Can{"*": All()}},
	}}
	if explanation := policy.Explain(unknownCaller, "dev"); explanation.Admitted || len(explanation.Permissions) != 0 {
		t.Fatalf("an unknown caller got in: %+v", explanation)
	}

	alice := UserPrincipal("alice")
	withUnknownGrant := Policy{Tenant: "t", Grants: []Grant{
		{Tenant: "t", Principals: []Principal{{Kind: PrincipalKind("robot")}}, Can: Can{"*": All()}},
		{Tenant: "t", Principals: []Principal{EveryonePrincipal()}, Can: Can{"*": All()}},
	}}
	if explanation := withUnknownGrant.Explain(alice, "dev"); explanation.Admitted || len(explanation.Permissions) != 0 {
		t.Fatalf("an unknown grant principal let alice in: %+v", explanation)
	}

	if _, err := NormalizePrincipal(Principal{Kind: PrincipalKind("robot")}); err == nil {
		t.Fatal("normalizing an unknown kind should fail")
	}
	var invalid *InvalidGrant
	if _, err := NormalizePrincipal(Principal{Kind: PrincipalKind("robot")}); !errors.As(err, &invalid) {
		t.Fatalf("want InvalidGrant, got %v", err)
	}
}

func TestCrossTenantGrantsAreIgnored(t *testing.T) {
	alice := UserPrincipal("alice")
	otherTenant := Policy{
		Tenant: "a",
		Grants: []Grant{
			{Tenant: "b", Principals: []Principal{alice}, Can: Can{"*": All()}},
		},
	}
	if explanation := otherTenant.Explain(alice, "dev"); explanation.Admitted || len(explanation.Permissions) != 0 {
		t.Fatalf("a grant from another tenant decided: %+v", explanation)
	}

	// An unlabeled grant is what a directly built policy uses; a store always
	// labels what it keeps.
	unlabeled := otherTenant
	unlabeled.Grants = []Grant{{Principals: []Principal{alice}, Can: Can{"*": Only(PermissionRead)}}}
	explanationPermissions(t, unlabeled.Explain(alice, "dev"), PermissionRead)

	sameTenant := otherTenant
	sameTenant.Tenant = "b"
	explanationPermissions(t, sameTenant.Explain(alice, "dev"), Permissions...)
}

func TestPolicyFailsClosedOnMalformedCan(t *testing.T) {
	alice := UserPrincipal("alice")
	policy := Policy{Tenant: "t", Grants: []Grant{
		// A grant the policy sees unnormalized: its bad permission names nothing.
		{Tenant: "t", Principals: []Principal{alice}, Can: Can{"*": {Permissions: []Permission{Permission("nope")}}}},
	}}
	explanation := policy.Explain(alice, "dev")
	if !explanation.Admitted {
		t.Fatalf("the grant names alice, so she is admitted: %+v", explanation)
	}
	if explanation.Pattern != "" || len(explanation.Permissions) != 0 {
		t.Fatalf("a malformed can names no pattern: %+v", explanation)
	}
}

func TestPrincipalKindsMatch(t *testing.T) {
	t.Run("project", func(t *testing.T) {
		policy := Policy{Tenant: "t", Grants: []Grant{{
			Tenant: "t",
			Principals: []Principal{{
				Kind:         PrincipalProject,
				ProjectID:    "prj_1",
				Environments: []string{"production", "preview"},
				TeamID:       "team-a",
			}},
			Can: Can{"*": All()},
		}}}
		match := Principal{Kind: PrincipalProject, ProjectID: "prj_1", Environment: "production", TeamID: "team-a"}
		if !policy.Can(match, "dev", PermissionRead) {
			t.Fatal("the production deployment should match")
		}
		preview := match
		preview.Environment = "preview"
		if !policy.Can(preview, "dev", PermissionRead) {
			t.Fatal("the preview deployment should match")
		}
		development := match
		development.Environment = "development"
		if policy.Can(development, "dev", PermissionRead) {
			t.Fatal("an environment the grant doesn't name shouldn't match")
		}
		otherTeam := match
		otherTeam.TeamID = "team-b"
		if policy.Can(otherTeam, "dev", PermissionRead) {
			t.Fatal("a project on another team shouldn't match")
		}
	})

	t.Run("all-projects", func(t *testing.T) {
		policy := Policy{Tenant: "t", TeamProject: true, Grants: []Grant{{
			Tenant:     "t",
			Principals: []Principal{{Kind: PrincipalAllProjects, Environments: []string{"production"}}},
			Can:        Can{"*": Only(PermissionRead)},
		}}}
		caller := Principal{Kind: PrincipalProject, ProjectID: "prj_1", Environment: "production"}
		explanation := policy.Explain(caller, "dev")
		if explanation.Level != LevelGroup || !explanation.Allows(PermissionRead) {
			t.Fatalf("all-projects = %+v", explanation)
		}
		preview := caller
		preview.Environment = "preview"
		if policy.Can(preview, "dev", PermissionRead) {
			t.Fatal("all-projects should only name its environments")
		}
		policy.TeamProject = false
		if policy.Can(caller, "dev", PermissionRead) {
			t.Fatal("all-projects needs a team project")
		}
	})

	t.Run("github", func(t *testing.T) {
		policy := Policy{Tenant: "t", Grants: []Grant{{
			Tenant: "t",
			Principals: []Principal{{
				Kind:         PrincipalGitHub,
				Repository:   "acme/web",
				RepositoryID: "123",
				Workflow:     "ci.yml",
				Branch:       "main",
				Event:        "push",
			}},
			Can: Can{"ci/": Only(PermissionPost)},
		}}}
		caller := Principal{
			Kind:         PrincipalGitHub,
			Repository:   "acme/web",
			RepositoryID: "123",
			WorkflowRef:  "acme/web/.github/workflows/ci.yml@refs/heads/main",
			Ref:          "refs/heads/main",
			Event:        "push",
		}
		if !policy.Can(caller, "ci", PermissionPost) {
			t.Fatal("the matching run should post")
		}
		wrongEvent := caller
		wrongEvent.Event = "pull_request"
		if policy.Can(wrongEvent, "ci", PermissionPost) {
			t.Fatal("the event should narrow the run")
		}
		wrongBranch := caller
		wrongBranch.Ref = "refs/heads/release"
		wrongBranch.WorkflowRef = "acme/web/.github/workflows/ci.yml@refs/heads/release"
		if policy.Can(wrongBranch, "ci", PermissionPost) {
			t.Fatal("the branch should narrow the run")
		}
		wrongWorkflow := caller
		wrongWorkflow.WorkflowRef = "acme/web/.github/workflows/ci.yml@refs/heads/main2"
		if policy.Can(wrongWorkflow, "ci", PermissionPost) {
			t.Fatal("a workflow_ref for another ref shouldn't match")
		}
		wrongRepo := caller
		wrongRepo.RepositoryID = "999"
		if policy.Can(wrongRepo, "ci", PermissionPost) {
			t.Fatal("the repository ID should narrow the run")
		}
		byName := caller
		byName.RepositoryID = ""
		if !policy.Can(byName, "ci", PermissionPost) {
			t.Fatal("without an ID, the repository name should match")
		}
	})

	t.Run("oidc", func(t *testing.T) {
		policy := Policy{Tenant: "t", Grants: []Grant{{
			Tenant: "t",
			Principals: []Principal{{
				Kind:   PrincipalOIDC,
				Issuer: "https://gitlab.com",
				Claims: map[string][]string{
					"sub":        {"project_path:acme/api:*"},
					"project_id": {"42"},
				},
			}},
			Can: Can{"ci/": Only(PermissionPost)},
		}}}
		caller := OIDCPrincipal("https://gitlab.com", map[string][]string{
			"sub":        {"project_path:acme/api:job-1"},
			"project_id": {"42"},
		})
		if !policy.Can(caller, "ci", PermissionPost) {
			t.Fatal("the matching token should post")
		}
		evil := OIDCPrincipal("https://gitlab.com", map[string][]string{
			"sub":        {"project_path:acme/api-evil:job-1"},
			"project_id": {"42"},
		})
		if policy.Can(evil, "ci", PermissionPost) {
			t.Fatal("a sub prefix shouldn't let acme/api-evil in for acme/api")
		}
		wrongClaim := OIDCPrincipal("https://gitlab.com", map[string][]string{
			"sub":        {"project_path:acme/api:job-1"},
			"project_id": {"43"},
		})
		if policy.Can(wrongClaim, "ci", PermissionPost) {
			t.Fatal("every claim should match")
		}
		wrongIssuer := OIDCPrincipal("https://github.com", map[string][]string{
			"sub":        {"project_path:acme/api:job-1"},
			"project_id": {"42"},
		})
		if policy.Can(wrongIssuer, "ci", PermissionPost) {
			t.Fatal("the issuer should match")
		}
	})

	t.Run("app", func(t *testing.T) {
		policy := Policy{Tenant: "t", TeamApp: true, Grants: []Grant{{
			Tenant:     "t",
			Principals: []Principal{{Kind: PrincipalApp, ClientID: "ship"}},
			Can:        Can{"ship/": Only(PermissionPost)},
		}}}
		caller := AppPrincipal("ship")
		if !policy.Can(caller, "ship", PermissionPost) {
			t.Fatal("the app should post")
		}
		if policy.Can(AppPrincipal("other"), "ship", PermissionPost) {
			t.Fatal("another app shouldn't post")
		}
		if policy.Can(Principal{Kind: PrincipalApp, ClientID: "ship"}, "ship", PermissionRead) {
			t.Fatal("post-only is write-only")
		}
	})

	t.Run("all-users", func(t *testing.T) {
		policy := Policy{Tenant: "t", Member: true, Grants: []Grant{{
			Tenant:     "t",
			Principals: []Principal{{Kind: PrincipalAllUsers}},
			Can:        Can{"*": Only(PermissionRead)},
		}}}
		caller := UserPrincipal("alice")
		explanation := policy.Explain(caller, "dev")
		if explanation.Level != LevelGroup || !explanation.Allows(PermissionRead) {
			t.Fatalf("all-users = %+v", explanation)
		}
		policy.Member = false
		if policy.Can(caller, "dev", PermissionRead) {
			t.Fatal("all-users needs membership")
		}
	})
}
