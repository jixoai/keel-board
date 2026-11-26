package access

import (
	"context"
	"errors"
	"testing"

	"github.com/jixoai/keel-board/protocol"
)

func TestAuthorizeAnywhere(t *testing.T) {
	ctx := context.Background()
	alice := aliceActor()

	tests := []struct {
		name       string
		grants     []Grant
		scope      *Can
		tenant     string
		permission protocol.Permission
		allowed    bool
	}{
		{
			name:       "no grant refuses",
			permission: protocol.PermissionPost,
		},
		{
			name:       "read only refuses post and sync",
			grants:     []Grant{{Principals: []Principal{UserPrincipal(alice.Principal)}, Can: Can{"work": Only(PermissionRead)}}},
			permission: protocol.PermissionPost,
		},
		{
			name:       "post somewhere allows post",
			grants:     []Grant{{Principals: []Principal{UserPrincipal(alice.Principal)}, Can: Can{"work": Only(PermissionPost)}}},
			permission: protocol.PermissionPost,
			allowed:    true,
		},
		{
			name:       "post somewhere refuses sync",
			grants:     []Grant{{Principals: []Principal{UserPrincipal(alice.Principal)}, Can: Can{"work": Only(PermissionPost)}}},
			permission: protocol.PermissionSync,
		},
		{
			name:       "sync on a subtree allows sync",
			grants:     []Grant{{Principals: []Principal{UserPrincipal(alice.Principal)}, Can: Can{"work/": Only(PermissionSync)}}},
			permission: protocol.PermissionSync,
			allowed:    true,
		},
		{
			name:       "manage implies post and sync",
			grants:     []Grant{{Principals: []Principal{UserPrincipal(alice.Principal)}, Can: Can{"*": Only(PermissionManage)}}},
			permission: protocol.PermissionPost,
			allowed:    true,
		},
		{
			name: "a closed exact channel doesn't close every channel",
			grants: []Grant{{Principals: []Principal{UserPrincipal(alice.Principal)}, Can: Can{
				"*":      Only(PermissionPost),
				"closed": None(),
			}}},
			permission: protocol.PermissionPost,
			allowed:    true,
		},
		{
			name:       "a scope can close the only grant",
			grants:     []Grant{{Principals: []Principal{UserPrincipal(alice.Principal)}, Can: Can{"*": Only(PermissionPost)}}},
			scope:      &Can{"*": None()},
			permission: protocol.PermissionPost,
		},
		{
			name:       "a scope can keep one channel",
			grants:     []Grant{{Principals: []Principal{UserPrincipal(alice.Principal)}, Can: Can{"*": Only(PermissionPost)}}},
			scope:      &Can{"work": Only(PermissionPost)},
			permission: protocol.PermissionPost,
			allowed:    true,
		},
		{
			name:       "another tenant's grant doesn't count",
			grants:     []Grant{{Principals: []Principal{UserPrincipal(alice.Principal)}, Can: Can{"*": Only(PermissionPost)}}},
			tenant:     "tenant-b",
			permission: protocol.PermissionPost,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := NewMemoryStore()
			for _, grant := range test.grants {
				mustCreateGrant(t, store, "tenant-a", grant)
			}
			authorizer := &Authorizer{Store: store}
			if test.scope != nil {
				scope := test.scope
				authorizer.Scope = func(context.Context, string, protocol.Actor) *Can {
					return scope
				}
			}
			tenant := test.tenant
			if tenant == "" {
				tenant = "tenant-a"
			}
			err := authorizer.AuthorizeAnywhere(ctx, tenant, alice, test.permission)
			if test.allowed && err != nil {
				t.Fatalf("expected allowed: %v", err)
			}
			if !test.allowed && err == nil {
				t.Fatal("expected a refusal")
			}
			if !test.allowed {
				expectAccessError(t, err, protocol.CodeForbidden, 403)
			}
		})
	}
}

func TestAuthorizeAnywhereFailsClosed(t *testing.T) {
	ctx := context.Background()
	authorizer := &Authorizer{Store: failingStore{err: errors.New("grants down")}}
	if err := authorizer.AuthorizeAnywhere(ctx, "default", aliceActor(), protocol.PermissionPost); err == nil {
		t.Fatal("a store failure must refuse")
	}
	if _, ok := protocol.AsError(authorizer.AuthorizeAnywhere(ctx, "default", aliceActor(), protocol.PermissionPost)); !ok {
		t.Fatal("a store failure should be a board error")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	authorizer = &Authorizer{Store: failingStore{err: context.Canceled}}
	if err := authorizer.AuthorizeAnywhere(canceled, "default", aliceActor(), protocol.PermissionPost); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected the cancellation, got %v", err)
	}
}

func TestAnywhereChannelsCoverSubtreeFinerRules(t *testing.T) {
	facts := Facts{
		Grants: []Grant{
			{Can: Can{"work/": Only(PermissionPost)}},
			{Can: Can{"work/closed/": None()}},
			{Can: Can{"work/closed/only": None()}},
		},
	}
	channels := anywhereChannels(facts)
	free := false
	for _, channel := range channels {
		if channel == "work/anywhere" {
			free = true
		}
	}
	if !free {
		t.Fatalf("expected a representative under work/ that no finer rule names: %+v", channels)
	}
}
