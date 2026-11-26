package access

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jixoai/keel-board/protocol"
	"github.com/jixoai/keel-board/server"
)

// Authorizer implements server.Authorizer over a GrantStore: it resolves the
// caller from the actor the server verified, loads that tenant's grants, and
// asks the policy. The tenant is passed explicitly by the server, so a
// multi-tenant deployment cannot authorize a request against another board's
// grants.
//
// Refusals are the board's 404 for what the caller can't read, and its 403
// naming what's missing where it can. A bad x-board-can is a 400
// invalid_scope, never a silent zero-permission caller.
type Authorizer struct {
	// Store is the tenant's grants. Required.
	Store GrantStore
	// Tenant normalizes the tenant the server resolved. It may return another
	// name, but never an empty one; the server's tenant is used otherwise.
	Tenant func(ctx context.Context, tenant string, actor protocol.Actor) string
	// Principal resolves who a verified actor is in tenant. Default:
	// ActorPrincipal.
	Principal func(ctx context.Context, tenant string, actor protocol.Actor) (Principal, error)
	// Team is the verified team a caller belongs to. It fills or replaces the
	// project caller's TeamID.
	Team func(ctx context.Context, tenant string, principal Principal) string
	// Member reports whether a user is a person on the board's team, which
	// admits them and lets all-users name them.
	Member func(ctx context.Context, tenant string, principal Principal) bool
	// TeamProject reports whether a project caller belongs to the team, which
	// lets all-projects name it.
	TeamProject func(ctx context.Context, tenant string, principal Principal) bool
	// TeamApp reports whether an app caller is installed on the team, which
	// lets all-apps name it.
	TeamApp func(ctx context.Context, tenant string, principal Principal) bool
	// Cap is the most a caller may do anywhere: a person's team role, an app's
	// scopes. Nil caps nothing.
	Cap func(ctx context.Context, tenant string, principal Principal) []Permission
	// Scope is a token's narrowing, resolved by the host. Nil doesn't narrow.
	Scope func(ctx context.Context, tenant string, actor protocol.Actor) *Can
	// ScopeText is a token's narrowing as x-board-can writes it. When set it
	// wins over Scope; text that doesn't parse is a 400 invalid_scope.
	ScopeText func(ctx context.Context, tenant string, actor protocol.Actor) (string, bool)
	// RevealUnreadable answers 403 where the default answers 404 for a channel
	// the caller can't read. Keep it false to hide what the caller can't read.
	RevealUnreadable bool
}

var _ server.Authorizer = (*Authorizer)(nil)

// ActorPrincipal is the default mapping from a verified actor to the
// principal grants name: a person (or agent) is a user by its principal ID, a
// deployment is a project in its environment, and a workflow is a GitHub run
// by its principal. A host whose tokens carry more (a repository's owner and
// name, an issuer's claims, a project's team) replaces it.
func ActorPrincipal(actor protocol.Actor) (Principal, error) {
	principal := strings.TrimSpace(actor.Principal)
	if principal == "" {
		return Principal{}, errors.New("the actor names no principal")
	}
	switch actor.Kind {
	case protocol.ActorDeployment:
		environment := "production"
		if actor.Via != nil && actor.Via.Environment != nil && *actor.Via.Environment != "" {
			environment = *actor.Via.Environment
		}
		return Principal{Kind: PrincipalProject, ProjectID: principal, Environment: environment}, nil
	case protocol.ActorWorkflow:
		return Principal{Kind: PrincipalGitHub, Repository: principal}, nil
	default:
		return Principal{Kind: PrincipalUser, UserID: principal}, nil
	}
}

// Authorize implements server.Authorizer.
func (a *Authorizer) Authorize(ctx context.Context, tenant string, actor protocol.Actor, channel string, permission protocol.Permission) error {
	explanation, err := a.Explain(ctx, tenant, actor, channel)
	if err != nil {
		return err
	}
	if explanation.Allows(permission) {
		return nil
	}
	return a.refuse(channel, permission, explanation)
}

// Can reports whether the actor may do permission in channel on tenant. It is
// the boolean form of Authorize, for callers that filter rather than fail.
func (a *Authorizer) Can(ctx context.Context, tenant string, actor protocol.Actor, channel string, permission protocol.Permission) bool {
	explanation, err := a.Explain(ctx, tenant, actor, channel)
	return err == nil && explanation.Allows(permission)
}

// Explain returns what decides the actor's access in channel on tenant.
func (a *Authorizer) Explain(ctx context.Context, tenant string, actor protocol.Actor, channel string) (Explanation, error) {
	if a == nil || a.Store == nil {
		return Explanation{}, protocol.NewError(protocol.CodeInternal, "access: no grant store")
	}
	tenant = a.tenant(ctx, tenant, actor)
	resolve := a.Principal
	var principal Principal
	var err error
	if resolve != nil {
		principal, err = resolve(ctx, tenant, actor)
	} else {
		principal, err = ActorPrincipal(actor)
	}
	if err != nil {
		return Explanation{}, protocol.NewError(protocol.CodeUnauthorized, err.Error())
	}
	grants, err := a.Store.List(ctx, tenant)
	if err != nil {
		return Explanation{}, protocol.NewError(protocol.CodeInternal, "the board couldn't read its grants")
	}
	caller := Caller{Principal: principal}
	if a.Team != nil {
		if team := strings.TrimSpace(a.Team(ctx, tenant, principal)); team != "" {
			caller.Principal.TeamID = team
		}
	}
	if a.Member != nil {
		caller.Member = a.Member(ctx, tenant, principal)
	}
	if a.TeamProject != nil {
		caller.TeamProject = a.TeamProject(ctx, tenant, principal)
	}
	if a.TeamApp != nil {
		caller.TeamApp = a.TeamApp(ctx, tenant, principal)
	}
	if a.Cap != nil {
		caller.Cap = a.Cap(ctx, tenant, principal)
	}
	if a.ScopeText != nil {
		if text, ok := a.ScopeText(ctx, tenant, actor); ok {
			scope, err := ParseCan(text)
			if err != nil {
				return Explanation{}, invalidScopeError(err)
			}
			caller.Scope = &scope
		}
	} else if a.Scope != nil {
		caller.Scope = a.Scope(ctx, tenant, actor)
	}
	return Evaluate(caller.facts(tenant, grants), channel), nil
}

func (a *Authorizer) tenant(ctx context.Context, tenant string, actor protocol.Actor) string {
	if a.Tenant != nil {
		if normalized := strings.TrimSpace(a.Tenant(ctx, tenant, actor)); normalized != "" {
			return normalized
		}
	}
	if tenant = strings.TrimSpace(tenant); tenant != "" {
		return tenant
	}
	if fromContext, ok := TenantFromContext(ctx); ok {
		return fromContext
	}
	return "default"
}

func (a *Authorizer) refuse(channel string, permission protocol.Permission, explanation Explanation) error {
	if !a.RevealUnreadable && !explanation.Allows(PermissionRead) {
		return protocol.NotFound(protocol.CodeNotFound, "channel not found: "+channel)
	}
	if !explanation.Admitted {
		return protocol.Forbidden(fmt.Sprintf("no grant lets in this caller on %s", channel))
	}
	return protocol.Forbidden(fmt.Sprintf("this token can't %s", permissionPhrase(permission, channel)))
}

func invalidScopeError(err error) *protocol.Error {
	return &protocol.Error{Status: 400, Code: CodeInvalidScope, Message: err.Error()}
}

func permissionPhrase(permission protocol.Permission, channel string) string {
	switch permission {
	case PermissionRead:
		return "read " + channel
	case PermissionPost:
		return "post in " + channel
	case PermissionReact:
		return "react in " + channel
	case PermissionLabel:
		return "label in " + channel
	case PermissionSync:
		return "sync in " + channel
	case PermissionManage:
		return "manage " + channel
	default:
		return fmt.Sprintf("%s in %s", permission, channel)
	}
}

// tenantContextKey is the key WithTenant stores a tenant under.
type tenantContextKey struct{}

// WithTenant returns ctx carrying tenant. An explicit tenant passed to
// Authorize or Explain wins over it; it remains useful for code that asks the
// policy directly.
func WithTenant(ctx context.Context, tenant string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, tenantContextKey{}, tenant)
}

// TenantFromContext returns the tenant WithTenant put in ctx.
func TenantFromContext(ctx context.Context) (string, bool) {
	if ctx == nil {
		return "", false
	}
	tenant, ok := ctx.Value(tenantContextKey{}).(string)
	return tenant, ok && tenant != ""
}
