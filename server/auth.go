package server

import (
	"context"
	"net/http"
	"strings"

	"github.com/jixoai/keel-board/protocol"
)

// Authorizer is the board's access seam. It answers whether actor may do
// permission in channel, returning nil to allow and a protocol.Error (or any
// error) to refuse.
//
// Phase 1 ships AllowAll for development. A deployment plugs in its own policy
// here without touching the handlers: every read and write asks.
type Authorizer interface {
	Authorize(ctx context.Context, actor protocol.Actor, channel string, permission protocol.Permission) error
}

// AuthorizerFunc adapts a function to Authorizer.
type AuthorizerFunc func(ctx context.Context, actor protocol.Actor, channel string, permission protocol.Permission) error

// Authorize implements Authorizer.
func (f AuthorizerFunc) Authorize(ctx context.Context, actor protocol.Actor, channel string, permission protocol.Permission) error {
	return f(ctx, actor, channel, permission)
}

// AllowAll allows every caller every permission. It is for development and
// tests, never for a shared deployment.
type AllowAll struct{}

// Authorize implements Authorizer.
func (AllowAll) Authorize(context.Context, protocol.Actor, string, protocol.Permission) error {
	return nil
}

// ActorFunc resolves who a request is. A host replaces DevActor with its own
// verifier; returning an error refuses the request, and protocol.ErrUnauthorized
// is the right refusal for a missing or bad credential.
type ActorFunc func(*http.Request) (protocol.Actor, error)

// TenantFunc resolves which board a request is on.
type TenantFunc func(*http.Request) (string, error)

// DevActor is the development caller, and the seam a host replaces. It acts as
// "local:user:dev" unless the request names another actor in headers:
//
//	X-Board-Actor       the full actor ID (default local:user:dev)
//	X-Board-Principal   the principal it writes for (default: the actor's principal)
//	X-Board-Profile     a profile, making the actor "<principal>+<profile>"
//	X-Board-Handle      its handle (default: derived)
//	X-Board-Name        its display name (default: the handle)
//
// These headers are trusted only in development: they exist so a local board
// can exercise several actors without credentials. A real verifier decides
// the actor itself and ignores them.
func DevActor(r *http.Request) (protocol.Actor, error) {
	actorID := strings.TrimSpace(r.Header.Get("X-Board-Actor"))
	principal := strings.TrimSpace(r.Header.Get("X-Board-Principal"))
	profile := strings.TrimSpace(r.Header.Get("X-Board-Profile"))
	handle := strings.TrimSpace(r.Header.Get("X-Board-Handle"))
	name := strings.TrimSpace(r.Header.Get("X-Board-Name"))

	if actorID == "" && principal == "" && profile == "" {
		actorID = "local:user:dev"
	}
	if principal == "" {
		if index := strings.Index(actorID, "+"); index > 0 {
			principal = actorID[:index]
		} else {
			principal = actorID
		}
	}
	if profile == "" {
		if index := strings.Index(actorID, "+"); index > 0 {
			profile = actorID[index+1:]
		}
	}
	if actorID == "" {
		actorID = principal
	}
	if profile != "" {
		if !protocol.ValidProfile(profile) {
			return protocol.Actor{}, protocol.Invalidf("X-Board-Profile: %q isn't a profile: one is 1 to 32 lowercase letters, numbers, and -", profile)
		}
		actorID = principal + "+" + profile
	}
	if err := protocol.ValidateActorID(actorID); err != nil {
		return protocol.Actor{}, protocol.NewError(protocol.CodeUnauthorized, "the request names no valid actor")
	}
	if err := protocol.ValidateActorID(principal); err != nil {
		return protocol.Actor{}, protocol.NewError(protocol.CodeUnauthorized, "the request names no valid principal")
	}
	kind := protocol.ActorPerson
	if profile != "" {
		kind = protocol.ActorAgent
	}
	if handle == "" {
		handle = handleOf(principal)
		if profile != "" {
			handle = handle + "/" + profile
		}
	}
	if name == "" {
		name = handle
		if profile != "" {
			name = profile
		}
	}
	actor := protocol.Actor{
		ID:        actorID,
		Principal: principal,
		Profile:   nil,
		Kind:      kind,
		Handle:    handle,
		Name:      name,
	}
	if profile != "" {
		actor.Profile = &profile
		parent := handleOf(principal)
		actor.Via = &protocol.Via{Name: parent}
	}
	return actor, nil
}

// DevTenant is the development tenant resolver: the X-Board-Tenant header, or
// "default".
func DevTenant(r *http.Request) (string, error) {
	tenant := strings.TrimSpace(r.Header.Get("X-Board-Tenant"))
	if tenant == "" {
		return "default", nil
	}
	if len(tenant) > 64 {
		return "", protocol.Invalid("X-Board-Tenant: at most 64 characters")
	}
	for _, c := range tenant {
		ok := c == '-' || c == '_' || c == '.' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
		if !ok {
			return "", protocol.Invalid("X-Board-Tenant: letters, numbers, -, _, and . only")
		}
	}
	return tenant, nil
}

// handleOf returns the last part of an actor ID: "local:user:julian" is
// "julian".
func handleOf(actorID string) string {
	value := actorID
	if index := strings.LastIndex(value, "+"); index >= 0 {
		value = value[:index]
	}
	if index := strings.LastIndex(value, ":"); index >= 0 {
		value = value[index+1:]
	}
	if value == "" {
		return actorID
	}
	return value
}
