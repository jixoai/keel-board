// Package storage declares the dialect-neutral capability contract shared by
// the board's storage adapters. It carries no driver and no SQL: a dialect
// adapter (PostgreSQL today) implements the domain interfaces in store,
// syncstore, audit and automation, and reports what its backend guarantees
// here.
//
// A capability is a promise, not a feature flag a caller may ignore. An
// adapter that does not declare a capability must return an error wrapping
// ErrUnsupported from any operation that needs it, so a deployment can tell
// "this backend cannot do that" from "that operation failed".
package storage

import (
	"errors"
	"fmt"
)

// Capability names one guarantee an adapter may offer.
type Capability string

const (
	// Durable means rows survive the process that wrote them.
	Durable Capability = "durable"
	// CrossProcessLeases means leases and locks are safe with more than one
	// process sharing the backend.
	CrossProcessLeases Capability = "cross_process_leases"
	// Migrations means the adapter owns its schema and applies migrations.
	Migrations Capability = "migrations"
	// ListenNotify means appends push wakeups (for example PostgreSQL
	// LISTEN/NOTIFY) instead of the waiter polling the head.
	ListenNotify Capability = "listen_notify"
)

// Capabilities declares what one adapter guarantees. The zero value is an
// in-process, non-durable store with no schema of its own and no push wakeups.
type Capabilities struct {
	Durable            bool
	CrossProcessLeases bool
	Migrations         bool
	ListenNotify       bool
}

// Supports reports whether the adapter declares capability.
func (c Capabilities) Supports(capability Capability) bool {
	switch capability {
	case Durable:
		return c.Durable
	case CrossProcessLeases:
		return c.CrossProcessLeases
	case Migrations:
		return c.Migrations
	case ListenNotify:
		return c.ListenNotify
	default:
		return false
	}
}

// Require returns a typed error when an adapter does not declare capability.
// A caller that needs durable rows or cross-process leases calls it before
// relying on them, so an unsupported backend is explicit rather than silently
// weaker.
func (c Capabilities) Require(capability Capability) error {
	if c.Supports(capability) {
		return nil
	}
	return Unsupported(capability)
}

// ErrUnsupported is returned, wrapped, by an adapter asked for a capability it
// does not declare.
var ErrUnsupported = errors.New("storage: capability is not supported")

// Unsupported builds the error an adapter returns for a capability it does not
// declare.
func Unsupported(capability Capability) error {
	return fmt.Errorf("%w: %s", ErrUnsupported, capability)
}

// Reporter is implemented by adapters that can state their guarantees.
type Reporter interface {
	Capabilities() Capabilities
}
