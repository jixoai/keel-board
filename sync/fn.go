package sync

import (
	"context"
	"sort"
)

// RouteFunc reports whether a sync entry's mapping names a platform channel.
// The webhook keeps an event whose channel it names, even when the board has
// never seen the event's thread.
type RouteFunc func(ctx context.Context, channel string) (bool, error)

// Gate decides what a webhook keeps. An event is kept when the entry's own
// RouteFunc maps its channel, or when the board already syncs its thread.
// Everything else is dropped at the webhook, even in channels the app is in.
//
// Tenant and key are explicit: one process may run the same key for more than
// one tenant, and a gate's answer must be scoped to the connection that
// received the webhook. A handler must not share one static gate across
// tenants; a factory that builds one per (tenant, key) is the intended shape.
//
// The gate receives the tenant and entry key, because a tenant may run several
// connections and each has its own mappings and thread records. A gate that
// can't tell yet returns an error: the webhook answers 500 so the platform
// sends the event again, rather than storing every channel's events while a
// name is unknown.
type Gate interface {
	Routes(ctx context.Context, tenant string, key Key, channel string) (bool, error)
	ThreadSynced(ctx context.Context, tenant string, key Key, thread string) (bool, error)
}

// GateFuncs adapts functions to a Gate.
type GateFuncs struct {
	RoutesFunc func(ctx context.Context, tenant string, key Key, channel string) (bool, error)
	SyncedFunc func(ctx context.Context, tenant string, key Key, thread string) (bool, error)
}

// Routes calls RoutesFunc, or keeps nothing when it is nil.
func (g GateFuncs) Routes(ctx context.Context, tenant string, key Key, channel string) (bool, error) {
	if g.RoutesFunc == nil {
		return false, nil
	}
	return g.RoutesFunc(ctx, tenant, key, channel)
}

// ThreadSynced calls SyncedFunc, or reports nothing when it is nil.
func (g GateFuncs) ThreadSynced(ctx context.Context, tenant string, key Key, thread string) (bool, error) {
	if g.SyncedFunc == nil {
		return false, nil
	}
	return g.SyncedFunc(ctx, tenant, key, thread)
}

// Mirror is a two-way mirror of one board channel. An inbound event lands in
// channel; a delivered reply or change goes out; a new root in channel starts
// a thread in the platform channel the mapping offered. It is what a string
// value means under Channels.
func Mirror(channel string) SyncFunc {
	return func(ctx context.Context, call *Call) error {
		switch call.Direction {
		case DirectionIn:
			_, err := call.Import(ctx, channel, ImportOptions{})
			return err
		case DirectionOut:
			if call.Target != nil {
				_, err := call.Deliver(ctx, DeliverOptions{})
				return err
			}
			if call.Channel != "" && call.Message != nil && call.Message.Channel == channel {
				_, err := call.Start(ctx, call.Channel)
				return err
			}
			return nil
		default:
			return nil
		}
	}
}

// Into imports into a board channel and sends nothing back. It is what a
// string value means under a repository mapping: GitHub is often public, and
// what is said elsewhere may not be.
func Into(channel string) SyncFunc {
	return func(ctx context.Context, call *Call) error {
		if call.Direction != DirectionIn {
			return nil
		}
		_, err := call.Import(ctx, channel, ImportOptions{})
		return err
	}
}

// Render passes renderer to every Deliver in fn.
func Render(renderer Renderer, fn SyncFunc) SyncFunc {
	return func(ctx context.Context, call *Call) error {
		if call.Direction != DirectionOut {
			return fn(ctx, call)
		}
		wrapped := *call
		wrapped.Renderer = renderer
		return fn(ctx, &wrapped)
	}
}

// Channels routes each event by its platform channel. Keys are native channel
// IDs; values are what that channel does. A new root is offered to each
// mapped channel's function with Call.Channel set. An event in a thread the
// board syncs outside the map follows both ways (imports, and delivers).
//
// The returned RouteFunc is the entry's Routes, so the webhook keeps an event
// whose channel the map names.
func Channels(routes map[string]SyncFunc) (SyncFunc, RouteFunc) {
	names := make([]string, 0, len(routes))
	for name, fn := range routes {
		if fn != nil {
			names = append(names, name)
		}
	}
	sort.Strings(names)

	follow := func(ctx context.Context, call *Call) error {
		if call.Direction == DirectionIn {
			_, err := call.Import(ctx, "", ImportOptions{})
			return err
		}
		_, err := call.Deliver(ctx, DeliverOptions{})
		return err
	}

	fn := func(ctx context.Context, call *Call) error {
		switch call.Direction {
		case DirectionIn:
			if unit, ok := routes[call.Event.Channel]; ok {
				return unit(ctx, call)
			}
			return follow(ctx, call)
		case DirectionOut:
			if call.Target == nil {
				if call.Channel != "" {
					if unit, ok := routes[call.Channel]; ok {
						return unit(ctx, call)
					}
					return nil
				}
				for _, name := range names {
					offered := *call
					offered.Channel = name
					if err := routes[name](ctx, &offered); err != nil {
						return err
					}
				}
				return nil
			}
			if unit, ok := routes[call.Target.Channel]; ok {
				return unit(ctx, call)
			}
			return follow(ctx, call)
		default:
			return nil
		}
	}

	route := func(_ context.Context, channel string) (bool, error) {
		_, ok := routes[channel]
		return ok, nil
	}
	return fn, route
}

// Noop is a sync function that does nothing: nothing comes in, nothing goes
// out. It makes an explicitly disabled entry visible in configuration.
func Noop(context.Context, *Call) error { return nil }
