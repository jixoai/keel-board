# keel-board

A Go translation of the [vercel/board](https://github.com/vercel/board) message
and task standard, built for [keel](https://github.com/jixoai/keel) and usable
on its own.

The upstream project is TypeScript and tied to Vercel's API, OIDC, Connect and
deployment model. This repository keeps the parts that make the board a
standard — one durable log, channels and threads, tasks with claims and
leases, needs and labels — and replaces the backend with a plain Go library,
an append-only log and HTTP/SSE. Watchers, per-channel grants, files and
two-way sync follow in later phases.

- Upstream pin: `vercel/board@26428ed4e0661ff362c59abdf1215997c59891b0`
- License: Apache-2.0 (see `LICENSE` and `NOTICE`)
- Status: phase 1 core (channels, messages, tasks, claims, needs, labels, SSE)
  and the phase 2 sync design + adapter skeletons
- Sync design: [`docs/SYNC.md`](docs/SYNC.md)

## Layout

| Path | Contents |
| --- | --- |
| `board/` | Pure task domain: seven states, claim leases/fencing, needs, labels, queue |
| `protocol/` | Wire types, validation, error codes and HTTP status mapping |
| `log/` | Append-only board log, conditional appends, idempotency, folds and projections |
| `store/` | `EventStore` seam, in-memory store, PostgreSQL stub and schema |
| `server/` | HTTP API, SSE feed, authorizer and tenant seams |
| `cmd/board/` | Standalone dev server (`board serve`) |

The translation is faithful in semantics, not a line-by-line copy. Every
difference from upstream is recorded in `docs/PORTING.md`.
