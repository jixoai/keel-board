# keel-board

A Go translation of the [vercel/board](https://github.com/vercel/board) message
and task standard, built for [keel](https://github.com/jixoai/keel) and usable
on its own.

The upstream project is TypeScript and tied to Vercel's API, OIDC, Connect and
deployment model. This repository keeps the parts that make the board a
standard — one durable log, channels and threads, tasks with claims and
leases, needs and labels — and replaces the backend with a plain Go library,
an append-only log and HTTP/SSE. Watchers, grant HTTP routes, files and
two-way sync follow in later phases.

- Upstream pin: `vercel/board@26428ed4e0661ff362c59abdf1215997c59891b0`
- License: Apache-2.0 (see `LICENSE` and `NOTICE`)
- Status: phase 1 core (channels, messages, tasks, claims, needs, labels, SSE),
  the phase 2 sync design + adapter skeletons, and a PostgreSQL log store
- Sync design: [`docs/SYNC.md`](docs/SYNC.md)

## Running

```sh
go run ./cmd/board serve --addr 127.0.0.1:8790
go run ./cmd/board serve --addr 127.0.0.1:8790 \
  --dsn 'postgres://user:pass@localhost:5432/board?sslmode=disable'
```

Without `--dsn` the log is in memory and lost on exit. With `--dsn` it lives in
PostgreSQL: a restart replays it, and an append is one transaction over the
tenant's head, its events, and its idempotency row.

Several processes may share one PostgreSQL log. A board rechecks the store's
head at most once a second, a write that loses a race catches up and decides
again once, and an SSE cursor ahead of the log is refused rather than clamped.

## Layout

| Path | Contents |
| --- | --- |
| `board/` | Pure task domain: seven states, claim leases/fencing, needs, labels, queue |
| `protocol/` | Wire types, validation, error codes and HTTP status mapping |
| `log/` | Append-only board log, conditional appends, idempotency, folds and projections |
| `store/` | `EventStore` seam, in-memory and PostgreSQL stores, migrations |
| `access/` | Per-channel permissions, grants, patterns and the authorizer |
| `server/` | HTTP API, SSE feed, authorizer and tenant seams |
| `cmd/board/` | Standalone dev server (`board serve`, memory or `--dsn`) |

The translation is faithful in semantics, not a line-by-line copy. Every
difference from upstream is recorded in `docs/PORTING.md`.
