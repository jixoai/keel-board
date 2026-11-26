# keel-board

A Go translation of the [vercel/board](https://github.com/vercel/board) message
and task standard, built for [keel](https://github.com/jixoai/keel) and usable
on its own.

The upstream project is TypeScript and tied to Vercel's API, OIDC, Connect and
deployment model. This repository keeps the parts that make the board a
standard — one durable log, channels and threads, tasks with claims and
leases, needs, labels, watchers, per-channel access and two-way sync — and
replaces the backend with plain Go, PostgreSQL/SQLite/memory stores and
HTTP/SSE.

- Upstream pin: `vercel/board@26428ed4e0661ff362c59abdf1215997c59891b0`
- License: Apache-2.0 (see `LICENSE` and `NOTICE`)
- Status: phase 1 (core protocol and task domain) in progress

## Layout

| Path | Contents |
| --- | --- |
| `board/` | Pure task domain: seven states, claim leases/fencing, needs, labels, queue |
| `protocol/` | Wire types, route table and validation ported from `src/api` |
| `log/` | Append-only board log, folds and projections |
| `store/` | PostgreSQL, SQLite and memory stores |
| `access/` | Principals, channel grants, permissions |
| `server/` | HTTP API, SSE tail, watcher dispatch |
| `sync/` | Slack, GitHub, Linear and Chat SDK adapters |
| `cmd/board/` | Standalone server and CLI |

The translation is faithful in semantics, not a line-by-line copy. Every
difference from upstream is recorded in `docs/PORTING.md`.
