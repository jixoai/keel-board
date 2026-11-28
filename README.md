# keel-board

A Go translation of the [vercel/board](https://github.com/vercel/board) message
and task standard, built for [keel](https://github.com/jixoai/keel) and usable
on its own.

The upstream project is TypeScript and tied to Vercel's API, OIDC, Connect and
deployment model. This repository keeps the parts that make the board a
standard — one durable log, channels and threads, tasks with claims and
leases, needs and labels — and replaces the backend with a plain Go library,
an append-only log and HTTP/SSE. Messages, reactions, files, blocks/mentions,
actors, batch tasks, the board's threads, watchers and schedules are served;
grant HTTP routes remain a later phase.

- Upstream pin: `vercel/board@26428ed4e0661ff362c59abdf1215997c59891b0`
- License: Apache-2.0 (see `LICENSE` and `NOTICE`)
- Status: phase 0 feature-complete for the standalone board: core log, message
  and task surface, access grants, durable watchers/schedules, the two-way sync
  engine with Slack/GitHub/Linear/Chat SDK adapters, management and source-read
  APIs, read-only sync/automation health (external targets, oldest pending lag,
  24h failures, watcher dead/error facts since `v0.1.2`), a PostgreSQL store
  with forced RLS, and the E2E harness. Phase 1 (the keel import and UI) is
  tracked in keel's `keel-board-v1` change and its `feat-keel-board-sync-v2`
  follow-up.
- Sync design: [`docs/SYNC.md`](docs/SYNC.md)

## Running

```sh
go run ./cmd/board serve --addr 127.0.0.1:8790
go run ./cmd/board serve --addr 127.0.0.1:8790 \
  --dsn 'postgres://user:pass@localhost:5432/board?sslmode=disable'
go run ./cmd/board serve --addr 127.0.0.1:8790 --blobs .board/files
```

Without `--dsn` the log is in memory and lost on exit. With `--dsn` it lives in
PostgreSQL: a restart replays it, and an append is one transaction over the
tenant's head, its events, and its idempotency row.

Several processes may share one PostgreSQL log. A board rechecks the store's
head at most once a second, a write that loses a race catches up and decides
again once, and an SSE cursor ahead of the log is refused rather than clamped.

## Projection checks and rebuilds

The board's projection is an in-process fold of the durable log. Two
operational routes need `manage` on `*`:

- `POST /v1/board/admin/projection/check` reports the tenant's log head, the
  projection's watermark, and a SHA-256 digest of the projection against a full
  replay. `consistent` is true only when both the watermark and the digest
  match.
- `POST /v1/board/admin/projection/rebuild` replaces the projection with a
  replay of the log. It is idempotent, never writes or deletes an event, and
  keeps the old projection serving if the load is interrupted.

`board rebuild --dsn <dsn> [--tenant <tenant>]...` does the same offline for
every tenant in the log and exits non-zero on an inconsistent report. Each
process owns its own projection, so a multi-instance deployment rebuilds every
instance. A
PostgreSQL log whose `board_heads` row and newest event disagree refuses
writes with 503 `unavailable`; a rebuild cannot repair the log itself, so
restore the event or the head row first.

## Files

Attachments are bytes under a `blob.Store`: memory by default, or a directory
with `board serve --blobs <dir>`. Uploads are at most 4 MB in one request, a
message carries at most 10, and `POST /attachments/uploads` hands out a signed
direct-upload URL when the configured store implements `blob.UploadSigner`.
Reading a file needs its uploader's principal or a readable message carrying
it; deleting a message hides its files.

## Testing

```sh
go test ./...
go test -race ./...
```

PostgreSQL tests run only when `KEEL_BOARD_TEST_DSN` (or `TEST_DSN`) names a
local server; each test creates and drops its own schema. CI must set it:
with `CI` set and no DSN the PostgreSQL tests fail instead of skipping. The
sync and automation tables enable and force row-level security; the stores set
`keel.tenant_id` transaction-locally, and an unset setting matches no rows.

## Layout

| Path | Contents |
| --- | --- |
| `board/` | Pure task domain: seven states, claim leases/fencing, needs, labels, queue |
| `protocol/` | Wire types, validation, error codes and HTTP status mapping |
| `log/` | Append-only board log, conditional appends, idempotency, folds and projections |
| `blob/` | Attachment byte stores: memory, FS, and the optional signing seam |
| `store/` | `EventStore` seam, in-memory and PostgreSQL stores, migrations |
| `access/` | Per-channel permissions, grants, patterns and the authorizer |
| `server/` | HTTP API, SSE feed, authorizer and tenant seams |
| `sync/` | Provider registry (tenant-qualified), normalized events, runners and adapters |
| `syncstore/` | Durable sync inbox/outbox, copies and threads, webhook handler, workers |
| `automation/` | Durable watchers/schedules, action registry, workers, management-store seam |
| `cmd/board/` | Standalone dev server (`board serve`, memory or `--dsn`, `--blobs`, automation workers) |

The translation is faithful in semantics, not a line-by-line copy. Every
difference from upstream is recorded in `docs/PORTING.md`.
