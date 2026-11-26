# Porting notes

Source: `vercel/board@26428ed4e0661ff362c59abdf1215997c59891b0` (Apache-2.0).

## Kept

- A board is one append-only log; projections are derived.
- Channels, threads, messages, labels, reactions and files.
- Tasks are thread roots; replies are the work log.
- Seven task states: waiting, ready, claimed, blocked, completed, failed, canceled.
- Claims are leases with an epoch and a one-time token (only its hash is stored).
- Needs form a bounded DAG; only completed prerequisites ready dependents.
- `board.*` labels are reserved; `board.task` mirrors the state.
- Per-channel access (read/post/react/label/sync/manage) and grants.
- Watchers and live tail over SSE with resumable cursors.
- Two-way sync keyed by Slack channels, GitHub repos and Linear teams.

## Replaced

- Vercel API/OIDC/team/project/deployment identity → keel actors and Go auth seams.
- Vercel Connect credentials → adapter-owned secrets.
- TypeScript runtime → Go library + HTTP server.
- SQLite-only local engine → PostgreSQL/SQLite/memory behind one interface.

## Phase 1

Implemented, end to end:

- Channels (`GET`/`POST /channels`), messages (`GET`/`POST /messages`,
  `GET /messages/{id}`) and label writes (`POST /messages/{id}/labels`).
- Tasks (`GET`/`POST /tasks`), `claim`/`claim-ready`, `renew`, `release`,
  `complete`, `fail`, `block`, `unblock`, `reopen`, `assign`, `needs`, `cancel`,
  with lease expiry, epoch/token fencing, owned-or-manage changes, and
  completion readying dependents.
- One append-only log per tenant: monotonic seqs, conditional appends,
  idempotency key plus request digest (persisted with the batch), folds to
  messages/threads/channels/actors/tasks, replay from the store, and SSE with
  `Last-Event-ID` resumption.
- `Authorize(ctx, actor, channel, permission)` and tenant resolution seams; the
  development server allows everything and trusts actor headers.

Not in phase 1: grants and per-channel access policy (only the seam), sync and
its receipts, reactions, files and attachments, schedules, schemas and blocks,
and watchers. Their wire types are not served, and requests that name them are
refused rather than half-accepted.

## Deliberate differences

- Go time comparisons use UTC `time.Time`; upstream uses JS Date strings.
- The pure task domain exposes `Commit(current, base, next)` so a store can make
  conditional appends explicit.
- Phase 1 shipped the task/thread/channel core.

## Sync

Phase 2 adds the sync model as Go packages (`sync/`) and adapter skeletons for
Slack, GitHub, Linear, and a generic Chat SDK bridge. The complete design is in
[`SYNC.md`](SYNC.md). Deliberate differences from upstream:

- `SyncProvider` becomes a Go `Provider`; the engine's `board-sync` consumer
  becomes the `Host`/`Runner` seam. The durable inbox/outbox, subscription,
  backfill, files, typing, and per-key ownership are the server's next phase.
- Vercel Connect, OIDC, and deployment identity are replaced by adapter-owned
  secrets and platform webhook verification.
- The upstream pin keeps every adapter in `src/sync/`; here they are Go
  packages `sync/slack`, `sync/github`, `sync/linear`, and `sync/chatsdk`.
- GitHub writes are denied unless `WriteRepos` allows the repo, and GitHub
  comments carry a hidden marker as a second echo guard. Upstream relies on
  the app's bot user ID and allows a `mirror` function to write.
- Slack reaction webhooks don't carry `thread_ts`; the skeleton anchors a
  reaction at `channel:item.ts` and leaves parent-thread lookup to the host.
  Upstream reads the parent from the Chat SDK message.
- The Chat SDK bridge is a Go `Adapter` interface, not a JavaScript runtime.
- Providers emit platform-native author IDs; a host re-keys them under the
  entry key with `sync.KeyedID`, as upstream's engine does automatically.

## Storage and multi-process behavior

- PostgreSQL is the durable log; the in-memory store is process-local.
- One append is one transaction: the tenant's `board_heads` row is locked and
  its `seq` checked against the expected head, then the batch's events and its
  `board_requests` idempotency row are written. A keyed retry with the same
  digest answers the first result without writing.
- Payloads are JSON values (`jsonb`). Supply valid JSON; empty means `null`, a
  NUL is refused, and timestamps keep microseconds. The representation may be
  normalized, so compare payloads by value, not bytes.
- A board caches each tenant's folded log. Against a store that reports a head
  (PostgreSQL), it rechecks at most once per `Freshness` window (default 1s), so
  a read sees another process's writes within that bound. A write that collides
  with another process's append catches up and runs its decision once more.
- An SSE feed forces a catch-up when it opens, so `last` replay and a client's
  `Last-Event-ID` see the store's events. A cursor ahead of the log is refused
  (`invalid_cursor`), never silently clamped downward.

Known deviations and deferred work:

- The idempotency digest in `server/messages.go` and `server/tasks.go` is coarse
  (`channel`/`re`); it is not content-addressed yet. port/messages owns this.
- The HTTP task-adopt route (`POST /tasks` with `re`) refuses `id` in this
  snapshot, so a keyed adopt is exercised through `Board.Update`; the replay
  path and store contract are ready if the handler allows the key later.
- There is no migration version table: migration files are idempotent and run
  under a session advisory lock. No `CREATE INDEX CONCURRENTLY`.
- There is no LISTEN/NOTIFY: `Wait` polls the tenant head every 50ms per waiter.
  No log compaction and no `board_requests` retention.
- There is no statement timeout: read-side store work (first load, catch-up,
  head check) is bounded at 5s; writes use the caller's request context.
