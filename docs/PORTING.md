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
- `Authorize(ctx, tenant, actor, channel, permission)` and tenant resolution
  seams; the development server allows everything and trusts actor headers.

Grant and access HTTP routes are not in phase 1, but the policy is a Go package
now (`access/`): grants, channel patterns, principal matching, reach,
admission and lockout checks, and a tenant-aware authorizer. Sync and its
receipts, reactions, files and attachments, schedules, schemas and blocks, and
watchers remain outside phase 1; their wire types are not served, and requests
that name them are refused rather than half-accepted.

## Deliberate differences

- Go time comparisons use UTC `time.Time`; upstream uses JS Date strings.
- The pure task domain exposes `Commit(current, base, next)` so a store can make
  conditional appends explicit.
- Phase 1 shipped the task/thread/channel core.

## Access

`access/` transposes the per-channel policy of `packages/board/src/server/access.ts`
and `grants.ts` to plain Go data and functions.

Kept:

- Permissions and implications: `read`, `post`, `react`, `label`, `sync`,
  `manage`; every message-addressing verb implies `read`, `manage` implies all,
  `post` implies nothing.
- Channel patterns: exact, `subtree/`, and `*`; an exact channel wins over the
  longest subtree, which wins over `*`.
- Principal levels: principal, group (`all-users`, `all-projects`, `all-apps`),
  and `everyone`; the closest level at a pattern wins and grants tied there add
  up. `everyone` never admits a caller by itself.
- Grants, written `Can`, caps, token scopes, `Explain` (winning pattern, the
  patterns it won over, level, grants, capped flag, permissions), and the
  reach/admission/lockout rules of grant changes, including 409
  `grant_lockout`.
- The small `x-board-can` helpers: `ParseCan`, `CanText`, 4000-character and
  64-pattern bounds for headers.

Replaced or simplified:

- Vercel OIDC/team/project/deployment identity → `protocol.Actor`, the
  `ActorPrincipal` default, and host hooks for `Principal`, `Team`, `Member`,
  `TeamProject`, `TeamApp`, `Cap`, and scope.
- Vercel API grant storage → `GrantStore` with a concurrency-safe
  `MemoryStore`. `GrantService` owns write policy; seed a board's first grants
  directly through the store (or with an unrestricted owner), since no caller
  can have manage before a grant gives it.
- The server's `Authorize` seam now passes the tenant the server resolved, so a
  multi-tenant board can't authorize against another tenant's grants. A
  `Tenant` hook may confirm the name but not replace it: returning a different
  tenant is an error, since only the server decides which board a request is
  on. `BoardTeam`/`TeamFunc` must be set before the service serves writes;
  changing them concurrently isn't supported.
- `GrantService.Create`/`Update` force project `TeamID` from `BoardTeam` or
  `TeamFunc`; a team-scoped grant no longer matches a caller whose token names
  no team. A project moving teams keeps none of its old team's grants.
- `Grant.Capped` is recomputed from the writing caller's `Cap`; a client value
  is not trusted. A grant naming a person is capped when the caller has a cap.
- Grant listings and explanations are gated: `List` and explaining another
  caller require manage at `*`. A caller that can't list sees its own
  permissions for a channel a grant admits it to, but no pattern, level, or
  grants; an unknown or unreachable target is a uniform 403. `Update` and
  `Revoke` do reach on the grant as it is before reading a change, and a caller
  that can't list gets the same 403 for a missing grant, an unreachable one, a
  removal that names nothing, or the last principal; only a lister sees a 404
  or `not_in_grant`/`grant_empty`. This closes the 404/403 enumeration oracle.
- A bad `x-board-can`, a canceled request, or a store failure that a filtering
  handler would otherwise turn into an empty 200 is caught before the handler
  runs: an Authorizer may implement `server.RequestValidator`, and `access`
  does, so GET /channels and GET /tasks answer 400, 499, 503, or 500 rather than an
  empty 200.
- A sandboxed owner (owner with a token scope or cap) still passes reach and
  admission; only the lockout is waived for an owner.
- `Removes` compares a GitHub principal by identity: a grant carrying a
  `repositoryId` matches only that ID, and a caller without an ID doesn't fall
  back to the repository name. Removing a principal the grant doesn't name is
  400 `not_in_grant`; removing the last principal is 400 `grant_empty`.
- `all-projects` accepts any environment name; upstream limits it to
  production/preview/development. A project's environments still default to
  production.

Known deviations, deliberately not implemented now:

- No batch grant writes (`createGrants`), no grant HTTP routes, no `/me`, and
  no `grants/explain` query API. `GrantService` and `Authorizer` are the Go
  seams.
- No PostgreSQL store and no version CAS. `MemoryStore` is authoritative for
  one process, and `GrantService.mu` serializes its own read-check-write. A PG
  store must make `Update` conditional or run the check in one transaction;
  the `GrantStore` comment records this.
- No named-channel 403 distinction: the authorizer can't tell a channel named
  by a write from a resource looked up by ID, so an unreadable channel answers
  404 by default. `RevealUnreadable` opts into 403 for hosts that know the
  channel was named.
- No `O(channels × grants)` indexing or grant cache; `Authorize` reloads the
  tenant's grants per check, and `RequestValidator`'s preflight adds one more
  read per request.
- A bad `x-board-can` is 400 `invalid_scope` when the host uses `ScopeText`; a
  programmatic `Scope` is trusted host code. When both are configured they
  compose by intersection, and neither can widen the other. A programmatic
  scope over 64 patterns is applied pattern by pattern instead of being
  rejected, while a parsed header over the limit is 400.


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
