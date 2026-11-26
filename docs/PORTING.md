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

## Phase 1 and 3 (message surface)

Implemented, end to end:

- Channels (`GET`/`POST /channels`), messages (`GET`/`POST /messages`,
  `GET /messages/{id}`) and the full message surface (`POST edit`, `POST
  delete`, `POST reactions`, `GET`/`POST labels`).
- Threads (`GET /threads`) with latest-activity ordering, `after` and `older`
  (`before`) paging, root/reply attribution, per-channel counts, and thread
  summaries (`message`, live `replies`, `last`).
- Actors (`GET /actors`) and actor snapshots on messages (`by`/`author`) and
  tasks (`task.author`); edit, delete, reaction, label, and task writes record
  their actor.
- Attachments: `POST /attachments` (4 MB body), `GET`/`DELETE
  /attachments/{id}`, `POST /attachments/uploads` and `POST
  /attachments/uploads/{id}` for signed direct uploads, at most 10 files per
  message, content-addressed IDs, and a `blob.Store` seam with memory and FS
  implementations.
- JSON blocks: `json#name@version` fences are parsed, checked against a
  schema, and shown in `message.blocks`; mentions (`@[actor-id]` and `@handle`)
  are parsed and exact IDs validated; `mentions`, `label`, `text`, `by`, `re`,
  and `from` narrow `/threads`.
- Batch tasks (`POST /tasks/batch`, `GET /tasks/batch/{id}`): several new
  task threads in one channel, made in one append, with `key`-based needs,
  external prerequisites, assignees, attachments, and an `id`/`request`
  idempotency digest.
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
admission and lockout checks, a tenant-aware authorizer, and the anywhere seam
uploads use. Sync and its receipts, schedules, a writable schema registry, and
watchers remain outside phase 1; their wire types are not served, and requests
that name them are refused rather than half-accepted.

### Message surface semantics

- An edit or delete is its author's (`post` in the channel, and the principal
  or the exact profile that wrote it). Someone else's answers 403 where the
  caller can read it and 404 where it can't, unless it is the caller's own
  blind post.
- `revision` moves only on a post, edit, or delete; every change moves
  `version`. An edit/delete may carry the `revision` the caller read: a moved
  message answers 409 `revision_conflict` and nothing lands.
- Deleting a message leaves a tombstone: its text, labels, reactions, files,
  and blocks empty, its ID, channel, thread, author, and timestamps kept, and
  its replies still in the thread. A retried delete is a no-op; an edit of a
  tombstone answers `message_deleted` (or `revision_conflict` at a stale
  revision).
- Deleting a task's root cancels the task (`reason: deleted`) in the same
  append. Editing a task's root by anyone but its owner holds the task for its
  owner (`blocked.kind: edited`) and ends any claim.
- Reactions are stored under Chat SDK names (`thumbs_up`), with common Slack
  aliases normalized. A repeated add/remove is a no-op. A reaction needs
  `read`, as well as `react`: an unreadable message is 404, never a 200/403
  that would confirm it exists.
- Label writes under `board.` are refused, and a write that leaves labels as
  they are emits nothing.
- Idempotency keys are per tenant and name the actor without ambiguity:
  `namespace:<len(actorID)>:<actorID>:<client id>`. `local:user` + `bob:k` and
  `local:user:bob` + `k` are different writes; tenants never share a key.
- A retry is answered before validation, permission checks, and attachment
  resolution: `POST /messages`, `POST /tasks`, and `POST /tasks/batch` first
  look up the idempotent result, then check the caller may have it. A landed
  message is returned only to the actor/principal/profile that wrote it and
  only while the caller can read its channel; a blind write gets back just the
  text it sent, with no labels, files, reactions, or task. A batch read
  (`GET /tasks/batch/{id}`) applies the same rule and answers 404 when any root
  isn't the caller's or isn't readable.
- `GET /tasks/batch/{id}` and a batch's POST replay reuse the landed roots;
  neither re-checks `post`, re-binds files, nor needs the current request body
  to validate. A tx-level replay (another process landed the key between the
  early lookup and the append) goes through the same `replayMessage` /
  `replayBatch` author and read checks.
- An empty tenant from a custom `TenantFunc` becomes `default` before the log,
  the authorizer, or a blob key sees it, so a board can't write under `""` and
  read under `default`.
- PostgreSQL and a shared store: `ReplayKey` refreshes the tenant and asks
  `store.RequestStore`, so a cold cache or another process's write still
  answers a retry; a fresh cache isn't assumed. Writes keep main's
  freshness/RequestResult behavior and the store's head/request races.

### Deliberate simplifications

- **Blocks use built-in schemas only.** There is no writable schema registry
  yet: `git-commit@1` and `vercel-deployment@1` are known, and any other
  `json#name@version` fence is refused with `invalid_block`. The scanner reads
  top-level fences, not upstream's full Markdown containers (quotes/list
  items). `BlocksIn` reads a message's current text; a legacy message whose
  text holds an invalid block simply shows none, the write-time invariant is
  the only guarantee.
- **Mentions are parsed, not settled.** `@[actor-id]` is validated as an actor
  ID; `@handle` stays as typed. A `mentions` filter matches an exact term, an
  actor ID's `+` profiles (`local:user:alice` matches
  `@[local:user:alice+claude]`), or a handle's `/` profiles (`alice` matches
  `@alice/claude`, never `@alicebob`); `me` matches exactly, never a profile.
  Upstream also rewrites a unique `@handle` to `@[actor-id]` and accepts email
  mentions from sync.
- **Emoji normalization is a small table.** Short names and common Slack
  aliases (`+1`, `white_check_mark`, …) are canonicalized; Unicode emoji
  characters aren't.
- **Attachments gained an explicit delete.** Upstream has no delete route; here
  the uploader may delete bytes no live message carries, messages hide their
  files when deleted, and the uploader keeps read access (as upstream).
  Blob keys and records are tenant-scoped (`tenants/<sha256(tenant)>/...`), so
  one tenant's ID reads nothing on another. A file name is refused with
  `invalid_attachment` when it is a dotfile or holds invisible formatting
  characters; it is never silently renamed. Attachment responses carry
  `cache-control: private, max-age=31536000, immutable`; deleting removes the
  bytes first and the record second, so a failed store leaves a retryable
  record.
- **Upload permission is an optional seam.** When the configured
  `server.Authorizer` also implements `server.AnywhereAuthorizer`, uploads and
  direct-upload starts need `post` or `sync` anywhere. `access.Authorizer`
  implements it over the tenant's grants, fail-closed, and a policy that only
  implements the per-channel seam keeps the old behavior: uploads are
  authenticated only, and binding a file to a message still requires `post`
  there.
- **Two blob reads stay under the board's lock.** Binding re-reads the records
  inside the write transaction (so a delete that won the race can't leave a
  dangling reference), and `DELETE /attachments/{id}` checks live references
  and deletes under the lock (so a concurrent post can't bind what it removes).
  The first read that resolves attachment metadata happens outside the lock.
- **Thread filters are computed over the folded log.** There's no search
  index; `from` matches only `board` (nothing records sync origins yet). One
  message must satisfy every predicate: channel is the root's, and a reply may
  fall back to the root's labels (labels are thread state), but a root's text
  and a reply's label or author don't combine. A `by` handle that names a
  person covers that person's profiles; a handle that names one profile covers
  only that profile.
- **Batch is the plan.** Upstream has no separate plan entry; a batch whose
  tasks wait on each other by `key` is that plan. The batch `request` digest is
  trusted as a fronting board's, as upstream does.
- **Not implemented:** `GET /posts/{id}`, typing, schedules, the schema
  registry, `scanned` on a thread page, and watchers. `older` is the thread
  page's only backward cursor.
- **`message.by` is an ActorView here.** Upstream's `MessageView.by` is the
  author record (`actor`, `principal`, `profile`) and `author` is the
  ActorView; this port has `by` and `author` both as ActorView.
- **No message-surface migration.** Every new fact rides the existing
  `payload jsonb` column; the PostgreSQL migration is main's, with no column
  added for reactions, files, edits, or blocks.

### Known deferred

- `BlocksIn` trusts a message's current text; old data isn't revalidated.
- `by` query shape and handle resolution are small string matches, not
  upstream's full actor-index resolver.
- Cursor details (a thread page's `after` naming a deleted message, a
  cross-channel cursor) aren't normalized.
- Label names are plain Go map keys, so inherited names (`constructor`,
  `__proto__`) are ordinary data and safe, but no compatibility shim exists.
- A same-text edit is a no-op and doesn't advance `revision` (upstream would
  append an edit); this makes retries idempotent.
- Error precedence isn't fixed: which of two invalid fields is reported first
  may differ from upstream.

Tests: `go test -count=1 ./...`, `go test -race ./...`, `go vet ./...`,
`gofmt -l .`.

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
  multi-tenant board can't authorize against another tenant's grants.
  `Authorizer.AuthorizeAnywhere` answers the upload seam: it asks whether the
  caller may `post` or `sync` in any pattern the tenant's grants or the token
  scope names (one representative channel per pattern), and fails closed. A
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
