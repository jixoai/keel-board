# Sync

Two-way sync connects a board thread to external threads: a Slack thread, a
GitHub PR or issue, a Linear issue, or any platform a Chat SDK-style adapter
speaks. A message written in the thread, wherever it was written, is delivered
to every external thread it is synced with except the one it came from.
Replies, edits, deletes, reactions, labels, and lifecycle events follow the
same path.

This document is the Go translation of sync in
[vercel/board](https://github.com/vercel/board) at
`26428ed4e0661ff362c59abdf1215997c59891b0` (Apache-2.0), and the design for
the `sync/` packages in this repository. It is deliberately more than an
interface sketch: the core, the dedupe/retry/inbox/outbox semantics, signature
verification, the durable PostgreSQL state, the HTTP server and worker daemon,
and the Slack/GitHub/Linear/Chat SDK adapters compile and are tested. The
board and sync surfaces are exercised end to end by `scripts/e2e` against fake
platform servers; a live platform credential is deployment configuration, not
a code gap.

## Status

| Area | State |
| --- | --- |
| `sync/` core: `Provider`, `Registry`, normalized `Event`, `ThreadRef`, `OutboundMessage`, `SyncFunc`, `Call`, `Host`, `Runner` | Implemented and tested |
| Dedupe keys and markers, retry policy, memory inbox (lanes, leases, dead letters), outbox (lane order, retries, dead letters) | Implemented and tested |
| `sync/slack`: signature, webhook normalization (messages, edits, deletes, reactions, files, links), targets, post/edit/delete/react, files both ways, typing status, source read/join opt-in, channel lookup, permalink, root recovery | Implemented; Vercel deployment tables and full mrkdwn conversion are open |
| `sync/github`: signature, delivery ID, repo and installation allowlists, comments, PRs, issues, reviews, workflow runs, comment marker, REST comments/edits/deletes/reactions, viewer detection, root recovery, PR/issue/comment links | Implemented; reaction removal, review-thread URL variants beyond `#discussion_r`/`#r`, and review-conversation resolution are open |
| `sync/linear`: signature, timestamp, team allowlist, issues, comments, GraphQL issue/comment/reaction calls, identifier resolution, root recovery, source reads (team/project/issue), permalinks, outbound labels/state/properties | Implemented and tested; non-issue comment payloads are ignored, links (`Locate`/`URL`) and reaction removal are open |
| `sync/chatsdk`: signed HTTP bridge v1, HMAC/nonce/timestamp, self identity, inbound batch intake, resolve/deliver client, optional edit/delete/react/root/link hooks | Implemented and tested; reference Go sidecar fixture only, no third-party Chat SDK adapter bundled |
| `sync/fake`: programmable `Provider` and `Host` | Implemented and used by tests |
| `syncstore`: tenant-qualified durable inbox and outbox, leases, dedupe, lane order, copies (with file IDs), failure receipts, RLS, webhook handler, inbound/outbound workers | Implemented and tested |
| `server` sync wiring: `POST /webhooks/{key}`, supervised board-event subscription, thread attach/backfill with future-only targets, the tenant-scoped management API (entries/status/inbox/outbox retries, thread attach, source read/join), and `cmd/board` workers | Implemented and tested in `server/` and `cmd/board` |
| PostgreSQL end-to-end: Host, copies, mappings, future-only `since_seq`, outbox restart, concurrent root creation, target uniqueness | Implemented and tested with `KEEL_BOARD_TEST_DSN` |
| Production engine gaps: cross-process lease for key ownership and full lifecycle reconciliation | Seams in place; listed under Open items. Inbound and outbound file bytes, typing status, and link snapshots are wired through the server host and workers |

The upstream pin keeps all sync code in `packages/board/src/sync/`; the
`src/sync-slack/`, `src/sync-github/`, `src/sync-linear/`, and
`src/sync-chat-sdk/` paths named in some documentation are package entry
points, not separate source directories.

## Architecture

```text
INBOUND

  platform
     │  POST /webhooks/{key}
     ▼
  Provider.Receive          verify signature; parse; normalize; emit Records
     │
     ▼
  Registry.HandleWebhook    gate: mapped channel, or a thread already synced
     │
     ▼
  Inbox.Push                dedupe by Record.Dedupe; store before answering
     │
     ▼  2xx
  platform                  redelivers only if storage failed (5xx)

  worker
     │  Inbox.Lease        oldest available event per external thread
     ▼
  Runner.Inbound            call the entry's SyncFunc (direction "in")
     │
     ▼  SyncFunc.Import
  Host.Import               idempotent by inbox event ID and call order
     │
     ▼
  board log                 message.posted {origin, from}

OUTBOUND

  board log                 message.posted / edited / deleted / reaction / labels
     │  sync subscription (a consumer, like a watcher)
     ▼
  TargetsForDelivery        every synced target except where it came from
     │
     ▼
  Runner.Outbound           call the entry's SyncFunc (direction "out")
     │
     ▼  SyncFunc.Deliver
  Provider.Post/Edit/...    platform API
     │
     ▼
  Host.Record               message.synced / message.files_synced / sync_failed
```

Two rules hold the shape together:

1. **Store before acknowledging.** A webhook answers 2xx only after its kept
   records are in the inbox. If storing fails, answer 5xx; the platform retries.
2. **One lane per conversation.** Inbound lanes are keyed by external thread
   (or its precise sub-thread); outbound lanes by board thread. Events on one
   lane run in order; different lanes run in parallel.

## Core model

### Entries, keys, and platforms

A sync entry is one platform account plus one function:

```go
entry := boardsync.Entry{
    Tenant:   "tenant-a",          // the board that owns this connection
    Key:      "slack",             // chooses external IDs and the webhook path
    Platform: "slack",             // must match Provider.Platform()
    Provider: slackProvider,        // the platform
    Sync:     boardsync.Mirror("support"),
    Routes:   routeFunc,            // optional: which platform channels this entry maps
}
```

* `Key` is the connection, not the platform. Two Slack workspaces are
  `slack` and `acme`; each key has its own users, targets, and webhook path.
* `Platform` is `slack`, `github`, `linear`, or a Chat SDK platform name.
* A key appears in every external ID (`slack:C0123:1727.9`), in targets, in
  `--source`-style filters, and in `/webhooks/{key}`.
* `boardsync.KeyedID(key, platform, id)` names a platform user under the
  entry's key: `slack:user:U0456` through key `slack-eu` becomes
  `slack-eu:user:U0456`. A user belongs to the connection, so renaming a key
  changes its users' IDs.

### External IDs and `ThreadRef`

`ThreadRef` names one conversation by parts. `Thread` is native and includes
what the platform needs to find it; an empty `Thread` is a channel target, not
a thread.

```go
type ThreadRef struct {
    Key       Key
    Platform  Platform
    Channel   string
    Thread    string
    Subthread string
}
```

| Platform | Channel | Thread | Sub-thread |
| --- | --- | --- | --- |
| Slack | `slack:C0123` | `slack:C0123:1727000000.123456` | none |
| GitHub | `github:acme/www:pulls` / `:issues` | `github:acme/www:123` (PR), `github:acme/www:issue:45` | `github:acme/www:123:rc:345` (review thread) |
| Linear | `linear:ENG` | `linear:ENG:<issueUUID>` | `linear:ENG:<issueUUID>:c:<commentID>` |
| Chat SDK | `key:<adapter channel>` | key-prefixed adapter thread | only when the adapter groups one |

A sub-thread always extends its thread's ID; the board refuses an import whose
sub-thread isn't inside its thread. A review thread is not a sync target:
syncing `github:acme/www:123:rc:345` syncs its PR, `github:acme/www:123`.

`ResolvedTarget` is a target a provider has read: key, platform, channel,
thread (empty for a channel), kind (`channel` or `thread`). Targets handed to
provider methods are native, without the key, except that the `Key` field
travels alongside for the adapter to check.

### Normalized events

A provider turns a platform payload into one or more `Record`s:

```go
type Record struct {
    Dedupe string          // occurrence identity; becomes the inbox ID
    Event  Event
    Raw    json.RawMessage // platform payload, bounded
}
```

`Event` is the normalized event a sync function reads:

| Field | Meaning |
| --- | --- |
| `ID` | Inbox event ID, stable across retries; the idempotency key for writes |
| `Key`, `Platform` | The entry and platform |
| `Kind` | `message`, `edited`, `deleted`, `reaction`, or `lifecycle` |
| `Type` | Narrow name: `message`, `comment`, `message.edited`, `reaction.added`, `pr.opened`, `ci.failed`, `issue.created`, … |
| `Channel`, `Thread`, `Subthread` | Native IDs; the key is on `Key`, not baked in |
| `Message`, `IsRoot`, `Text`, `Author`, `Context` | The message and its author; `Context` is `path:line` for a review comment |
| `Emoji`, `Added` | A reaction |
| `Files`, `Links`, `Mentioned` | Platform attachments, linked messages, and users the text mentions |
| `Lifecycle` | A lifecycle event: type, title, URL, body, labels, unset keys, owned prefixes, reply, create-root, retitled |
| `Raw` | The platform's payload, for debugging and root overrides |

`Event.Validate` enforces the invariants an import relies on: a valid key, a
known kind, channel and thread present, an author on messages and reactions, a
message ID where one is needed, a lifecycle type, and bounded text/raw sizes.
Adapters clamp text with `StorableText`, which removes NULs, repairs invalid
UTF-8, and cuts at a rune boundary.

### Files, links, mentions, and typing

Four side channels need explicit rules; none of them may lose a message.

**Inbound files.** A provider emits `File` metadata (name, type, size, URL)
and never bytes. `Call.Import` passes the entry's `FileFetcher` to the host.
The host keeps a file only when its URL is https, on one of
`SyncHostOptions.FileHosts`, without credentials or a literal IP, within
`MaxFiles` (clamped to the protocol's ten attachments per message), and within
`MaxFileBytes`. It gives each file a deterministic
22-character attachment ID derived from the inbox event, so a retry reuses the
bytes and record instead of writing a second copy. The adapter fetches its own
platform's URL (Slack's is Slack-domain only, with per-hop redirect checks and
a client timeout); the host adds its own `FileFetchTimeout` and a final bound
on the returned bytes. A successful fetch is written to the blob store and
bound to the board message's `Attachments`. A refused, timed-out, oversized, or
empty file keeps its metadata as a placeholder attachment and adds a note
(`📎 name (size): reason`) to the message text. A file whose URL is missing or
was stripped as unsafe keeps the placeholder and a note that its download URL
isn't available; a board with no fetcher keeps the metadata placeholder and
adds no fetch note. The message is always imported; no fetch failure is an
import failure.

**Outbound files.** `OutboundOptions.Files` is a `FileLoader`; the outbox
worker supplies the host's loader, which reads each attachment's bytes from the
blob store just before a post. A provider whose `Capabilities.UploadsFiles` is
false gets a note per attachment and no bytes: GitHub and Linear comments can't
carry files. A provider that can upload either takes the files in `Post`
(Slack posts text first, then the files into the same thread) or gets a second
`FileUploader.UploadFiles` call. A loader error is a board read failure and
degrades to a note on the message; it is never retried as a post, because the
text may already have landed. An upload failure is added to the already posted
text with a best-effort edit and the receipt records `Files: []`,
`FilesSet: true`; it never causes a retry that would duplicate the message.
The receipt's file IDs are stored on the copy before the `message.synced`
receipt, so a receipt retry preserves `Files`/`FilesSet`.

**Links.** A link keeps only a safe https URL on an allowed host; the board
never fetches one. A snapshot is stored only when the platform supplied it with
the link (Slack's unfurl), as JSON in `LinkView.Snapshot`; there is no link
fetch or snapshot crawl. An unsafe or disallowed URL becomes an `unavailable`
placeholder without a URL. Outbound links are ordinary Markdown in the message
text; no adapter resolves or unfurls them.

**Mentions.** A platform mention is normalized to the board's exact token
`@[<key>:user:<id>]`: Slack's `<@U…>`, GitHub's `@login`. Outbound, an adapter
converts only tokens under its own key to its native mention; another
connection's token is left literally as written, which notifies no one.
GitHub's `@org/team` stays text because it names a team, not a user; a login
inside a URL path, with a trailing hyphen, or in an email local part is not a
mention. Linear's API renders a mention from a profile URL and this adapter has
no workspace slug, so Linear posts tokens as text rather than fabricating an
`@name`.
Outbound conversions and GitHub's inbound conversion leave code spans and
fenced blocks as literal text; Slack's inbound mrkdwn converter is the older
whole-text path.

**Typing.** `Capabilities.Typing` plus the optional `Typist` interface carry an
ephemeral status. `Runner.Typing` validates the entry and thread and answers
`ErrUnsupported` when the platform can't show one. The outbox worker shows a
typing status before a thread post, ignores every failure, and remembers an
unsupported answer per key so it doesn't ask again. A platform with no typing
is skipped, never an error for the message.

### `Import`, `Deliver`, and `Start`

A `SyncFunc` is called once per event in both directions:

```go
type SyncFunc func(ctx context.Context, call *Call) error
```

`Call` carries either the inbound event or the outbound board event. It exposes
three mutating operations:

| Operation | Direction | Meaning |
| --- | --- | --- |
| `Import(channel, opts)` | in | Apply the default import. A channel places a new root; without one, only a thread the board already syncs receives the event. Returns root and message IDs. |
| `Deliver(opts)` | out | Send this event to `Call.Target`. A new root has no target yet, so Deliver sends nothing. Returns a receipt. |
| `Start(channel)` | out | Sync the message's thread with a platform channel, never the channel the message came from. Records `thread.synced`. |

Each operation runs at most once per call. A second call returns what the
first did; a call used after the function returns answers `ErrCallFinished`.
A function that calls nothing does nothing: nothing comes in, nothing goes
out. A returned error is retried (inbound on the thread's lane, outbound by
the outbox).

`ImportOptions` mirrors the upstream overrides: `Text`, `Labels`, `Unset`,
`NoSync`, and a synchronous `Root` function that shapes a root the import
creates. `NoSync` keeps an imported reply on the board; a new root always
syncs, so its thread can.

### Mapping vs handler

Most entries don't write a function by hand. `Mirror`, `Into`, `Render`, and
`Channels` compose:

| Helper | Meaning |
| --- | --- |
| `Mirror(channel)` | Import inbound events into `channel`; deliver outbound replies and changes; start a thread for a new root in `channel`. |
| `Into(channel)` | Import only. Used for GitHub: what's said elsewhere may not be public. |
| `Render(renderer, fn)` | Use `renderer` for every `Deliver` in `fn`. |
| `Channels(map[string]SyncFunc)` | Route each event by native platform channel. A new root is offered to every mapped channel's function with `Call.Channel` set. A thread synced outside the map follows both ways. Returns the entry's `RouteFunc`. |

A handler of your own gets every event the webhook keeps. Keep the helper
outermost if you want its route filtering; put custom code in the map values.

## Inbound: platform to board

### Webhook intake

`Provider.Receive(ctx, key, req)` must verify the request before parsing it.
`Registry.HandleWebhook` then:

1. Looks up the entry by key; unknown keys are 404.
2. Validates the request shape and calls the provider.
3. For each record, keeps it when:
   * `Gate.Routes(channel)` says the entry maps the channel, or
   * `Gate.ThreadSynced(externalThread)` says the board already syncs the
     thread.
   Everything else is dropped at the webhook, even in channels the app is in.
4. Pushes all kept records to the inbox in one atomic call, then returns the
   provider's response (a Slack challenge, an empty 200, …).

A gate that can't answer (a Slack name listing failed while the name could be
any channel) returns an error: the registry answers 500, so the platform
retries rather than the event being lost. An event outside the mapped set
whose thread isn't synced is dropped.

`WebhookRequest` is a bounded copy of the HTTP request: method, path, headers,
body (default cap 1–2 MiB per adapter), and remote address. Adapters never
read the network. `NewWebhookRequest(r, max)` builds one from an `*http.Request`.

### Dedupe

The inbox dedupes by `Record.Dedupe`, scoped by entry key. Identity must be:

* **Stable across redelivery** — the same platform event produces the same
  value.
* **Distinct for a later identical action** — remove, add, and add again are
  three occurrences.

| Provider | Message | Edit | Delete | Reaction | Lifecycle |
| --- | --- | --- | --- | --- | --- |
| Slack | `message:<key>:<thread>\|<ts>` | `edited:<key>:…\|hash(text)\|edited.ts` | `deleted:<key>:<thread>\|<ts>` | `reaction:<key>:…\|event_id` (or `event_ts`) | n/a |
| GitHub | `message:<key>:<lane>\|<comment id>` | `edited:<key>:…\|hash(body)\|updated_at` | `deleted:<key>:<lane>\|<comment id>` | n/a | `lifecycle:<key>:<delivery>\|<type>\|<repo#number>` |
| Linear | `message:<key>:<subthread>\|<comment id>` | `edited:<key>:…\|hash(body)\|updated_at` | `deleted:<key>:…` | ignored (no issue in payload) | `lifecycle:<key>:<delivery>\|<type>\|<identifier>` |
| Chat SDK | Adapter-provided | Adapter-provided | Adapter-provided | Adapter-provided occurrence ID | Adapter-provided |

`boardsync.Hash` is base64url SHA-256; long composites keep their prefix and
are replaced by a hash, so inbox IDs stay under 300 bytes. A reaction identity
must include the platform's occurrence ID or timestamp; there is no
current-time fallback, because two occurrences in byte-identical bodies would
otherwise look like one. Where a platform has no occurrence ID, the Chat SDK
bridge hashes the webhook body — the same body on a retry, a new body for each
occurrence.

**ETags are not event identity.** The webhook path is push-based, so there is
no ETag to compare. A future poller or source read may use
`If-None-Match`/`ETag` to avoid re-reading a platform resource, but an ETag
must still translate into a stable `Dedupe` value (or be ignored in favour of
the platform's event ID). Two webhook deliveries with the same ETag are not
one occurrence; two polls that return the same ETag are. Never use an ETag as
a substitute for the platform ID of a message or reaction.

### Markers

A marker is a hidden HTML comment a provider adds to a comment it posts, and
reads before importing one. It is the second line of defence against echo,
beside the platform's bot user ID: it survives a dashboard renaming the app,
and it names the board message that made the comment.

```html
<!-- keel-board:sync key=github message=812 thread=github:acme/www:123 -->
```

`boardsync.WithMarker`, `ParseMarker`, `StripMarker`, and `IsOwnMarker`
implement it. GitHub comments use markers. Slack and Linear do not: their text
is visible, and their bot identity is enough. Markers are not a substitute for
bot identity: an attacker can copy a marker into a comment. The adapter treats
a matching marker as fail-closed and drops the comment; losing a genuine
comment is preferable to echoing a copy, and bot identity remains the primary
check.

### Ordering, leases, and recovery

`Inbox.Lease` returns the oldest available event of each external thread (or
precise sub-thread), one at a time per lane. An event behind a leased or
backed-off one waits, so a thread's imports stay in order while other threads
proceed in parallel. The item is leased for a TTL; an ack must name the live
lease.

`Inbox.Ack` either removes the item (success) or schedules it again. A failure
raises the attempt count; after the retry policy's limit the item is
dead-lettered and the lane moves on. A crashed worker's lease expires and the
item is leased again; its old ack is refused with `ErrLeaseLost`. The memory
inbox in `sync/inbox.go` is the executable form of these rules; a deployment
replaces it with a durable store shared by workers.

The defaults:

| Setting | Default | Upstream analogue |
| --- | --- | --- |
| Inbound attempts | 10 | `local()` gives up after 10; Vercel's board after 5 |
| Retry delay | 1s doubling to 30s, 20% jitter | Same shape |
| Lease TTL | 1 minute | Platform retry windows are seconds to minutes |
| Outbound attempts | 5 | Retries on the thread's lane, then records `message.sync_failed` |

## Outbound: board to platforms

### Targets and echo suppression

Every board event is offered once per synced target, never back to where the
message came from. Two checks implement that:

* `Message.From` is the sync key an import came through; a target under that
  key is skipped even if it is another channel of the same connection.
* `Message.Origin.Thread` is the exact external thread; that target is skipped.

`TargetsForDelivery(message, targets)` applies both. The server must use it
before calling `Runner.Outbound`; `Call.Deliver` itself does not re-check,
because it is already bound to one target. Only the board's own posts are
skipped; other bots' messages (Sentry, Vercel alerts) sync like anyone's.

### Delivery and receipts

| Board event | What `Deliver` does |
| --- | --- |
| `message.posted` | Post the reply (or a new root for a channel target). |
| `message.edited` | Edit the copy. |
| `message.deleted` | Delete the copy, then its files if they were a separate message. |
| `reaction.added` / `reaction.removed` | Add or remove the reaction on the copy. |
| `labels.changed` | Edit the copy only when a renderer reads labels; the default renderer reads none, so it writes nothing. |
| `thread.synced` | A channel target posts the root (and, in the full engine, backfills replies in order); a thread target joins and receives only what's written after. |

On a successful post the runner records a receipt:

* `message.synced` with the platform thread and message IDs;
* `message.files_synced` with file IDs (or `null` when the platform refused
  files and the text carries a note instead);
* `message.sync_failed` with the error when the outbox gives up.

Edits, deletes, and reactions are best-effort: a platform that can't do one
skips it and logs why, and no receipt is written for them.

The full engine must also write the failure into the message's `undelivered`
targets so a reader sees `✗ didn't reach slack:C0123: not_in_channel` until a
later delivery succeeds. That projection belongs to the board log, not to this
package.

### Outbox

`Outbox` is a per-lane FIFO with retries. Deliveries on one board thread's lane
run in order; a job that fails all its tries is dead-lettered and the lane
moves on, so one bad message never blocks the next. `RetryPolicy` classifies:

* permanent: `ErrUnsupported`, `ErrUnauthorized`, `ErrBadRequest`, `ErrNotFound`,
  `ErrCallFinished`, or anything wrapped with `Permanent`;
* retryable: platform rate limits and transport/5xx failures.

A permanent failure is dead-lettered at once; a retryable one follows the
policy. The memory `Outbox` in `sync/retry.go` is the executable form; a
deployment backs the queue with the board log or a durable queue and calls
`Drain` from its consumer.

Slack's `chat.postMessage` has no idempotency key: a crash after Slack accepts
a post but before the receipt is recorded can post a duplicate on retry. That
is a documented limit, not something the outbox can fix. GitHub and Linear
accept a marker or an ID-bearing response, so their retries are safer.

### New roots

A board root, written by anyone or by a watcher, is offered to each platform
that can start threads (`Capabilities.StartThreads`) with `Call.Target == nil`
and `Call.Channel` set. The function may call `Start(channel)`; the runner
records `thread.synced` with cause `config`, and the backfill comes back
through the function as a normal `thread.synced` delivery. Slack and Linear
can start threads; GitHub cannot, so a root never opens an issue there.
`Start` refuses the channel the message came from.

A root posted with `NoSync` starts nothing, and an imported reply with
`NoSync` is kept on the board: no other platform gets it, now or in a
backfill.

## Credentials and untrusted input

### Secrets

`sync.Secret` wraps every credential:

* `%v`, `%s`, `%q`, `%#v`, JSON, and `log/slog` all show `[redacted]`;
* `Reveal()` is the explicit act that lets a token reach an HTTP call;
* `RedactError` scrubs a secret from a platform error before it is wrapped;
* `SecretFromEnv` reads a named variable and never prints its value.

Rules:

* One secret per entry. Never share a Slack bot token between keys; the users
  and targets belong to the key.
* Load secrets from the environment or a keel secret store, never from the
  board log, an event's `Raw`, a marker, or a URL.
* Do not log request headers, form bodies, GraphQL variables that contain a
  token, or a provider's raw error without redaction.
* Tests contain no real tokens; the `testToken` values in `_test.go` are
  literals for `httptest` servers and never leave the process.

Scope checklists, from the upstream platform pages:

| Platform | Needed |
| --- | --- |
| Slack | `channels:history`, `groups:history`, `channels:read`, `chat:write`, `reactions:read/write`, `users:read`, `files:read/write`, `groups:read` for private names, `chat:write.customize` for sender names, `assistant:write` for typing status, `channels:join` only if on-demand join is enabled |
| GitHub | Issues and Pull requests read/write, Actions read; webhooks for Pull requests, Pull request reviews, Pull request review comments, Issues, Issue comments, Workflow runs |
| Linear | `read`, `write`, `comments:create`, `issues:create`; webhooks for Issues and Comments; app actor |
| Chat SDK adapter | Whatever the platform's adapter documents |

### Webhook verification

| Provider | Header(s) | Scheme |
| --- | --- | --- |
| Slack | `X-Slack-Signature`, `X-Slack-Request-Timestamp` | `v0=` + hex HMAC-SHA256 of `v0:<timestamp>:<body>`; timestamp within 5 minutes |
| GitHub | `X-Hub-Signature-256` | `sha256=` + hex HMAC-SHA256 of the raw body; `sha1=` refused |
| Linear | `Linear-Signature` (or `X-Linear-Signature`) | hex HMAC-SHA256 of the raw body; optional `webhookTimestamp` within 5 minutes |
| Chat SDK | configurable, default `X-Webhook-Signature` | HMAC-SHA256, optional `sha256=` prefix, optional timestamp header |

All comparisons use `hmac.Equal`/constant time. Verification happens before
parsing, and a failure is `ErrUnauthorized` (HTTP 401). A missing secret is a
configuration error, not an empty credential that accepts everything.

### Other untrusted input

* **Body size** is capped before parsing (`WebhookRequest`, adapter
  `MaxBodyBytes`); a larger body is refused, not buffered.
* **Repository and team allowlists** decide which webhooks are read at all.
  GitHub requires `Repos` or `AllowAllRepos`; Linear requires `TeamKeys` or
  `AllowAllTeams`. An event from an unlisted repo/team is dropped before
  normalization, so it can't create a board thread.
* **Write allowlists** are separate. GitHub's `WriteRepos` is empty by default:
  a board never comments on a public PR unless an operator explicitly allows
  it. `AllowAllWrites` is the explicit opt-out. Linear writes follow its team
  allowlist; Slack writes follow the bot's channel membership.
* **Bot identity**: each adapter skips its own posts by user ID, bot ID, or
  login. Markers add a second check for GitHub.
* **URLs**: no adapter fetches a URL from a message. Link previews are data a
  platform supplies (Slack unfurls) or a platform API read with a token; a
  link's URL is never fetched on the board's behalf.
* **Markers** are sanitized: `-->`, whitespace, and overlong thread values
  cannot break the comment structure or inject a second comment. A marker is
  a fail-closed signal: a copied marker makes the import drop that comment,
  which is safer than importing a copy of our own post.
* **Actor IDs** are strings from a platform until a credential validates the
  webhook; the board must not treat `github:...` or `local:...` as
  authenticated just because a payload prefixes it. The host's import path
  rejects an author that doesn't belong to a sync key the caller owns.

## The board log seam

Sync's records live on the board's backend. Raw platform events live in the
inbox, apart from the board's events, so they never reach a tail. Board facts
sync writes are ordinary board log events.

`Host` is the service interface this package expects:

```go
type Host interface {
    Import(ctx context.Context, req ImportRequest) (ImportResult, error)
    Record(ctx context.Context, event BoardEvent) error
}
```

`ImportRequest` carries the tenant, entry key, the inbox event ID, the
destination board channel for a new root, the normalized event, and the
overrides. `Host.Import` must:

* derive its writes' IDs from `EventID` and the order of calls, so a retry
  that writes in the same order finds its own messages instead of posting
  twice;
* return the same root/message IDs when retried after success;
* append the import as its platform author, with key-prefixed `origin` IDs
  (`slack:C0123`, `slack:C0123:1727.9`), `from` (the sync key), and the default
  state labels;
* not import an event whose platform thread it doesn't sync when the sync
  function passed no channel.

`Host.Record` appends `thread.synced`, `message.synced`,
`message.files_synced`, or `message.sync_failed`. These receipts are what makes
`vb sync <id>`, `vb read <link>`, and `--source` filters work. In the full
service they also carry the principal that recorded them, so another
deployment's copy isn't changed by this one. A `BoardEvent` carries its
`Tenant`. `Host.Record` must be idempotent per `(message, target)`: a
`message.synced` may be retried after its copy was saved, and the retry must
not duplicate the fact or drop `Files`/`FilesSet`. The sync-server `Host`
implements this; `sync/fake.Host` records the calls for tests.

The subscription that feeds `Runner.Outbound` is a board consumer, like a
watcher: it follows the log (or its feed) and sees board events in order. It is
not SSE. SSE is for clients; sync is internal. A watcher is triggered by the
same board events; a label change that a watcher sees is also a delivery to
every synced target. Sync never writes to a platform from a webhook request
directly; it stores the import event, and the outbound consumer sees the
resulting board event.

Concrete sequence for a Slack reply:

```text
Slack event
  -> Provider.Receive: verify, normalize
  -> Registry.HandleWebhook: gate, inbox.Push
  -> 200
worker:
  -> Inbox.Lease
  -> Runner.Inbound -> Mirror("support").Import
  -> Host.Import: append message.posted {origin: slack:..., from: slack}
  -> Inbox.Ack
subscription:
  -> board message.posted -> TargetsForDelivery skips slack, keeps github PR
  -> Runner.Outbound -> SyncFunc.Deliver -> github.Provider.Post
  -> Host.Record: message.synced
```

Multi-process ownership: one process (deployment, worker) holds each key, so a
message is never delivered twice. The registry is keyed by `(tenant, key)`: two
tenants may both run `slack` with different providers and credentials. The full
service still needs a lease or registration for cross-process ownership, as
upstream's deployment ownership does.

### Server wiring in this repository

`server.SyncOptions` mounts sync on a board server:

* `Store` and `Registry` are the process's durable state and connections.
* `Tenant` resolves the board a webhook belongs to and **must** come from the
  deployment's authentication (a fixed tenant or a verified token), never from
  a header the platform can set.
* `Owned` lists the `(tenant, key)` connections whose workers and subscription
  this process runs. A key is owned by one process. The webhook handler answers
  200 for a key the tenant doesn't own or has disabled (before reading the
  body), 503 for an enabled key this process hasn't registered yet, and 500 for
  a platform mismatch; only the owning process should mount the route.
* `StartChannels` maps a `(tenant, key)` connection to the platform channels a
  new board root may start a thread in; the entry's own `Mirror`/`Channels`
  route still decides.
* `FileHosts` is the inbound file/link host allowlist. Empty refuses every URL;
  a listed host is fetched only through the adapter's `FileFetcher`, with the
  host's size, count, literal-IP, and timeout checks. `--sync-file-host` sets
  it from the command line.

Routes:

| Route | Purpose |
| --- | --- |
| `POST /webhooks/{key}` | `syncstore.WebhookHandler`: tenant, entry, gate, store-before-ack |
| `GET /v1/board/sync/entries` | connections, enabled/owned, no secrets |
| `GET /v1/board/sync/entries/{key}/status` | one connection's status view: enabled/owned, inbox depth, tenant outbox depth |
| `GET /v1/board/sync/status` | per-key inbox depth and the tenant's outbox depth |
| `GET /v1/board/sync/inbox?key=` | dead-lettered inbound events (IDs, thread, attempts) |
| `GET /v1/board/sync/outbox/failures?key=` | dead deliveries (message, target, attempts, error class) |
| `POST /v1/board/sync/inbox/{id}/retry?key=` | retry a dead inbound event |
| `POST /v1/board/sync/outbox/{id}/retry` | retry a dead delivery |
| `GET /v1/board/sync/threads?thread=` | a board thread's attachment rows |
| `POST /v1/board/sync/threads` | attach `{key, thread, target}`; a channel target posts the root and backfills, a thread target receives only future messages |
| `GET /v1/board/sources/{key}/read?target=&limit=&after=` | on-demand platform read (`sync.SourceReader`), normalized and not stored; `limit` defaults to 50 and caps at 200 |
| `POST /v1/board/sources/{key}/join` | opt-in platform join (`sync.SourceJoiner`), body `{target}` |

Every management route needs `manage` at `*`; the attach route also needs
`read` on the board thread's channel. Another tenant's thread, a missing one,
and one the caller can't read all answer 404. Management views carry no
platform credential (the store holds none), no message text and no raw
payload. A source read answers `sync.SourceRead` and writes nothing: no board
event, no projection row. A provider that doesn't implement `SourceReader` (or
whose join isn't configured) answers a stable 501 `unsupported`; an unknown
key is 404 whether or not it is well formed.

The subscription follows each owned key's durable `board-events` cursor,
delivers only `message.posted/edited/deleted`, `reaction.added/removed`, and
`labels.changed`, and skips every receipt (`thread.synced`,
`message.synced`, `message.files_synced`, `message.sync_failed`). A first start
begins at the current head, not at seq 1: a deployment does not dump a board's
history into platforms. A channel target's root delivery and its reply
backfill are enqueued from `Host.Record` (the receipt is the trigger), so a
restart resumes both through the durable outbox.

Thread identity and future-only attachment:

* Root creation is serialized per `(tenant, key, external thread)` with a
  store target lock, and its log idempotency key is the thread, not the inbox
  event. Two racing root deliveries produce one root; the loser imports as a
  reply. The mapping is written in the same locked window.
* `board_sync_threads` has a partial unique index over
  `(tenant, key, platform, channel, thread, subthread)` for `kind='thread'`:
  one external thread maps to one board thread. A channel target may repeat,
  because several board roots can start threads in one channel.
* A thread mapping stores `since_seq`, the board sequence at attach. Delivery
  skips messages at or before it, so attaching a thread is future-only even
  when the subscription replays from an older cursor or a process restarts.
  Backfill runs only for a root post that came from a channel target.
* An attach writes its mapping before `thread.synced`: a refused or conflicting
  attach leaves no receipt behind. A duplicate attach is idempotent.

Best-effort changes: an edit, delete, or reaction is not a message failure.
A missing copy is a skip for edits and labels, and a short retry for reactions
(the copy may still be landing). A dead change writes no
`message.sync_failed`/`undelivered`; a successful receipt clears an earlier
failure for the same target. A delete propagates to every copy of the board
message and removes those copy rows as it goes.

Each connection must also recognize its own platform posts. Slack and Linear
without a bot identity are refused at `board serve` startup; adapters expose
`SelfIDs` and the registry drops a matching author before the inbox, beside
each adapter's own check. The generic Chat SDK bridge follows the same rule:
`chatsdk.New` and `chatsdk.NewRemoteAdapter` require self author IDs and fail
closed without them. There is no marker fallback in bridge v1.

The subscription is supervised per key: a board/store error is logged with
`tenant` and `key` and the consumer restarts with exponential backoff
(1s to 30s), so a transient store failure cannot silently end sync.

`cmd/board` enables sync only with an explicit `--sync-key key=platform`,
`--sync-mirror key=platformChannel:boardChannel`, and (optionally)
`--sync-start`. Credentials come from `KEEL_SYNC_<KEY>_*` environment
variables. One process must own each key; a second process serving the same
DSN must not be given the key.

## Adapters

### Slack

Config:

```go
slack.New(slack.Config{
    Key: "slack",
    SigningSecret: secret, // required
    BotToken: token,       // required for API calls
    BotUserID: "UAPP123",  // skip the app's own messages
    Now: time.Now, Tolerance: 5*time.Minute,
})
```

IDs: channel `slack:C0123`; thread `slack:C0123:1727.9`; no sub-thread.

Webhook: `url_verification` returns the challenge; `event_callback` handles
`message` (plain, `file_share`, `thread_broadcast`, `bot_message`), `message_changed`,
`message_deleted`, `reaction_added`, and `reaction_removed`. `app_mention` is
ignored because `message.channels` already carries the message. Subtypes such
as joins, leaves, and topic changes are ignored.

Mapping:

| Event | Kind | Thread |
| --- | --- | --- |
| message with no `thread_ts` | message, `IsRoot` | `channel:ts` |
| reply | message | `channel:thread_ts` |
| `message_changed` | edited | `message.thread_ts` or `message.ts` |
| `message_deleted` | deleted | `previous_message.thread_ts` or deleted ts |
| reaction | reaction | `channel:item.ts` (limitation below) |

Text conversion is deliberately small: HTML escapes, `<@U…>` →
`@[slack:user:U…]`, `<#C…|name>` → `#name`, `<url|text>` → `[text](url)`,
`<!channel>` → `@channel`. Slack's `*bold*`, `_italic_`, and `~strike~` are
left as written; a full mrkdwn converter belongs in the adapter, not the
board.

Outbound: `chat.postMessage` with `channel`, optional `thread_ts`, `text`,
`markdown_text`, `username`/`icon_url` only for a verified sender, and unfurls
off. A channel target starts a root; a thread target replies. `chat.update`,
`chat.delete`, and `reactions.add/remove` cover changes. `#name` resolves
through one cached `conversations.list`; IDs never wait. `Locate` reads
permalinks; `Permalink` calls `chat.getPermalink`.

Known limits of the adapter:

* **Reaction thread**: Slack reaction webhooks don't carry `thread_ts`, so the
  native thread is `channel:item.ts`. A reaction on a reply needs the host to
  find the copy by channel + platform message ID, or a richer adapter that
  caches the message's thread. Upstream gets it from the Chat SDK message.
* Vercel deployment tables are not implemented; the sender name, scope check,
  and file upload paths are.
* A message with no text and no files is dropped. A file-only message posts
  its text first (the attachment notes when there is no text) and then the
  files into the same thread.

### GitHub

Config:

```go
github.New(github.Config{
    Key: "github",
    WebhookSecret: secret, // required
    Token: token,          // installation token
    BotUserID: 4242,
    BotLogin: "keel-board[bot]",
    InstallationID: 777,   // optional: drop events from any other installation
    DetectViewer: true,    // user token only; App tokens use BotUserID
    Repos: []string{"acme/www"},          // required inbound allowlist
    WriteRepos: []string{"acme/www"},    // empty means read-only
    // AllowAllRepos / AllowAllWrites are explicit opt-outs.
})
```

IDs: channels `github:acme/www:pulls` and `:issues`; PR `github:acme/www:123`;
issue `github:acme/www:issue:45`; review thread
`github:acme/www:123:rc:345` grouped under its PR.

Webhook: `X-GitHub-Event` + `X-GitHub-Delivery` + `X-Hub-Signature-256`.
The delivery ID is required and bounded; it is the replay identity. GitHub
sends no timestamp, so there is no timestamp window to check: a duplicate
delivery produces the same dedupe and the inbox keeps one event. `ping` is
acknowledged with no records. An unlisted or malformed repo, a missing
installation when `InstallationID` is set, and an installation that isn't the
entry's are dropped before normalization: fail closed. Handled events:

| Event | Mapping |
| --- | --- |
| `issue_comment` created/edited/deleted | message/edited/deleted reply; PR conversation or issue thread |
| `pull_request_review_comment` | reply in the PR thread, sub-thread `…:rc:<in_reply_to or comment id>`, `Context` `path:line` |
| `pull_request` opened | lifecycle `pr.opened`, labels `pr=owner/repo#n`, `state=open\|draft`, `head=<sha>` |
| `pull_request` edited | `pr.edited`, `Retitled` |
| `pull_request` ready_for_review / converted_to_draft | `pr.ready` / `pr.draft`, create root if unseen |
| `pull_request` reopened | `pr.reopened`, reply "reopened by …", create root if unseen |
| `pull_request` synchronize | `pr.synchronized`, new `head` (an invalid or absent SHA unsets the old head), owns `ci.<repo id>-<n>.` so stale CI labels unset |
| `pull_request` closed | reply "merged by …" or "closed by …", `state=merged\|closed` |
| `pull_request` labeled/unlabeled/assigned/unassigned | reconcile `tag.*` / `assignee.*`, no reply |
| `issues` opened/edited/closed/reopened | lifecycle `issue.*` with `state`, `issue=owner/repo#n`, `tag.*`, `assignee.*` |
| `issues` labeled/unlabeled/assigned/unassigned on a PR | reconcile `tag.*` / `assignee.*` on the PR thread; the `pull_request` event owns state and replies |
| `pull_request_review` submitted approved/changes_requested | reply "approved by …" / "changes requested by …", label `review` |
| `pull_request_review` dismissed | unset `review` |
| `workflow_run` completed success/failure | `ci.passed` unsets only its exact key, `ci.failed` replies from `github-actions[bot]` and sets only `ci.<repo id>-<n>.<workflow file>=<conclusion>` |

A `workflow_run` owns no label prefix. One workflow finishing must never unset
another workflow's failure; only `pr.synchronize` owns the whole
`ci.<repo id>-<n>.` prefix and clears it when the head changes.

Tag and assignee keys are keyed by name, not platform: `good first issue` →
`tag.good-first-issue=good first issue`, `grace` →
`assignee.grace=grace`, lowercased, runs of other characters turned into `-`,
and the key is bounded to 64 characters. A CI key longer than 64 characters keeps its owned prefix and
ends in a short hash, so two workflow names can't collide. Every PR/issue
event carries the full label and assignee arrays in GitHub's payload, so the
mirror reconciles what it owns: `state`, `review`, `tag.*`, `assignee.*`. A
label change that changes nothing writes nothing. Board labels never sync out.

A PR's `issues` view is deliberately not a second lifecycle source: GitHub
sends both events, and only `pull_request` knows whether a close was a merge.
The issue view is used for label and assignee reconciliation on the PR thread.

Outbound: issue comments and (with `ThreadRef.Subthread`) review-thread
replies. `Post` appends a marker and bounds the body to GitHub's 65,536
characters without cutting the marker; files are refused rather than silently
dropped. A successful post's receipt keeps the sub-thread, so a host can build
the copy target for edits, deletes, and reactions from `message.synced`.
`Edit`/`Delete` choose the issue-comment or review-comment endpoint from the
sub-thread; a missing review comment is idempotent, while a missing comment on
a bare thread is an error (the endpoint is ambiguous if a receipt lost its
sub-thread). `React` maps a small emoji table to GitHub's eight reactions.
Roots never flow into GitHub: GitHub threads start on GitHub. `FetchRoot`
recovers a PR/issue when a comment arrives on a thread the board hasn't seen.
`Viewer` reads the token's own account through `GET /user`; with
`DetectViewer`, its comments are skipped even without `BotUserID` (a failed or
empty lookup is retried at most once a minute). `GET /user` needs a user
token: a GitHub App installation token cannot call it, so `DetectViewer` with
an installation token is rejected at configuration time. App entries use
`BotUserID`/`BotLogin` instead.

Echo suppression fails closed. With `DisableMarker` set, an entry that has
neither `BotUserID`/`BotLogin` nor a resolvable `Viewer` refuses `Post`,
`Edit`, `Delete`, and `React` instead of writing a comment it cannot recognise
on the way back. Marker detection scans every marker in a comment, so a foreign
marker placed before ours cannot hide ours.

Rate-limit errors expose `RateLimitError.RetryAfter` (from `Retry-After` or
`X-RateLimit-Reset`). The in-memory outbox still uses its generic backoff; a
deployment that runs its own retry loop should honour that field or treat
`ErrRateLimited` as retryable.

Known limits:

* Reaction removal needs a reaction ID in the API; only adding is implemented.
* `check_run`/`check_suite` and `pull_request_review` body edits are not
  normalized; CI status comes from `workflow_run`. A `workflow_run` whose
  payload lists no pull request produces no record.
* `Locate`/`URL` cover PR, issue, and comment links; review comments are read
  from `#discussion_r…` and the files-view `#r…` anchor, and a review thread is
  never a target. Other review-thread URL shapes (for example a
  `#pullrequestreview-…` link, which names a review, not a comment) are open.
* Resolving review conversations is not synced.
* File upload is not implemented: GitHub comments can't carry files, so a
  board message with attachments delivers a note instead, and inbound GitHub
  events carry no files. Deployment-specific formatting is pending. Bot identity
  can be configured (`BotUserID`/`BotLogin`) or detected from a user token
  (`Viewer`); a GitHub App installation token must configure the IDs. When
  neither is known, the hidden marker remains the echo guard.

### Linear

Config:

```go
linear.New(linear.Config{
    Key: "linear",
    WebhookSecret: secret, // required
    APIToken: token,       // GraphQL
    TeamKeys: []string{"ENG"}, // required inbound allowlist
    BotUserID: "app-user-id",
    // AllowAllTeams is the explicit opt-out.
})
```

IDs: team channel `linear:ENG`; issue thread `linear:ENG:<issueUUID>`;
comment sub-thread `linear:ENG:<issueUUID>:c:<commentID>`. The UUID stays the
same when the issue is renamed. A target may name an issue by identifier
(`ENG-123`), UUID, or native thread.

Webhook: `Linear-Signature` hex HMAC-SHA256; `Linear-Delivery` is the dedupe
delivery ID; `webhookTimestamp` (ms) is checked against a 5-minute tolerance.

| Event | Mapping |
| --- | --- |
| `Issue` create | lifecycle `issue.created`, title `ENG-123: <title>`, URL, description, labels `issue`, `state`, `priority`, `project`, `tag.*`, `assignee.*`; create root |
| `Issue` update | `issue.updated`; state/property labels reconcile; a changed title/description sets `Retitled`; a person's edit may create the root (automations don't); archiving/removal doesn't |
| `Comment` create/update/remove | message/edited/deleted reply in the issue thread, sub-thread `:c:<comment id>` |
| `Reaction` | ignored: Linear's reaction webhook doesn't name the issue |

State is the workflow state's type: `triage`, `backlog`, `unstarted`,
`started`, `completed`, `canceled`, whatever the team calls its states. An
issue that moves to another team starts a new thread in that team's channel;
the team is part of the ID.

Outbound: a thread target creates a comment; a channel target opens an issue,
with the first line as title and the whole message as description. `Edit`,
`Delete`, and `React` operate on comments; Linear's API can't take a reaction
back, so a removal is unsupported. `FetchRoot` reads an issue the board hasn't
seen.

Known limits of the adapter:

* A comment payload without an issue object is refused: the adapter needs the
  team and identifier to place it. The full adapter fetches the issue by ID.
* Links (`Locate`/`URL`) are not implemented; the workspace slug is not part
  of the native IDs this adapter stores.
* File upload is not implemented: Linear comments can't carry files, so a
  board message with attachments delivers a note instead. Board mentions stay
  literal text too: Linear renders a mention from a profile URL and this
  adapter has no workspace slug. Issue move handling beyond a new thread and
  automated re-parenting are pending.

### Chat SDK bridge

`chatsdk` is for a platform without a native adapter. It defines an `Adapter`
interface with four methods — `Platform`, `Verify`, `Decode`, `Send` — and
optional `Editor`, `Deleter`, `Reactor`, `ChannelResolver`, `TargetResolver`,
`RootFetcher`, `Linker`, `FileFetcher`, and `Permalinker` interfaces.

```go
bridge, _ := chatsdk.New(chatsdk.Config{
    Key: "telegram",
    Adapter: myAdapter{...},
    SelfIDs: []string{"telegram:user:77"},
    Caps:        boardsync.Capabilities{StartThreads: false},
})
```

The bridge verifies, decodes, pins the key and platform on each record, and
delegates sends and optional changes. `HMACVerifier` is a ready generic
verifier (header, optional `sha256=` prefix, optional timestamp). Without a
`TargetResolver`, a target is refused rather than guessed: an adapter that
can't say whether an ID is a channel or a thread must not start a thread by
accident. `FuncAdapter` is a function-based adapter for tests and small
platforms.

This package does **not** wrap the JavaScript Chat SDK or create a `Chat`
instance. A Go platform client implements `Adapter` directly; that is the
whole point of the bridge.

## Testing

Tests use `net/http/httptest` and embedded fixtures; they make no real network
calls and need no credentials.

| Area | What is covered |
| --- | --- |
| `sync/*_test.go` | Event/ThreadRef validation, dedupe keys, markers, secret redaction, retry policy, outbox lane order/retries/dead letters, inbox dedupe/order/lease/retry/dead letters, `Runner.Inbound` once semantics, `Runner.Outbound` receipts, mirror start, origin refusal, registry gate/drop/failure statuses |
| `sync/slack` | Signature vector, stale timestamp, challenge, message/reply/edit/delete/reaction/file/link fixtures, own-bot and marker skip, stable and distinct dedupe, store-before-ack, target parsing, `#name` lookup and cache, `chat.postMessage` form and customize fallback, edit/delete/react paths, file retry/refusal/partial success, typing status, source read/join, permalink, API error and Retry-After classification, fake Slack end-to-end |
| `sync/github` | Signature vector, delivery ID and duplicate-delivery dedupe, ping, issue/PR/review comments, PR/issue full lifecycle, review approval/dismissal, CI pass/fail, repo/installation allowlists, marker and bot/viewer echo skip, target parsing, comment/review-reply posting, edit/delete/react paths, comment length and file limits, error classification, root recovery, links, fake-server end-to-end |
| `sync/linear` | Signature and timestamp, issue created/updated, comment created/updated/removed (real `issueId`-only payloads), team allowlist, own comment skip, identifier/native target resolution, comment/issue creation, edit/delete/react, labels/state/priority/project/assignee, source read (team/project/issue), permalinks, errors/limits, fake GraphQL end-to-end |
| `sync/chatsdk` | Bridge v1 schema/wire, HMAC/timestamp/nonce replay, self identity, body/event caps, NUL/ID bounds, resolve/deliver retry, sidecar fixture, PG bridge intake, provider conformance |
| `sync/conformance` | Shared provider suite for Slack, GitHub, Linear and a bridge-backed adapter |

Run:

```bash
go test ./...
go vet ./...
```

What the tests prove is semantics: verification before parsing, stable
identity, per-lane order, store-before-ack, retries and dead letters, and no
echo back to the origin. They do not prove a platform's current API, a real
webhook's exact schema in all actions, file transfer, typing, permissions, or
the full backfill. Those need integration tests against a sandbox account and
recorded fixtures.

## Integrating with keel

Recommended shape, matching the repository layout:

1. **Package.** Keep `sync/` independent of `log/`, `server/`, and `store/`.
   The server imports `sync` and implements `Host`; adapters import only
   `sync`. No adapter imports another adapter.
2. **Routes.** `POST /webhooks/{key}` → resolve the tenant → build a
   `WebhookRequest` with a body cap → `Registry.HandleWebhook(ctx, tenant, key,
   req, gate, inbox)` → write the returned `WebhookResponse`. Add `GET` only
   where a platform requires it. `syncstore.WebhookHandler` implements the
   tenant/entry checks and store-before-ack path.
   The handler answers a silent 200 without reading the body for a key the
   tenant doesn't own or has disabled; 503 for an owned, enabled entry this
   process hasn't registered (platform retries); and 500 for an entry/provider
   platform mismatch.
3. **Gate.** `Routes` first consults the entry's own `RouteFunc` (from
   `Channels` / `Repos`-style mappings); if the entry has none, it asks the
   tenant/key-scoped gate. `ThreadSynced` asks a gate built for the same
   `(tenant, key)` whether the external thread is attached; `syncstore.StoreGate`
   answers from `board_sync_threads`. A name that can't be resolved returns an
   error, not false. A nil gate is fail-closed: only an entry `Routes` match keeps
   a record. Do not share one static gate across tenants; build it per request
   through `WebhookConfig.GateFactory`.
4. **Inbox.** Start with `MemoryInbox`, which is single-tenant and for
   development only; use `syncstore.Store`'s tenant-scoped inbox for a
   deployment. Persist raw events in the same transaction as the dedupe insert.
5. **Workers.** One consumer per key (or a lease per lane) calls
   `Inbox.Lease`, then `Runner.Inbound`, then `Ack` with the error. The
   `Event.ID` is the idempotency key for `Host.Import`.
6. **Outbound.** Subscribe to board events. For each event, load the thread's
   targets, call `TargetsForDelivery`, and call `Store.EnqueueOutbox`; it
   creates one deterministic job per target on the board thread's lane.
   `OutboundWorker` drains it and calls `Runner.OutboundWithOptions` with a
   `CopySink` that persists the copy after the provider succeeds and before
   `Host.Record`. A receipt retry therefore finds the copy and doesn't post
   again; it rebuilds the copy target from the stored copy, including a review
   sub-thread. A `SaveCopy` failure still leaves the narrow provider-success-to-
   copy-commit window in which a retry can duplicate a platform post; a host
   that needs exactness must make its platform call idempotent or persist the
   copy in the same durable step. Record receipts and failures through
   `Host.Record`; the message projection shows `undelivered`.
7. **Secrets.** Per entry, from the environment or keel's secret store:
   `KEEL_SYNC_<KEY>_WEBHOOK_SECRET`, `KEEL_SYNC_<KEY>_TOKEN`, plus non-secret
   config (`repos`, `teams`, `botUserID`, base URLs). Never place a token in a
   board label, event, marker, log field, or error.
8. **Ownership.** One server process holds each key. A second process answers
   that key's webhook with 200 before reading the body and runs no sync there;
   it keeps running watchers. This is the upstream deployment-ownership rule
   and prevents double delivery.
9. **Watchers.** Watchers are independent consumers of the board log; a
   watcher writes ordinary messages, which then sync. Do not call a provider
   from a watcher directly. A watcher's own writes never wake it, but a
   scheduled message does; that belongs to the server's watcher layer.
10. **Observability.** Log the entry key, event ID, external thread, attempt,
    and a redacted error code. Never log a raw token or an unredacted platform
    error. Expose inbox depth, dead letters, delivery failures, and per-key
    ownership as metrics.

A suggested rollout:

* Phase 1: Slack mirror on a test workspace and a Linear read-only import,
  using the memory inbox and a single worker.
* Phase 2: durable inbox/outbox and GitHub read-only import with the repo
  allowlist; add markers and write allowlists after the read path is trusted.
* Phase 3: outbound GitHub/Linear writes, backfill, files, typing, links, and
  per-key ownership.

## Deliberate differences from upstream

Recorded in `docs/PORTING.md` as well:

* **Go interfaces, not TypeScript classes.** `Provider`, `Call`, `Host`, and
  `Runner` replace `SyncProvider`, the sync function's `Sync`, and the engine.
  Semantics are kept; there is no JavaScript Chat SDK runtime.
* **No Vercel Connect, OIDC, or deployment identity.** Credentials are
  adapter-owned secrets; webhook verification is the platform's own scheme.
  Keel's auth and key ownership replace Vercel's project/team model.
* **No `sync-slack` / `sync-github` / `sync-linear` source directories.** The
  upstream pin puts every adapter in `src/sync/`; this repository uses Go
  packages `sync/slack`, `sync/github`, `sync/linear`, `sync/chatsdk`.
* **GitHub writes are denied by default.** Upstream's `into` imports without
  writing; this adapter also requires `WriteRepos` for any outbound comment.
  That is stricter than a configured `mirror` upstream, where a function can
  deliver.
* **GitHub markers are added.** Upstream relies on the app's bot user ID; the
  marker is an additional, renaming-resistant echo guard. A deployment can
  disable it with `DisableMarker`.
* **Slack reaction threads.** Upstream's Chat SDK message carries the parent
  thread; the raw Slack webhook doesn't, so the adapter anchors a reaction at
  `channel:item.ts` and leaves parent lookup to the host.
* **The engine is a seam plus a durable store.** The memory inbox/outbox,
  `Runner`, and tenant-qualified `Registry` are executable; `syncstore` adds
  the durable PostgreSQL/memory inbox and outbox, copies, workers, webhook
  handler and RLS. Subscription, backfill, file bytes (through the server
  host's blob store), typing, and cross-process key ownership are implemented
  by the server host and `cmd/board`; the remaining production gap is a
  cross-process lease for key ownership.
* **The Chat SDK bridge is a Go interface.** `chatsdk.Adapter` is a Go
  contract; it does not load `chat` or call a JavaScript adapter. A platform
  client implements it directly.
* **Identity re-keying is explicit.** Adapters emit platform-native author
  IDs; `KeyedID` names them under the entry key. Upstream's engine does this
  automatically for every inbound event; a Go host must call it (or the
  equivalent) when it imports.

## Open items

* SQLite durable `Inbox`/`Outbox` stores. PostgreSQL and memory are in
  `syncstore` with the same lease/lane semantics as `MemoryInbox`/`Outbox`.
* File bytes: Slack uploads both ways. GitHub and Linear have no upload path,
  so their outbound attachments become notes; their inbound webhooks carry no
  files. A platform without a `FileFetcher` keeps metadata placeholders, and an
  inbound URL outside `FileHosts` is never fetched. The blob store holds bytes
  under tenant- and attachment-ID keys; there is no garbage collection for
  bytes written before an import's board write failed.
* Typing is Slack's assistant thread status. Other platforms skip it
  (`ErrUnsupported`) and the worker remembers the refusal per key. Link
  snapshots come only from a platform's payload (Slack unfurl); the board never
  crawls a link.
* Cross-process key ownership: `Owned` is explicit configuration, not a lease.
  A second process must not be configured with the same key.
* The PostgreSQL restart test replaces process-local stores, board caches,
  servers, and providers over the same schema; it does not fork an OS process.
* Platform root recovery: a reply on a thread the board hasn't mapped is a
  new root built from the reply when an import channel is given, rather than a
  `RootFetcher` read of the platform's real root. `FetchRoot` is not called.
* Chat SDK identity: the bridge has no implicit bot identity. A sidecar must
  name its own author IDs (`SelfIDs`/`KEEL_SYNC_<KEY>_SELF_IDS`), and provider
  construction fails closed without them. There is no marker fallback in v1; a
  platform that strips HTML comments still works because identity, not a
  marker, is the echo guard.
* Full lifecycle reconciliation: labels and a reply are imported, and a root
  can be created, but `Owns` prefixes, review conversations, and issue moves
  are not reconciled.
* Slack reaction parent-thread lookup: the adapter anchors a reaction at
  `channel:item.ts`; `Host.Import` falls back to the single copy for that
  external message, but a message copied to several targets is ambiguous.
* Management `clear`: retrying a dead letter is implemented; clearing its
  history is not. `syncedHere` is not projected.
* Full Slack mrkdwn ↔ Markdown conversion and sender/avatar/scope checks.
* GitHub reaction removal (needs a reaction ID lookup), review-thread
  conversation resolution, and review-comment URLs from the board.
* Linear issue re-parenting after an issue move, workspace links, and reaction
  removal.
* Permissions: sync writes run as the entry's bot/actor; the embedding server
  must configure its `Authorizer` for channel access, and management/source
  routes require `manage` at `*`. The board binary's development server allows
  everything and is not a deployment.
* Webhook replay tests for every adapter action and recorded real fixtures
  from sandbox accounts.
