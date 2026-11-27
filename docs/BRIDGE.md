# Chat SDK bridge contract, version 1

The bridge lets a platform without a native adapter mirror a board thread
without loading a Node runtime into the board. The deployment runs its own
Chat SDK adapter sidecar; the board and the sidecar exchange normalized events
and delivery requests over signed HTTP.

```text
platform ──webhook──▶ sidecar ──POST /v1/board/sync/bridge/{key}/events──▶ board
board ──POST {bridgeURL}/deliver──▶ sidecar ──Chat SDK adapter──▶ platform
board ──POST {bridgeURL}/resolve──▶ sidecar
```

The contract is versioned: `v1`. `sync/chatsdk` implements both directions;
`docs/bridge-v1.schema.json` is the machine-readable shape. The board never
trusts a URL named by an event or a platform payload. The sidecar's base URL
is deployment configuration, and it must use HTTPS unless it is a loopback
test server.

## Authentication

Every request in either direction carries the same three headers:

| Header | Value |
| --- | --- |
| `X-Keel-Board-Timestamp` | Unix seconds at signing time |
| `X-Keel-Board-Nonce` | 8–128 printable ASCII bytes, unique per entry in the window |
| `X-Keel-Board-Signature` | `v1=` + lowercase hex HMAC-SHA256 |

The signed bytes are exactly:

```text
HMAC-SHA256(secret, timestamp_decimal + nonce + body)
```

- The timestamp must be within ±5 minutes of the receiver's clock. A request
  outside that window is `401`, even when its signature is otherwise valid.
- A nonce is single-use inside the window. The board stores
  `(tenant, entry key, nonce)` durably in `board_sync_bridge_nonces` before
  the events reach the inbox. A sidecar keeps the same rule for deliveries it
  accepts.
- **Retries need a fresh nonce and timestamp.** The delivery ID, not the
  nonce, is the idempotency key; a sidecar must answer a repeated
  `deliveryId` idempotently rather than re-posting.
- The secret is per sync entry. Tenant A's secret never verifies tenant B's
  entry, and the board never sends a platform token to the sidecar. Logs,
  responses and errors never carry the secret or the request body.
- `docs/bridge-v1.schema.json` is version 1. A future contract version uses a
  different signature prefix and is refused by this implementation.

## Limits

Both directions enforce the same bounds before parsing:

| Limit | Value |
| --- | --- |
| HTTP body | 1 MiB (`MaxBodyBytes`) |
| Events per inbound batch | 100 (`MaxEvents`) |
| Sidecar response body | 64 KiB (`MaxResponseBytes`) |
| Dedupe / delivery ID | 300 bytes |
| Normalized text | 256 KiB, clamped at a rune boundary |
| Files | Not in v1; a file-bearing outbound message is `unsupported` |

The board cleans inbound text the same way native adapters do: NULs are
removed, invalid UTF-8 becomes U+FFFD, and text over the limit is cut at a
rune boundary. A batch is all-or-nothing: one malformed event rejects the
batch and stores nothing.

## Inbound: sidecar to board

The sidecar verifies the platform's own webhook signature first, then pushes
normalized events. The board path is:

```text
POST /v1/board/sync/bridge/{key}/events
Content-Type: application/json
X-Keel-Board-Timestamp: 1727000400
X-Keel-Board-Nonce: 6b1f0a9f4d2c4e6f8a0b1c2d3e4f5a6b
X-Keel-Board-Signature: v1=...
```

```json
{
  "version": "1",
  "events": [
    {
      "dedupe": "telegram:update:991",
      "event": {
        "kind": "message",
        "type": "message",
        "channel": "-1001234567890",
        "thread": "-1001234567890:42",
        "message": "42",
        "isRoot": true,
        "text": "ship it",
        "author": { "id": "telegram:user:7", "name": "Ada" },
        "platform": "telegram",
        "key": "telegram"
      }
    }
  ]
}
```

- `dedupe` is the occurrence identity. It becomes the inbox ID and the
  board's idempotency key; a platform redelivery keeps the same value, a
  later identical action gets a new one.
- `event.key` and `event.platform` are optional. When present they must match
  the entry; the board fills them when absent. The path key always wins for
  tenant/key isolation.
- The event shape is `sync.Event`: `kind` is one of `message`, `edited`,
  `deleted`, `reaction`, `lifecycle`; the narrow `type` is the platform's
  name for it. A reaction needs `message` and `emoji`; a lifecycle needs
  `lifecycle.type`, an author, and a title/body/label.
- The board answers `{"stored": n}` after the inbox commit. It never answers
  2xx before storing: a store failure is `500` and the sidecar retries.
- A disabled entry answers `200` without reading the body, so a sidecar can
  stop retrying a switched-off connection. An unknown key is `404`.
- Replaying a nonce in the window is `409`; a bad signature, timestamp or
  missing header is `401`; an oversized body is `413`; a malformed batch is
  `400`.

## Outbound: board to sidecar

The board posts to `{bridgeURL}/deliver` with the same HMAC headers.

```json
{
  "deliveryId": "delivery:telegram:812:-1001234567890:42:message",
  "key": "telegram",
  "target": {
    "key": "telegram",
    "platform": "telegram",
    "channel": "-1001234567890",
    "thread": "-1001234567890:42",
    "kind": "thread"
  },
  "kind": "message",
  "body": {
    "markdown": "**Ada**: ship it",
    "sender": { "name": "Ada", "attribution": "" },
    "boardMessage": 812,
    "boardThread": 812
  }
}
```

`kind` is `message`, `edit`, `delete`, or `reaction`. `message` names the
native message for edit/delete/reaction; `emoji` and `added` are set for a
reaction. A successful answer is:

```json
{ "thread": "-1001234567890:42", "message": "43" }
```

The sidecar answers with `thread` and `message` (and optional `files`) as
`sync.Posted`. Errors use the body below; the HTTP status is derived from the
code:

| Code | Status | Retry |
| --- | --- | --- |
| `auth` | 401 | no |
| `unsupported` | 422 | no |
| `permanent` | 422 | no |
| `rate_limited` | 429 | yes, after `retryAfter` |
| `transient` | 503 | yes |

```json
{ "code": "transient", "message": "platform is restarting", "retryAfter": 2 }
```

The board retries `transient`, `rate_limited`, network errors and 5xx with the
standard sync policy (five tries, 1s doubling to 30s, jitter), re-signing each
attempt with a fresh nonce and timestamp but the same `deliveryId`. `auth`,
`unsupported` and `permanent` are not retried; they surface as a permanent
delivery failure and the board's outbox moves on.

## Resolve

`POST {bridgeURL}/resolve` uses the same signature and body limit.

```json
{ "name": "#support" }
```

or

```json
{ "target": "-1001234567890:42" }
```

The answer is `{"channel":"-1001234567890"}` for a name or
`{"target":{...ResolvedTarget...}}` for a target. A target must include
`channel` and `kind`; a thread target also includes `thread`. The sidecar does
not return credentials, another entry's IDs, or a URL the board should call.

## Board-side implementation

- `sync/chatsdk.DecodeInbound` validates and pins a batch; the board handler
  (`POST /v1/board/sync/bridge/{key}/events`) verifies the headers, claims the
  nonce, decodes the batch, applies the entry's routes/thread gate, and stores
  it in the durable inbox.
- `sync/chatsdk.Client` signs and retries `deliver`/`resolve`; it only uses its
  configured base URL and refuses a redirect that leaves that origin.
- `sync/chatsdk.RemoteAdapter` adapts a sidecar `Client` to the normal
  `sync.Provider` contract. Its `Receive`/`Verify`/`Decode` refuse raw platform
  webhooks: inbound events must arrive through the bridge route.
- The reference sidecar fixture lives in `sync/chatsdk/sidecartest` and is
  test-only. It is never wired into the production server.
- The shared provider contract (`sync/conformance`) runs unchanged against
  Slack, GitHub, Linear, and a bridge-backed provider, so the generic path is
  substitutable for a native platform adapter.
