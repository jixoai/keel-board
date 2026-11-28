# Storage boundary

The board's domain, server and sync packages name what they need; the
PostgreSQL adapter owns how that need is met. Nothing above the adapter imports
`pgx` or `database/sql`, and `TestStorageBoundary` fails if one starts to.

## Packages

| Package | Owns |
| --- | --- |
| `store`, `syncstore`, `audit`, `automation` | Interfaces and dialect-neutral types: the append log, sync entries/inbox/outbox/health, the operational audit, watchers/schedules/deliveries. In-memory implementations live here. |
| `storage` | Capability contract: `Capability`, `Capabilities`, `ErrUnsupported`, `Reporter`. No driver, no SQL. |
| `pgadapter` | PostgreSQL implementations of `syncstore.Store`, `audit.Store` and `automation.Store`; SQL, transactions, advisory locks and migrations. |
| `pgadapter/logstore` | PostgreSQL `store.EventStore`; the log's SQL and transactions. |
| `pgadapter/schema` | The embedded migrations and their schema-derived advisory lock. |

The split keeps `pgadapter/logstore` free of the automation package (which
imports `log`), so even `log`'s internal tests can open a PostgreSQL log
through the adapter without an import cycle.

## Capabilities

An adapter declares what its backend guarantees; callers must not assume more.
`storage.Capabilities.Require` returns a typed, wrapped
`storage.ErrUnsupported` when a capability is absent.

| Capability | PostgreSQL | In-memory log | In-memory sync/audit/automation |
| --- | --- | --- | --- |
| `Durable` | yes | no | no |
| `CrossProcessLeases` | yes (advisory locks) | no | no |
| `Migrations` | yes (`pgadapter/schema`) | no | no |
| `ListenNotify` | no (wait polls the head) | yes (in-process wakeup) | no |

`ListenNotify` is deliberately false for PostgreSQL today: `LogStore.Wait`
polls the head at a fixed interval. If LISTEN/NOTIFY is added, it belongs in
`pgadapter/logstore` and the capability becomes true; no domain package
changes.

## Conformance

`conformance.RunStorage` is the dialect-neutral suite. It runs the append-log
contract plus sync entry/lease/health, automation lease, audit and capability
assertions through the domain interfaces only. It imports no driver.

- `pgadapter/storage_conformance_test.go` opts PostgreSQL in with a fresh
  schema and asserts the adapter's declared capabilities.
- `conformance/storage_memory_test.go` runs the same suite against the
  in-memory adapters, proving the suite is dialect-neutral.

Run them with a database:

```sh
KEEL_BOARD_TEST_DSN='postgres://user@127.0.0.1:5432/postgres?sslmode=disable' \
  go test ./conformance ./pgadapter/...
```

## Boundary proof

```sh
go list -deps ./store ./syncstore ./audit ./automation ./server ./sync/... ./log \
  | grep jackc/pgx
```

The command prints nothing. The same check is enforced by
`TestStorageBoundary` in `pgadapter/boundary_test.go`, which also asserts that
`pgadapter` still owns the driver.
