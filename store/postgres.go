package store

import "context"

// Postgres is the seam for a PostgreSQL-backed log. It is deliberately not
// implemented in phase 1: a half-finished store is worse than a clear one.
//
// The shape it must take:
//
//   - One table per board (or a tenant column) holding the events of
//     migrations/0001_board_log.sql, with (tenant, seq) as the primary key.
//   - Append opens a transaction at SERIALIZABLE, locks the tenant's head row
//     (or takes an advisory lock keyed by tenant), verifies expectedHead, and
//     inserts the batch. A serialization failure is retried by the caller,
//     which is why Append's condition is explicit.
//   - Load reads the tenant's events in Seq order at a repeatable-read
//     snapshot.
//   - Idempotency keys and digests ride the batch's records (see Record), so
//     a board rebuilt from the log answers a retry without a second table. A
//     unique index on (tenant, request_key) where request_key is not null
//     makes a racing duplicate fail at the database.
//
// No pgx dependency is taken until this is implemented: the package must not
// carry a driver it doesn't use.
type Postgres struct {
	// DSN is where the database is. Empty means the store isn't configured.
	DSN string
}

// NewPostgres returns the PostgreSQL seam. It doesn't connect.
func NewPostgres(dsn string) *Postgres { return &Postgres{DSN: dsn} }

// Load implements EventStore. TODO(phase 2): read the tenant's events.
func (*Postgres) Load(context.Context, string) ([]Record, error) {
	return nil, ErrNotImplemented
}

// Append implements EventStore. TODO(phase 2): conditional insert as the type
// comment describes.
func (*Postgres) Append(context.Context, string, []Record, uint64) error {
	return ErrNotImplemented
}

var _ EventStore = (*Postgres)(nil)
