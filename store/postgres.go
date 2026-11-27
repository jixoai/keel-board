package store

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/fs"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrations embed.FS

const (
	// WaitPoll is how often Wait checks the head for a write another process
	// made. It is a poller, never a lock: a writer is never blocked by it.
	WaitPoll = 50 * time.Millisecond
)

// PostgresOptions configure a PostgreSQL-backed log.
type PostgresOptions struct {
	// DSN is the database. It is required.
	DSN string
	// SearchPath, when set, confines the store's tables to one schema. It is
	// how tests isolate themselves; a deployment may leave it empty and use
	// the database's own default.
	SearchPath string
}

// Postgres is a PostgreSQL-backed EventStore. A board's whole log is one
// transaction away: Append takes the tenant's head row, checks it against the
// caller's expected head, writes the batch and its idempotency row, and
// commits; a failure rolls all of it back.
type Postgres struct {
	pool *pgxpool.Pool
}

// Open connects to dsn and applies the log's schema.
func Open(ctx context.Context, dsn string) (*Postgres, error) {
	return OpenPostgres(ctx, PostgresOptions{DSN: dsn})
}

// OpenPostgres connects with opts and applies the log's schema. The schema's
// statements are idempotent, so opening an already-migrated database is safe.
func OpenPostgres(ctx context.Context, opts PostgresOptions) (*Postgres, error) {
	if strings.TrimSpace(opts.DSN) == "" {
		return nil, errors.New("store: a PostgreSQL DSN is required")
	}
	config, err := pgxpool.ParseConfig(opts.DSN)
	if err != nil {
		return nil, fmt.Errorf("store: parsing the PostgreSQL DSN: %w", err)
	}
	if opts.SearchPath != "" {
		config.ConnConfig.RuntimeParams["search_path"] = pgx.Identifier{opts.SearchPath}.Sanitize()
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("store: opening PostgreSQL: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: connecting to PostgreSQL: %w", err)
	}
	postgres := &Postgres{pool: pool}
	if err := postgres.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return postgres, nil
}

// Close closes the connection pool.
func (p *Postgres) Close() {
	p.pool.Close()
}

// defaultMigrationLockID names the advisory lock when a connection has no
// current schema.
const defaultMigrationLockID int64 = 0x6b65656c5f626f61

// migrationLockID names the advisory lock a process holds while it applies
// migrations. It is derived from the current schema: two servers on the same
// schema serialize, while test schemas (or tenants of one database confined by
// search_path) migrate in parallel.
func migrationLockID(schema string) int64 {
	if schema == "" {
		return defaultMigrationLockID
	}
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(schema))
	return int64(hash.Sum64())
}

// ApplyMigrations applies the embedded schema through pool. It is the same
// run OpenPostgres performs, exported for a package that owns its own pool
// (syncstore opens one for the sync tables). Migrations are idempotent, so
// applying them twice changes nothing.
func ApplyMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	if pool == nil {
		return errors.New("store: a PostgreSQL pool is required")
	}
	return migrate(ctx, pool)
}

// migrate applies every embedded migration, in name order. Each file is
// idempotent, so a second open changes nothing. One connection holds a session
// advisory lock for the whole run; migrations each run in their own
// transaction, so a failure leaves no half-migrated file.
func (p *Postgres) migrate(ctx context.Context) error {
	return migrate(ctx, p.pool)
}

func migrate(ctx context.Context, pool *pgxpool.Pool) error {
	entries, err := fs.ReadDir(migrations, "migrations")
	if err != nil {
		return fmt.Errorf("store: reading migrations: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sql") {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("store: reserving a migration connection: %w", err)
	}
	defer conn.Release()
	var schema string
	if err := conn.QueryRow(ctx, `SELECT COALESCE(current_schema(), '')`).Scan(&schema); err != nil {
		return fmt.Errorf("store: reading the migration schema: %w", err)
	}
	lockID := migrationLockID(schema)
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, lockID); err != nil {
		return fmt.Errorf("store: taking the migration lock: %w", err)
	}
	defer conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, lockID)
	for _, name := range names {
		statement, err := migrations.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("store: reading migration %s: %w", name, err)
		}
		// One transaction per file. Keep migrations transactional (no CREATE
		// INDEX CONCURRENTLY).
		tx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("store: starting migration %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, string(statement)); err != nil {
			tx.Rollback(ctx)
			return fmt.Errorf("store: applying migration %s: %w", name, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("store: committing migration %s: %w", name, err)
		}
	}
	return nil
}

// recordColumns is the projection every read shares. request_last and
// request_result come from the batch's request row, so board_events stays one
// row per event and a rebuilt log answers a keyed retry without the post events
// (an adopted thread's batch has none).
const recordColumns = `e.seq, e.kind, COALESCE(e.channel, ''), e.payload, e.created_at,
	COALESCE(e.request_key, ''), COALESCE(e.digest, ''),
	COALESCE((r.result->>'lastSeq')::bigint, 0),
	COALESCE(r.result, 'null'::jsonb)`

const recordFrom = ` FROM board_events e
	LEFT JOIN board_requests r
	  ON r.tenant_id = e.tenant_id AND r.request_key = e.request_key`

// Load implements EventStore.
func (p *Postgres) Load(ctx context.Context, tenant string) ([]Record, error) {
	return p.readRecords(ctx, tenant, 0, 0)
}

// History implements HistoryStore.
func (p *Postgres) History(ctx context.Context, tenant string, after uint64, limit int) ([]Record, error) {
	if limit <= 0 {
		limit = HistoryDefaultLimit
	}
	return p.readRecords(ctx, tenant, after, limit)
}

// readRecords reads a tenant's records after seq, at one repeatable-read
// snapshot, so a batch and its request row are always seen together.
func (p *Postgres) readRecords(ctx context.Context, tenant string, after uint64, limit int) ([]Record, error) {
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if err := setTenant(ctx, tx, tenant); err != nil {
		return nil, err
	}

	query := `SELECT ` + recordColumns + recordFrom + ` WHERE e.tenant_id = $1 AND e.seq > $2 ORDER BY e.seq`
	arguments := []any{tenant, after}
	if limit > 0 {
		query += ` LIMIT $3`
		arguments = append(arguments, limit)
	}
	rows, err := tx.Query(ctx, query, arguments...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	records := make([]Record, 0)
	for rows.Next() {
		var record Record
		if err := rows.Scan(
			&record.Seq,
			&record.Type,
			&record.Channel,
			&record.Payload,
			&record.At,
			&record.RequestKey,
			&record.Digest,
			&record.RequestLast,
			&record.RequestResult,
		); err != nil {
			return nil, err
		}
		if record.RequestKey == "" {
			// A non-keyed row's join is empty: don't carry a JSON null note.
			record.RequestResult = nil
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return records, nil
}

// Append implements EventStore.
func (p *Postgres) Append(ctx context.Context, tenant string, records []Record, expectedHead uint64) (AppendResult, error) {
	if len(records) == 0 {
		return AppendResult{}, nil
	}
	normalized, err := normalizeRecords(records)
	if err != nil {
		return AppendResult{}, err
	}
	records = normalized
	key, digest := requestOf(records)
	result := batchResultOf(records)

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return AppendResult{}, err
	}
	defer tx.Rollback(ctx)
	if err := setTenant(ctx, tx, tenant); err != nil {
		return AppendResult{}, err
	}

	// A retry is answered before the batch or the head is checked: it may name
	// a fresh seq and a stale head, and it must still get the first result.
	if key != "" {
		stored, found, err := requestInTx(ctx, tx, tenant, key)
		if err != nil {
			return AppendResult{}, err
		}
		if found {
			if stored.digest != digest {
				return AppendResult{}, ErrDuplicateRequest
			}
			return AppendResult{Replayed: true, BatchResult: stored.result}, nil
		}
	}
	if err := checkBatch(records, expectedHead); err != nil {
		return AppendResult{}, err
	}

	// Take the tenant's head row and compare it while holding its lock, so
	// writers for one tenant serialize and a stale expected head is refused.
	// A new tenant starts at expected head 0; the insert's conflict guard
	// catches another writer that created the row first.
	var version uint64
	var current uint64
	err = tx.QueryRow(ctx, `SELECT seq FROM board_heads WHERE tenant_id = $1 FOR UPDATE`, tenant).Scan(&current)
	switch {
	case err == nil:
		// Check the log's own consistency before comparing the caller's head:
		// a head row that names a seq the log doesn't hold is corruption, not
		// a lost race, and must not be retried into.
		if err := ensureLogEnd(ctx, tx, tenant, current); err != nil {
			return AppendResult{}, err
		}
		if current != expectedHead {
			// The head moved while this transaction waited. If it moved
			// because this very key landed, answer a replay or a duplicate
			// instead of a blind retry.
			if key != "" {
				stored, found, lookupErr := requestInTx(ctx, tx, tenant, key)
				if lookupErr != nil {
					return AppendResult{}, lookupErr
				}
				if found {
					if stored.digest != digest {
						return AppendResult{}, ErrDuplicateRequest
					}
					return AppendResult{Replayed: true, BatchResult: stored.result}, nil
				}
			}
			return AppendResult{}, ErrHeadMoved
		}
		if err := tx.QueryRow(ctx, `
			UPDATE board_heads
			SET seq = $2, version = version + 1
			WHERE tenant_id = $1
			RETURNING version`, tenant, result.LastSeq).Scan(&version); err != nil {
			return AppendResult{}, err
		}
	case errors.Is(err, pgx.ErrNoRows):
		// Check for events before deciding this is a new tenant: no head row
		// with events is corruption, even when the caller expected a head.
		if err := ensureEmptyLog(ctx, tx, tenant); err != nil {
			return AppendResult{}, err
		}
		if expectedHead != 0 {
			return AppendResult{}, ErrHeadMoved
		}
		err = tx.QueryRow(ctx, `
			INSERT INTO board_heads (tenant_id, seq, version)
			VALUES ($1, $2, 1)
			ON CONFLICT (tenant_id) DO NOTHING
			RETURNING version`, tenant, result.LastSeq).Scan(&version)
		if errors.Is(err, pgx.ErrNoRows) {
			// Another writer created the head between the read and the insert.
			if key != "" {
				stored, found, lookupErr := requestInTx(ctx, tx, tenant, key)
				if lookupErr != nil {
					return AppendResult{}, lookupErr
				}
				if found {
					if stored.digest != digest {
						return AppendResult{}, ErrDuplicateRequest
					}
					return AppendResult{Replayed: true, BatchResult: stored.result}, nil
				}
			}
			return AppendResult{}, ErrHeadMoved
		}
		if err != nil {
			return AppendResult{}, err
		}
	default:
		return AppendResult{}, err
	}
	if version == 0 {
		return AppendResult{}, errors.New("store: the head was not fenced")
	}

	if key != "" {
		raw, err := json.Marshal(result)
		if err != nil {
			return AppendResult{}, fmt.Errorf("store: encoding request %q: %w", key, err)
		}
		tag, err := tx.Exec(ctx, `
			INSERT INTO board_requests (tenant_id, request_key, digest, result)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (tenant_id, request_key) DO NOTHING`, tenant, key, digest, raw)
		if err != nil {
			return AppendResult{}, err
		}
		if tag.RowsAffected() == 0 {
			// The head CAS makes this unreachable for writers of this tenant;
			// should a row exist anyway, answer it and roll the CAS back.
			stored, found, err := requestInTx(ctx, tx, tenant, key)
			if err != nil {
				return AppendResult{}, err
			}
			if !found {
				return AppendResult{}, fmt.Errorf("store: request %q is gone", key)
			}
			if stored.digest != digest {
				return AppendResult{}, ErrDuplicateRequest
			}
			return AppendResult{Replayed: true, BatchResult: stored.result}, nil
		}
	}

	for _, record := range records {
		_, err := tx.Exec(ctx, `
			INSERT INTO board_events
				(tenant_id, seq, channel, kind, payload, request_key, digest, created_at)
			VALUES ($1, $2, NULLIF($3, ''), $4, $5, NULLIF($6, ''), NULLIF($7, ''), $8)`,
			tenant, record.Seq, record.Channel, record.Type, record.Payload,
			record.RequestKey, record.Digest, record.At)
		if err != nil {
			return AppendResult{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return AppendResult{}, err
	}
	return AppendResult{BatchResult: result}, nil
}

// ensureLogEnd refuses an append when the tenant's head row and its newest
// event disagree. The caller holds the head row's lock, so no other writer can
// move either half while this reads them.
func ensureLogEnd(ctx context.Context, tx pgx.Tx, tenant string, head uint64) error {
	var maxSeq uint64
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(MAX(seq), 0) FROM board_events WHERE tenant_id = $1`, tenant).Scan(&maxSeq); err != nil {
		return err
	}
	if maxSeq != head {
		return fmt.Errorf("%w: head #%d, newest event #%d", ErrLogInconsistent, head, maxSeq)
	}
	return nil
}

// ensureEmptyLog refuses to create a tenant's head when events already exist.
// The first SELECT may miss a head row another writer commits before the MAX
// read (READ COMMITTED gives each statement its own snapshot), so a re-read
// that finds the row is a lost race, not a corrupt log.
func ensureEmptyLog(ctx context.Context, tx pgx.Tx, tenant string) error {
	var maxSeq uint64
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(MAX(seq), 0) FROM board_events WHERE tenant_id = $1`, tenant).Scan(&maxSeq); err != nil {
		return err
	}
	if maxSeq == 0 {
		return nil
	}
	var seq uint64
	err := tx.QueryRow(ctx, `SELECT seq FROM board_heads WHERE tenant_id = $1`, tenant).Scan(&seq)
	switch {
	case err == nil:
		return ErrHeadMoved
	case errors.Is(err, pgx.ErrNoRows):
		return fmt.Errorf("%w: no head row, newest event #%d", ErrLogInconsistent, maxSeq)
	default:
		return err
	}
}

// Integrity implements IntegrityStore in one statement, so the head row and
// the log end are read at one snapshot.
func (p *Postgres) Integrity(ctx context.Context, tenant string) (Integrity, error) {
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return Integrity{}, err
	}
	defer tx.Rollback(ctx)
	if err := setTenant(ctx, tx, tenant); err != nil {
		return Integrity{}, err
	}
	var integrity Integrity
	if err := tx.QueryRow(ctx, `
		SELECT
			COALESCE((SELECT seq FROM board_heads WHERE tenant_id = $1), 0),
			COALESCE((SELECT MAX(seq) FROM board_events WHERE tenant_id = $1), 0)`,
		tenant).Scan(&integrity.Head, &integrity.MaxSeq); err != nil {
		return Integrity{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Integrity{}, err
	}
	return integrity, nil
}

// Tenants implements TenantStore over both the head rows and the log, so a
// tenant whose head row is missing is still offered to a rebuild. It is an
// operator read: it is the one caller that sets the RLS bypass, because
// listing every tenant is exactly what tenant isolation forbids a request.
func (p *Postgres) Tenants(ctx context.Context) ([]string, error) {
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT set_config('keel.bypass_rls', 'on', true)`); err != nil {
		return nil, fmt.Errorf("store: setting the tenant-list scope: %w", err)
	}
	rows, err := tx.Query(ctx, `
		SELECT tenant_id FROM board_heads
		UNION
		SELECT tenant_id FROM board_events
		ORDER BY tenant_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tenants := make([]string, 0)
	for rows.Next() {
		var tenant string
		if err := rows.Scan(&tenant); err != nil {
			return nil, err
		}
		tenants = append(tenants, tenant)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return tenants, nil
}

// Request implements RequestStore.
func (p *Postgres) Request(ctx context.Context, tenant, key string) (Request, bool, error) {
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return Request{}, false, err
	}
	defer tx.Rollback(ctx)
	if err := setTenant(ctx, tx, tenant); err != nil {
		return Request{}, false, err
	}
	stored, found, err := requestInTx(ctx, tx, tenant, key)
	if err != nil {
		return Request{}, false, err
	}
	if !found {
		return Request{}, false, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return Request{}, false, err
	}
	return Request{Digest: stored.digest, Result: stored.result}, true, nil
}

// queryer is what both a pool and a transaction can do: the one read every
// request lookup needs.
type queryer interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type storedRequest struct {
	digest string
	result BatchResult
}

// requestInTx reads a keyed batch's answer. It is safe inside a transaction
// and outside one.
func requestInTx(ctx context.Context, q queryer, tenant, key string) (storedRequest, bool, error) {
	var stored storedRequest
	var raw []byte
	err := q.QueryRow(ctx, `
		SELECT digest, result FROM board_requests
		WHERE tenant_id = $1 AND request_key = $2`, tenant, key).Scan(&stored.digest, &raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return storedRequest{}, false, nil
	}
	if err != nil {
		return storedRequest{}, false, err
	}
	if err := json.Unmarshal(raw, &stored.result); err != nil {
		return storedRequest{}, false, fmt.Errorf("store: decoding request %q: %w", key, err)
	}
	return stored, true, nil
}

// Head implements HeadStore with one indexed lookup on the tenant's head row.
func (p *Postgres) Head(ctx context.Context, tenant string) (uint64, error) {
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	if err := setTenant(ctx, tx, tenant); err != nil {
		return 0, err
	}
	var head uint64
	err = tx.QueryRow(ctx, `SELECT seq FROM board_heads WHERE tenant_id = $1`, tenant).Scan(&head)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return head, nil
}

// setTenant scopes the current transaction to tenant. Every board_* table has
// a FORCE RLS policy that reads keel.tenant_id, so a transaction that loses its
// tenant predicate sees no rows instead of another tenant's. SET LOCAL keeps
// the setting from following the connection back into the pool.
func setTenant(ctx context.Context, tx pgx.Tx, tenant string) error {
	if _, err := tx.Exec(ctx, `SELECT set_config('keel.tenant_id', $1, true)`, tenant); err != nil {
		return fmt.Errorf("store: setting tenant scope: %w", err)
	}
	return nil
}

// Wait implements Waiter by polling the tenant's head. It holds no lock, so
// writers never wait on a reader.
func (p *Postgres) Wait(ctx context.Context, tenant string, after uint64) error {
	ticker := time.NewTicker(WaitPoll)
	defer ticker.Stop()
	for {
		head, err := p.Head(ctx, tenant)
		if err != nil {
			return err
		}
		if head > after {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

var (
	_ EventStore     = (*Postgres)(nil)
	_ HistoryStore   = (*Postgres)(nil)
	_ HeadStore      = (*Postgres)(nil)
	_ Waiter         = (*Postgres)(nil)
	_ RequestStore   = (*Postgres)(nil)
	_ IntegrityStore = (*Postgres)(nil)
	_ TenantStore    = (*Postgres)(nil)
)
