package pgschema

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"hash/fnv"
	"io/fs"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// The PostgreSQL adapter owns the board's schema: no other package embeds or
// applies these files. Migrations are idempotent and run one transaction per
// file under a schema-derived advisory lock.
//
//go:embed migrations/*.sql
var migrations embed.FS

// auditMigration is the migration that creates board_audit. It is the only
// migration an audit-only database needs.
const auditMigration = "0009_audit.sql"

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

// ApplyAll applies every embedded migration in name order. It is the only
// schema entry point a PostgreSQL adapter needs.
func ApplyAll(ctx context.Context, pool *pgxpool.Pool) error {
	if pool == nil {
		return errors.New("pgschema: a PostgreSQL pool is required")
	}
	names, err := migrationNames()
	if err != nil {
		return err
	}
	return applyMigrations(ctx, pool, names)
}

// ApplyAudit applies only the audit table's migration. An audit store
// may share a database with the board, or have its own: on its own, this
// creates board_audit without requiring ownership of the board's or sync's
// tables.
func ApplyAudit(ctx context.Context, pool *pgxpool.Pool) error {
	if pool == nil {
		return errors.New("pgschema: a PostgreSQL pool is required")
	}
	return applyMigrations(ctx, pool, []string{auditMigration})
}

// migrationNames lists the embedded migrations in apply order.
func migrationNames() ([]string, error) {
	entries, err := fs.ReadDir(migrations, "migrations")
	if err != nil {
		return nil, fmt.Errorf("pgschema: reading migrations: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sql") {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	return names, nil
}

// applyMigrations applies the named embedded migrations. One connection holds
// a session advisory lock for the whole run; migrations each run in their own
// transaction, so a failure leaves no half-migrated file.
func applyMigrations(ctx context.Context, pool *pgxpool.Pool, names []string) error {
	if len(names) == 0 {
		return nil
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("pgschema: reserving a migration connection: %w", err)
	}
	defer conn.Release()
	var schema string
	if err := conn.QueryRow(ctx, `SELECT COALESCE(current_schema(), '')`).Scan(&schema); err != nil {
		return fmt.Errorf("pgschema: reading the migration schema: %w", err)
	}
	lockID := migrationLockID(schema)
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, lockID); err != nil {
		return fmt.Errorf("pgschema: taking the migration lock: %w", err)
	}
	defer conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, lockID)
	for _, name := range names {
		statement, err := migrations.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("pgschema: reading migration %s: %w", name, err)
		}
		// One transaction per file. Keep migrations transactional (no CREATE
		// INDEX CONCURRENTLY).
		tx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("pgschema: starting migration %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, string(statement)); err != nil {
			tx.Rollback(ctx)
			return fmt.Errorf("pgschema: applying migration %s: %w", name, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("pgschema: committing migration %s: %w", name, err)
		}
	}
	return nil
}
