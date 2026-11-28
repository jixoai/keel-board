package pgadapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jixoai/keel-board/pgadapter/schema"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	. "github.com/jixoai/keel-board/audit"
)

// AuditOptions configure a PostgreSQL-backed audit store.
type AuditOptions struct {
	// DSN is the database. Required.
	DSN string
	// SearchPath confines the table to one schema, which is how tests isolate
	// themselves.
	SearchPath string
}

// AuditStore is a PostgreSQL-backed Store. Every statement is tenant-scoped in
// the predicate and in the transaction's keel.tenant_id, which the table's
// FORCE RLS policy checks.
type AuditStore struct {
	pool *pgxpool.Pool
}

// OpenAudit connects and applies only the audit table's migration
// (0009_audit.sql). An audit-only database therefore needs privileges for
// board_audit and its sequence, not for the board's or sync's tables.
func OpenAudit(ctx context.Context, opts AuditOptions) (*AuditStore, error) {
	if strings.TrimSpace(opts.DSN) == "" {
		return nil, errors.New("audit: a PostgreSQL DSN is required")
	}
	config, err := pgxpool.ParseConfig(opts.DSN)
	if err != nil {
		return nil, fmt.Errorf("audit: parsing the PostgreSQL DSN: %w", err)
	}
	if opts.SearchPath != "" {
		config.ConnConfig.RuntimeParams["search_path"] = pgx.Identifier{opts.SearchPath}.Sanitize()
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("audit: opening PostgreSQL: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("audit: connecting to PostgreSQL: %w", err)
	}
	if err := pgschema.ApplyAudit(ctx, pool); err != nil {
		pool.Close()
		return nil, fmt.Errorf("audit: applying migrations: %w", err)
	}
	return &AuditStore{pool: pool}, nil
}

// Close closes the connection pool.
func (p *AuditStore) Close() { p.pool.Close() }

// setTenant scopes the current transaction. SET LOCAL keeps the setting from
// following the connection back into the pool.
func setAuditTenant(ctx context.Context, tx pgx.Tx, tenant string) error {
	if _, err := tx.Exec(ctx, `SELECT set_config('keel.tenant_id', $1, true)`, tenant); err != nil {
		return fmt.Errorf("audit: setting tenant scope: %w", err)
	}
	return nil
}

// Append implements Store.
func (p *AuditStore) Append(ctx context.Context, event Event) (Event, error) {
	prepared, err := Prepare(event)
	if err != nil {
		return Event{}, err
	}
	detail := []byte("{}")
	if len(prepared.Detail) > 0 {
		detail, err = json.Marshal(prepared.Detail)
		if err != nil {
			return Event{}, fmt.Errorf("audit: encoding detail: %w", err)
		}
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return Event{}, err
	}
	defer tx.Rollback(ctx)
	if err := setAuditTenant(ctx, tx, prepared.Tenant); err != nil {
		return Event{}, err
	}
	err = tx.QueryRow(ctx, `
		INSERT INTO board_audit
			(tenant_id, "at", kind, "key", platform, target, delivery_id, status, attempts, detail)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		RETURNING id, "at"`,
		prepared.Tenant, prepared.At, prepared.Kind, prepared.Key, prepared.Platform,
		prepared.Target, prepared.DeliveryID, prepared.Status, prepared.Attempts, detail,
	).Scan(&prepared.ID, &prepared.At)
	if err != nil {
		return Event{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Event{}, err
	}
	return prepared, nil
}

// Recent implements Store: the tenant's newest rows, newest first.
func (p *AuditStore) Recent(ctx context.Context, tenant string, limit int) ([]Event, error) {
	if err := ValidTenant(tenant); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if err := setAuditTenant(ctx, tx, tenant); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `
		SELECT id, tenant_id, "at", kind, "key", platform, target, delivery_id, status, attempts, detail
		FROM board_audit
		WHERE tenant_id = $1
		ORDER BY "at" DESC, id DESC
		LIMIT $2`, tenant, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	events := make([]Event, 0, limit)
	for rows.Next() {
		var event Event
		var detail []byte
		if err := rows.Scan(&event.ID, &event.Tenant, &event.At, &event.Kind, &event.Key,
			&event.Platform, &event.Target, &event.DeliveryID, &event.Status, &event.Attempts, &detail); err != nil {
			return nil, err
		}
		if len(detail) > 0 && string(detail) != "{}" && string(detail) != "null" {
			if err := json.Unmarshal(detail, &event.Detail); err != nil {
				return nil, fmt.Errorf("audit: decoding detail for row %d: %w", event.ID, err)
			}
		}
		event.At = event.At.Truncate(time.Microsecond)
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return events, nil
}

var _ Store = (*AuditStore)(nil)
