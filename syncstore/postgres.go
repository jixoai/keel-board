package syncstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jixoai/keel-board/store"
	boardsync "github.com/jixoai/keel-board/sync"
)

// PostgresOptions configure a PostgreSQL sync store.
type PostgresOptions struct {
	// DSN is the database. It is required.
	DSN string
	// SearchPath confines every sync table to one schema. Tests use it for
	// isolation; a deployment may leave it empty for the database default.
	SearchPath string
	// Config bounds retries and leases.
	Config Config
}

// Postgres is a PostgreSQL-backed Store. Each lease is one transaction that
// takes an advisory lock for its tenant and lane kind, so two workers can't
// hand out the same row, while different tenants and keys proceed in
// parallel. Every statement carries the tenant predicate.
type Postgres struct {
	pool *pgxpool.Pool
	cfg  Config
}

// OpenPostgres connects with opts, applies the board and sync schema, and
// returns a store. The schema is idempotent, so opening a migrated database
// again changes nothing.
func OpenPostgres(ctx context.Context, opts PostgresOptions) (*Postgres, error) {
	if strings.TrimSpace(opts.DSN) == "" {
		return nil, errors.New("syncstore: a PostgreSQL DSN is required")
	}
	cfg, err := opts.Config.withDefaults()
	if err != nil {
		return nil, err
	}
	config, err := pgxpool.ParseConfig(opts.DSN)
	if err != nil {
		return nil, fmt.Errorf("syncstore: parsing the PostgreSQL DSN: %w", err)
	}
	if opts.SearchPath != "" {
		config.ConnConfig.RuntimeParams["search_path"] = pgx.Identifier{opts.SearchPath}.Sanitize()
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("syncstore: opening PostgreSQL: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("syncstore: connecting to PostgreSQL: %w", err)
	}
	if err := store.ApplyMigrations(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}
	return &Postgres{pool: pool, cfg: cfg}, nil
}

// Close closes the connection pool.
func (p *Postgres) Close() {
	p.pool.Close()
}

// withTenant runs fn in a transaction whose session variable names the
// tenant. Every sync table has FORCE ROW LEVEL SECURITY with a policy over that
// variable, so a statement without this setting sees and writes no rows. The
// variable is transaction-local, so a pooled connection never carries one
// tenant's identity into another's work.
func (p *Postgres) withTenant(ctx context.Context, tenant string, fn func(pgx.Tx) error) error {
	if err := validTenant(tenant); err != nil {
		return err
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT set_config('keel.tenant_id', $1, true)`, tenant); err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// scanner is what a row and a row set share.
type scanner interface {
	Scan(dest ...any) error
}

const entryColumns = `tenant_id, sync_key, platform, enabled, created_at, updated_at`

func scanEntry(row scanner) (Entry, error) {
	var entry Entry
	err := row.Scan(&entry.Tenant, &entry.Key, &entry.Platform, &entry.Enabled, &entry.CreatedAt, &entry.UpdatedAt)
	return entry, err
}

// UpsertEntry implements Store.
func (p *Postgres) UpsertEntry(ctx context.Context, entry Entry) (Entry, error) {
	if err := entry.Validate(); err != nil {
		return Entry{}, err
	}
	var stored Entry
	err := p.withTenant(ctx, entry.Tenant, func(tx pgx.Tx) error {
		var err error
		stored, err = scanEntry(tx.QueryRow(ctx, `
			INSERT INTO board_sync_entries (tenant_id, sync_key, platform, enabled)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (tenant_id, sync_key) DO UPDATE
			SET platform = EXCLUDED.platform, updated_at = now()
			RETURNING `+entryColumns,
			entry.Tenant, entry.Key, entry.Platform, entry.Enabled))
		return err
	})
	if err != nil {
		return Entry{}, err
	}
	return stored, nil
}

// Entry implements Store.
func (p *Postgres) Entry(ctx context.Context, tenant string, key boardsync.Key) (Entry, bool, error) {
	if err := expectKey(key); err != nil {
		return Entry{}, false, err
	}
	var entry Entry
	err := p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		var err error
		entry, err = scanEntry(tx.QueryRow(ctx, `
			SELECT `+entryColumns+` FROM board_sync_entries
			WHERE tenant_id = $1 AND sync_key = $2`, tenant, key))
		return err
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return Entry{}, false, nil
	}
	if err != nil {
		return Entry{}, false, err
	}
	return entry, true, nil
}

// Entries implements Store.
func (p *Postgres) Entries(ctx context.Context, tenant string) ([]Entry, error) {
	entries := make([]Entry, 0)
	err := p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT `+entryColumns+` FROM board_sync_entries
			WHERE tenant_id = $1 ORDER BY sync_key`, tenant)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			entry, err := scanEntry(rows)
			if err != nil {
				return err
			}
			entries = append(entries, entry)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

// SetEntryEnabled implements Store.
func (p *Postgres) SetEntryEnabled(ctx context.Context, tenant string, key boardsync.Key, enabled bool) (bool, error) {
	if err := expectKey(key); err != nil {
		return false, err
	}
	var changed bool
	err := p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			UPDATE board_sync_entries SET enabled = $3, updated_at = now()
			WHERE tenant_id = $1 AND sync_key = $2`, tenant, key, enabled)
		if err != nil {
			return err
		}
		changed = tag.RowsAffected() > 0
		return nil
	})
	if err != nil {
		return false, err
	}
	return changed, nil
}

const inboxItemColumns = `seq, id, dedupe, lane, event, COALESCE(raw, ''::bytea), attempts`

func scanInboxItem(row scanner) (boardsync.InboxItem, string, error) {
	var (
		seq      int64
		item     boardsync.InboxItem
		lane     string
		event    []byte
		raw      []byte
		attempts int
	)
	if err := row.Scan(&seq, &item.ID, &item.Dedupe, &lane, &event, &raw, &attempts); err != nil {
		return boardsync.InboxItem{}, "", err
	}
	var decoded boardsync.Event
	if err := json.Unmarshal(event, &decoded); err != nil {
		return boardsync.InboxItem{}, "", fmt.Errorf("syncstore: decoding inbox event %q: %w", item.ID, err)
	}
	item.Event = decoded
	item.Raw = append([]byte(nil), raw...)
	item.Thread = lane
	item.Attempts = attempts
	return item, lane, nil
}

// PushInbox implements Store.
func (p *Postgres) PushInbox(ctx context.Context, tenant string, key boardsync.Key, records []boardsync.Record) (InboxPush, error) {
	prepared, err := prepareRecords(key, records)
	if err != nil {
		return InboxPush{}, err
	}
	added := 0
	err = p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		for _, record := range prepared {
			event, err := json.Marshal(record.Event)
			if err != nil {
				return fmt.Errorf("%w: event %q can't be stored", boardsync.ErrBadRequest, record.Event.ID)
			}
			lane := record.Event.Thread
			if record.Event.Subthread != "" {
				lane = record.Event.Subthread
			}
			raw := record.Raw
			if len(raw) == 0 {
				raw = record.Event.Raw
			}
			tag, err := tx.Exec(ctx, `
				INSERT INTO board_sync_inbox
					(tenant_id, sync_key, id, dedupe, lane, platform, kind, event, raw)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
				ON CONFLICT (tenant_id, sync_key, dedupe) DO NOTHING`,
				tenant, key, record.Event.ID, record.Dedupe, lane,
				record.Event.Platform, record.Event.Kind, event, raw)
			if err != nil {
				return err
			}
			added += int(tag.RowsAffected())
		}
		return nil
	})
	if err != nil {
		return InboxPush{}, err
	}
	return InboxPush{Added: added, Skipped: len(prepared) - added}, nil
}

// LeaseInbox implements Store.
func (p *Postgres) LeaseInbox(ctx context.Context, tenant string, key boardsync.Key, limit int, owner string) ([]boardsync.InboxItem, error) {
	if err := expectKey(key); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 10
	}
	owner = leaseOwner(owner)
	out := make([]boardsync.InboxItem, 0, limit)
	err := p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`,
			advisoryLockKey("inbox", tenant, string(key))); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `
			SELECT seq FROM (
				SELECT seq, lane,
					row_number() OVER (PARTITION BY lane ORDER BY seq) AS position,
					state, available_at, lease_until
				FROM board_sync_inbox
				WHERE tenant_id = $1 AND sync_key = $2 AND state IN ('pending', 'leased')
			) ranked
			WHERE position = 1
				AND ((state = 'pending' AND available_at <= now())
					OR (state = 'leased' AND lease_until <= now()))
			ORDER BY seq
			LIMIT $3`, tenant, key, limit)
		if err != nil {
			return err
		}
		seqs := make([]int64, 0, limit)
		for rows.Next() {
			var seq int64
			if err := rows.Scan(&seq); err != nil {
				rows.Close()
				return err
			}
			seqs = append(seqs, seq)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, seq := range seqs {
			token, err := newLeaseToken()
			if err != nil {
				return err
			}
			item, lane, err := scanInboxItem(tx.QueryRow(ctx, `
				UPDATE board_sync_inbox
				SET state = 'leased', lease_owner = $3, lease_token = $4,
					lease_until = now() + make_interval(secs => $5), updated_at = now()
				WHERE tenant_id = $1 AND sync_key = $2 AND seq = $6
					AND ((state = 'pending' AND available_at <= now())
						OR (state = 'leased' AND lease_until <= now()))
				RETURNING `+inboxItemColumns,
				tenant, key, owner, token, p.cfg.LeaseTTL.Seconds(), seq))
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}
			if err != nil {
				return err
			}
			item.Key = key
			item.Lease = token
			item.Thread = lane
			out = append(out, item)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// AckInbox implements Store.
func (p *Postgres) AckInbox(ctx context.Context, tenant string, key boardsync.Key, id, lease string, jobErr error) (AckResult, error) {
	if err := expectKey(key); err != nil {
		return AckResult{}, err
	}
	var result AckResult
	err := p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		var (
			seq       int64
			attempts  int
			state     string
			token     string
			leaseLive bool
		)
		err := tx.QueryRow(ctx, `
			SELECT seq, attempts, state, lease_token, COALESCE(lease_until > now(), false)
			FROM board_sync_inbox
			WHERE tenant_id = $1 AND sync_key = $2 AND id = $3
			FOR UPDATE`, tenant, key, id).
			Scan(&seq, &attempts, &state, &token, &leaseLive)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrLeaseLost
		}
		if err != nil {
			return err
		}
		if state != StateLeased || token == "" || token != lease || !leaseLive {
			return ErrLeaseLost
		}
		if jobErr == nil {
			_, err = tx.Exec(ctx, `
				UPDATE board_sync_inbox
				SET state = 'done', lease_owner = '', lease_token = '', lease_until = NULL,
					updated_at = now(), completed_at = now()
				WHERE tenant_id = $1 AND sync_key = $2 AND seq = $3`, tenant, key, seq)
			if err != nil {
				return err
			}
			result = AckResult{State: StateDone, Attempts: attempts, Acked: true}
			return nil
		}
		attempts++
		code := ErrorCode(jobErr)
		if boardsync.IsPermanent(jobErr) || attempts >= p.cfg.InboundRetry.MaxAttempts {
			_, err = tx.Exec(ctx, `
				UPDATE board_sync_inbox
				SET state = 'dead', attempts = $4, last_error = $5,
					lease_owner = '', lease_token = '', lease_until = NULL,
					updated_at = now(), completed_at = now()
				WHERE tenant_id = $1 AND sync_key = $2 AND seq = $3`, tenant, key, seq, attempts, code)
			if err != nil {
				return err
			}
			result = AckResult{State: StateDead, Attempts: attempts, Dead: true}
			return nil
		}
		_, err = tx.Exec(ctx, `
			UPDATE board_sync_inbox
			SET state = 'pending', attempts = $4, last_error = $5,
				available_at = now() + make_interval(secs => $6),
				lease_owner = '', lease_token = '', lease_until = NULL,
				updated_at = now(), completed_at = NULL
			WHERE tenant_id = $1 AND sync_key = $2 AND seq = $3`,
			tenant, key, seq, attempts, code, p.cfg.InboundRetry.Delay(attempts).Seconds())
		if err != nil {
			return err
		}
		result = AckResult{State: StatePending, Attempts: attempts, Retried: true}
		return nil
	})
	if err != nil {
		return AckResult{}, err
	}
	return result, nil
}

// ReleaseInbox implements Store.
func (p *Postgres) ReleaseInbox(ctx context.Context, tenant string, key boardsync.Key, id, lease string, delay time.Duration) error {
	if err := expectKey(key); err != nil {
		return err
	}
	if delay < 0 {
		delay = 0
	}
	return p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			UPDATE board_sync_inbox
			SET state = 'pending', available_at = now() + make_interval(secs => $4),
				lease_owner = '', lease_token = '', lease_until = NULL, updated_at = now()
			WHERE tenant_id = $1 AND sync_key = $2 AND id = $3
				AND state = 'leased' AND lease_token = $5 AND lease_until > now()`,
			tenant, key, id, delay.Seconds(), lease)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrLeaseLost
		}
		return nil
	})
}

// DeadInbox implements Store.
func (p *Postgres) DeadInbox(ctx context.Context, tenant string, key boardsync.Key, limit int) ([]boardsync.InboxItem, error) {
	if err := validTenant(tenant); err != nil {
		return nil, err
	}
	if err := expectKey(key); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 100
	}
	out := make([]boardsync.InboxItem, 0)
	err := p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT `+inboxItemColumns+` FROM board_sync_inbox
			WHERE tenant_id = $1 AND sync_key = $2 AND state = 'dead'
			ORDER BY seq LIMIT $3`, tenant, key, limit)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			item, lane, err := scanInboxItem(rows)
			if err != nil {
				return err
			}
			item.Key = key
			item.Thread = lane
			out = append(out, item)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// RetryInbox implements Store.
func (p *Postgres) RetryInbox(ctx context.Context, tenant string, key boardsync.Key, id string) error {
	if err := expectKey(key); err != nil {
		return err
	}
	return p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			UPDATE board_sync_inbox
			SET state = 'pending', attempts = 0, last_error = '', available_at = now(),
				lease_owner = '', lease_token = '', lease_until = NULL,
				updated_at = now(), completed_at = NULL
			WHERE tenant_id = $1 AND sync_key = $2 AND id = $3 AND state = 'dead'`,
			tenant, key, id)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrNotDead
		}
		return nil
	})
}

// InboxDepth implements Store.
func (p *Postgres) InboxDepth(ctx context.Context, tenant string, key boardsync.Key) (InboxDepth, error) {
	if err := expectKey(key); err != nil {
		return InboxDepth{}, err
	}
	var depth InboxDepth
	err := p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `
			SELECT
				count(*) FILTER (WHERE state = 'pending'),
				count(*) FILTER (WHERE state = 'leased'),
				count(*) FILTER (WHERE state = 'done'),
				count(*) FILTER (WHERE state = 'dead')
			FROM board_sync_inbox WHERE tenant_id = $1 AND sync_key = $2`, tenant, key).
			Scan(&depth.Pending, &depth.Leased, &depth.Done, &depth.Dead)
	})
	if err != nil {
		return InboxDepth{}, err
	}
	return depth, nil
}

// EnqueueOutbox implements Store.
func (p *Postgres) EnqueueOutbox(ctx context.Context, tenant string, key boardsync.Key, event boardsync.BoardEvent, message boardsync.Message, targets []boardsync.ResolvedTarget) (EnqueueResult, error) {
	jobs, err := prepareOutbox(tenant, key, event, message, targets)
	if err != nil {
		return EnqueueResult{}, err
	}
	if len(jobs) == 0 {
		return EnqueueResult{Skipped: len(targets)}, nil
	}
	added := 0
	err = p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		for _, job := range jobs {
			eventJSON, err := json.Marshal(job.event)
			if err != nil {
				return fmt.Errorf("%w: board event %d can't be queued", boardsync.ErrBadRequest, job.event.ID)
			}
			messageJSON, err := json.Marshal(job.message)
			if err != nil {
				return fmt.Errorf("%w: board message %d can't be queued", boardsync.ErrBadRequest, job.message.ID)
			}
			targetJSON, err := json.Marshal(job.target)
			if err != nil {
				return fmt.Errorf("%w: target can't be queued", boardsync.ErrBadRequest)
			}
			tag, err := tx.Exec(ctx, `
				INSERT INTO board_sync_outbox
					(tenant_id, delivery_id, lane, sync_key, event_id, event_type,
					 board_thread, board_message, target_key, target, event, message)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
				ON CONFLICT (tenant_id, delivery_id) DO NOTHING`,
				tenant, job.id, job.lane, job.key, job.event.ID, job.event.Type,
				job.event.Thread, job.message.ID, job.targetKey, targetJSON, eventJSON, messageJSON)
			if err != nil {
				return err
			}
			added += int(tag.RowsAffected())
		}
		return nil
	})
	if err != nil {
		return EnqueueResult{}, err
	}
	return EnqueueResult{Added: added, Skipped: len(targets) - added}, nil
}

const deliveryColumns = `id, delivery_id, lane, sync_key, event, message, target,
	attempts, state, available_at, lease_token, last_error, created_at`

func scanDelivery(row scanner) (Delivery, string, string, error) {
	var (
		rowID      int64
		delivery   Delivery
		event      []byte
		message    []byte
		target     []byte
		leaseToken string
		targetKey  string
	)
	err := row.Scan(&rowID, &delivery.ID, &delivery.Lane, &delivery.Key, &event, &message, &target,
		&delivery.Attempts, &delivery.State, &delivery.AvailableAt, &leaseToken,
		&delivery.LastError, &delivery.CreatedAt)
	_ = rowID
	if err != nil {
		return Delivery{}, "", "", err
	}
	if err := json.Unmarshal(event, &delivery.Event); err != nil {
		return Delivery{}, "", "", fmt.Errorf("syncstore: decoding delivery %q event: %w", delivery.ID, err)
	}
	if err := json.Unmarshal(message, &delivery.Message); err != nil {
		return Delivery{}, "", "", fmt.Errorf("syncstore: decoding delivery %q message: %w", delivery.ID, err)
	}
	if err := json.Unmarshal(target, &delivery.Target); err != nil {
		return Delivery{}, "", "", fmt.Errorf("syncstore: decoding delivery %q target: %w", delivery.ID, err)
	}
	delivery.Lease = leaseToken
	targetKey = targetKeyOf(delivery.Target)
	return delivery, delivery.Lane, targetKey, nil
}

// LeaseOutbox implements Store.
func (p *Postgres) LeaseOutbox(ctx context.Context, tenant string, keys []boardsync.Key, limit int, owner string) ([]Delivery, error) {
	owned, err := ownedKeys(keys)
	if err != nil {
		return nil, err
	}
	if len(owned) == 0 {
		return nil, nil
	}
	keyStrings := make([]string, 0, len(owned))
	for key := range owned {
		keyStrings = append(keyStrings, string(key))
	}
	if limit <= 0 {
		limit = 10
	}
	owner = leaseOwner(owner)
	out := make([]Delivery, 0, limit)
	err = p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`,
			advisoryLockKey("outbox", tenant)); err != nil {
			return err
		}
		// Rank every lane's head first and filter ownership after: a worker
		// must not jump over another key's earlier job on the same board thread.
		rows, err := tx.Query(ctx, `
			SELECT id FROM (
				SELECT id, lane, sync_key,
					row_number() OVER (PARTITION BY lane ORDER BY id) AS position,
					state, available_at, lease_until
				FROM board_sync_outbox
				WHERE tenant_id = $1 AND state IN ('pending', 'leased')
			) ranked
			WHERE position = 1
				AND sync_key = ANY($2)
				AND ((state = 'pending' AND available_at <= now())
					OR (state = 'leased' AND lease_until <= now()))
			ORDER BY id
			LIMIT $3`, tenant, keyStrings, limit)
		if err != nil {
			return err
		}
		ids := make([]int64, 0, limit)
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			ids = append(ids, id)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, id := range ids {
			token, err := newLeaseToken()
			if err != nil {
				return err
			}
			delivery, _, _, err := scanDelivery(tx.QueryRow(ctx, `
				UPDATE board_sync_outbox
				SET state = 'leased', lease_owner = $3, lease_token = $4,
					lease_until = now() + make_interval(secs => $5), updated_at = now()
				WHERE tenant_id = $1 AND id = $2 AND state IN ('pending', 'leased')
				RETURNING `+deliveryColumns,
				tenant, id, owner, token, p.cfg.LeaseTTL.Seconds()))
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}
			if err != nil {
				return err
			}
			delivery.State = StateLeased
			delivery.Lease = token
			out = append(out, delivery)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// AckOutbox implements Store.
func (p *Postgres) AckOutbox(ctx context.Context, tenant, deliveryID, lease string, jobErr error, copy *Copy) (AckResult, error) {
	if err := validTenant(tenant); err != nil {
		return AckResult{}, err
	}
	if deliveryID == "" || lease == "" {
		return AckResult{}, fmt.Errorf("%w: an ack needs a delivery and a lease", boardsync.ErrBadRequest)
	}
	if copy != nil {
		if err := copy.Validate(); err != nil {
			return AckResult{}, err
		}
	}
	if copy != nil && jobErr != nil {
		return AckResult{}, fmt.Errorf("%w: a failed delivery has no copy", boardsync.ErrBadRequest)
	}
	var result AckResult
	err := p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		var (
			rowID        int64
			attempts     int
			key          string
			targetKey    string
			boardThread  int64
			boardMessage int64
			targetJSON   []byte
			state        string
			token        string
			leaseLive    bool
		)
		err := tx.QueryRow(ctx, `
			SELECT id, attempts, sync_key, target_key, board_thread, board_message,
				target, state, lease_token, COALESCE(lease_until > now(), false)
			FROM board_sync_outbox
			WHERE tenant_id = $1 AND delivery_id = $2
			FOR UPDATE`, tenant, deliveryID).
			Scan(&rowID, &attempts, &key, &targetKey, &boardThread, &boardMessage,
				&targetJSON, &state, &token, &leaseLive)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrLeaseLost
		}
		if err != nil {
			return err
		}
		if state != StateLeased || token == "" || token != lease || !leaseLive {
			return ErrLeaseLost
		}
		if jobErr == nil {
			_, err = tx.Exec(ctx, `
				UPDATE board_sync_outbox
				SET state = 'done', lease_owner = '', lease_token = '', lease_until = NULL,
					updated_at = now(), completed_at = now()
				WHERE tenant_id = $1 AND id = $2`, tenant, rowID)
			if err != nil {
				return err
			}
			if copy != nil {
				var target boardsync.ResolvedTarget
				if err := json.Unmarshal(targetJSON, &target); err != nil {
					return fmt.Errorf("syncstore: decoding delivery target: %w", err)
				}
				if copy.Key != boardsync.Key(key) || copy.Target != target {
					return fmt.Errorf("%w: a copy's target isn't the delivery's", boardsync.ErrBadRequest)
				}
				if copy.BoardThread != uint64(boardThread) || copy.BoardMessage != uint64(boardMessage) {
					return fmt.Errorf("%w: a copy's board message isn't the delivery's", boardsync.ErrBadRequest)
				}
				copy.ID = copyIDOf(copy.Key, copy.Target, copy.BoardMessage)
				_, err = tx.Exec(ctx, `
					INSERT INTO board_sync_copies
						(tenant_id, id, board_message, board_thread, sync_key, platform,
						 channel, thread, subthread, kind, external_thread, external_message,
						 files, files_set)
					VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
					ON CONFLICT (tenant_id, id) DO UPDATE
					SET external_thread = EXCLUDED.external_thread,
						external_message = EXCLUDED.external_message,
						files = EXCLUDED.files,
						files_set = EXCLUDED.files_set,
						updated_at = now()`,
					tenant, copy.ID, copy.BoardMessage, copy.BoardThread, copy.Key,
					copy.Target.Platform, copy.Target.Channel, copy.Target.Thread,
					copy.Target.Subthread, copy.Target.Kind, copy.ExternalThread, copy.ExternalMessage,
					nonNilFiles(copy.Files), copy.FilesSet)
				if err != nil {
					return err
				}
			}
			_, err = tx.Exec(ctx, `
				UPDATE board_sync_outbox
				SET cleared_at = now(), updated_at = now()
				WHERE tenant_id = $1 AND sync_key = $2 AND target_key = $3
					AND state = 'dead' AND cleared_at IS NULL`, tenant, key, targetKey)
			if err != nil {
				return err
			}
			result = AckResult{State: StateDone, Attempts: attempts, Acked: true}
			return nil
		}
		attempts++
		code := ErrorCode(jobErr)
		if boardsync.IsPermanent(jobErr) || attempts >= p.cfg.OutboundRetry.MaxAttempts {
			_, err = tx.Exec(ctx, `
				UPDATE board_sync_outbox
				SET state = 'dead', attempts = $3, last_error = $4,
					lease_owner = '', lease_token = '', lease_until = NULL,
					updated_at = now(), completed_at = now(), cleared_at = NULL
				WHERE tenant_id = $1 AND id = $2`, tenant, rowID, attempts, code)
			if err != nil {
				return err
			}
			result = AckResult{State: StateDead, Attempts: attempts, Dead: true}
			return nil
		}
		_, err = tx.Exec(ctx, `
			UPDATE board_sync_outbox
			SET state = 'pending', attempts = $3, last_error = $4,
				available_at = now() + make_interval(secs => $5),
				lease_owner = '', lease_token = '', lease_until = NULL,
				updated_at = now(), completed_at = NULL
			WHERE tenant_id = $1 AND id = $2`,
			tenant, rowID, attempts, code, p.cfg.OutboundRetry.Delay(attempts).Seconds())
		if err != nil {
			return err
		}
		result = AckResult{State: StatePending, Attempts: attempts, Retried: true}
		return nil
	})
	if err != nil {
		return AckResult{}, err
	}
	return result, nil
}

// ReleaseOutbox implements Store.
func (p *Postgres) ReleaseOutbox(ctx context.Context, tenant, deliveryID, lease string, delay time.Duration) error {
	if deliveryID == "" || lease == "" {
		return fmt.Errorf("%w: a release needs a delivery and a lease", boardsync.ErrBadRequest)
	}
	if delay < 0 {
		delay = 0
	}
	return p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			UPDATE board_sync_outbox
			SET state = 'pending', available_at = now() + make_interval(secs => $4),
				lease_owner = '', lease_token = '', lease_until = NULL, updated_at = now()
			WHERE tenant_id = $1 AND delivery_id = $2
				AND state = 'leased' AND lease_token = $3 AND lease_until > now()`,
			tenant, deliveryID, lease, delay.Seconds())
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrLeaseLost
		}
		return nil
	})
}

// DeadOutbox implements Store.
func (p *Postgres) DeadOutbox(ctx context.Context, tenant string, key boardsync.Key, limit int) ([]Delivery, error) {
	if key != "" {
		if err := expectKey(key); err != nil {
			return nil, err
		}
	}
	if limit <= 0 {
		limit = 100
	}
	query := `SELECT ` + deliveryColumns + ` FROM board_sync_outbox
		WHERE tenant_id = $1 AND state = 'dead'`
	arguments := []any{tenant}
	if key != "" {
		query += ` AND sync_key = $2`
		arguments = append(arguments, key)
	}
	query += ` ORDER BY id LIMIT $` + strconv.Itoa(len(arguments)+1)
	arguments = append(arguments, limit)
	out := make([]Delivery, 0)
	err := p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, query, arguments...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			delivery, _, _, err := scanDelivery(rows)
			if err != nil {
				return err
			}
			delivery.Lease = ""
			out = append(out, delivery)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// RetryOutbox implements Store.
func (p *Postgres) RetryOutbox(ctx context.Context, tenant, deliveryID string) error {
	return p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			UPDATE board_sync_outbox
			SET state = 'pending', attempts = 0, last_error = '', available_at = now(),
				lease_owner = '', lease_token = '', lease_until = NULL,
				updated_at = now(), completed_at = NULL, cleared_at = NULL
			WHERE tenant_id = $1 AND delivery_id = $2 AND state = 'dead'`, tenant, deliveryID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrNotDead
		}
		return nil
	})
}

// OutboxDepth implements Store.
func (p *Postgres) OutboxDepth(ctx context.Context, tenant string) (OutboxDepth, error) {
	var depth OutboxDepth
	err := p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `
			SELECT
				count(*) FILTER (WHERE state = 'pending'),
				count(*) FILTER (WHERE state = 'leased'),
				count(*) FILTER (WHERE state = 'done'),
				count(*) FILTER (WHERE state = 'dead')
			FROM board_sync_outbox WHERE tenant_id = $1`, tenant).
			Scan(&depth.Pending, &depth.Leased, &depth.Done, &depth.Dead)
	})
	if err != nil {
		return OutboxDepth{}, err
	}
	return depth, nil
}

const copyColumns = `id, board_message, board_thread, sync_key, platform, channel,
	thread, subthread, kind, external_thread, external_message, files, files_set,
	created_at, updated_at`

func scanCopy(row scanner) (Copy, error) {
	var (
		copy      Copy
		platform  string
		channel   string
		thread    string
		subthread string
		kind      boardsync.TargetKind
	)
	err := row.Scan(&copy.ID, &copy.BoardMessage, &copy.BoardThread, &copy.Key,
		&platform, &channel, &thread, &subthread, &kind,
		&copy.ExternalThread, &copy.ExternalMessage, &copy.Files, &copy.FilesSet,
		&copy.CreatedAt, &copy.UpdatedAt)
	if err != nil {
		return Copy{}, err
	}
	copy.Target = boardsync.ResolvedTarget{
		Key: copy.Key, Platform: boardsync.Platform(platform), Channel: channel,
		Thread: thread, Subthread: subthread, Kind: kind,
	}
	return copy, nil
}

// Copy implements Store.
func (p *Postgres) Copy(ctx context.Context, tenant string, key boardsync.Key, target boardsync.ResolvedTarget, boardMessage uint64) (Copy, bool, error) {
	if err := expectKey(key); err != nil {
		return Copy{}, false, err
	}
	if err := target.Validate(); err != nil {
		return Copy{}, false, err
	}
	if boardMessage == 0 {
		return Copy{}, false, fmt.Errorf("%w: a copy lookup needs a board message", boardsync.ErrBadRequest)
	}
	var copy Copy
	err := p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		var err error
		copy, err = scanCopy(tx.QueryRow(ctx, `
			SELECT `+copyColumns+` FROM board_sync_copies
			WHERE tenant_id = $1 AND id = $2`, tenant, copyIDOf(key, target, boardMessage)))
		return err
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return Copy{}, false, nil
	}
	if err != nil {
		return Copy{}, false, err
	}
	return copy, true, nil
}

// CopyForMessage implements Store.
func (p *Postgres) CopyForMessage(ctx context.Context, tenant string, key boardsync.Key, boardMessage uint64) ([]Copy, error) {
	if err := expectKey(key); err != nil {
		return nil, err
	}
	out := make([]Copy, 0, 2)
	err := p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT `+copyColumns+` FROM board_sync_copies
			WHERE tenant_id = $1 AND sync_key = $2 AND board_message = $3
			ORDER BY id`, tenant, key, boardMessage)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			copy, err := scanCopy(rows)
			if err != nil {
				return err
			}
			out = append(out, copy)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// CopyByExternal implements Store.
func (p *Postgres) CopyByExternal(ctx context.Context, tenant string, key boardsync.Key, target boardsync.ResolvedTarget, externalMessage string) (Copy, bool, error) {
	if err := expectKey(key); err != nil {
		return Copy{}, false, err
	}
	if err := target.Validate(); err != nil {
		return Copy{}, false, err
	}
	if target.Key != key {
		return Copy{}, false, fmt.Errorf("%w: target key isn't the lookup key", boardsync.ErrBadRequest)
	}
	if externalMessage == "" || len(externalMessage) > 1200 {
		return Copy{}, false, fmt.Errorf("%w: an external message ID is required", boardsync.ErrBadRequest)
	}
	var copy Copy
	err := p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		var err error
		copy, err = scanCopy(tx.QueryRow(ctx, `
			SELECT `+copyColumns+` FROM board_sync_copies
			WHERE tenant_id = $1 AND sync_key = $2 AND platform = $3
				AND channel = $4 AND thread = $5 AND subthread = $6 AND kind = $7
				AND external_message = $8
			ORDER BY updated_at DESC LIMIT 1`,
			tenant, key, target.Platform, target.Channel, target.Thread,
			target.Subthread, target.Kind, externalMessage))
		return err
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return Copy{}, false, nil
	}
	if err != nil {
		return Copy{}, false, err
	}
	return copy, true, nil
}

// CopyByExternalMessage implements Store.
func (p *Postgres) CopyByExternalMessage(ctx context.Context, tenant string, key boardsync.Key, externalMessage string) (Copy, bool, error) {
	if err := expectKey(key); err != nil {
		return Copy{}, false, err
	}
	if externalMessage == "" || len(externalMessage) > 1200 {
		return Copy{}, false, fmt.Errorf("%w: an external message ID is required", boardsync.ErrBadRequest)
	}
	var copies []Copy
	err := p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT `+copyColumns+` FROM board_sync_copies
			WHERE tenant_id = $1 AND sync_key = $2 AND external_message = $3
			ORDER BY updated_at DESC LIMIT 2`, tenant, key, externalMessage)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			copy, err := scanCopy(rows)
			if err != nil {
				return err
			}
			copies = append(copies, copy)
		}
		return rows.Err()
	})
	if err != nil {
		return Copy{}, false, err
	}
	switch len(copies) {
	case 0:
		return Copy{}, false, nil
	case 1:
		return copies[0], true, nil
	default:
		return Copy{}, false, fmt.Errorf("%w: external message %q has several copies", boardsync.ErrBadRequest, externalMessage)
	}
}

// SaveCopy implements Store.
func (p *Postgres) SaveCopy(ctx context.Context, tenant string, copy Copy) (Copy, error) {
	if err := copy.Validate(); err != nil {
		return Copy{}, err
	}
	copy.ID = copyIDOf(copy.Key, copy.Target, copy.BoardMessage)
	err := p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO board_sync_copies
				(tenant_id, id, board_message, board_thread, sync_key, platform,
				 channel, thread, subthread, kind, external_thread, external_message,
				 files, files_set)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
			ON CONFLICT (tenant_id, id) DO UPDATE
			SET external_thread = EXCLUDED.external_thread,
				external_message = EXCLUDED.external_message,
				files = EXCLUDED.files,
				files_set = EXCLUDED.files_set,
				updated_at = now()`,
			tenant, copy.ID, copy.BoardMessage, copy.BoardThread, copy.Key,
			copy.Target.Platform, copy.Target.Channel, copy.Target.Thread,
			copy.Target.Subthread, copy.Target.Kind, copy.ExternalThread, copy.ExternalMessage,
			nonNilFiles(copy.Files), copy.FilesSet)
		return err
	})
	if err != nil {
		return Copy{}, err
	}
	return copy, nil
}

// TargetFailures implements Store.
func (p *Postgres) TargetFailures(ctx context.Context, tenant string, key boardsync.Key, limit int) ([]Delivery, error) {
	if err := expectKey(key); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 100
	}
	out := make([]Delivery, 0)
	err := p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT `+deliveryColumns+` FROM board_sync_outbox
			WHERE tenant_id = $1 AND sync_key = $2
				AND state = 'dead' AND cleared_at IS NULL
			ORDER BY id LIMIT $3`, tenant, key, limit)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			delivery, _, _, err := scanDelivery(rows)
			if err != nil {
				return err
			}
			delivery.Lease = ""
			out = append(out, delivery)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

const threadColumns = `tenant_id, id, board_thread, sync_key, platform, channel,
	thread, subthread, kind, created_at, updated_at`

func scanThread(row scanner) (Thread, error) {
	var thread Thread
	err := row.Scan(&thread.Tenant, &thread.ID, &thread.BoardThread, &thread.Key,
		&thread.Platform, &thread.Channel, &thread.Thread, &thread.Subthread,
		&thread.Kind, &thread.CreatedAt, &thread.UpdatedAt)
	return thread, err
}

// UpsertThread implements Store.
func (p *Postgres) UpsertThread(ctx context.Context, thread Thread) (Thread, error) {
	if err := thread.Validate(); err != nil {
		return Thread{}, err
	}
	thread.ID = threadIDOf(thread.BoardThread, thread.Key, boardsync.ResolvedTarget{
		Key: thread.Key, Platform: thread.Platform, Channel: thread.Channel,
		Thread: thread.Thread, Subthread: thread.Subthread, Kind: thread.Kind,
	})
	var stored Thread
	err := p.withTenant(ctx, thread.Tenant, func(tx pgx.Tx) error {
		var err error
		stored, err = scanThread(tx.QueryRow(ctx, `
			INSERT INTO board_sync_threads
				(tenant_id, id, board_thread, sync_key, platform, channel, thread, subthread, kind)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			ON CONFLICT (tenant_id, id) DO UPDATE
			SET platform = EXCLUDED.platform, channel = EXCLUDED.channel,
				thread = EXCLUDED.thread, subthread = EXCLUDED.subthread,
				kind = EXCLUDED.kind, updated_at = now()
			RETURNING `+threadColumns,
			thread.Tenant, thread.ID, thread.BoardThread, thread.Key, thread.Platform,
			thread.Channel, thread.Thread, thread.Subthread, thread.Kind))
		return err
	})
	if err != nil {
		return Thread{}, err
	}
	return stored, nil
}

// Threads implements Store.
func (p *Postgres) Threads(ctx context.Context, tenant string, boardThread uint64) ([]Thread, error) {
	out := make([]Thread, 0)
	err := p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT `+threadColumns+` FROM board_sync_threads
			WHERE tenant_id = $1 AND board_thread = $2 ORDER BY id`, tenant, boardThread)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			thread, err := scanThread(rows)
			if err != nil {
				return err
			}
			out = append(out, thread)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ThreadByTarget implements Store.
func (p *Postgres) ThreadByTarget(ctx context.Context, tenant string, key boardsync.Key, target boardsync.ResolvedTarget) (Thread, bool, error) {
	if err := expectKey(key); err != nil {
		return Thread{}, false, err
	}
	if target.Key != key {
		return Thread{}, false, fmt.Errorf("%w: target key isn't the lookup key", boardsync.ErrBadRequest)
	}
	if err := target.Validate(); err != nil {
		return Thread{}, false, err
	}
	var thread Thread
	err := p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		var err error
		thread, err = scanThread(tx.QueryRow(ctx, `
			SELECT `+threadColumns+` FROM board_sync_threads
			WHERE tenant_id = $1 AND sync_key = $2 AND platform = $3
				AND channel = $4 AND thread = $5 AND subthread = $6 AND kind = $7`,
			tenant, key, target.Platform, target.Channel, target.Thread,
			target.Subthread, target.Kind))
		return err
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return Thread{}, false, nil
	}
	if err != nil {
		return Thread{}, false, err
	}
	return thread, true, nil
}

// ThreadSynced implements Store.
func (p *Postgres) ThreadSynced(ctx context.Context, tenant string, key boardsync.Key, external string) (bool, error) {
	if err := expectKey(key); err != nil {
		return false, err
	}
	if err := expectLabel(external, "external thread", 1200); err != nil {
		return false, err
	}
	var synced bool
	err := p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM board_sync_threads
				WHERE tenant_id = $1 AND sync_key = $2
					AND ($2 || ':' || CASE WHEN thread = '' THEN channel ELSE thread END) = $3
			)`, tenant, key, external).Scan(&synced)
	})
	if err != nil {
		return false, err
	}
	return synced, nil
}

// RemoveThread implements Store.
func (p *Postgres) RemoveThread(ctx context.Context, tenant, id string) error {
	if id == "" {
		return fmt.Errorf("%w: a thread removal needs an ID", boardsync.ErrBadRequest)
	}
	return p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			DELETE FROM board_sync_threads WHERE tenant_id = $1 AND id = $2`, tenant, id)
		return err
	})
}

// Cursor implements Store.
func (p *Postgres) Cursor(ctx context.Context, tenant string, key boardsync.Key, source string) (string, bool, error) {
	if err := expectKey(key); err != nil {
		return "", false, err
	}
	if err := expectLabel(source, "source", 200); err != nil {
		return "", false, err
	}
	var cursor string
	err := p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `
			SELECT cursor FROM board_sync_source_cursors
			WHERE tenant_id = $1 AND sync_key = $2 AND source = $3`, tenant, key, source).Scan(&cursor)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return cursor, true, nil
}

// SetCursor implements Store.
func (p *Postgres) SetCursor(ctx context.Context, tenant string, key boardsync.Key, source, cursor string) error {
	if err := expectKey(key); err != nil {
		return err
	}
	if err := expectLabel(source, "source", 200); err != nil {
		return err
	}
	if err := validCursor(cursor); err != nil {
		return err
	}
	return p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO board_sync_source_cursors (tenant_id, sync_key, source, cursor)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (tenant_id, sync_key, source) DO UPDATE
			SET cursor = EXCLUDED.cursor, updated_at = now()`, tenant, key, source, cursor)
		return err
	})
}

// ClaimNonce implements Store.
func (p *Postgres) ClaimNonce(ctx context.Context, tenant string, key boardsync.Key, nonce string, ttl time.Duration) (bool, error) {
	if err := expectKey(key); err != nil {
		return false, err
	}
	if err := expectLabel(nonce, "nonce", 500); err != nil {
		return false, err
	}
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	var claimed string
	err := p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `
			INSERT INTO board_sync_bridge_nonces (tenant_id, sync_key, nonce, expires_at)
			VALUES ($1, $2, $3, now() + make_interval(secs => $4))
			ON CONFLICT (tenant_id, sync_key, nonce) DO UPDATE
			SET seen_at = now(), expires_at = EXCLUDED.expires_at
			WHERE board_sync_bridge_nonces.expires_at <= now()
			RETURNING nonce`, tenant, key, nonce, ttl.Seconds()).Scan(&claimed)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// PruneNonces implements Store.
func (p *Postgres) PruneNonces(ctx context.Context, tenant string, before time.Time) (int64, error) {
	if before.IsZero() {
		before = time.Now()
	}
	var removed int64
	err := p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			DELETE FROM board_sync_bridge_nonces
			WHERE tenant_id = $1 AND expires_at <= $2`, tenant, before)
		if err != nil {
			return err
		}
		removed = tag.RowsAffected()
		return nil
	})
	if err != nil {
		return 0, err
	}
	return removed, nil
}

var (
	_ Store = (*Postgres)(nil)
)
