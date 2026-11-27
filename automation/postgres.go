package automation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jixoai/keel-board/protocol"
	"github.com/jixoai/keel-board/store"
)

// PostgresOptions configure a PostgreSQL automation store.
type PostgresOptions struct {
	// DSN is the database. It is required.
	DSN string
	// SearchPath confines every automation table to one schema. Tests use it
	// for isolation; a deployment may leave it empty for the database default.
	SearchPath string
	// Config bounds retries and leases.
	Config Config
}

// Postgres is a PostgreSQL-backed Store. Each lease is one transaction that
// takes an advisory lock for its tenant, so two workers can't hand out the same
// row while different tenants proceed in parallel. Every statement carries the
// tenant predicate.
type Postgres struct {
	pool       *pgxpool.Pool
	cfg        Config
	searchPath string
}

// OpenPostgres connects with opts, applies the board and automation schema,
// and returns a store. The schema is idempotent, so opening a migrated
// database again changes nothing.
func OpenPostgres(ctx context.Context, opts PostgresOptions) (*Postgres, error) {
	if strings.TrimSpace(opts.DSN) == "" {
		return nil, errors.New("automation: a PostgreSQL DSN is required")
	}
	cfg, err := opts.Config.withDefaults()
	if err != nil {
		return nil, err
	}
	config, err := pgxpool.ParseConfig(opts.DSN)
	if err != nil {
		return nil, fmt.Errorf("automation: parsing the PostgreSQL DSN: %w", err)
	}
	if opts.SearchPath != "" {
		config.ConnConfig.RuntimeParams["search_path"] = pgx.Identifier{opts.SearchPath}.Sanitize()
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("automation: opening PostgreSQL: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("automation: connecting to PostgreSQL: %w", err)
	}
	if err := store.ApplyMigrations(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}
	searchPath := ""
	if opts.SearchPath != "" {
		searchPath = pgx.Identifier{opts.SearchPath}.Sanitize()
	}
	return &Postgres{pool: pool, cfg: cfg, searchPath: searchPath}, nil
}

// Close closes the connection pool.
func (p *Postgres) Close() { p.pool.Close() }

// withTenant runs fn in a transaction whose session variable names the tenant.
// Every automation table has FORCE ROW LEVEL SECURITY with a policy over that
// variable, so a statement without this setting sees and writes no rows.
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
	if p.searchPath != "" {
		if _, err := tx.Exec(ctx, `SELECT set_config('search_path', $1, true)`, p.searchPath); err != nil {
			return err
		}
	}
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

type scanner interface {
	Scan(dest ...any) error
}

const watcherColumns = `tenant_id, watcher_id, channel, filter, action, bot_id, enabled, cursor_seq, cursor_set, created_at, updated_at`

func scanWatcher(row scanner) (Watcher, error) {
	var (
		watcher        Watcher
		filter, action []byte
		cursorSeq      int64
	)
	if err := row.Scan(&watcher.Tenant, &watcher.ID, &watcher.Channel, &filter, &action,
		&watcher.Bot, &watcher.Enabled, &cursorSeq, &watcher.CursorSet,
		&watcher.CreatedAt, &watcher.UpdatedAt); err != nil {
		return Watcher{}, err
	}
	watcher.Cursor = uint64(cursorSeq)
	if len(filter) > 0 {
		if err := json.Unmarshal(filter, &watcher.Filter); err != nil {
			return Watcher{}, fmt.Errorf("automation: decoding watcher %s filter: %w", watcher.ID, err)
		}
	}
	if len(action) > 0 {
		if err := json.Unmarshal(action, &watcher.Action); err != nil {
			return Watcher{}, fmt.Errorf("automation: decoding watcher %s action: %w", watcher.ID, err)
		}
	}
	return watcher, nil
}

// CreateWatcher implements Store.
func (p *Postgres) CreateWatcher(ctx context.Context, watcher Watcher) (Watcher, error) {
	if err := watcher.Validate(); err != nil {
		return Watcher{}, err
	}
	filter, err := json.Marshal(watcher.Filter)
	if err != nil {
		return Watcher{}, protocol.Invalid("filter: it isn't JSON")
	}
	action, err := json.Marshal(watcher.Action)
	if err != nil {
		return Watcher{}, protocol.Invalid("action: it isn't JSON")
	}
	var stored Watcher
	err = p.withTenant(ctx, watcher.Tenant, func(tx pgx.Tx) error {
		var err error
		stored, err = scanWatcher(tx.QueryRow(ctx, `
			INSERT INTO board_watchers
				(tenant_id, watcher_id, channel, filter, action, bot_id, enabled, cursor_seq, cursor_set)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			RETURNING `+watcherColumns,
			watcher.Tenant, watcher.ID, watcher.Channel, filter, action, watcher.Bot,
			watcher.Enabled, int64(watcher.Cursor), watcher.CursorSet || watcher.Cursor > 0))
		return err
	})
	if isUniqueViolation(err) {
		return Watcher{}, ErrExists
	}
	if err != nil {
		return Watcher{}, err
	}
	return stored, nil
}

// Watcher implements Store.
func (p *Postgres) Watcher(ctx context.Context, tenant, id string) (Watcher, bool, error) {
	var stored Watcher
	err := p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		var err error
		stored, err = scanWatcher(tx.QueryRow(ctx, `
			SELECT `+watcherColumns+` FROM board_watchers
			WHERE tenant_id = $1 AND watcher_id = $2`, tenant, id))
		return err
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return Watcher{}, false, nil
	}
	if err != nil {
		return Watcher{}, false, err
	}
	return stored, true, nil
}

// Watchers implements Store.
func (p *Postgres) Watchers(ctx context.Context, tenant string) ([]Watcher, error) {
	var out []Watcher
	err := p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT `+watcherColumns+` FROM board_watchers
			WHERE tenant_id = $1 ORDER BY watcher_id`, tenant)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			watcher, err := scanWatcher(rows)
			if err != nil {
				return err
			}
			out = append(out, watcher)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// UpdateWatcher implements Store.
func (p *Postgres) UpdateWatcher(ctx context.Context, watcher Watcher) (Watcher, error) {
	if err := watcher.Validate(); err != nil {
		return Watcher{}, err
	}
	filter, err := json.Marshal(watcher.Filter)
	if err != nil {
		return Watcher{}, protocol.Invalid("filter: it isn't JSON")
	}
	action, err := json.Marshal(watcher.Action)
	if err != nil {
		return Watcher{}, protocol.Invalid("action: it isn't JSON")
	}
	var stored Watcher
	err = p.withTenant(ctx, watcher.Tenant, func(tx pgx.Tx) error {
		var err error
		stored, err = scanWatcher(tx.QueryRow(ctx, `
			UPDATE board_watchers
			SET channel = $3, filter = $4, action = $5, bot_id = $6, updated_at = now()
			WHERE tenant_id = $1 AND watcher_id = $2
			RETURNING `+watcherColumns,
			watcher.Tenant, watcher.ID, watcher.Channel, filter, action, watcher.Bot))
		return err
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return Watcher{}, ErrNotFound
	}
	if err != nil {
		return Watcher{}, err
	}
	return stored, nil
}

// SetWatcherEnabled implements Store.
func (p *Postgres) SetWatcherEnabled(ctx context.Context, tenant, id string, enabled bool, cursor *uint64) (Watcher, error) {
	var cursorValue *int64
	if cursor != nil {
		value := int64(*cursor)
		cursorValue = &value
	}
	var stored Watcher
	err := p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		var err error
		stored, err = scanWatcher(tx.QueryRow(ctx, `
			UPDATE board_watchers
			SET enabled = $3,
				cursor_seq = GREATEST(cursor_seq, COALESCE($4::bigint, cursor_seq)),
				cursor_set = cursor_set OR $4::bigint IS NOT NULL,
				updated_at = now()
			WHERE tenant_id = $1 AND watcher_id = $2
			RETURNING `+watcherColumns, tenant, id, enabled, cursorValue))
		return err
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return Watcher{}, ErrNotFound
	}
	if err != nil {
		return Watcher{}, err
	}
	return stored, nil
}

// DeleteWatcher implements Store.
func (p *Postgres) DeleteWatcher(ctx context.Context, tenant, id string) error {
	return p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			DELETE FROM board_watch_deliveries WHERE tenant_id = $1 AND watcher_id = $2`, tenant, id); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `
			DELETE FROM board_watchers WHERE tenant_id = $1 AND watcher_id = $2`, tenant, id)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrNotFound
		}
		return nil
	})
}

// AdvanceWatcher implements Store.
func (p *Postgres) AdvanceWatcher(ctx context.Context, tenant, id string, cursor uint64, events []uint64) (int, error) {
	added := 0
	err := p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		var current int64
		var enabled bool
		err := tx.QueryRow(ctx, `
			SELECT cursor_seq, enabled FROM board_watchers
			WHERE tenant_id = $1 AND watcher_id = $2
			FOR UPDATE`, tenant, id).Scan(&current, &enabled)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if !enabled {
			return ErrWatcherDisabled
		}
		if cursor <= uint64(current) {
			_, err := tx.Exec(ctx, `
				UPDATE board_watchers SET cursor_set = true, updated_at = now()
				WHERE tenant_id = $1 AND watcher_id = $2`, tenant, id)
			return err
		}
		seen := make(map[uint64]struct{}, len(events))
		sorted := make([]uint64, 0, len(events))
		for _, seq := range events {
			if seq == 0 || seq <= uint64(current) || seq > cursor {
				continue
			}
			if _, duplicate := seen[seq]; duplicate {
				continue
			}
			seen[seq] = struct{}{}
			sorted = append(sorted, seq)
		}
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
		for _, seq := range sorted {
			tag, err := tx.Exec(ctx, `
				INSERT INTO board_watch_deliveries (tenant_id, watcher_id, event_seq)
				VALUES ($1, $2, $3)
				ON CONFLICT (tenant_id, watcher_id, event_seq) DO NOTHING`,
				tenant, id, int64(seq))
			if err != nil {
				return err
			}
			added += int(tag.RowsAffected())
		}
		_, err = tx.Exec(ctx, `
			UPDATE board_watchers SET cursor_seq = $3, cursor_set = true, updated_at = now()
			WHERE tenant_id = $1 AND watcher_id = $2`, tenant, id, int64(cursor))
		return err
	})
	if err != nil {
		return 0, err
	}
	return added, nil
}

// deliveryColumns selects a delivery without its lease credential.
const deliveryColumns = `tenant_id, watcher_id, event_seq, state, attempts, next_at,
	last_error, lease_owner, lease_token, lease_until, created_at, updated_at, completed_at`

func scanDelivery(row scanner) (Delivery, error) {
	var (
		delivery   Delivery
		leaseUntil pgtype.Timestamptz
		completed  pgtype.Timestamptz
		eventSeq   int64
	)
	if err := row.Scan(&delivery.Tenant, &delivery.WatcherID, &eventSeq, &delivery.State,
		&delivery.Attempts, &delivery.NextAt, &delivery.LastError, &delivery.LeaseOwner,
		&delivery.Lease, &leaseUntil, &delivery.CreatedAt, &delivery.UpdatedAt, &completed); err != nil {
		return Delivery{}, err
	}
	delivery.EventSeq = uint64(eventSeq)
	if leaseUntil.Valid {
		delivery.LeaseUntil = leaseUntil.Time
	}
	if completed.Valid {
		at := completed.Time
		delivery.Completed = &at
	}
	return delivery, nil
}

// LeaseDeliveries implements Store.
func (p *Postgres) LeaseDeliveries(ctx context.Context, tenant string, limit int, owner string) ([]Delivery, error) {
	if limit <= 0 {
		limit = 10
	}
	owner = leaseOwner(owner)
	out := make([]Delivery, 0, limit)
	err := p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, advisoryLockKey("watch-deliveries", tenant)); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `
			SELECT watcher_id, event_seq FROM (
				SELECT watcher_id, event_seq, state, next_at, lease_until,
					row_number() OVER (PARTITION BY watcher_id ORDER BY event_seq) AS position
				FROM board_watch_deliveries
				WHERE tenant_id = $1 AND state IN ('pending', 'leased')
					AND watcher_id IN (
						SELECT watcher_id FROM board_watchers
						WHERE tenant_id = $1 AND enabled
					)
			) ranked
			WHERE position = 1
				AND ((state = 'pending' AND next_at <= now())
					OR (state = 'leased' AND lease_until <= now()))
			ORDER BY event_seq
			LIMIT $2`, tenant, limit)
		if err != nil {
			return err
		}
		type head struct {
			watcherID string
			eventSeq  int64
		}
		heads := make([]head, 0, limit)
		for rows.Next() {
			var item head
			if err := rows.Scan(&item.watcherID, &item.eventSeq); err != nil {
				rows.Close()
				return err
			}
			heads = append(heads, item)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, item := range heads {
			token, err := newLeaseToken()
			if err != nil {
				return err
			}
			delivery, err := scanDelivery(tx.QueryRow(ctx, `
				UPDATE board_watch_deliveries
				SET state = 'leased', lease_owner = $4, lease_token = $5,
					lease_until = now() + make_interval(secs => $6), updated_at = now()
				WHERE tenant_id = $1 AND watcher_id = $2 AND event_seq = $3
					AND ((state = 'pending' AND next_at <= now())
						OR (state = 'leased' AND lease_until <= now()))
				RETURNING `+deliveryColumns,
				tenant, item.watcherID, item.eventSeq, owner, token, p.cfg.LeaseTTL.Seconds()))
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}
			if err != nil {
				return err
			}
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

// AckDelivery implements Store.
func (p *Postgres) AckDelivery(ctx context.Context, tenant, watcherID string, eventSeq uint64, lease string, jobErr error) (AckResult, error) {
	var result AckResult
	err := p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		var (
			state     string
			attempts  int
			token     string
			leaseLive bool
		)
		err := tx.QueryRow(ctx, `
			SELECT state, attempts, lease_token, COALESCE(lease_until > now(), false)
			FROM board_watch_deliveries
			WHERE tenant_id = $1 AND watcher_id = $2 AND event_seq = $3
			FOR UPDATE`, tenant, watcherID, int64(eventSeq)).
			Scan(&state, &attempts, &token, &leaseLive)
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
				UPDATE board_watch_deliveries
				SET state = 'done', lease_owner = '', lease_token = '', lease_until = NULL,
					updated_at = now(), completed_at = now()
				WHERE tenant_id = $1 AND watcher_id = $2 AND event_seq = $3`,
				tenant, watcherID, int64(eventSeq))
			if err != nil {
				return err
			}
			result = AckResult{State: StateDone, Attempts: attempts, Acked: true}
			return nil
		}
		attempts++
		code := ErrorCode(jobErr)
		if IsPermanent(jobErr) || attempts >= p.cfg.Retry.MaxAttempts {
			_, err = tx.Exec(ctx, `
				UPDATE board_watch_deliveries
				SET state = 'dead', attempts = $4, last_error = $5,
					lease_owner = '', lease_token = '', lease_until = NULL,
					updated_at = now(), completed_at = now()
				WHERE tenant_id = $1 AND watcher_id = $2 AND event_seq = $3`,
				tenant, watcherID, int64(eventSeq), attempts, code)
			if err != nil {
				return err
			}
			result = AckResult{State: StateDead, Attempts: attempts, Dead: true}
			return nil
		}
		_, err = tx.Exec(ctx, `
			UPDATE board_watch_deliveries
			SET state = 'pending', attempts = $4, last_error = $5,
				next_at = now() + make_interval(secs => $6),
				lease_owner = '', lease_token = '', lease_until = NULL,
				updated_at = now(), completed_at = NULL
			WHERE tenant_id = $1 AND watcher_id = $2 AND event_seq = $3`,
			tenant, watcherID, int64(eventSeq), attempts, code, p.cfg.Retry.Delay(attempts).Seconds())
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

// ReleaseDelivery implements Store.
func (p *Postgres) ReleaseDelivery(ctx context.Context, tenant, watcherID string, eventSeq uint64, lease string, delay time.Duration) error {
	if delay < 0 {
		delay = 0
	}
	return p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			UPDATE board_watch_deliveries
			SET state = 'pending', next_at = now() + make_interval(secs => $5),
				lease_owner = '', lease_token = '', lease_until = NULL, updated_at = now()
			WHERE tenant_id = $1 AND watcher_id = $2 AND event_seq = $3
				AND state = 'leased' AND lease_token = $4 AND lease_until > now()`,
			tenant, watcherID, int64(eventSeq), lease, delay.Seconds())
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrLeaseLost
		}
		return nil
	})
}

// Deliveries implements Store.
func (p *Postgres) Deliveries(ctx context.Context, tenant, watcherID string, limit int) ([]Delivery, error) {
	return p.listDeliveries(ctx, tenant, watcherID, "", limit)
}

// DeadDeliveries implements Store.
func (p *Postgres) DeadDeliveries(ctx context.Context, tenant, watcherID string, limit int) ([]Delivery, error) {
	return p.listDeliveries(ctx, tenant, watcherID, StateDead, limit)
}

func (p *Postgres) listDeliveries(ctx context.Context, tenant, watcherID, state string, limit int) ([]Delivery, error) {
	if limit <= 0 {
		limit = 100
	}
	var out []Delivery
	err := p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		query := `SELECT ` + deliveryColumns + ` FROM board_watch_deliveries
			WHERE tenant_id = $1 AND watcher_id = $2`
		args := []any{tenant, watcherID}
		if state != "" {
			query += ` AND state = $3`
			args = append(args, state)
		}
		query += ` ORDER BY event_seq LIMIT $` + fmt.Sprint(len(args)+1)
		args = append(args, limit)
		rows, err := tx.Query(ctx, query, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			delivery, err := scanDelivery(rows)
			if err != nil {
				return err
			}
			out = append(out, delivery)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// RetryDelivery implements Store.
func (p *Postgres) RetryDelivery(ctx context.Context, tenant, watcherID string, eventSeq uint64) error {
	return p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			UPDATE board_watch_deliveries
			SET state = 'pending', attempts = 0, last_error = '', next_at = now(),
				completed_at = NULL, updated_at = now()
			WHERE tenant_id = $1 AND watcher_id = $2 AND event_seq = $3 AND state = 'dead'`,
			tenant, watcherID, int64(eventSeq))
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrNotDead
		}
		return nil
	})
}

// ClearDeliveries implements Store.
func (p *Postgres) ClearDeliveries(ctx context.Context, tenant, watcherID string) (int64, error) {
	var removed int64
	err := p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		var exists bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (SELECT 1 FROM board_watchers WHERE tenant_id = $1 AND watcher_id = $2)`,
			tenant, watcherID).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return ErrNotFound
		}
		tag, err := tx.Exec(ctx, `
			DELETE FROM board_watch_deliveries
			WHERE tenant_id = $1 AND watcher_id = $2 AND state = 'dead'`, tenant, watcherID)
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

// DeliveryDepth implements Store.
func (p *Postgres) DeliveryDepth(ctx context.Context, tenant, watcherID string) (Depth, error) {
	var depth Depth
	err := p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `
			SELECT
				count(*) FILTER (WHERE state = 'pending'),
				count(*) FILTER (WHERE state = 'leased'),
				count(*) FILTER (WHERE state = 'done'),
				count(*) FILTER (WHERE state = 'dead')
			FROM board_watch_deliveries
			WHERE tenant_id = $1 AND watcher_id = $2`, tenant, watcherID).
			Scan(&depth.Pending, &depth.Leased, &depth.Done, &depth.Dead)
	})
	if err != nil {
		return Depth{}, err
	}
	return depth, nil
}

const scheduleColumns = `tenant_id, schedule_id, channel, COALESCE(re, 0), text, labels,
	creator_actor, next_run_at, COALESCE(every_seconds, 0), until_at, COALESCE(times, 0),
	sent, status, attempts, last_error, cancel_reason, lease_owner, lease_token,
	lease_until, created_at, updated_at, completed_at`

func scanSchedule(row scanner) (Schedule, error) {
	var (
		schedule     Schedule
		re           int64
		everySeconds int
		times        int
		labels       []byte
		untilAt      pgtype.Timestamptz
		leaseUntil   pgtype.Timestamptz
		completed    pgtype.Timestamptz
	)
	if err := row.Scan(&schedule.Tenant, &schedule.ID, &schedule.Channel, &re,
		&schedule.Text, &labels, &schedule.Creator, &schedule.NextRunAt,
		&everySeconds, &untilAt, &times, &schedule.Sent, &schedule.Status,
		&schedule.Attempts, &schedule.LastError, &schedule.CancelReason,
		&schedule.LeaseOwner, &schedule.Lease, &leaseUntil,
		&schedule.CreatedAt, &schedule.UpdatedAt, &completed); err != nil {
		return Schedule{}, err
	}
	if len(labels) > 0 {
		if err := json.Unmarshal(labels, &schedule.Labels); err != nil {
			return Schedule{}, fmt.Errorf("automation: decoding schedule %s labels: %w", schedule.ID, err)
		}
	}
	if re != 0 {
		value := uint64(re)
		schedule.Re = &value
	}
	schedule.EverySeconds = everySeconds
	schedule.Times = times
	if untilAt.Valid {
		value := untilAt.Time
		schedule.UntilAt = &value
	}
	if leaseUntil.Valid {
		schedule.LeaseUntil = leaseUntil.Time
	}
	if completed.Valid {
		value := completed.Time
		schedule.Completed = &value
	}
	return schedule, nil
}

func marshalLabels(labels map[string]string) ([]byte, error) {
	if labels == nil {
		labels = map[string]string{}
	}
	raw, err := json.Marshal(labels)
	if err != nil {
		return nil, protocol.Invalid("labels: they aren't JSON")
	}
	return raw, nil
}

// CreateSchedule implements Store.
func (p *Postgres) CreateSchedule(ctx context.Context, schedule Schedule) (Schedule, error) {
	if schedule.ID == "" {
		id, err := NewScheduleID()
		if err != nil {
			return Schedule{}, err
		}
		schedule.ID = id
	}
	if schedule.Status == "" {
		schedule.Status = SchedulePending
	}
	if err := schedule.Validate(); err != nil {
		return Schedule{}, err
	}
	if schedule.Status != SchedulePending {
		return Schedule{}, protocol.Invalid("status: a new schedule is pending")
	}
	labels, err := marshalLabels(schedule.Labels)
	if err != nil {
		return Schedule{}, err
	}
	var re *int64
	if schedule.Re != nil {
		value := int64(*schedule.Re)
		re = &value
	}
	var every, times *int
	if schedule.EverySeconds > 0 {
		value := schedule.EverySeconds
		every = &value
	}
	if schedule.Times > 0 {
		value := schedule.Times
		times = &value
	}
	var stored Schedule
	err = p.withTenant(ctx, schedule.Tenant, func(tx pgx.Tx) error {
		var err error
		stored, err = scanSchedule(tx.QueryRow(ctx, `
			INSERT INTO board_schedules
				(tenant_id, schedule_id, channel, re, text, labels, creator_actor,
				 next_run_at, every_seconds, until_at, times, status)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'pending')
			RETURNING `+scheduleColumns,
			schedule.Tenant, schedule.ID, schedule.Channel, re, schedule.Text, labels,
			schedule.Creator, schedule.NextRunAt, every, schedule.UntilAt, times))
		return err
	})
	if isUniqueViolation(err) {
		return Schedule{}, ErrExists
	}
	if err != nil {
		return Schedule{}, err
	}
	return stored, nil
}

// Schedule implements Store.
func (p *Postgres) Schedule(ctx context.Context, tenant, id string) (Schedule, bool, error) {
	var stored Schedule
	err := p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		var err error
		stored, err = scanSchedule(tx.QueryRow(ctx, `
			SELECT `+scheduleColumns+` FROM board_schedules
			WHERE tenant_id = $1 AND schedule_id = $2`, tenant, id))
		return err
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return Schedule{}, false, nil
	}
	if err != nil {
		return Schedule{}, false, err
	}
	return stored, true, nil
}

// Schedules implements Store.
func (p *Postgres) Schedules(ctx context.Context, tenant string) ([]Schedule, error) {
	var out []Schedule
	err := p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT `+scheduleColumns+` FROM board_schedules
			WHERE tenant_id = $1 ORDER BY next_run_at, schedule_id`, tenant)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			schedule, err := scanSchedule(rows)
			if err != nil {
				return err
			}
			out = append(out, schedule)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// UpdateSchedule implements Store.
func (p *Postgres) UpdateSchedule(ctx context.Context, schedule Schedule) (Schedule, error) {
	var stored Schedule
	err := p.withTenant(ctx, schedule.Tenant, func(tx pgx.Tx) error {
		current, err := scanSchedule(tx.QueryRow(ctx, `
			SELECT `+scheduleColumns+` FROM board_schedules
			WHERE tenant_id = $1 AND schedule_id = $2
			FOR UPDATE`, schedule.Tenant, schedule.ID))
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if current.LeaseUntil.After(time.Now().UTC()) {
			return ErrLeased
		}
		updated := cloneSchedule(schedule)
		updated.Status = current.Status
		updated.Sent = current.Sent
		updated.Attempts = current.Attempts
		updated.LastError = current.LastError
		updated.CancelReason = current.CancelReason
		updated.CreatedAt = current.CreatedAt
		updated.Completed = current.Completed
		updated.Lease = ""
		updated.LeaseOwner = ""
		updated.LeaseUntil = time.Time{}
		if err := updated.Validate(); err != nil {
			return err
		}
		labels, err := marshalLabels(updated.Labels)
		if err != nil {
			return err
		}
		var re *int64
		if updated.Re != nil {
			value := int64(*updated.Re)
			re = &value
		}
		var every, times *int
		if updated.EverySeconds > 0 {
			value := updated.EverySeconds
			every = &value
		}
		if updated.Times > 0 {
			value := updated.Times
			times = &value
		}
		stored, err = scanSchedule(tx.QueryRow(ctx, `
			UPDATE board_schedules
			SET channel = $3, re = $4, text = $5, labels = $6, next_run_at = $7,
				every_seconds = $8, until_at = $9, times = $10, updated_at = now()
			WHERE tenant_id = $1 AND schedule_id = $2
			RETURNING `+scheduleColumns,
			updated.Tenant, updated.ID, updated.Channel, re, updated.Text, labels,
			updated.NextRunAt, every, updated.UntilAt, times))
		return err
	})
	if err != nil {
		return Schedule{}, err
	}
	return stored, nil
}

// SetScheduleEnabled implements Store.
func (p *Postgres) SetScheduleEnabled(ctx context.Context, tenant, id string, enabled bool, now time.Time) (Schedule, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	var stored Schedule
	err := p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		current, err := scanSchedule(tx.QueryRow(ctx, `
			SELECT `+scheduleColumns+` FROM board_schedules
			WHERE tenant_id = $1 AND schedule_id = $2
			FOR UPDATE`, tenant, id))
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if current.LeaseUntil.After(now) {
			return ErrLeased
		}
		if enabled {
			switch current.Status {
			case SchedulePending:
				if current.NextRunAt.Before(now) {
					current.NextRunAt = now
				}
			case ScheduleDisabled, ScheduleDead:
				current.Status = SchedulePending
				current.Attempts = 0
				current.LastError = ""
				current.CancelReason = ""
				current.Completed = nil
				if current.NextRunAt.Before(now) {
					current.NextRunAt = now
				}
			default:
				return ErrScheduleNotPending
			}
		} else {
			switch current.Status {
			case SchedulePending, ScheduleDisabled:
				current.Status = ScheduleDisabled
			default:
				return ErrScheduleNotPending
			}
		}
		stored, err = scanSchedule(tx.QueryRow(ctx, `
			UPDATE board_schedules
			SET status = $3, next_run_at = $4, attempts = $5, last_error = $6,
				cancel_reason = $7, completed_at = $8,
				lease_owner = '', lease_token = '', lease_until = NULL, updated_at = now()
			WHERE tenant_id = $1 AND schedule_id = $2
			RETURNING `+scheduleColumns,
			tenant, id, current.Status, current.NextRunAt, current.Attempts,
			current.LastError, current.CancelReason, current.Completed))
		return err
	})
	if err != nil {
		return Schedule{}, err
	}
	return stored, nil
}

// DeleteSchedule implements Store.
func (p *Postgres) DeleteSchedule(ctx context.Context, tenant, id string) error {
	return p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			DELETE FROM board_schedules WHERE tenant_id = $1 AND schedule_id = $2`, tenant, id)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrNotFound
		}
		return nil
	})
}

// LeaseSchedules implements Store.
func (p *Postgres) LeaseSchedules(ctx context.Context, tenant string, limit int, owner string) ([]Schedule, error) {
	if limit <= 0 {
		limit = 10
	}
	owner = leaseOwner(owner)
	out := make([]Schedule, 0, limit)
	err := p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, advisoryLockKey("watch-schedules", tenant)); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `
			SELECT schedule_id FROM board_schedules
			WHERE tenant_id = $1 AND status = 'pending' AND next_run_at <= now()
				AND (lease_until IS NULL OR lease_until <= now())
			ORDER BY next_run_at, schedule_id
			LIMIT $2`, tenant, limit)
		if err != nil {
			return err
		}
		ids := make([]string, 0, limit)
		for rows.Next() {
			var id string
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
			schedule, err := scanSchedule(tx.QueryRow(ctx, `
				UPDATE board_schedules
				SET lease_owner = $3, lease_token = $4,
					lease_until = now() + make_interval(secs => $5), updated_at = now()
				WHERE tenant_id = $1 AND schedule_id = $2 AND status = 'pending'
					AND next_run_at <= now()
					AND (lease_until IS NULL OR lease_until <= now())
				RETURNING `+scheduleColumns,
				tenant, id, owner, token, p.cfg.LeaseTTL.Seconds()))
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}
			if err != nil {
				return err
			}
			schedule.Lease = token
			out = append(out, schedule)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// AckSchedule implements Store.
func (p *Postgres) AckSchedule(ctx context.Context, tenant, id, lease string, result ScheduleResult) (Schedule, error) {
	var stored Schedule
	err := p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		schedule, err := scanSchedule(tx.QueryRow(ctx, `
			SELECT `+scheduleColumns+` FROM board_schedules
			WHERE tenant_id = $1 AND schedule_id = $2
			FOR UPDATE`, tenant, id))
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrLeaseLost
		}
		if err != nil {
			return err
		}
		if schedule.Lease != lease || !schedule.LeaseUntil.After(time.Now().UTC()) {
			return ErrLeaseLost
		}
		now := time.Now().UTC()
		if result.At.IsZero() {
			result.At = now
		}
		var (
			status       = schedule.Status
			sent         = schedule.Sent
			attempts     = schedule.Attempts
			lastError    = schedule.LastError
			cancelReason = schedule.CancelReason
			nextRunAt    = schedule.NextRunAt
			completed    *time.Time
		)
		switch result.Outcome {
		case ScheduleSent:
			sent++
			attempts = 0
			lastError = ""
			done := result.Done || schedule.EverySeconds == 0
			if !done && schedule.Times > 0 && sent >= schedule.Times {
				done = true
			}
			if !done && schedule.UntilAt != nil && !result.NextRunAt.Before(*schedule.UntilAt) {
				done = true
			}
			if done {
				status = ScheduleDone
				completed = &result.At
			} else {
				status = SchedulePending
				nextRunAt = result.NextRunAt
				if nextRunAt.IsZero() {
					nextRunAt = result.At
				}
			}
		case ScheduleRetry:
			attempts++
			lastError = ErrorCode(result.Err)
			if IsPermanent(result.Err) || attempts >= p.cfg.Retry.MaxAttempts {
				status = ScheduleDead
				completed = &now
			} else {
				status = SchedulePending
				nextRunAt = now.Add(p.cfg.Retry.Delay(attempts))
			}
		case ScheduleRevoked:
			status = ScheduleCanceled
			cancelReason = result.Reason
			if cancelReason == "" {
				cancelReason = "revoked"
			}
			completed = &now
		default:
			return protocol.Invalidf("outcome: %q isn't a schedule outcome", result.Outcome)
		}
		stored, err = scanSchedule(tx.QueryRow(ctx, `
			UPDATE board_schedules
			SET status = $3, sent = $4, attempts = $5, last_error = $6,
				cancel_reason = $7, next_run_at = $8, completed_at = $9,
				lease_owner = '', lease_token = '', lease_until = NULL, updated_at = now()
			WHERE tenant_id = $1 AND schedule_id = $2
			RETURNING `+scheduleColumns,
			tenant, id, status, sent, attempts, lastError, cancelReason, nextRunAt, completed))
		return err
	})
	if err != nil {
		return Schedule{}, err
	}
	return stored, nil
}

// ReleaseSchedule implements Store.
func (p *Postgres) ReleaseSchedule(ctx context.Context, tenant, id, lease string, delay time.Duration) error {
	if delay < 0 {
		delay = 0
	}
	return p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			UPDATE board_schedules
			SET next_run_at = CASE WHEN status = 'pending'
					THEN now() + make_interval(secs => $4) ELSE next_run_at END,
				lease_owner = '', lease_token = '', lease_until = NULL, updated_at = now()
			WHERE tenant_id = $1 AND schedule_id = $2
				AND lease_token = $3 AND lease_until > now()`,
			tenant, id, lease, delay.Seconds())
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrLeaseLost
		}
		return nil
	})
}

// RetrySchedule implements Store.
func (p *Postgres) RetrySchedule(ctx context.Context, tenant, id string) error {
	return p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		var status string
		var leaseLive bool
		err := tx.QueryRow(ctx, `
			SELECT status, COALESCE(lease_until > now(), false) FROM board_schedules
			WHERE tenant_id = $1 AND schedule_id = $2
			FOR UPDATE`, tenant, id).Scan(&status, &leaseLive)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if leaseLive {
			return ErrLeased
		}
		if status != ScheduleDead {
			return ErrNotDead
		}
		_, err = tx.Exec(ctx, `
			UPDATE board_schedules
			SET status = 'pending', attempts = 0, last_error = '', cancel_reason = '',
				completed_at = NULL, next_run_at = now(), updated_at = now()
			WHERE tenant_id = $1 AND schedule_id = $2`, tenant, id)
		return err
	})
}

// ClearSchedule implements Store.
func (p *Postgres) ClearSchedule(ctx context.Context, tenant, id string) error {
	return p.withTenant(ctx, tenant, func(tx pgx.Tx) error {
		var status string
		err := tx.QueryRow(ctx, `
			SELECT status FROM board_schedules
			WHERE tenant_id = $1 AND schedule_id = $2
			FOR UPDATE`, tenant, id).Scan(&status)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if status != ScheduleDead {
			return ErrNotDead
		}
		_, err = tx.Exec(ctx, `
			DELETE FROM board_schedules WHERE tenant_id = $1 AND schedule_id = $2`, tenant, id)
		return err
	})
}

// isUniqueViolation reports whether err is a PostgreSQL unique-index conflict.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// advisoryLockKey is the per-tenant/per-operation PostgreSQL advisory lock a
// lease transaction takes.
func advisoryLockKey(parts ...string) int64 {
	hash := fnv.New64a()
	for _, part := range parts {
		_, _ = hash.Write([]byte(part))
		_, _ = hash.Write([]byte{0})
	}
	return int64(hash.Sum64())
}

var _ Store = (*Postgres)(nil)
