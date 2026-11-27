-- Automation identity and idempotency metadata added during review.
--
-- creator_kind and creator_via keep the stored creator's actor kind and Via,
-- so a trigger-time permission check reconstructs the same caller a policy
-- saw at creation. creation_key/creation_digest make a POST /schedules retry
-- answer the first plan; the partial unique index is per tenant, so a key can
-- never name two schedules and an empty key never conflicts.

ALTER TABLE board_schedules
    ADD COLUMN IF NOT EXISTS creator_kind text NOT NULL DEFAULT '';
ALTER TABLE board_schedules
    ADD COLUMN IF NOT EXISTS creator_via jsonb;
ALTER TABLE board_schedules
    ADD COLUMN IF NOT EXISTS creation_key text NOT NULL DEFAULT '';
ALTER TABLE board_schedules
    ADD COLUMN IF NOT EXISTS creation_digest text NOT NULL DEFAULT '';

CREATE UNIQUE INDEX IF NOT EXISTS board_schedules_creation
    ON board_schedules (tenant_id, creation_key)
    WHERE creation_key <> '';
