-- One external conversation maps to one board thread. A channel target may
-- repeat (several board roots start threads in one channel), but a concrete
-- thread target is unique per connection. Host.Record and the attach API check
-- this under the target's advisory lock; the index is the cross-process
-- guarantee.
--
-- since_seq is the board sequence at which the mapping began. A thread target
-- delivers only messages with a sequence greater than it, so attaching a
-- thread is future-only even across a restart.

ALTER TABLE board_sync_threads
    ADD COLUMN IF NOT EXISTS since_seq bigint NOT NULL DEFAULT 0;

CREATE UNIQUE INDEX IF NOT EXISTS board_sync_threads_unique_thread
    ON board_sync_threads (tenant_id, sync_key, platform, channel, thread, subthread)
    WHERE kind = 'thread';
