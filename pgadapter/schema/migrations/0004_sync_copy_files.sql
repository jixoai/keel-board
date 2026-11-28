-- Copy files travel with the copy, not only with the receipt: a crash after a
-- platform post but before message.synced must not lose the file IDs. The
-- board_sync_copies row is written before the receipt, so the retry reads the
-- files from here.

ALTER TABLE board_sync_copies
    ADD COLUMN IF NOT EXISTS files text[] NOT NULL DEFAULT '{}';

ALTER TABLE board_sync_copies
    ADD COLUMN IF NOT EXISTS files_set boolean NOT NULL DEFAULT false;
