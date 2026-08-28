CREATE TABLE IF NOT EXISTS failed_event_queue_receipts (
  failed_event_id TEXT NOT NULL,
  channel TEXT NOT NULL CHECK (channel IN ('RETRY', 'DLQ')),
  state TEXT NOT NULL CHECK (state IN ('PENDING', 'CONFIRMED')),
  owner TEXT,
  fence INTEGER NOT NULL DEFAULT 0 CHECK (fence >= 0),
  lease_expires_at INTEGER,
  next_attempt_at INTEGER NOT NULL,
  delivery_delay_seconds INTEGER NOT NULL DEFAULT 0
    CHECK (delivery_delay_seconds >= 0 AND delivery_delay_seconds <= 43200),
  -- Durable processing attempt for which a fresh transport pointer was last
  -- rolled over. -1 means only the producer's original pointer was sent.
  source_attempt INTEGER NOT NULL DEFAULT -1 CHECK (source_attempt >= -1),
  failure_count INTEGER NOT NULL DEFAULT 0 CHECK (failure_count >= 0),
  confirmed_at INTEGER,
  refresh_at INTEGER,
  last_error TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  expires_at INTEGER NOT NULL,
  PRIMARY KEY (failed_event_id, channel)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS failed_event_queue_receipts_due_idx
  ON failed_event_queue_receipts
    (channel, state, next_attempt_at, failed_event_id);

CREATE INDEX IF NOT EXISTS failed_event_queue_receipts_refresh_idx
  ON failed_event_queue_receipts
    (channel, state, refresh_at, failed_event_id);

CREATE INDEX IF NOT EXISTS failed_event_queue_receipts_expiry_idx
  ON failed_event_queue_receipts (expires_at, failed_event_id, channel);

CREATE INDEX IF NOT EXISTS failed_event_queue_receipts_terminal_cleanup_idx
  ON failed_event_queue_receipts (channel, updated_at, failed_event_id);

-- The backend's generic retry processor must never scan or claim callback rows.
-- These partial expression indexes make that ownership predicate indexable
-- without adding a second mutable source of truth beside metadata.
CREATE INDEX IF NOT EXISTS workflow_failed_events_non_callback_retry_idx
  ON workflow_failed_events (status, next_retry_at, id)
  WHERE coalesce(
    CASE WHEN json_valid(metadata)
      THEN json_extract(metadata, '$.recoveryOwner')
    END,
    ''
  ) <> 'callback-queue';

CREATE INDEX IF NOT EXISTS workflow_failed_events_non_callback_lease_idx
  ON workflow_failed_events (status, updated_at, id)
  WHERE coalesce(
    CASE WHEN json_valid(metadata)
      THEN json_extract(metadata, '$.recoveryOwner')
    END,
    ''
  ) <> 'callback-queue';

-- The bounded receipt-repair sweep discovers callback rows missing a receipt
-- through this index; it never performs an unindexed JSON fleet scan.
CREATE INDEX IF NOT EXISTS workflow_failed_events_callback_recovery_idx
  ON workflow_failed_events (status, updated_at, id)
  WHERE CASE WHEN json_valid(metadata)
    THEN json_extract(metadata, '$.recoveryOwner')
  END = 'callback-queue';
