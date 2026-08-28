CREATE TABLE IF NOT EXISTS workflow_dispatch_receipts (
  workflow_identity TEXT NOT NULL,
  workflow_name TEXT NOT NULL,
  instance_id TEXT NOT NULL,
  envelope_hash TEXT NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('PENDING', 'ABSENCE_PROVEN', 'CREATED')),
  owner TEXT,
  fence INTEGER NOT NULL CHECK (fence > 0),
  lease_expires_at INTEGER,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  check_after INTEGER NOT NULL,
  PRIMARY KEY (workflow_identity, instance_id)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS workflow_dispatch_receipts_cleanup_idx
  ON workflow_dispatch_receipts (
    check_after,
    workflow_identity,
    instance_id
  );
