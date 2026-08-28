import type {
  CloudflareWorkflowReceiptClaim,
  CloudflareWorkflowReceiptClaimInput,
  CloudflareWorkflowReceiptCleanupCandidate,
  CloudflareWorkflowReceiptRecord,
  CloudflareWorkflowReceiptStore,
} from "@abshahin/workflows-sdk/cloudflare";

type ReceiptRow = {
  workflow_name: string;
  envelope_hash: string;
  state: string;
  owner: string | null;
  fence: number;
  lease_expires_at: number | null;
  check_after: number;
};

type CleanupReceiptRow = ReceiptRow & {
  workflow_identity: string;
  instance_id: string;
};

type D1ReceiptDatabase = Pick<D1DatabaseSession, "prepare">;

export class D1WorkflowReceiptStore implements CloudflareWorkflowReceiptStore {
  private readonly database: D1ReceiptDatabase;

  constructor(database: D1Database) {
    // Receipt writes and their follow-up reads must share a primary-anchored
    // session. Reading a replica after a conditional write could otherwise
    // misclassify a won fence as still pending.
    this.database = database.withSession("first-primary");
  }

  async claim(
    input: CloudflareWorkflowReceiptClaimInput,
  ): Promise<CloudflareWorkflowReceiptClaim> {
    const inserted = await this.database
      .prepare(
        `INSERT OR IGNORE INTO workflow_dispatch_receipts (
           workflow_identity, workflow_name, instance_id, envelope_hash, state,
           owner, fence, lease_expires_at, created_at, updated_at, check_after
         ) VALUES (?, ?, ?, ?, 'PENDING', ?, 1, ?, ?, ?, ?)`,
      )
      .bind(
        input.workflowIdentity,
        input.workflowName,
        input.instanceId,
        input.envelopeHash,
        input.owner,
        input.leaseExpiresAt,
        input.now,
        input.now,
        input.checkAfter,
      )
      .run();
    if ((inserted.meta.changes ?? 0) === 1) {
      return { outcome: "claimed", fence: 1, previousState: "new" };
    }

    const current = await this.read(input.workflowIdentity, input.instanceId);
    if (!current) {
      // A status-backed cleanup may have removed the row between INSERT OR
      // IGNORE and SELECT. Bound the D1 work and let the caller retry.
      return { outcome: "busy", retryAfter: input.now };
    }
    const classified = classifyReceipt(input, current);
    if (classified) return classified;

    const claimed = await this.database
      .prepare(
        `UPDATE workflow_dispatch_receipts
         SET owner=?, fence=fence + 1, lease_expires_at=?, updated_at=?
         WHERE workflow_identity=? AND workflow_name=? AND instance_id=?
           AND envelope_hash=?
           AND state IN ('PENDING', 'ABSENCE_PROVEN')
           AND coalesce(lease_expires_at, 0) <= ?
         RETURNING workflow_name, envelope_hash, state, owner, fence,
                   lease_expires_at, check_after`,
      )
      .bind(
        input.owner,
        input.leaseExpiresAt,
        input.now,
        input.workflowIdentity,
        input.workflowName,
        input.instanceId,
        input.envelopeHash,
        input.now,
      )
      .first<ReceiptRow>();
    if (claimed) {
      const record = parseReceiptRow(claimed);
      return {
        outcome: "claimed",
        fence: record.fence,
        previousState:
          record.state === "ABSENCE_PROVEN" ? "absence_proven" : "pending",
      };
    }

    const raced = await this.read(input.workflowIdentity, input.instanceId);
    if (!raced) return { outcome: "busy", retryAfter: input.now };
    return (
      classifyReceipt(input, raced) ?? {
        outcome: "busy",
        retryAfter: raced.leaseExpiresAt ?? input.now,
      }
    );
  }

  async get(input: {
    workflowIdentity: string;
    instanceId: string;
  }): Promise<CloudflareWorkflowReceiptRecord | null> {
    return this.read(input.workflowIdentity, input.instanceId);
  }

  async markAbsenceProven(input: {
    workflowIdentity: string;
    instanceId: string;
    envelopeHash: string;
    owner: string;
    fence: number;
    now: number;
  }): Promise<boolean> {
    const result = await this.database
      .prepare(
        `UPDATE workflow_dispatch_receipts
         SET state='ABSENCE_PROVEN', updated_at=?
         WHERE workflow_identity=? AND instance_id=? AND envelope_hash=?
           AND state='PENDING' AND owner=? AND fence=?`,
      )
      .bind(
        input.now,
        input.workflowIdentity,
        input.instanceId,
        input.envelopeHash,
        input.owner,
        input.fence,
      )
      .run();
    return (result.meta.changes ?? 0) === 1;
  }

  async markCreated(input: {
    workflowIdentity: string;
    instanceId: string;
    envelopeHash: string;
    owner: string;
    fence: number;
    now: number;
  }): Promise<boolean> {
    const result = await this.database
      .prepare(
        `UPDATE workflow_dispatch_receipts
         SET state='CREATED', owner=NULL, lease_expires_at=NULL, updated_at=?
         WHERE workflow_identity=? AND instance_id=? AND envelope_hash=?
           AND state='ABSENCE_PROVEN' AND owner=? AND fence=?`,
      )
      .bind(
        input.now,
        input.workflowIdentity,
        input.instanceId,
        input.envelopeHash,
        input.owner,
        input.fence,
      )
      .run();
    return (result.meta.changes ?? 0) === 1;
  }

  async release(input: {
    workflowIdentity: string;
    instanceId: string;
    envelopeHash: string;
    owner: string;
    fence: number;
    now: number;
  }): Promise<boolean> {
    const result = await this.database
      .prepare(
        `UPDATE workflow_dispatch_receipts
         SET lease_expires_at=?, updated_at=?
         WHERE workflow_identity=? AND instance_id=? AND envelope_hash=?
           AND state IN ('PENDING', 'ABSENCE_PROVEN')
           AND owner=? AND fence=?`,
      )
      .bind(
        input.now,
        input.now,
        input.workflowIdentity,
        input.instanceId,
        input.envelopeHash,
        input.owner,
        input.fence,
      )
      .run();
    return (result.meta.changes ?? 0) === 1;
  }

  async listCleanupCandidates(input: {
    checkBefore: number;
    limit: number;
  }): Promise<CloudflareWorkflowReceiptCleanupCandidate[]> {
    const result = await this.database
      .prepare(
        `SELECT workflow_identity, workflow_name, instance_id, envelope_hash,
                state, owner, fence, lease_expires_at, check_after
         FROM workflow_dispatch_receipts
         WHERE check_after <= ? AND (
           state='CREATED' OR (
             state IN ('PENDING', 'ABSENCE_PROVEN') AND
             coalesce(lease_expires_at, 0) <= ?
           )
         )
         ORDER BY check_after ASC, workflow_identity ASC, instance_id ASC
         LIMIT ?`,
      )
      .bind(input.checkBefore, input.checkBefore, input.limit)
      .all<CleanupReceiptRow>();
    return result.results.map(parseCleanupReceiptRow);
  }

  async deleteCleanupCandidate(
    candidate: CloudflareWorkflowReceiptCleanupCandidate,
  ): Promise<boolean> {
    const result = await this.database
      .prepare(
        `DELETE FROM workflow_dispatch_receipts
         WHERE workflow_identity=? AND workflow_name=? AND instance_id=?
           AND envelope_hash=? AND state=? AND owner IS ? AND fence=?
           AND lease_expires_at IS ? AND check_after=?`,
      )
      .bind(...candidateSnapshot(candidate))
      .run();
    return (result.meta.changes ?? 0) === 1;
  }

  async deferCleanupCandidate(input: {
    candidate: CloudflareWorkflowReceiptCleanupCandidate;
    now: number;
    checkAfter: number;
  }): Promise<boolean> {
    const result = await this.database
      .prepare(
        `UPDATE workflow_dispatch_receipts
         SET check_after=?, updated_at=?
         WHERE workflow_identity=? AND workflow_name=? AND instance_id=?
           AND envelope_hash=? AND state=? AND owner IS ? AND fence=?
           AND lease_expires_at IS ? AND check_after=?`,
      )
      .bind(input.checkAfter, input.now, ...candidateSnapshot(input.candidate))
      .run();
    return (result.meta.changes ?? 0) === 1;
  }

  private async read(
    workflowIdentity: string,
    instanceId: string,
  ): Promise<CloudflareWorkflowReceiptRecord | null> {
    const row = await this.database
      .prepare(
        `SELECT workflow_name, envelope_hash, state, owner, fence,
                lease_expires_at, check_after
         FROM workflow_dispatch_receipts
         WHERE workflow_identity=? AND instance_id=? LIMIT 1`,
      )
      .bind(workflowIdentity, instanceId)
      .first<ReceiptRow>();
    return row ? parseReceiptRow(row) : null;
  }
}

function classifyReceipt(
  input: CloudflareWorkflowReceiptClaimInput,
  current: CloudflareWorkflowReceiptRecord,
): CloudflareWorkflowReceiptClaim | null {
  if (
    current.envelopeHash !== input.envelopeHash ||
    current.workflowName !== input.workflowName
  ) {
    return { outcome: "conflict" };
  }
  if (current.state === "CREATED") return { outcome: "created" };
  if ((current.leaseExpiresAt ?? 0) > input.now) {
    return {
      outcome: "busy",
      retryAfter: current.leaseExpiresAt ?? input.now,
    };
  }
  return null;
}

function parseReceiptRow(row: ReceiptRow): CloudflareWorkflowReceiptRecord {
  if (
    row.state !== "PENDING" &&
    row.state !== "ABSENCE_PROVEN" &&
    row.state !== "CREATED"
  ) {
    throw new Error(`Invalid Workflow receipt state: ${row.state}`);
  }
  if (!row.workflow_name) {
    throw new Error("Invalid Workflow receipt name");
  }
  if (!Number.isSafeInteger(row.fence) || row.fence <= 0) {
    throw new Error("Invalid Workflow receipt fence");
  }
  if (!Number.isSafeInteger(row.check_after)) {
    throw new Error("Invalid Workflow receipt cleanup timestamp");
  }
  return {
    workflowName: row.workflow_name,
    envelopeHash: row.envelope_hash,
    state: row.state,
    owner: row.owner,
    fence: row.fence,
    leaseExpiresAt: row.lease_expires_at,
    checkAfter: row.check_after,
  };
}

function parseCleanupReceiptRow(
  row: CleanupReceiptRow,
): CloudflareWorkflowReceiptCleanupCandidate {
  return {
    workflowIdentity: row.workflow_identity,
    instanceId: row.instance_id,
    ...parseReceiptRow(row),
  };
}

function candidateSnapshot(
  candidate: CloudflareWorkflowReceiptCleanupCandidate,
): [string, string, string, string, string, string | null, number, number | null, number] {
  return [
    candidate.workflowIdentity,
    candidate.workflowName,
    candidate.instanceId,
    candidate.envelopeHash,
    candidate.state,
    candidate.owner,
    candidate.fence,
    candidate.leaseExpiresAt,
    candidate.checkAfter,
  ];
}
