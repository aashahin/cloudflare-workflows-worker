import type { Env } from "../env.js";
import { createFailedEventRecordPointerMessage } from "./failed-event-pointer.js";

export const FAILED_EVENT_ENQUEUE_SWEEP_CRON = "*/15 * * * *";
export const FAILED_EVENT_DELIVERY_SWEEP_LIMIT = 25;
export const FAILED_EVENT_RECEIPT_DISCOVERY_LIMIT = 10;
export const FAILED_EVENT_RECEIPT_CLEANUP_LIMIT = 100;
export const FAILED_EVENT_RECEIPT_CLEANUP_MAX_PAGES = 4;
/** Exceeds Cloudflare's 15-minute Worker invocation wall-clock ceiling. */
export const FAILED_EVENT_DELIVERY_LEASE_MS = 16 * 60_000;
export const FAILED_EVENT_QUEUE_REFRESH_MS = 3 * 24 * 60 * 60_000;
export const FAILED_EVENT_RECEIPT_RETENTION_MS = 31 * 24 * 60 * 60_000;
export const FAILED_EVENT_TERMINAL_CLEANUP_GRACE_MS = 24 * 60 * 60_000;

const MAX_QUEUE_DELAY_SECONDS = 43_200;
const MAX_ENQUEUE_BACKOFF_MS = 6 * 60 * 60_000;
const BASE_ENQUEUE_BACKOFF_MS = 5 * 60_000;

type DeliveryChannel = "RETRY" | "DLQ";
type DeliveryEnv = Pick<
  Env,
  "CONTROL_DB" | "FAILED_EVENTS_QUEUE" | "FAILED_EVENTS_DLQ"
>;
type QueueBindings = {
  FAILED_EVENTS_QUEUE?: Queue;
  FAILED_EVENTS_DLQ?: Queue;
};
type DeliveryDatabase = Pick<D1DatabaseSession, "prepare">;

interface DeliveryClaimRow {
  fence: number;
  delivery_delay_seconds: number;
  failure_count: number;
}

interface DeliveryCandidateRow {
  failed_event_id: string;
  channel: DeliveryChannel;
}

interface MissingReceiptRow {
  id: string;
  metadata: string | null;
}

export type FailedEventDeliveryOutcome =
  | "sent"
  | "suppressed"
  | "ambiguous";

export interface FailedEventDeliverySweepResult {
  discovered: number;
  attempted: number;
  sent: number;
  suppressed: number;
  ambiguous: number;
  cleanedExpired: number;
  cleanedTerminal: number;
}

/**
 * Insert the durable RETRY receipt before publishing. Exact and concurrent
 * callers race on the receipt's primary key/fence, so only one Queue send wins.
 */
export async function enqueueFailedEventRetry(
  env: Pick<Env, "CONTROL_DB" | "FAILED_EVENTS_QUEUE">,
  failedEventId: string,
  input: { delaySeconds: number; now?: number },
): Promise<FailedEventDeliveryOutcome> {
  const now = input.now ?? Date.now();
  const db = primarySession(env.CONTROL_DB);
  await ensureReceipt(
    db,
    failedEventId,
    "RETRY",
    now,
    boundDelaySeconds(input.delaySeconds),
  );
  return deliverReceipt(db, env, failedEventId, "RETRY", now);
}

/**
 * Roll a nearly exhausted physical Queue delivery onto one fresh pointer per
 * durable processing attempt. The source-attempt CAS prevents duplicate
 * high-attempt messages from each manufacturing another Queue message.
 */
export async function rearmFailedEventRetry(
  env: Pick<Env, "CONTROL_DB" | "FAILED_EVENTS_QUEUE">,
  failedEventId: string,
  input: { delaySeconds: number; now?: number },
): Promise<FailedEventDeliveryOutcome> {
  const now = input.now ?? Date.now();
  const delaySeconds = boundDelaySeconds(input.delaySeconds);
  const db = primarySession(env.CONTROL_DB);
  await ensureReceipt(db, failedEventId, "RETRY", now, delaySeconds);
  await db
    .prepare(
      `UPDATE failed_event_queue_receipts
       SET state='PENDING', owner=NULL, fence=fence+1, lease_expires_at=NULL,
           next_attempt_at=?, delivery_delay_seconds=?, source_attempt=(
             SELECT f.attempts FROM workflow_failed_events AS f
             WHERE f.id=failed_event_queue_receipts.failed_event_id
           ),
           failure_count=0, confirmed_at=NULL, refresh_at=NULL,
           last_error=NULL, updated_at=?, expires_at=?
       WHERE failed_event_id=? AND channel='RETRY'
         AND source_attempt < coalesce((
           SELECT f.attempts FROM workflow_failed_events AS f
           WHERE f.id=failed_event_queue_receipts.failed_event_id
             AND CASE WHEN json_valid(f.metadata)
               THEN json_extract(f.metadata, '$.recoveryOwner')
             END = 'callback-queue'
             AND f.status IN ('PENDING','PROCESSING')
         ), source_attempt)`,
    )
    .bind(
      now,
      delaySeconds,
      now,
      now + FAILED_EVENT_RECEIPT_RETENTION_MS,
      failedEventId,
    )
    .run();
  return deliverReceipt(db, env, failedEventId, "RETRY", now);
}

/** Publish one operator DLQ pointer per terminal recovery cycle. */
export async function publishFailedEventDlq(
  env: Pick<Env, "CONTROL_DB" | "FAILED_EVENTS_DLQ">,
  failedEventId: string,
  input: { now?: number } = {},
): Promise<FailedEventDeliveryOutcome> {
  const now = input.now ?? Date.now();
  const db = primarySession(env.CONTROL_DB);
  await ensureReceipt(db, failedEventId, "DLQ", now, 0);
  return deliverReceipt(db, env, failedEventId, "DLQ", now);
}

/**
 * Bounded repair/sweep. The 15-minute cadence heals the row→receipt crash
 * window and failed/ambiguous sends. Confirmed RETRY pointers refresh every
 * three days, before Cloudflare Queue's default four-day retention, but never
 * more often during a long consumer outage.
 */
export async function sweepFailedEventDeliveries(
  env: DeliveryEnv,
  input: { now?: number } = {},
): Promise<FailedEventDeliverySweepResult> {
  const now = input.now ?? Date.now();
  const db = primarySession(env.CONTROL_DB);
  const missing = await db
    .prepare(
      `SELECT f.id, f.metadata
       FROM workflow_failed_events AS f
       WHERE f.status IN ('PENDING', 'PROCESSING')
         AND CASE WHEN json_valid(f.metadata)
           THEN json_extract(f.metadata, '$.recoveryOwner')
         END = 'callback-queue'
         AND NOT EXISTS (
           SELECT 1 FROM failed_event_queue_receipts AS r
           WHERE r.failed_event_id=f.id AND r.channel='RETRY'
         )
       ORDER BY f.updated_at, f.id
       LIMIT ?`,
    )
    .bind(FAILED_EVENT_RECEIPT_DISCOVERY_LIMIT)
    .all<MissingReceiptRow>();

  for (const row of missing.results) {
    const retryAt = readQueueDeliveryNotBefore(row.metadata);
    await ensureReceipt(
      db,
      row.id,
      "RETRY",
      now,
      retryAt === undefined
        ? 0
        : boundDelaySeconds(Math.ceil((retryAt - now) / 1_000)),
    );
  }

  const candidates: DeliveryCandidateRow[] = [];
  await appendPendingCandidates(db, candidates, "RETRY", now);
  await appendPendingCandidates(db, candidates, "DLQ", now);
  if (candidates.length < FAILED_EVENT_DELIVERY_SWEEP_LIMIT) {
    const refreshed = await db
      .prepare(
        `SELECT failed_event_id, channel
         FROM failed_event_queue_receipts
         WHERE channel='RETRY' AND state='CONFIRMED' AND refresh_at <= ?
         ORDER BY refresh_at, failed_event_id
         LIMIT ?`,
      )
      .bind(now, FAILED_EVENT_DELIVERY_SWEEP_LIMIT - candidates.length)
      .all<DeliveryCandidateRow>();
    candidates.push(...refreshed.results);
  }

  let sent = 0;
  let suppressed = 0;
  let ambiguous = 0;
  for (const candidate of candidates) {
    const outcome = await deliverReceipt(
      db,
      env,
      candidate.failed_event_id,
      candidate.channel,
      now,
    ).catch((error) => {
      log("error", "failed_event.delivery_sweep_send_failed", {
        failedEventId: candidate.failed_event_id,
        channel: candidate.channel,
        error: boundedError(error),
      });
      return "suppressed" as const;
    });
    if (outcome === "sent") sent += 1;
    else if (outcome === "ambiguous") ambiguous += 1;
    else suppressed += 1;
  }

  const cleanedExpired = await cleanupExpiredReceipts(db, now);
  const cleanedTerminal = await cleanupTerminalRetryReceipts(db, now);
  return {
    discovered: missing.results.length,
    attempted: candidates.length,
    sent,
    suppressed,
    ambiguous,
    cleanedExpired,
    cleanedTerminal,
  };
}

async function ensureReceipt(
  db: DeliveryDatabase,
  failedEventId: string,
  channel: DeliveryChannel,
  now: number,
  delaySeconds: number,
): Promise<void> {
  await db
    .prepare(
      `INSERT OR IGNORE INTO failed_event_queue_receipts (
         failed_event_id, channel, state, owner, fence, lease_expires_at,
         next_attempt_at, delivery_delay_seconds, source_attempt, failure_count,
         confirmed_at, refresh_at, last_error, created_at, updated_at, expires_at
       ) VALUES (?, ?, 'PENDING', NULL, 0, NULL, ?, ?, -1, 0, NULL, NULL, NULL, ?, ?, ?)`,
    )
    .bind(
      failedEventId,
      channel,
      now,
      delaySeconds,
      now,
      now,
      now + FAILED_EVENT_RECEIPT_RETENTION_MS,
    )
    .run();
}

async function deliverReceipt(
  db: DeliveryDatabase,
  env: QueueBindings,
  failedEventId: string,
  channel: DeliveryChannel,
  now: number,
): Promise<FailedEventDeliveryOutcome> {
  const owner = crypto.randomUUID();
  const claim = await db
    .prepare(
      `UPDATE failed_event_queue_receipts
       SET state='PENDING', owner=?, fence=fence+1, lease_expires_at=?, updated_at=?
       WHERE failed_event_id=? AND channel=? AND next_attempt_at <= ?
         AND (
           (state='PENDING' AND coalesce(lease_expires_at, 0) <= ?) OR
           (channel='RETRY' AND state='CONFIRMED' AND refresh_at <= ?)
         )
         AND EXISTS (
           SELECT 1 FROM workflow_failed_events AS f
           WHERE f.id=failed_event_queue_receipts.failed_event_id
             AND CASE WHEN json_valid(f.metadata)
               THEN json_extract(f.metadata, '$.recoveryOwner')
             END = 'callback-queue'
             AND (
               (failed_event_queue_receipts.channel='RETRY' AND f.status IN ('PENDING','PROCESSING')) OR
               (failed_event_queue_receipts.channel='DLQ' AND f.status='DEAD')
             )
         )
       RETURNING fence, delivery_delay_seconds, failure_count`,
    )
    .bind(
      owner,
      now + FAILED_EVENT_DELIVERY_LEASE_MS,
      now,
      failedEventId,
      channel,
      now,
      now,
      now,
    )
    .first<DeliveryClaimRow>();
  if (claim === null) return "suppressed";

  const pointer = createFailedEventRecordPointerMessage(failedEventId);
  const queue =
    channel === "RETRY" ? env.FAILED_EVENTS_QUEUE : env.FAILED_EVENTS_DLQ;
  try {
    if (queue === undefined) {
      throw new Error(`Missing ${channel} Queue binding`);
    }
    await queue.send(
      pointer,
      claim.delivery_delay_seconds > 0
        ? { delaySeconds: claim.delivery_delay_seconds }
        : undefined,
    );
  } catch (error) {
    await releaseReceiptAfterDefiniteFailure(
      db,
      failedEventId,
      channel,
      owner,
      claim,
      now,
      error,
    ).catch((releaseError) => {
      // An unconfirmed send can be ambiguous at the platform boundary. Keeping
      // the lease intact makes the bounded sweeper the sole later duplicator.
      log("error", "failed_event.delivery_release_failed", {
        failedEventId,
        channel,
        error: boundedError(releaseError),
      });
    });
    throw error;
  }

  const refreshAt =
    channel === "RETRY" ? now + FAILED_EVENT_QUEUE_REFRESH_MS : null;
  try {
    const confirmed = await db
      .prepare(
        `UPDATE failed_event_queue_receipts
         SET state='CONFIRMED', owner=NULL, lease_expires_at=NULL,
             next_attempt_at=?, delivery_delay_seconds=0, failure_count=0,
             confirmed_at=?, refresh_at=?, last_error=NULL, updated_at=?, expires_at=?
         WHERE failed_event_id=? AND channel=? AND state='PENDING'
           AND owner=? AND fence=?`,
      )
      .bind(
        refreshAt ?? now + FAILED_EVENT_RECEIPT_RETENTION_MS,
        now,
        refreshAt,
        now,
        now + FAILED_EVENT_RECEIPT_RETENTION_MS,
        failedEventId,
        channel,
        owner,
        claim.fence,
      )
      .run();
    if ((confirmed.meta.changes ?? 0) === 1) return "sent";
  } catch (error) {
    log("error", "failed_event.delivery_confirm_ambiguous", {
      failedEventId,
      channel,
      error: boundedError(error),
    });
    return "ambiguous";
  }

  // Queue accepted the pointer, but a newer fence won before confirmation.
  // Do not mutate that owner; its bounded sweep may duplicate this send once.
  return "ambiguous";
}

async function releaseReceiptAfterDefiniteFailure(
  db: DeliveryDatabase,
  failedEventId: string,
  channel: DeliveryChannel,
  owner: string,
  claim: DeliveryClaimRow,
  now: number,
  error: unknown,
): Promise<void> {
  const failures = claim.failure_count + 1;
  const nextAttemptAt = now + enqueueBackoffMs(failures);
  const released = await db
    .prepare(
      `UPDATE failed_event_queue_receipts
       SET state='PENDING', owner=NULL, lease_expires_at=NULL,
           next_attempt_at=?, delivery_delay_seconds=0, failure_count=?,
           last_error=?, updated_at=?, expires_at=?
       WHERE failed_event_id=? AND channel=? AND state='PENDING'
         AND owner=? AND fence=?`,
    )
    .bind(
      nextAttemptAt,
      failures,
      boundedError(error),
      now,
      now + FAILED_EVENT_RECEIPT_RETENTION_MS,
      failedEventId,
      channel,
      owner,
      claim.fence,
    )
    .run();
  if ((released.meta.changes ?? 0) !== 1) {
    throw new Error(`Lost ${channel} enqueue fence for ${failedEventId}`);
  }
}

async function appendPendingCandidates(
  db: DeliveryDatabase,
  candidates: DeliveryCandidateRow[],
  channel: DeliveryChannel,
  now: number,
): Promise<void> {
  if (candidates.length >= FAILED_EVENT_DELIVERY_SWEEP_LIMIT) return;
  const rows = await db
    .prepare(
      `SELECT failed_event_id, channel
       FROM failed_event_queue_receipts
       WHERE channel=? AND state='PENDING' AND next_attempt_at <= ?
         AND coalesce(lease_expires_at, 0) <= ?
       ORDER BY next_attempt_at, failed_event_id
       LIMIT ?`,
    )
    .bind(
      channel,
      now,
      now,
      FAILED_EVENT_DELIVERY_SWEEP_LIMIT - candidates.length,
    )
    .all<DeliveryCandidateRow>();
  candidates.push(...rows.results);
}

async function cleanupExpiredReceipts(
  db: DeliveryDatabase,
  now: number,
): Promise<number> {
  return cleanupReceiptPages(async () => {
    const result = await db
      .prepare(
        `DELETE FROM failed_event_queue_receipts
         WHERE (failed_event_id, channel) IN (
           SELECT failed_event_id, channel
           FROM failed_event_queue_receipts
           WHERE expires_at <= ? AND coalesce(lease_expires_at, 0) <= ?
           ORDER BY expires_at, failed_event_id, channel
           LIMIT ?
         )`,
      )
      .bind(now, now, FAILED_EVENT_RECEIPT_CLEANUP_LIMIT)
      .run();
    return result.meta.changes ?? 0;
  });
}

async function cleanupTerminalRetryReceipts(
  db: DeliveryDatabase,
  now: number,
): Promise<number> {
  const cutoff = now - FAILED_EVENT_TERMINAL_CLEANUP_GRACE_MS;
  return cleanupReceiptPages(async () => {
    const result = await db
      .prepare(
        `DELETE FROM failed_event_queue_receipts
         WHERE (failed_event_id, channel) IN (
           SELECT r.failed_event_id, r.channel
           FROM failed_event_queue_receipts AS r
           WHERE r.channel='RETRY' AND r.updated_at <= ?
             AND coalesce(r.lease_expires_at, 0) <= ?
             AND NOT EXISTS (
               SELECT 1 FROM workflow_failed_events AS f
               WHERE f.id=r.failed_event_id
                 AND CASE WHEN json_valid(f.metadata)
                   THEN json_extract(f.metadata, '$.recoveryOwner')
                 END = 'callback-queue'
                 AND f.status IN ('PENDING','PROCESSING')
             )
           ORDER BY r.updated_at, r.failed_event_id
           LIMIT ?
         )`,
      )
      .bind(cutoff, now, FAILED_EVENT_RECEIPT_CLEANUP_LIMIT)
      .run();
    return result.meta.changes ?? 0;
  });
}

async function cleanupReceiptPages(
  deletePage: () => Promise<number>,
): Promise<number> {
  let deleted = 0;
  for (
    let page = 0;
    page < FAILED_EVENT_RECEIPT_CLEANUP_MAX_PAGES;
    page += 1
  ) {
    const pageDeleted = await deletePage();
    deleted += pageDeleted;
    if (pageDeleted < FAILED_EVENT_RECEIPT_CLEANUP_LIMIT) break;
  }
  return deleted;
}

function primarySession(database: D1Database): DeliveryDatabase {
  return database.withSession("first-primary");
}

function readQueueDeliveryNotBefore(metadata: string | null): number | undefined {
  if (metadata === null) return undefined;
  try {
    const parsed: unknown = JSON.parse(metadata);
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
      return undefined;
    }
    const record = parsed as Record<string, unknown>;
    const candidates = [
      record.queueRetryNotBefore,
      record.queueLeaseExpiresAt,
    ].filter(
      (value): value is number =>
        typeof value === "number" && Number.isFinite(value),
    );
    return candidates.length === 0 ? undefined : Math.max(...candidates);
  } catch {
    return undefined;
  }
}

function enqueueBackoffMs(failureCount: number): number {
  return Math.min(
    MAX_ENQUEUE_BACKOFF_MS,
    BASE_ENQUEUE_BACKOFF_MS * 2 ** Math.min(Math.max(failureCount - 1, 0), 7),
  );
}

function boundDelaySeconds(value: number): number {
  if (!Number.isFinite(value)) return 0;
  return Math.min(MAX_QUEUE_DELAY_SECONDS, Math.max(0, Math.ceil(value)));
}

function boundedError(error: unknown): string {
  return (error instanceof Error ? error.message : String(error))
    .replace(/[\u0000-\u001f\u007f]/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 1_000);
}

function log(
  level: "info" | "error",
  event: string,
  fields: Record<string, unknown>,
): void {
  const line = JSON.stringify({ level, event, ...fields });
  if (level === "error") console.error(line);
  else console.info(line);
}
