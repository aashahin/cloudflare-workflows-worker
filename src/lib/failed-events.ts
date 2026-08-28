// ─── Failed Events Queue ─────────────────────────────────────────────────────
// Persists permanently-failed workflow events into a Cloudflare Queue so the
// queue consumer can re-execute them directly against the backend with a
// durable D1 attempt counter and retry-not-before timestamp.
//
// Queue delivery attempts are transport state only: duplicate pointers and
// claim-busy deliveries never advance the durable processing attempt. A
// high-attempt delivery is rolled onto a fresh pointer before the transport
// DLQ ceiling so it cannot prematurely exhaust a recoverable D1 record.
//
// Retry schedule (initial delayed delivery + 9 retries; max_retries is 9):
//   1m, 3m, 5m, 10m, 15m, 20m, 30m, 45m, 60m, 90m

import type { BackendCallbackFailedEvent } from "@abshahin/workflows-sdk";
import { assertCloudflareJsonSerializable } from "@abshahin/workflows-sdk/cloudflare";
import type { Env } from "../env.js";
import { callBackendService, isNonRetryableFailure } from "./backend.js";
import {
  enqueueFailedEventRetry,
  publishFailedEventDlq,
  rearmFailedEventRetry,
} from "./failed-event-delivery.js";
import {
  createFailedEventRecordPointerMessage,
  createFailedEventPointerMessage,
  type FailedEventPointerMessage,
  type FailedEventRecordPointerMessage,
} from "./failed-event-pointer.js";
import { createControlPlaneRecoveryCallbackSteps } from "./workflow-policy.js";

// ─── Types ───────────────────────────────────────────────────────────────────

export interface FailedEventMessage extends BackendCallbackFailedEvent {
  createdAt: string;
}

interface FailedEventBackendStep {
  backendPath: string;
  backendEventId: string;
}

interface LegacyFailedEventMessage {
  eventId: string;
  workflowType?: string;
  eventName: string;
  backendPath?: string;
  backendEventId?: string;
  data: Record<string, unknown>;
  idempotencyKey: string;
  error: string;
  createdAt: string;
}

export type FailedEventQueueMessage =
  | FailedEventRecordPointerMessage
  | FailedEventPointerMessage
  | FailedEventMessage
  | LegacyFailedEventMessage;

/**
 * Bound the record before a D1 write. Workflow envelopes are capped at 96 KB;
 * this allows recovery metadata while preventing unbounded storage/billing.
 */
export const MAX_FAILED_EVENT_RECORD_BYTES = 120_000;
export const FAILED_EVENT_MAX_TOTAL_ATTEMPTS = 10;
/** Exceeds Cloudflare Queues' 15-minute invocation wall-clock ceiling. */
export const FAILED_EVENT_CLAIM_LEASE_MS = 16 * 60_000;

const QUEUE_LEASE_OWNER_PATH = "$.queueLeaseOwner";
const QUEUE_LEASE_EXPIRES_AT_PATH = "$.queueLeaseExpiresAt";
const QUEUE_RETRY_NOT_BEFORE_PATH = "$.queueRetryNotBefore";
const QUEUE_DELIVERY_ROLLOVER_ATTEMPT =
  FAILED_EVENT_MAX_TOTAL_ATTEMPTS - 1;

interface StoredFailedEventRow {
  id: string;
  event_id: string | null;
  event_name: string;
  event_data: string;
  idempotency_key: string | null;
  metadata: string | null;
  error: string;
  status: "PENDING" | "PROCESSING" | "COMPLETED" | "DEAD";
  attempts: number;
  max_attempts: number;
  updated_at: number;
}

interface StoredFailedEventClaim {
  id: string;
  owner: string;
}

type ResolvedFailedEvent =
  | {
      record: BackendCallbackFailedEvent;
      claim?: undefined;
    }
  | {
      storedRow: StoredFailedEventRow;
      claim: StoredFailedEventClaim;
    }
  | {
      record: null;
      storedId: string;
      status: "PENDING" | "PROCESSING" | "COMPLETED" | "DEAD";
      retryAt?: number;
    };

class LostFailedEventClaimError extends Error {
  constructor(readonly claim: StoredFailedEventClaim) {
    super(`Lost failed-event processing claim for ${claim.id}`);
    this.name = "LostFailedEventClaimError";
  }
}

// ─── Retry schedule (seconds) — first entry is used on queue.send ────────────

const RETRY_DELAYS_SECONDS = [
  60, //  1 min
  180, //  3 min
  300, //  5 min
  600, // 10 min
  900, // 15 min
  1200, // 20 min
  1800, // 30 min
  2700, // 45 min
  3600, // 60 min
  5400, // 90 min
];
const INITIAL_RETRY_DELAY_SECONDS = RETRY_DELAYS_SECONDS[0] ?? 60;

/** Get the retry delay in seconds for the given attempt number (1-based). */
function getRetryDelay(attempt: number): number {
  const idx = Math.min(
    Math.max(attempt, 0),
    RETRY_DELAYS_SECONDS.length - 1,
  );
  return RETRY_DELAYS_SECONDS[idx] ?? 60;
}

// ─── Store a failed event ────────────────────────────────────────────────────

export async function storeFailedEvent(
  env: Pick<Env, "CONTROL_DB" | "FAILED_EVENTS_QUEUE">,
  params: BackendCallbackFailedEvent,
): Promise<void> {
  const recordBytes = assertCloudflareJsonSerializable(
    params,
    "Failed-event recovery record",
  );
  if (recordBytes > MAX_FAILED_EVENT_RECORD_BYTES) {
    throw new Error(
      `Failed-event recovery record exceeds ${MAX_FAILED_EVENT_RECORD_BYTES} bytes`,
    );
  }

  const failedEventId = `callback_${params.eventId}`;
  const now = Date.now();
  const backendSteps = getBackendSteps(params);
  const callbackSteps = createControlPlaneRecoveryCallbackSteps(
    params.workflowName,
  );
  const metadata = {
    recoveryOwner: "callback-queue",
    recoveryEventId: params.eventId,
    backendPath: params.backendPath,
    backendEventId: params.backendEventId,
    ...(params.backendSteps === undefined ? {} : { backendSteps }),
    queueRetryNotBefore: now + INITIAL_RETRY_DELAY_SECONDS * 1_000,
    // Payout replays its full admitted plan; callback execution claims
    // deduplicate steps that completed before the recovery record was stored.
    ...(callbackSteps === undefined ? {} : { callbackSteps }),
  };

  const inserted = await env.CONTROL_DB.prepare(
    `INSERT INTO workflow_failed_events
       (id, event_id, event_name, event_data, trace_id, idempotency_key,
        scheduled_at, metadata, error, status, attempts, max_attempts,
        next_retry_at, resolved_at, created_at, updated_at)
     VALUES (?, ?, ?, ?, NULL, ?, NULL, ?, ?, 'PENDING', 0, 10, ?, NULL, ?, ?)
     ON CONFLICT DO NOTHING`,
  )
    .bind(
      failedEventId,
      params.eventId,
      params.workflowName,
      JSON.stringify(params.payload),
      params.idempotencyKey,
      JSON.stringify(metadata),
      params.error,
      now + INITIAL_RETRY_DELAY_SECONDS * 1_000,
      now,
      now,
    )
    .run();

  const stored = await readStoredFailedEvent(env, failedEventId);
  if (stored === null) {
    throw new Error(
      `Failed-event recovery record ${failedEventId} was not stored`,
    );
  }
  assertCompatibleStoredFailedEvent(stored, params);

  // A Workflow step can be replayed after this function has already succeeded.
  // Never revive or re-enqueue terminal rows, and do not add another Queue
  // delivery while an owner is actively processing the durable record.
  if (stored.status === "COMPLETED" || stored.status === "DEAD") {
    log("info", "failed_event.duplicate_terminal", {
      eventId: params.eventId,
      failedEventId,
      status: stored.status,
    });
    return;
  }
  if ((inserted.meta.changes ?? 0) === 0 && stored.status === "PROCESSING") {
    log("info", "failed_event.duplicate_processing", {
      eventId: params.eventId,
      failedEventId,
    });
    return;
  }

  const delivery = await enqueueFailedEventRetry(env, failedEventId, {
    delaySeconds: INITIAL_RETRY_DELAY_SECONDS,
    now,
  });

  log("info", `failed_event.enqueue_${delivery}`, {
    eventId: params.eventId,
    failedEventId,
    workflowName: params.workflowName,
    recordBytes,
    delaySeconds: INITIAL_RETRY_DELAY_SECONDS,
  });
}

// ─── Process a batch of failed event messages (queue consumer) ───────────────
// Calls the backend directly. A successful atomic D1 claim increments the
// durable attempt; transient failure persists its retry-not-before before the
// Queue delivery is retried or rolled over. Queue delivery attempts never
// decide whether a stored recovery record is exhausted.

export async function processFailedEventBatch(
  batch: MessageBatch<FailedEventQueueMessage>,
  env: Env,
): Promise<void> {
  for (const message of batch.messages) {
    let claim: StoredFailedEventClaim | undefined;
    let durableAttempt: number | undefined;
    let durableMaxAttempts: number | undefined;
    let storedRecoveryId = validatedStoredPointerId(message.body);
    try {
      const resolved = await resolveFailedEventMessage(
        message.body,
        env,
        crypto.randomUUID(),
      );
      if ("storedRow" in resolved) {
        claim = resolved.claim;
        storedRecoveryId = resolved.claim.id;
        durableAttempt = resolved.storedRow.attempts;
        durableMaxAttempts = resolved.storedRow.max_attempts;
      } else if (resolved.record === null) {
        await settleUnclaimedStoredDelivery(message, env, resolved);
        continue;
      }

      const record =
        "storedRow" in resolved
          ? parseStoredFailedEventRecord(resolved.storedRow)
          : resolved.record;
      const attempt = durableAttempt ?? message.attempts;
      const traceId = `${record.eventId}:retry:${attempt}`;
      // Call backend directly for the failed step and any dependent steps that
      // would have run after it in the original workflow.
      for (const step of getBackendSteps(record)) {
        if (claim !== undefined) {
          await renewStoredFailedEventClaim(env, claim);
        }
        await callBackendService(
          env,
          step.backendPath,
          record.payload,
          traceId,
          step.backendEventId,
        );
      }

      if (
        claim !== undefined &&
        !(await markStoredFailedEventCompleted(env, claim))
      ) {
        throw new LostFailedEventClaimError(claim);
      }

      message.ack();
      log("info", "failed_event.retry_succeeded", {
        eventId: record.eventId,
        workflowName: record.workflowName,
        attempt,
      });
    } catch (err) {
      if (err instanceof LostFailedEventClaimError) {
        await settleLostStoredClaim(message, env, err.claim);
        continue;
      }
      const attempt = durableAttempt ?? message.attempts;

      // Permanent/malformed events cannot recover by retrying. Copy the
      // original record to the operator DLQ before acknowledging it. If that
      // write fails, retain the source message and retry instead of losing it.
      if (isNonRetryableFailure(err)) {
        let storedMarkedDead = claim === undefined;
        try {
          if (
            claim !== undefined &&
            !(await markStoredFailedEventDead(
              env,
              claim,
              err,
            ))
          ) {
            throw new LostFailedEventClaimError(claim);
          }
          storedMarkedDead = true;
          if (claim === undefined) {
            await env.FAILED_EVENTS_DLQ.send(message.body);
          } else {
            await publishFailedEventDlq(env, claim.id);
          }
          message.ack();
          log("error", "failed_event.permanent_failure_dead_lettered", {
            messageId: message.id,
            attempt,
            error: boundedLogError(err),
          });
        } catch (deadLetterError) {
          if (deadLetterError instanceof LostFailedEventClaimError) {
            await settleLostStoredClaim(
              message,
              env,
              deadLetterError.claim,
            );
            continue;
          }
          const delay = getRetryDelay(attempt);
          log("error", "failed_event.dead_letter_failed", {
            messageId: message.id,
            attempt,
            error: boundedLogError(err),
            deadLetterError: boundedLogError(deadLetterError),
            delaySeconds: delay,
          });
          if (claim !== undefined && !storedMarkedDead) {
            await retryStoredDelivery(
              message,
              env,
              claim.id,
              Date.now() + FAILED_EVENT_CLAIM_LEASE_MS,
              "terminal_transition_failed",
              deadLetterError,
            );
          } else {
            message.retry({ delaySeconds: delay });
          }
        }
        continue;
      }

      const delay = getRetryDelay(attempt);
      const retryAt = Date.now() + delay * 1_000;
      const errorMsg = err instanceof Error ? err.message : String(err);

      if (
        storedRecoveryId === undefined
          ? message.attempts >= FAILED_EVENT_MAX_TOTAL_ATTEMPTS
          : claim !== undefined &&
            durableAttempt !== undefined &&
            durableMaxAttempts !== undefined &&
            durableAttempt >= durableMaxAttempts
      ) {
        let storedMarkedDead = claim === undefined;
        try {
          if (
            claim !== undefined &&
            !(await markStoredFailedEventDead(
              env,
              claim,
              err,
            ))
          ) {
            throw new LostFailedEventClaimError(claim);
          }
          storedMarkedDead = true;
          if (claim === undefined) {
            await env.FAILED_EVENTS_DLQ.send(message.body);
          } else {
            await publishFailedEventDlq(env, claim.id);
          }
          message.ack();
          log("error", "failed_event.retry_exhausted_dead_lettered", {
            messageId: message.id,
            attempt,
            error: boundedLogError(errorMsg),
          });
        } catch (deadLetterError) {
          if (deadLetterError instanceof LostFailedEventClaimError) {
            await settleLostStoredClaim(
              message,
              env,
              deadLetterError.claim,
            );
            continue;
          }
          // At the configured ceiling Cloudflare will route this retry to the
          // consumer DLQ; the D1 row is already terminal when that write won.
          log("error", "failed_event.retry_exhausted_dead_letter_failed", {
            messageId: message.id,
            attempt,
            error: boundedLogError(errorMsg),
            deadLetterError: boundedLogError(deadLetterError),
          });
          if (claim !== undefined && !storedMarkedDead) {
            await retryStoredDelivery(
              message,
              env,
              claim.id,
              Date.now() + FAILED_EVENT_CLAIM_LEASE_MS,
              "terminal_transition_failed",
              deadLetterError,
            );
          } else {
            message.retry({ delaySeconds: delay });
          }
        }
        continue;
      }

      if (claim !== undefined) {
        try {
          if (
            !(await releaseStoredFailedEventClaim(
              env,
              claim,
              retryAt,
              err,
            ))
          ) {
            await settleLostStoredClaim(message, env, claim);
            continue;
          }
        } catch (releaseError) {
          log("error", "failed_event.claim_release_failed", {
            messageId: message.id,
            attempt,
            error: boundedLogError(releaseError),
            delaySeconds: delay,
          });
          await retryStoredDelivery(
            message,
            env,
            claim.id,
            Date.now() + FAILED_EVENT_CLAIM_LEASE_MS,
            "claim_release_failed",
            releaseError,
          );
          continue;
        }
      }

      log("warn", "failed_event.retry_failed", {
        messageId: message.id,
        attempt,
        error: boundedLogError(errorMsg),
        delaySeconds: delay,
      });

      if (storedRecoveryId === undefined) {
        // Legacy payload-carrying messages have no durable D1 retry state.
        message.retry({ delaySeconds: delay });
      } else {
        await retryStoredDelivery(
          message,
          env,
          storedRecoveryId,
          retryAt,
          "retry_failed",
        );
      }
    }
  }
}

async function resolveFailedEventMessage(
  message: unknown,
  env: Env,
  owner: string,
): Promise<ResolvedFailedEvent> {
  if (!isRecord(message)) {
    throw invalidQueueMessage("Queue body must be an object");
  }
  if (isFailedEventRecordPointer(message)) {
    let pointer: FailedEventRecordPointerMessage;
    try {
      pointer = createFailedEventRecordPointerMessage(message.failedEventId);
    } catch (error) {
      throw invalidQueueMessage(boundedLogError(error));
    }
    const claim = { id: pointer.failedEventId, owner };
    const row = await claimStoredFailedEvent(env, claim);
    if (row !== null) return { storedRow: row, claim };

    let existing = await readStoredFailedEvent(env, pointer.failedEventId);
    if (existing === null) {
      throw invalidQueueMessage("Stored recovery record does not exist");
    }
    if (existing.attempts >= existing.max_attempts) {
      const exhausted = await markExhaustedStoredFailedEvent(env, existing);
      if (exhausted !== null) existing = exhausted;
      else {
        existing = await readStoredFailedEvent(env, pointer.failedEventId);
        if (existing === null) {
          throw invalidQueueMessage("Stored recovery record does not exist");
        }
      }
    }
    const retryAt = storedFailedEventRetryAt(existing);
    return {
      record: null,
      storedId: existing.id,
      status: existing.status,
      ...(retryAt === undefined ? {} : { retryAt }),
    };
  }
  if (isFailedEventPointer(message)) {
    let pointer: FailedEventPointerMessage;
    try {
      pointer = createFailedEventPointerMessage(message.workflowInstanceId);
    } catch (error) {
      throw invalidQueueMessage(boundedLogError(error));
    }
    const instance = await env.WORKFLOW.get(pointer.workflowInstanceId);
    const status = await instance.status();
    if (status.status !== "complete") {
      if (
        ["errored", "terminated", "failed", "cancelled", "canceled"].includes(
          String(status.status).toLowerCase(),
        )
      ) {
        throw invalidQueueMessage(
          `Workflow recovery state is terminal (${status.status})`,
        );
      }
      throw new Error(
        `Workflow recovery state is not complete (${status.status})`,
      );
    }
    const output = isRecord(status.output) ? status.output : null;
    return { record: parseFailedEventRecord(output?.recovery) };
  }

  if ("workflowName" in message) {
    return { record: parseFailedEventRecord(message) };
  }

  return {
    record: parseFailedEventRecord({
      eventId: message.eventId,
      workflowName: message.eventName,
      backendPath: message.backendPath ?? message.eventName,
      backendEventId: message.backendEventId ?? message.eventId,
      payload: message.data,
      idempotencyKey: message.idempotencyKey,
      error: message.error,
    }),
  };
}

function getBackendSteps(
  record: BackendCallbackFailedEvent,
): FailedEventBackendStep[] {
  if (Array.isArray(record.backendSteps) && record.backendSteps.length > 0) {
    return record.backendSteps;
  }

  return [
    {
      backendPath: record.backendPath,
      backendEventId: record.backendEventId,
    },
  ];
}

function isFailedEventPointer(
  value: unknown,
): value is FailedEventPointerMessage {
  return (
    isRecord(value) &&
    value.v === 2 &&
    typeof value.workflowInstanceId === "string"
  );
}

function isFailedEventRecordPointer(
  value: unknown,
): value is FailedEventRecordPointerMessage {
  return (
    isRecord(value) && value.v === 3 && typeof value.failedEventId === "string"
  );
}

function validatedStoredPointerId(message: unknown): string | undefined {
  if (!isFailedEventRecordPointer(message)) return undefined;
  try {
    return createFailedEventRecordPointerMessage(message.failedEventId)
      .failedEventId;
  } catch {
    return undefined;
  }
}

async function readStoredFailedEvent(
  env: Pick<Env, "CONTROL_DB">,
  failedEventId: string,
): Promise<StoredFailedEventRow | null> {
  return env.CONTROL_DB.prepare(
    `SELECT id, event_id, event_name, event_data, idempotency_key, metadata,
            error, status, attempts, max_attempts, updated_at
     FROM workflow_failed_events WHERE id=? LIMIT 1`,
  )
    .bind(failedEventId)
    .first<StoredFailedEventRow>();
}

async function claimStoredFailedEvent(
  env: Pick<Env, "CONTROL_DB">,
  claim: StoredFailedEventClaim,
): Promise<StoredFailedEventRow | null> {
  const now = Date.now();
  const expiresAt = now + FAILED_EVENT_CLAIM_LEASE_MS;
  return env.CONTROL_DB.prepare(
    `UPDATE workflow_failed_events
     SET status='PROCESSING',
         attempts=attempts + 1,
         metadata=json_set(
           CASE
             WHEN json_valid(coalesce(metadata, '{}')) THEN coalesce(metadata, '{}')
             ELSE json_object('corruptRecoveryMetadata', metadata)
           END,
           '${QUEUE_LEASE_OWNER_PATH}', ?,
           '${QUEUE_LEASE_EXPIRES_AT_PATH}', ?
         ),
         updated_at=?
     WHERE id=? AND attempts < max_attempts AND (
       (status='PENDING' AND
        coalesce(
          CASE WHEN json_valid(metadata)
            THEN CAST(json_extract(metadata, '${QUEUE_RETRY_NOT_BEFORE_PATH}') AS INTEGER)
          END,
          0
        ) <= ?) OR (
         status='PROCESSING' AND
         coalesce(
           CASE WHEN json_valid(metadata)
             THEN CAST(json_extract(metadata, '${QUEUE_LEASE_EXPIRES_AT_PATH}') AS INTEGER)
           END,
           updated_at + ?
         ) <= ?
       )
     )
     RETURNING id, event_id, event_name, event_data, idempotency_key, metadata,
               error, status, attempts, max_attempts, updated_at`,
  )
    .bind(
      claim.owner,
      expiresAt,
      now,
      claim.id,
      now,
      FAILED_EVENT_CLAIM_LEASE_MS,
      now,
    )
    .first<StoredFailedEventRow>();
}

async function markExhaustedStoredFailedEvent(
  env: Pick<Env, "CONTROL_DB">,
  row: StoredFailedEventRow,
): Promise<StoredFailedEventRow | null> {
  const now = Date.now();
  return env.CONTROL_DB.prepare(
    `UPDATE workflow_failed_events
     SET status='DEAD',
         error='Failed-event durable retry attempts exhausted',
         metadata=json_remove(
           metadata,
           '${QUEUE_LEASE_OWNER_PATH}',
           '${QUEUE_LEASE_EXPIRES_AT_PATH}',
           '${QUEUE_RETRY_NOT_BEFORE_PATH}'
         ),
         resolved_at=?, updated_at=?
     WHERE id=? AND attempts >= max_attempts AND (
       status='PENDING' OR (
         status='PROCESSING' AND
         coalesce(
           CASE WHEN json_valid(metadata)
             THEN CAST(json_extract(metadata, '${QUEUE_LEASE_EXPIRES_AT_PATH}') AS INTEGER)
           END,
           updated_at + ?
         ) <= ?
       )
     )
     RETURNING id, event_id, event_name, event_data, idempotency_key, metadata,
               error, status, attempts, max_attempts, updated_at`,
  )
    .bind(now, now, row.id, FAILED_EVENT_CLAIM_LEASE_MS, now)
    .first<StoredFailedEventRow>();
}

async function renewStoredFailedEventClaim(
  env: Pick<Env, "CONTROL_DB">,
  claim: StoredFailedEventClaim,
): Promise<void> {
  const now = Date.now();
  const renewed = await env.CONTROL_DB.prepare(
    `UPDATE workflow_failed_events
     SET metadata=json_set(metadata, '${QUEUE_LEASE_EXPIRES_AT_PATH}', ?),
         updated_at=?
     WHERE id=? AND status='PROCESSING'
       AND json_valid(metadata)
       AND json_extract(metadata, '${QUEUE_LEASE_OWNER_PATH}')=?`,
  )
    .bind(now + FAILED_EVENT_CLAIM_LEASE_MS, now, claim.id, claim.owner)
    .run();
  if ((renewed.meta.changes ?? 0) !== 1) {
    throw new LostFailedEventClaimError(claim);
  }
}

function parseStoredFailedEventRecord(
  row: StoredFailedEventRow,
): BackendCallbackFailedEvent {
  const payload = parseStoredObject(row.event_data, "event_data");
  const metadata = parseStoredObject(row.metadata, "metadata");
  return parseFailedEventRecord({
    eventId: metadata.recoveryEventId,
    workflowName: row.event_name,
    backendPath: metadata.backendPath,
    backendEventId: metadata.backendEventId,
    backendSteps: metadata.backendSteps,
    payload,
    idempotencyKey: row.idempotency_key,
    error: row.error,
  });
}

function assertCompatibleStoredFailedEvent(
  row: StoredFailedEventRow,
  expected: BackendCallbackFailedEvent,
): void {
  let actual: BackendCallbackFailedEvent;
  try {
    actual = parseStoredFailedEventRecord(row);
  } catch {
    throw failedEventConflict(row.id);
  }

  if (
    row.event_id !== expected.eventId ||
    canonicalJson(recoveryEnvelopeIdentity(actual)) !==
      canonicalJson(recoveryEnvelopeIdentity(expected))
  ) {
    throw failedEventConflict(row.id);
  }
}

function recoveryEnvelopeIdentity(
  record: BackendCallbackFailedEvent,
): Record<string, unknown> {
  return {
    eventId: record.eventId,
    workflowName: record.workflowName,
    backendPath: record.backendPath,
    backendEventId: record.backendEventId,
    backendSteps: getBackendSteps(record),
    payload: record.payload,
    idempotencyKey: record.idempotencyKey,
  };
}

function canonicalJson(value: unknown): string {
  const encoded = JSON.stringify(value);
  return canonicalJsonValue(
    encoded === undefined ? null : (JSON.parse(encoded) as unknown),
  );
}

function canonicalJsonValue(value: unknown): string {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value) ?? "null";
  }
  if (Array.isArray(value)) {
    return `[${value.map(canonicalJsonValue).join(",")}]`;
  }
  return `{${Object.entries(value)
    .filter(([, item]) => item !== undefined)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, item]) => `${JSON.stringify(key)}:${canonicalJsonValue(item)}`)
    .join(",")}}`;
}

function failedEventConflict(failedEventId: string): Error {
  const error = new Error(
    `Failed-event recovery ID ${failedEventId} already belongs to a different envelope`,
  );
  error.name = "NonRetryableError";
  return error;
}

function storedFailedEventRetryAt(
  row: StoredFailedEventRow,
): number | undefined {
  if (row.status !== "PENDING" && row.status !== "PROCESSING") {
    return undefined;
  }
  try {
    const metadata = parseStoredObject(row.metadata, "metadata");
    const retryAt =
      row.status === "PROCESSING"
        ? metadata.queueLeaseExpiresAt
        : metadata.queueRetryNotBefore;
    if (typeof retryAt === "number" && Number.isFinite(retryAt)) {
      return retryAt;
    }
  } catch {
    // Pre-durable-backoff rows may not have either Queue timestamp yet.
  }
  return row.status === "PROCESSING"
    ? row.updated_at + FAILED_EVENT_CLAIM_LEASE_MS
    : Date.now() + INITIAL_RETRY_DELAY_SECONDS * 1_000;
}

async function settleUnclaimedStoredDelivery(
  message: Message<FailedEventQueueMessage>,
  env: Pick<
    Env,
    "CONTROL_DB" | "FAILED_EVENTS_QUEUE" | "FAILED_EVENTS_DLQ"
  >,
  state: Extract<ResolvedFailedEvent, { record: null }>,
): Promise<void> {
  if (state.status === "COMPLETED") {
    message.ack();
    log("info", "failed_event.duplicate_completed", {
      messageId: message.id,
      failedEventId: state.storedId,
    });
    return;
  }

  if (state.status === "DEAD") {
    try {
      await publishFailedEventDlq(env, state.storedId);
      message.ack();
      log("info", "failed_event.duplicate_dead_lettered", {
        messageId: message.id,
        failedEventId: state.storedId,
      });
    } catch (error) {
      const delaySeconds = getRetryDelay(message.attempts);
      message.retry({ delaySeconds });
      log("error", "failed_event.dead_letter_failed", {
        messageId: message.id,
        failedEventId: state.storedId,
        transportAttempt: message.attempts,
        delaySeconds,
        error: boundedLogError(error),
      });
    }
    return;
  }

  await retryStoredDelivery(
    message,
    env,
    state.storedId,
    state.retryAt,
    "claim_busy",
  );
}

async function settleLostStoredClaim(
  message: Message<FailedEventQueueMessage>,
  env: Pick<
    Env,
    "CONTROL_DB" | "FAILED_EVENTS_QUEUE" | "FAILED_EVENTS_DLQ"
  >,
  claim: StoredFailedEventClaim,
): Promise<void> {
  try {
    const row = await readStoredFailedEvent(env, claim.id);
    if (row === null) {
      try {
        await env.FAILED_EVENTS_DLQ.send(message.body);
        message.ack();
      } catch (error) {
        const delaySeconds = getRetryDelay(message.attempts);
        message.retry({ delaySeconds });
        log("error", "failed_event.claim_row_missing_dead_letter_failed", {
          messageId: message.id,
          failedEventId: claim.id,
          transportAttempt: message.attempts,
          delaySeconds,
          error: boundedLogError(error),
        });
      }
      return;
    }
    await settleUnclaimedStoredDelivery(message, env, {
      record: null,
      storedId: row.id,
      status: row.status,
      retryAt: storedFailedEventRetryAt(row),
    });
  } catch (error) {
    await retryStoredDelivery(
      message,
      env,
      claim.id,
      undefined,
      "claim_state_read_failed",
      error,
    );
  }
}

async function retryStoredDelivery(
  message: Message<FailedEventQueueMessage>,
  env: Pick<Env, "CONTROL_DB" | "FAILED_EVENTS_QUEUE">,
  failedEventId: string,
  retryAt: number | undefined,
  reason: string,
  error?: unknown,
): Promise<void> {
  const delaySeconds = Math.min(
    43_200,
    Math.max(
      60,
      retryAt === undefined
        ? getRetryDelay(message.attempts)
        : Math.ceil((retryAt - Date.now()) / 1_000),
    ),
  );
  if (message.attempts >= QUEUE_DELIVERY_ROLLOVER_ATTEMPT) {
    try {
      const delivery = await rearmFailedEventRetry(env, failedEventId, {
        delaySeconds,
      });
      message.ack();
      log("info", "failed_event.delivery_rolled_over", {
        messageId: message.id,
        failedEventId,
        transportAttempt: message.attempts,
        delaySeconds,
        delivery,
      });
      return;
    } catch (rolloverError) {
      log("error", "failed_event.delivery_rollover_failed", {
        messageId: message.id,
        failedEventId,
        transportAttempt: message.attempts,
        error: boundedLogError(rolloverError),
        delaySeconds,
      });
    }
  }

  message.retry({ delaySeconds });
  log(error === undefined ? "info" : "error", `failed_event.${reason}`, {
    messageId: message.id,
    transportAttempt: message.attempts,
    delaySeconds,
    ...(error === undefined ? {} : { error: boundedLogError(error) }),
  });
}

function parseStoredObject(
  value: string | null,
  field: string,
): Record<string, unknown> {
  if (value === null) {
    throw invalidQueueMessage(`Stored recovery ${field} is missing`);
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(value);
  } catch {
    throw invalidQueueMessage(`Stored recovery ${field} is not valid JSON`);
  }
  if (!isRecord(parsed)) {
    throw invalidQueueMessage(`Stored recovery ${field} must be an object`);
  }
  return parsed;
}

async function markStoredFailedEventCompleted(
  env: Pick<Env, "CONTROL_DB">,
  claim: StoredFailedEventClaim,
): Promise<boolean> {
  const now = Date.now();
  const completed = await env.CONTROL_DB.prepare(
    `UPDATE workflow_failed_events
     SET status='COMPLETED',
         metadata=json_remove(
           metadata,
           '${QUEUE_LEASE_OWNER_PATH}',
           '${QUEUE_LEASE_EXPIRES_AT_PATH}',
           '${QUEUE_RETRY_NOT_BEFORE_PATH}'
         ),
         resolved_at=?, updated_at=?
     WHERE id=? AND status='PROCESSING'
       AND json_valid(metadata)
       AND json_extract(metadata, '${QUEUE_LEASE_OWNER_PATH}')=?`,
  )
    .bind(now, now, claim.id, claim.owner)
    .run();
  return (completed.meta.changes ?? 0) === 1;
}

async function markStoredFailedEventDead(
  env: Pick<Env, "CONTROL_DB">,
  claim: StoredFailedEventClaim,
  error: unknown,
): Promise<boolean> {
  const now = Date.now();
  const dead = await env.CONTROL_DB.prepare(
    `UPDATE workflow_failed_events
     SET status='DEAD', error=?,
         metadata=json_remove(
           metadata,
           '${QUEUE_LEASE_OWNER_PATH}',
           '${QUEUE_LEASE_EXPIRES_AT_PATH}',
           '${QUEUE_RETRY_NOT_BEFORE_PATH}'
         ),
         resolved_at=?, updated_at=?
     WHERE id=? AND status='PROCESSING'
       AND json_valid(metadata)
       AND json_extract(metadata, '${QUEUE_LEASE_OWNER_PATH}')=?`,
  )
    .bind(
      boundedLogError(error).slice(0, 1_000),
      now,
      now,
      claim.id,
      claim.owner,
    )
    .run();
  return (dead.meta.changes ?? 0) === 1;
}

async function releaseStoredFailedEventClaim(
  env: Pick<Env, "CONTROL_DB">,
  claim: StoredFailedEventClaim,
  retryAt: number,
  error: unknown,
): Promise<boolean> {
  const now = Date.now();
  const released = await env.CONTROL_DB.prepare(
    `UPDATE workflow_failed_events
     SET status='PENDING',
         error=?,
         next_retry_at=?,
         metadata=json_remove(
           json_set(metadata, '${QUEUE_RETRY_NOT_BEFORE_PATH}', ?),
           '${QUEUE_LEASE_OWNER_PATH}',
           '${QUEUE_LEASE_EXPIRES_AT_PATH}'
         ),
         updated_at=?
     WHERE id=? AND status='PROCESSING'
       AND json_valid(metadata)
       AND json_extract(metadata, '${QUEUE_LEASE_OWNER_PATH}')=?`,
  )
    .bind(
      boundedLogError(error).slice(0, 1_000),
      retryAt,
      retryAt,
      now,
      claim.id,
      claim.owner,
    )
    .run();
  return (released.meta.changes ?? 0) === 1;
}

function parseFailedEventRecord(value: unknown): BackendCallbackFailedEvent {
  if (!isRecord(value)) {
    throw invalidQueueMessage("Workflow recovery record is missing");
  }

  const eventId = boundedString(value.eventId, "eventId", 100);
  const workflowName = boundedString(value.workflowName, "workflowName", 256);
  const backendPath = boundedString(value.backendPath, "backendPath", 512);
  const backendEventId = boundedString(
    value.backendEventId,
    "backendEventId",
    512,
  );
  const idempotencyKey = boundedString(
    value.idempotencyKey,
    "idempotencyKey",
    512,
  );
  const error = boundedString(value.error, "error", 2_048);
  if (!isRecord(value.payload)) {
    throw invalidQueueMessage("Workflow recovery payload must be an object");
  }

  const backendSteps = parseBackendSteps(value.backendSteps);
  const record: BackendCallbackFailedEvent = {
    eventId,
    workflowName,
    backendPath,
    backendEventId,
    ...(backendSteps === undefined ? {} : { backendSteps }),
    payload: value.payload,
    idempotencyKey,
    error,
  };
  try {
    assertCloudflareJsonSerializable(record, "Workflow recovery record");
  } catch (error) {
    throw invalidQueueMessage(boundedLogError(error));
  }
  return record;
}

function parseBackendSteps(
  value: unknown,
): BackendCallbackFailedEvent["backendSteps"] {
  if (value === undefined) return undefined;
  if (!Array.isArray(value) || value.length === 0 || value.length > 32) {
    throw invalidQueueMessage(
      "Workflow recovery backendSteps must contain 1-32 items",
    );
  }
  return value.map((step, index) => {
    if (!isRecord(step)) {
      throw invalidQueueMessage(
        `Workflow recovery backendSteps[${index}] is invalid`,
      );
    }
    return {
      backendPath: boundedString(
        step.backendPath,
        `backendSteps[${index}].backendPath`,
        512,
      ),
      backendEventId: boundedString(
        step.backendEventId,
        `backendSteps[${index}].backendEventId`,
        512,
      ),
    };
  });
}

function boundedString(
  value: unknown,
  field: string,
  maxLength: number,
): string {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    value.length > maxLength ||
    /[\u0000-\u001f\u007f]/.test(value)
  ) {
    throw invalidQueueMessage(
      `Workflow recovery ${field} must contain 1-${maxLength} printable characters`,
    );
  }
  return value;
}

function invalidQueueMessage(message: string): Error {
  const error = new Error(`Invalid failed-event Queue message: ${message}`);
  error.name = "NonRetryableError";
  return error;
}

function boundedLogError(error: unknown): string {
  const raw = error instanceof Error ? error.message : String(error);
  return raw
    .replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 512);
}

function log(
  level: "info" | "warn" | "error",
  event: string,
  fields: Record<string, unknown>,
): void {
  const line = JSON.stringify({ level, event, ...fields });
  if (level === "error") console.error(line);
  else if (level === "warn") console.warn(line);
  else console.info(line);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
