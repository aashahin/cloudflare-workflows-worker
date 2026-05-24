// ─── Failed Events Queue ─────────────────────────────────────────────────────
// Persists permanently-failed workflow events into a Cloudflare Queue so the
// queue consumer can re-execute them directly against the backend with
// progressive retry delays driven by RETRY_DELAYS_SECONDS.
//
// The consumer calls the backend directly (not via workflow.create) so that
// message.attempts increments naturally on each failure and message.retry()
// applies the correct progressive delay from the schedule.
//
// Retry schedule (10 attempts max, controlled by wrangler.jsonc max_retries):
//   1m, 3m, 5m, 10m, 15m, 20m, 30m, 45m, 60m, 90m

import type { ManhaliFailedWorkflowEvent } from "@manhali/workflows";
import type { Env } from "../env.js";
import { callBackendService, isNonRetryableFailure } from "./backend.js";

// ─── Types ───────────────────────────────────────────────────────────────────

export interface FailedEventMessage extends ManhaliFailedWorkflowEvent {
  createdAt: string;
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
  | FailedEventMessage
  | LegacyFailedEventMessage;

// ─── Retry schedule (seconds) — indexed by message.attempts ──────────────────

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

/** Get the retry delay in seconds for the given attempt number (1-based). */
function getRetryDelay(attempt: number): number {
  const idx = Math.min(
    Math.max(attempt - 1, 0),
    RETRY_DELAYS_SECONDS.length - 1,
  );
  return RETRY_DELAYS_SECONDS[idx] ?? 60;
}

// ─── Store a failed event ────────────────────────────────────────────────────

export async function storeFailedEvent(
  queue: Queue,
  params: ManhaliFailedWorkflowEvent,
): Promise<void> {
  const message: FailedEventMessage = {
    eventId: params.eventId,
    workflowName: params.workflowName,
    backendPath: params.backendPath,
    backendEventId: params.backendEventId,
    payload: params.payload,
    idempotencyKey: params.idempotencyKey,
    error: params.error,
    createdAt: new Date().toISOString(),
  };

  // First delivery uses RETRY_DELAYS_SECONDS[0] = 60s
  await queue.send(message, { delaySeconds: RETRY_DELAYS_SECONDS[0] });

  console.log(
    `[FailedEvents] Queued failed event ${params.eventId} (${params.workflowName}) - first retry in ${RETRY_DELAYS_SECONDS[0]}s`,
  );
}

// ─── Process a batch of failed event messages (queue consumer) ───────────────
// Calls the backend directly. On success → ack. On failure → message.retry()
// with progressive delay from RETRY_DELAYS_SECONDS based on message.attempts.
// After max_retries (wrangler.jsonc), Cloudflare routes to the DLQ.

export async function processFailedEventBatch(
  batch: MessageBatch<FailedEventQueueMessage>,
  env: Env,
): Promise<void> {
  for (const message of batch.messages) {
    const record = normalizeFailedEventMessage(message.body);
    const traceId = `${record.eventId}:retry:${message.attempts}`;

    try {
      // Call backend directly — same path the workflow step would use
      await callBackendService(
        env,
        record.backendPath,
        record.payload,
        traceId,
        record.backendEventId,
      );

      message.ack();
      console.log(
        `[FailedEvents] Retry #${message.attempts} succeeded for ${record.eventId} (${record.workflowName})`,
      );
    } catch (err) {
      // Non-retryable errors (4xx) will never succeed — ack to prevent
      // wasting all retry attempts on a permanently broken event.
      if (isNonRetryableFailure(err)) {
        message.ack();
        console.error(
          `[FailedEvents] Non-retryable error for ${record.eventId} (${record.workflowName}) - dropping: ${err instanceof Error ? err.message : String(err)}`,
        );
        continue;
      }

      const delay = getRetryDelay(message.attempts);
      const errorMsg = err instanceof Error ? err.message : String(err);

      console.warn(
        `[FailedEvents] Retry #${message.attempts} failed for ${record.eventId} (${record.workflowName}): ${errorMsg} - next retry in ${delay}s`,
      );

      // message.attempts increments on each delivery. After max_retries
      // (configured in wrangler.jsonc), Cloudflare dead-letters automatically.
      message.retry({ delaySeconds: delay });
    }
  }
}

function normalizeFailedEventMessage(
  message: FailedEventQueueMessage,
): FailedEventMessage {
  if ("workflowName" in message) return message;

  return {
    eventId: message.eventId,
    workflowName: message.eventName,
    backendPath: message.backendPath ?? message.eventName,
    backendEventId: message.backendEventId ?? message.eventId,
    payload: message.data,
    idempotencyKey: message.idempotencyKey,
    error: message.error,
    createdAt: message.createdAt,
  };
}
