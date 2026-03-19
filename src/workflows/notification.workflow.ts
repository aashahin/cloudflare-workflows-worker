// ─── Notification Workflow ────────────────────────────────────────────────────
// Cloudflare Workflow that processes notification events.

import { NOTIFICATION_EVENTS } from "@shahin/workflows-sdk";
import {
  WorkflowEntrypoint,
  type WorkflowEvent,
  type WorkflowStep,
} from "cloudflare:workers";
import type { Env } from "../env.js";
import {
  callBackendService,
  isNonRetryableFailure,
  msToDuration,
} from "../lib/backend.js";
import { storeFailedEvent } from "../lib/failed-events.js";

// ─── Workflow params shape ───────────────────────────────────────────────────

interface NotificationWorkflowParams {
  eventId: string;
  idempotencyKey: string;
  traceId: string;
  eventName: string;
  data: Record<string, unknown>;
  delayMs: number;
}

// ─── Retry config ────────────────────────────────────────────────────────────

const RETRY_CONFIG = {
  retries: {
    limit: 3,
    delay: "1 second" as const,
    backoff: "exponential" as const,
  },
};

// ─── Workflow class ──────────────────────────────────────────────────────────

export class NotificationWorkflow extends WorkflowEntrypoint<
  Env,
  NotificationWorkflowParams
> {
  override async run(
    event: WorkflowEvent<NotificationWorkflowParams>,
    step: WorkflowStep,
  ) {
    const { eventId, traceId, eventName, data, delayMs } = event.payload;

    // Bound helper for backend calls with trace + idempotency propagation
    const callBackend = (path: string) =>
      callBackendService(this.env, path, data, traceId, eventId);

    // Optional delay before processing
    if (delayMs > 0) {
      await step.sleep("delay-before-notify", msToDuration(delayMs));
    }

    try {
      switch (eventName) {
        case NOTIFICATION_EVENTS.CREATE:
          await step.do("create-notification", RETRY_CONFIG, () =>
            callBackend("notification/create"),
          );
          break;

        case NOTIFICATION_EVENTS.CREATE_FOR_CUSTOMER:
          await step.do("create-customer-notification", RETRY_CONFIG, () =>
            callBackend("notification/create-for-customer"),
          );
          break;

        case NOTIFICATION_EVENTS.BULK_CREATE:
          await step.do("bulk-create-notifications", RETRY_CONFIG, () =>
            callBackend("notification/bulk-create"),
          );
          break;

        default:
          console.error(`[NotificationWorkflow] Unknown event: ${eventName}`);
          throw new Error(`Unknown notification event: ${eventName}`);
      }
    } catch (err) {
      const errorMsg = err instanceof Error ? err.message : String(err);
      const isNonRetryable = isNonRetryableFailure(err);

      console.error(
        `[NotificationWorkflow] ${isNonRetryable ? "Non-retryable" : "Permanently"} failed — event=${eventName}, id=${eventId}, trace=${traceId}:`,
        errorMsg,
      );

      // Non-retryable errors (4xx, bad config, validation) will fail
      // identically on every attempt — skip the retry queue.
      if (!isNonRetryable) {
        await step.do(
          "persist-failed-event",
          { retries: { limit: 2, delay: "1 second", backoff: "linear" } },
          async () => {
            await storeFailedEvent(this.env.FAILED_EVENTS_QUEUE, {
              eventId,
              workflowType: "notification",
              eventName,
              data,
              idempotencyKey: event.payload.idempotencyKey,
              error: errorMsg,
            });
          },
        );
      }
    }

    return { status: "completed", eventId, eventName };
  }
}
