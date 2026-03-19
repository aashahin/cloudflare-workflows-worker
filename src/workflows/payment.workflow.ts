// ─── Payment Workflow ────────────────────────────────────────────────────────
// Cloudflare Workflow that orchestrates payout processing.
// Steps: validate → process → notify owner.

import { PAYMENT_EVENTS } from "@abshahin/workflows-sdk";
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

interface PaymentWorkflowParams {
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

export class PaymentWorkflow extends WorkflowEntrypoint<
  Env,
  PaymentWorkflowParams
> {
  override async run(
    event: WorkflowEvent<PaymentWorkflowParams>,
    step: WorkflowStep,
  ) {
    const { eventId, traceId, eventName, data, delayMs } = event.payload;

    console.log(
      `[PaymentWorkflow] Running — event=${eventName}, id=${eventId}, trace=${traceId}`,
    );

    // Bound helper for backend calls with trace + idempotency propagation
    const callBackend = (path: string) =>
      callBackendService(this.env, path, data, traceId, eventId);

    // Optional delay before processing
    if (delayMs > 0) {
      await step.sleep("delay-before-process", msToDuration(delayMs));
    }

    try {
      switch (eventName) {
        case PAYMENT_EVENTS.PROCESS_PAYOUT:
          // Step 1: Validate payout profile + transaction
          await step.do("validate-payout", RETRY_CONFIG, () =>
            callBackend("payment/validate-payout"),
          );

          // Step 2: Process the payout (mark processing, future: call gateway)
          await step.do("process-payout", RETRY_CONFIG, () =>
            callBackend("payment/process-payout"),
          );

          // Step 3: Notify tenant owner of withdrawal status
          await step.do("notify-payout-status", RETRY_CONFIG, () =>
            callBackend("payment/notify-payout-status"),
          );
          break;

        default:
          console.error(`[PaymentWorkflow] Unknown event: ${eventName}`);
          throw new Error(`Unknown payment event: ${eventName}`);
      }
    } catch (err) {
      const errorMsg = err instanceof Error ? err.message : String(err);
      const isNonRetryable = isNonRetryableFailure(err);

      console.error(
        `[PaymentWorkflow] ${isNonRetryable ? "Non-retryable" : "Permanently"} failed — event=${eventName}, id=${eventId}, trace=${traceId}:`,
        errorMsg,
      );

      if (!isNonRetryable) {
        await step.do(
          "persist-failed-event",
          { retries: { limit: 2, delay: "1 second", backoff: "linear" } },
          async () => {
            await storeFailedEvent(this.env.FAILED_EVENTS_QUEUE, {
              eventId,
              workflowType: "payment",
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
