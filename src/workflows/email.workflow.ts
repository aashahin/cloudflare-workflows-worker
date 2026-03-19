// ─── Email Workflow ──────────────────────────────────────────────────────────
// Cloudflare Workflow that processes email events.
// Each event type maps to a step that calls the backend email service.

import { EMAIL_EVENTS } from "@abshahin/workflows-sdk";
import {
  WorkflowEntrypoint,
  type WorkflowEvent,
  type WorkflowStep,
} from "cloudflare:workers";
import type { Env } from "../env.ts";
import {
  callBackendService,
  isNonRetryableFailure,
  msToDuration,
} from "../lib/backend.ts";
import { storeFailedEvent } from "../lib/failed-events.ts";

// ─── Workflow params shape ───────────────────────────────────────────────────

interface EmailWorkflowParams {
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

export class EmailWorkflow extends WorkflowEntrypoint<
  Env,
  EmailWorkflowParams
> {
  override async run(
    event: WorkflowEvent<EmailWorkflowParams>,
    step: WorkflowStep,
  ) {
    const { eventId, traceId, eventName, data, delayMs } = event.payload;

    console.log(
      `[EmailWorkflow] Running — event=${eventName}, id=${eventId}, trace=${traceId}, keys=${Object.keys(data).join(",")}`,
    );

    // Bound helper for backend calls with trace + idempotency propagation
    const callBackend = (path: string) =>
      callBackendService(this.env, path, data, traceId, eventId);

    // Optional delay before processing
    if (delayMs > 0) {
      await step.sleep("delay-before-send", msToDuration(delayMs));
    }

    // Route to the appropriate email handler
    try {
      switch (eventName) {
        case EMAIL_EVENTS.RESET_PASSWORD:
          await step.do("send-reset-password-email", RETRY_CONFIG, () =>
            callBackend("email/reset-password"),
          );
          break;

        case EMAIL_EVENTS.NEW_ACCOUNT_CREDENTIALS:
          await step.do(
            "send-new-account-credentials-email",
            RETRY_CONFIG,
            () => callBackend("email/new-account-credentials"),
          );
          break;

        case EMAIL_EVENTS.CHANGE_EMAIL_VERIFICATION:
          await step.do("send-change-email-verification", RETRY_CONFIG, () =>
            callBackend("email/change-email-verification"),
          );
          break;

        case EMAIL_EVENTS.VERIFICATION:
          await step.do("send-verification-email", RETRY_CONFIG, () =>
            callBackend("email/verification"),
          );
          break;

        case EMAIL_EVENTS.CART_RECOVERY:
          await step.do("send-cart-recovery-email", RETRY_CONFIG, () =>
            callBackend("email/cart-recovery"),
          );
          break;

        case EMAIL_EVENTS.INVITATION:
          await step.do("send-invitation-email", RETRY_CONFIG, () =>
            callBackend("email/invitation"),
          );
          break;

        case EMAIL_EVENTS.ENROLLMENT_CONFIRMATION:
          await step.do(
            "send-enrollment-confirmation-email",
            RETRY_CONFIG,
            () => callBackend("email/enrollment-confirmation"),
          );
          break;

        case EMAIL_EVENTS.TRIAL_REMINDER:
          await step.do("send-trial-reminder-email", RETRY_CONFIG, () =>
            callBackend("email/trial-reminder"),
          );
          break;

        case EMAIL_EVENTS.PAYMENT_RECEIPT:
          await step.do("send-payment-receipt-email", RETRY_CONFIG, () =>
            callBackend("email/payment-receipt"),
          );
          break;

        case EMAIL_EVENTS.WITHDRAWAL_STATUS:
          await step.do("send-withdrawal-status-email", RETRY_CONFIG, () =>
            callBackend("email/withdrawal-status"),
          );
          break;

        case EMAIL_EVENTS.FAILED_PAYMENT_ALERT:
          await step.do("send-failed-payment-alert-email", RETRY_CONFIG, () =>
            callBackend("email/failed-payment-alert"),
          );
          break;

        case EMAIL_EVENTS.REFUND_CONFIRMATION:
          await step.do("send-refund-confirmation-email", RETRY_CONFIG, () =>
            callBackend("email/refund-confirmation"),
          );
          break;

        default:
          console.error(`[EmailWorkflow] Unknown event: ${eventName}`);
          throw new Error(`Unknown email event: ${eventName}`);
      }
    } catch (err) {
      const errorMsg = err instanceof Error ? err.message : String(err);
      const isNonRetryable = isNonRetryableFailure(err);

      console.error(
        `[EmailWorkflow] ${isNonRetryable ? "Non-retryable" : "Permanently"} failed — event=${eventName}, id=${eventId}, trace=${traceId}:`,
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
              workflowType: "email",
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
