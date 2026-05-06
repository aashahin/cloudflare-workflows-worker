// ─── WhatsApp Workflow ──────────────────────────────────────────────────────
// Cloudflare Workflow that processes WhatsApp template events.

import { WHATSAPP_EVENTS } from "@abshahin/workflows-sdk";
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

interface WhatsappWorkflowParams {
  eventId: string;
  idempotencyKey: string;
  traceId: string;
  eventName: string;
  data: Record<string, unknown>;
  delayMs: number;
}

const RETRY_CONFIG = {
  retries: {
    limit: 3,
    delay: "1 second" as const,
    backoff: "exponential" as const,
  },
};

export class WhatsappWorkflow extends WorkflowEntrypoint<
  Env,
  WhatsappWorkflowParams
> {
  override async run(
    event: WorkflowEvent<WhatsappWorkflowParams>,
    step: WorkflowStep,
  ) {
    const { eventId, traceId, eventName, data, delayMs } = event.payload;

    console.log(
      `[WhatsappWorkflow] Running — event=${eventName}, id=${eventId}, trace=${traceId}, keys=${Object.keys(data).join(",")}`,
    );

    if (delayMs > 0) {
      await step.sleep("delay-before-send", msToDuration(delayMs));
    }

    try {
      switch (eventName) {
        case WHATSAPP_EVENTS.SEND_TEMPLATE:
          await step.do("send-whatsapp-template", RETRY_CONFIG, () =>
            callBackendService(
              this.env,
              "whatsapp/send-template",
              data,
              traceId,
              eventId,
            ),
          );
          break;

        default:
          console.error(`[WhatsappWorkflow] Unknown event: ${eventName}`);
          throw new Error(`Unknown WhatsApp event: ${eventName}`);
      }
    } catch (err) {
      const errorMsg = err instanceof Error ? err.message : String(err);
      const isNonRetryable = isNonRetryableFailure(err);

      console.error(
        `[WhatsappWorkflow] ${isNonRetryable ? "Non-retryable" : "Permanently"} failed — event=${eventName}, id=${eventId}, trace=${traceId}:`,
        errorMsg,
      );

      if (!isNonRetryable) {
        await step.do(
          "persist-failed-event",
          { retries: { limit: 2, delay: "1 second", backoff: "linear" } },
          async () => {
            await storeFailedEvent(this.env.FAILED_EVENTS_QUEUE, {
              eventId,
              workflowType: "whatsapp",
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