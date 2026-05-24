// ─── Workflows Worker — Generic SDK Registry Entrypoint ─────────────────────
// Reusable Cloudflare Worker that dispatches SDK envelopes to one generic
// Cloudflare Workflow binding and executes registered Manhali workflows.

import { WorkflowEntrypoint } from "cloudflare:workers";
import {
  createCloudflareDispatchHandler,
  createCloudflareWorkflowEntrypoint,
} from "@abshahin/workflows-sdk/cloudflare";
import {
  manhaliWorkflowRegistry,
  type ManhaliWorkflowServices,
} from "@manhali/workflows";
import type { Env } from "./env.js";
import {
  type FailedEventQueueMessage,
  processFailedEventBatch,
  storeFailedEvent,
} from "./lib/failed-events.js";
import { callBackendService } from "./lib/backend.js";

function timingSafeEqual(a: string, b: string): boolean {
  const encoder = new TextEncoder();
  const left = encoder.encode(a);
  const right = encoder.encode(b);
  if (left.byteLength !== right.byteLength) return false;

  let result = 0;
  for (let index = 0; index < left.byteLength; index++) {
    result |= left[index]! ^ right[index]!;
  }

  return result === 0;
}

function verifyAuth(header: string | null, expectedToken: string): boolean {
  if (!header?.startsWith("Bearer ")) return false;
  return timingSafeEqual(header.slice(7), expectedToken);
}

function createManhaliWorkflowServices(env: Env): ManhaliWorkflowServices {
  return {
    backend: {
      execute(path, payload, context) {
        return callBackendService(
          env,
          path,
          payload,
          context.traceId,
          context.eventId,
        );
      },
    },
    failedEvents: {
      record(event) {
        return storeFailedEvent(env.FAILED_EVENTS_QUEUE, event);
      },
    },
  };
}

type WorkflowBaseClass = abstract new (...args: any[]) => object;

const ManhaliWorkflowBase: WorkflowBaseClass = createCloudflareWorkflowEntrypoint<
  Env,
  ManhaliWorkflowServices
>(WorkflowEntrypoint<Env>, {
  registry: manhaliWorkflowRegistry,
  services: createManhaliWorkflowServices,
});

export class ManhaliWorkflow extends ManhaliWorkflowBase {}

// Compatibility exports for workflow instances created before the generic
// registry refactor. Wrangler now binds only ManhaliWorkflow, but keeping these
// class names available makes old class_name references safer during rollout.
export class EmailWorkflow extends ManhaliWorkflowBase {}
export class NotificationWorkflow extends ManhaliWorkflowBase {}
export class PaymentWorkflow extends ManhaliWorkflowBase {}
export class WhatsappWorkflow extends ManhaliWorkflowBase {}

const dispatchHandler = createCloudflareDispatchHandler<Env>({
  registry: manhaliWorkflowRegistry,
  auth: {
    bearerToken: (env) => env.AUTH_TOKEN,
  },
  maxRequestBytes: 1_048_576,
  rateLimit: {
    max: 500,
    windowMs: 60_000,
  },
  resolveWorkflow(eventName, env) {
    return manhaliWorkflowRegistry.has(eventName) ? env.WORKFLOW : null;
  },
});

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === "/failed-events" && request.method === "GET") {
      if (!verifyAuth(request.headers.get("Authorization"), env.AUTH_TOKEN)) {
        return new Response("Unauthorized", { status: 401 });
      }

      return Response.json({
        info: "Failed events are managed via Cloudflare Queues. Check the Cloudflare dashboard for queue metrics.",
        queues: {
          retry: "manhali-failed-events",
          deadLetter: "manhali-failed-events-dlq",
        },
      });
    }

    return dispatchHandler.fetch(request, env);
  },

  scheduled(
    controller: ScheduledController,
    env: Env,
  ): Promise<{ dispatched: number }> {
    return dispatchHandler.scheduled(controller, env);
  },

  async queue(
    batch: MessageBatch<FailedEventQueueMessage>,
    env: Env,
  ): Promise<void> {
    await processFailedEventBatch(batch, env);
  },
};
