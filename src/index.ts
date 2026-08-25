// ─── Workflows Worker — Generic SDK Registry Entrypoint ─────────────────────
// Reusable Cloudflare Worker that dispatches SDK envelopes to one generic
// Cloudflare Workflow binding and executes backend callback workflows.

import { WorkflowEntrypoint } from "cloudflare:workers";
import {
  createBackendCallbackWorkflowRegistry,
  type BackendCallbackWorkflowServices,
} from "@abshahin/workflows-sdk";
import {
  createCloudflareDispatchHandler,
  createCloudflareWorkflowDispatch,
  createCloudflareWorkflowEntrypoint,
} from "@abshahin/workflows-sdk/cloudflare";
import type { Env } from "./env.js";
import {
  type FailedEventQueueMessage,
  processFailedEventBatch,
  storeFailedEvent,
} from "./lib/failed-events.js";
import { callBackendService } from "./lib/backend.js";

async function sha256(bytes: Uint8Array): Promise<Uint8Array> {
  return new Uint8Array(await crypto.subtle.digest("SHA-256", bytes));
}

/** Hash then compare so token length is not leaked (analytics auth pattern). */
async function timingSafeEqual(a: string, b: string): Promise<boolean> {
  const encoder = new TextEncoder();
  const [left, right] = await Promise.all([
    sha256(encoder.encode(a)),
    sha256(encoder.encode(b)),
  ]);
  const subtle = crypto.subtle as SubtleCrypto & {
    timingSafeEqual?: (x: ArrayBufferView, y: ArrayBufferView) => boolean;
  };
  if (typeof subtle.timingSafeEqual === "function") {
    return subtle.timingSafeEqual(left, right);
  }
  let diff = 0;
  for (let i = 0; i < left.length; i++) diff |= left[i]! ^ right[i]!;
  return diff === 0;
}

async function verifyAuth(
  header: string | null,
  expectedToken: string,
): Promise<boolean> {
  if (!expectedToken) return false;
  if (!header?.startsWith("Bearer ")) return false;
  return timingSafeEqual(header.slice(7), expectedToken);
}

const backendCallbackWorkflowRegistry = createBackendCallbackWorkflowRegistry({
  defaultStepName: getBackendCallbackStepName,
});

function getBackendCallbackStepName(workflowName: string): string {
  if (workflowName.startsWith("email/")) {
    return `send-${workflowName.replace(/[^a-zA-Z0-9]+/g, "-")}`;
  }
  if (workflowName.startsWith("notification/")) {
    return `notify-${workflowName.replace(/[^a-zA-Z0-9]+/g, "-")}`;
  }
  if (workflowName === "whatsapp/send-template") {
    return "send-whatsapp-template";
  }
  return `callback-${workflowName.replace(/[^a-zA-Z0-9]+/g, "-")}`;
}

function createBackendCallbackWorkflowServices(
  env: Env,
): BackendCallbackWorkflowServices {
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

const BackendCallbackWorkflowBase: WorkflowBaseClass =
  createCloudflareWorkflowEntrypoint<Env, BackendCallbackWorkflowServices>(
    WorkflowEntrypoint<Env>,
    {
      registry: backendCallbackWorkflowRegistry,
      services: createBackendCallbackWorkflowServices,
      dispatch: createCloudflareWorkflowDispatch<Env>({
        registry: backendCallbackWorkflowRegistry,
        resolveWorkflow(eventName, env) {
          return backendCallbackWorkflowRegistry.has(eventName)
            ? env.WORKFLOW
            : null;
        },
      }),
    },
  );

export class BackendCallbackWorkflow extends BackendCallbackWorkflowBase {}

const dispatchHandler = createCloudflareDispatchHandler<Env>({
  registry: backendCallbackWorkflowRegistry,
  maxRequestBytes: 1_048_576,
  resolveWorkflow(eventName, env) {
    return backendCallbackWorkflowRegistry.has(eventName) ? env.WORKFLOW : null;
  },
});

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    if (!env.AUTH_TOKEN) {
      return new Response("Workflow worker auth is not configured", {
        status: 500,
      });
    }

    if (
      !(await verifyAuth(request.headers.get("Authorization"), env.AUTH_TOKEN))
    ) {
      return new Response("Unauthorized", { status: 401 });
    }

    const url = new URL(request.url);
    if (url.pathname === "/failed-events" && request.method === "GET") {
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

  async queue(
    batch: MessageBatch<FailedEventQueueMessage>,
    env: Env,
  ): Promise<void> {
    await processFailedEventBatch(batch, env);
  },

  async scheduled(
    controller: ScheduledController,
    env: Env,
  ): Promise<void> {
    await dispatchHandler.scheduled(controller, env);
  },
};
