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
  auth: {
    bearerToken: (env) => env.AUTH_TOKEN,
  },
  maxRequestBytes: 1_048_576,
  resolveWorkflow(eventName, env) {
    return backendCallbackWorkflowRegistry.has(eventName) ? env.WORKFLOW : null;
  },
});

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === "/failed-events" && request.method === "GET") {
      if (!env.AUTH_TOKEN) {
        return new Response("Workflow worker auth is not configured", {
          status: 500,
        });
      }

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
