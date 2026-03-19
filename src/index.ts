// ─── Workflows Worker — Entrypoint ───────────────────────────────────────────
// Cloudflare Worker that receives dispatched workflow events and starts
// the appropriate Workflow instance.

import { EMAIL_EVENTS, NOTIFICATION_EVENTS, PAYMENT_EVENTS } from "@shahin/workflows-sdk";
import type { Env } from "./env.js";
import {
  type FailedEventMessage,
  processFailedEventBatch,
} from "./lib/failed-events.js";

// Re-export workflow classes so Cloudflare can discover them
export { EmailWorkflow } from "./workflows/email.workflow.js";
export { NotificationWorkflow } from "./workflows/notification.workflow.js";
export { PaymentWorkflow } from "./workflows/payment.workflow.js";

// ─── Security helpers ────────────────────────────────────────────────────────────────────

/** Constant-time string comparison to prevent timing attacks on auth tokens. */
function timingSafeEqual(a: string, b: string): boolean {
  const encoder = new TextEncoder();
  const bufA = encoder.encode(a);
  const bufB = encoder.encode(b);
  if (bufA.byteLength !== bufB.byteLength) return false;
  let result = 0;
  for (let i = 0; i < bufA.byteLength; i++) {
    result |= bufA[i]! ^ bufB[i]!;
  }
  return result === 0;
}

/** Verify Bearer token from Authorization header. */
function verifyAuth(header: string | null, expectedToken: string): boolean {
  if (!header?.startsWith("Bearer ")) return false;
  return timingSafeEqual(header.slice(7), expectedToken);
}

// ─── Rate limiting ─────────────────────────────────────────────────────────────────────
// Per-isolate sliding-window rate limiter. Not precise across multiple isolates
// but provides a reasonable defense against runaway dispatch loops.

const rateLimit = {
  count: 0,
  windowStart: Date.now(),
  max: 500,
  windowMs: 60_000,
};

function checkRateLimit(): boolean {
  const now = Date.now();
  if (now - rateLimit.windowStart > rateLimit.windowMs) {
    rateLimit.count = 0;
    rateLimit.windowStart = now;
  }
  return ++rateLimit.count <= rateLimit.max;
}

// ─── Dispatch payload shape (matches SDK HttpTransport) ──────────────────────

interface DispatchEvent {
  id: string;
  idempotencyKey: string;
  traceId?: string;
  event: {
    name: string;
    data: Record<string, unknown>;
  };
  delayMs: number;
}

interface DispatchPayload {
  events: DispatchEvent[];
}

// ─── Worker fetch handler ────────────────────────────────────────────────────

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    // Health check
    if (url.pathname === "/health") {
      return Response.json({ status: "ok" });
    }

    // Failed events stats (debug/observability)
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

    // Dispatch endpoint
    if (url.pathname === "/dispatch" && request.method === "POST") {
      // Guard against oversized payloads (1 MB limit)
      const contentLength = request.headers.get("content-length");
      if (contentLength && parseInt(contentLength, 10) > 1_048_576) {
        return Response.json({ error: "Payload too large" }, { status: 413 });
      }
      return handleDispatch(request, env);
    }

    return new Response("Not Found", { status: 404 });
  },

  // ─── Queue handler — retries failed events from Cloudflare Queue ─────────
  async queue(
    batch: MessageBatch<FailedEventMessage>,
    env: Env,
  ): Promise<void> {
    await processFailedEventBatch(batch, env);
  },
};

// ─── Dispatch handler ────────────────────────────────────────────────────────

async function handleDispatch(request: Request, env: Env): Promise<Response> {
  // Rate limit
  if (!checkRateLimit()) {
    return Response.json(
      { error: "Rate limit exceeded — try again shortly" },
      { status: 429 },
    );
  }

  // Authenticate (timing-safe)
  if (!verifyAuth(request.headers.get("Authorization"), env.AUTH_TOKEN)) {
    return new Response("Unauthorized", { status: 401 });
  }

  // Parse body
  let body: DispatchPayload;
  try {
    body = (await request.json()) as DispatchPayload;
  } catch {
    return Response.json({ error: "Invalid JSON" }, { status: 400 });
  }

  if (!body.events || !Array.isArray(body.events)) {
    return Response.json({ error: "Missing events array" }, { status: 400 });
  }

  const ids: string[] = [];
  const errors: Array<{ id: string; error: string }> = [];

  for (const item of body.events) {
    try {
      // Validate item structure
      if (
        !item.id ||
        !item.event?.name ||
        typeof item.event?.data !== "object"
      ) {
        errors.push({
          id: item.id ?? "unknown",
          error:
            "Invalid event structure: missing id, event.name, or event.data",
        });
        continue;
      }

      const workflow = resolveWorkflow(item.event.name, env);

      if (!workflow) {
        errors.push({
          id: item.id,
          error: `Unknown event: ${item.event.name}`,
        });
        continue;
      }

      const traceId = item.traceId ?? item.id;

      // Use random event ID as instance ID. Cloudflare permanently stores
      // instance IDs — even after completion — so deterministic idempotency
      // keys would reject legitimate re-sends of identical payloads.
      await workflow.create({
        id: item.id,
        params: {
          eventId: item.id,
          idempotencyKey: item.idempotencyKey,
          traceId,
          eventName: item.event.name,
          data: item.event.data,
          delayMs: item.delayMs,
        },
      });

      ids.push(item.id);

      console.log(
        `[Dispatch] Created workflow for ${item.event.name} (id: ${item.id}, trace: ${traceId})`,
      );
    } catch (error) {
      errors.push({
        id: item.id,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  return Response.json({ ids, errors: errors.length > 0 ? errors : undefined });
}

// ─── Routing ─────────────────────────────────────────────────────────────────

const EMAIL_EVENT_NAMES = new Set<string>(Object.values(EMAIL_EVENTS));
const NOTIFICATION_EVENT_NAMES = new Set<string>(
  Object.values(NOTIFICATION_EVENTS),
);
const PAYMENT_EVENT_NAMES = new Set<string>(Object.values(PAYMENT_EVENTS));

function resolveWorkflow(eventName: string, env: Env): Workflow | null {
  if (EMAIL_EVENT_NAMES.has(eventName)) return env.EMAIL_WORKFLOW;
  if (NOTIFICATION_EVENT_NAMES.has(eventName)) return env.NOTIFICATION_WORKFLOW;
  if (PAYMENT_EVENT_NAMES.has(eventName)) return env.PAYMENT_WORKFLOW;
  return null;
}
