// ─── Workflows Worker — Entrypoint ───────────────────────────────────────────
// Cloudflare Worker that receives dispatched workflow events and starts
// the appropriate Workflow instance.

import {
  EMAIL_EVENTS,
  NOTIFICATION_EVENTS,
  PAYMENT_EVENTS,
  WHATSAPP_EVENTS,
} from "./contracts.js";
import type { Env } from "./env.js";
import {
  type FailedEventMessage,
  processFailedEventBatch,
} from "./lib/failed-events.js";

// Re-export workflow classes so Cloudflare can discover them
export { EmailWorkflow } from "./workflows/email.workflow.js";
export { NotificationWorkflow } from "./workflows/notification.workflow.js";
export { PaymentWorkflow } from "./workflows/payment.workflow.js";
export { WhatsappWorkflow } from "./workflows/whatsapp.workflow.js";

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

// ─── Dispatch payload shape (accepts v1 SDK envelopes and old transport items) ──────────────────────

interface DispatchEvent {
  id: string;
  idempotencyKey: string;
  traceId?: string;
  name?: string;
  payload?: Record<string, unknown>;
  event?: {
    name: string;
    data: Record<string, unknown>;
  };
  delayMs?: number;
  scheduledAt?: string;
}

interface DispatchPayload {
  events: DispatchEvent[];
}

interface PreparedDispatchEvent {
  id: string;
  eventName: string;
  traceId: string;
  idempotencyKey: string;
  scheduledAt?: string;
  delayMs: number;
  workflow: Workflow;
  params: {
    eventId: string;
    idempotencyKey: string;
    traceId: string;
    eventName: string;
    data: Record<string, unknown>;
    delayMs: number;
  };
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

    if (url.pathname.startsWith("/status/") && request.method === "GET") {
      if (!verifyAuth(request.headers.get("Authorization"), env.AUTH_TOKEN)) {
        return new Response("Unauthorized", { status: 401 });
      }

      const id = url.pathname.slice("/status/".length);
      const name = url.searchParams.get("name") ?? undefined;
      const status = await getWorkflowStatus(id, name, env);
      if (!status) {
        return Response.json({ error: "Workflow instance not found" }, { status: 404 });
      }

      return Response.json(status);
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

  const errors: Array<{ id: string; error: string }> = [];
  const preparedEvents: PreparedDispatchEvent[] = [];

  for (const item of body.events) {
    // Validate item structure
    const eventName = item.name ?? item.event?.name;
    const eventData = item.payload ?? item.event?.data;

    if (
      !item.id ||
      !item.idempotencyKey ||
      !eventName ||
      typeof eventData !== "object" ||
      eventData === null ||
      Array.isArray(eventData)
    ) {
      errors.push({
        id: item.id ?? "unknown",
        error:
          "Invalid event structure: missing id, idempotencyKey, name, or payload",
      });
      continue;
    }

    const workflow = resolveWorkflow(eventName, env);

    if (!workflow) {
      errors.push({
        id: item.id,
        error: `Unknown event: ${eventName}`,
      });
      continue;
    }

    const traceId = item.traceId ?? item.id;
    const delayMs =
      item.delayMs ??
      (item.scheduledAt
        ? Math.max(0, new Date(item.scheduledAt).getTime() - Date.now())
        : 0);

    preparedEvents.push({
      id: item.id,
      eventName,
      traceId,
      idempotencyKey: item.idempotencyKey,
      scheduledAt: item.scheduledAt,
      delayMs,
      workflow,
      params: {
        eventId: item.id,
        idempotencyKey: item.idempotencyKey,
        traceId,
        eventName,
        data: eventData,
        delayMs,
      },
    });
  }

  const instances = await createWorkflowInstances(preparedEvents, errors);

  return Response.json({
    ids: instances.map((instance) => instance.id),
    instances,
    errors: errors.length > 0 ? errors : undefined,
  });
}

async function createWorkflowInstances(
  events: PreparedDispatchEvent[],
  errors: Array<{ id: string; error: string }>,
): Promise<Array<{
  id: string;
  name: string;
  status: "queued" | "scheduled";
  traceId: string;
  idempotencyKey: string;
  scheduledAt?: string;
  updatedAt: string;
}>> {
  const instances: Array<{
    id: string;
    name: string;
    status: "queued" | "scheduled";
    traceId: string;
    idempotencyKey: string;
    scheduledAt?: string;
    updatedAt: string;
  }> = [];
  const groups = new Map<Workflow, PreparedDispatchEvent[]>();

  for (const event of events) {
    const group = groups.get(event.workflow);
    if (group) {
      group.push(event);
    } else {
      groups.set(event.workflow, [event]);
    }
  }

  for (const group of groups.values()) {
    for (const chunk of chunkEvents(group, 100)) {
      const accepted = await createWorkflowChunk(chunk, errors);
      instances.push(...accepted.map(toDispatchInstance));
    }
  }

  return instances;
}

async function createWorkflowChunk(
  events: PreparedDispatchEvent[],
  errors: Array<{ id: string; error: string }>,
): Promise<PreparedDispatchEvent[]> {
  if (events.length === 0) return [];

  const workflow = events[0]!.workflow;
  if (workflow.createBatch) {
    try {
      await workflow.createBatch(
        events.map((event) => ({
          id: event.id,
          params: event.params,
        })),
      );

      for (const event of events) {
        console.log(
          `[Dispatch] Accepted workflow for ${event.eventName} (id: ${event.id}, trace: ${event.traceId})`,
        );
      }

      return events;
    } catch (error) {
      console.warn(
        `[Dispatch] Batch create failed; retrying individually: ${error instanceof Error ? error.message : String(error)}`,
      );
    }
  }

  const accepted: PreparedDispatchEvent[] = [];
  for (const event of events) {
    try {
      await event.workflow.create({
        id: event.id,
        params: event.params,
      });
      accepted.push(event);

      console.log(
        `[Dispatch] Created workflow for ${event.eventName} (id: ${event.id}, trace: ${event.traceId})`,
      );
    } catch (error) {
      errors.push({
        id: event.id,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  return accepted;
}

function toDispatchInstance(event: PreparedDispatchEvent) {
  return {
    id: event.id,
    name: event.eventName,
    status: event.delayMs > 0 ? ("scheduled" as const) : ("queued" as const),
    traceId: event.traceId,
    idempotencyKey: event.idempotencyKey,
    scheduledAt: event.scheduledAt,
    updatedAt: new Date().toISOString(),
  };
}

function chunkEvents<T>(events: T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let index = 0; index < events.length; index += size) {
    chunks.push(events.slice(index, index + size));
  }
  return chunks;
}

// ─── Routing ─────────────────────────────────────────────────────────────────

const EMAIL_EVENT_NAMES = new Set<string>(Object.values(EMAIL_EVENTS));
const NOTIFICATION_EVENT_NAMES = new Set<string>(
  Object.values(NOTIFICATION_EVENTS),
);
const PAYMENT_EVENT_NAMES = new Set<string>(Object.values(PAYMENT_EVENTS));
const WHATSAPP_EVENT_NAMES = new Set<string>(Object.values(WHATSAPP_EVENTS));

function resolveWorkflow(eventName: string, env: Env): Workflow | null {
  if (EMAIL_EVENT_NAMES.has(eventName)) return env.EMAIL_WORKFLOW;
  if (NOTIFICATION_EVENT_NAMES.has(eventName)) return env.NOTIFICATION_WORKFLOW;
  if (PAYMENT_EVENT_NAMES.has(eventName)) return env.PAYMENT_WORKFLOW;
  if (WHATSAPP_EVENT_NAMES.has(eventName)) return env.WHATSAPP_WORKFLOW;
  return null;
}

async function getWorkflowStatus(
  id: string,
  eventName: string | undefined,
  env: Env,
): Promise<Record<string, unknown> | null> {
  const resolved = eventName ? resolveWorkflow(eventName, env) : null;
  const candidates: Array<{ name: string; workflow: Workflow }> = resolved
    ? [{ name: eventName!, workflow: resolved }]
    : [
        { name: "email", workflow: env.EMAIL_WORKFLOW },
        { name: "notification", workflow: env.NOTIFICATION_WORKFLOW },
        { name: "payment", workflow: env.PAYMENT_WORKFLOW },
        { name: "whatsapp", workflow: env.WHATSAPP_WORKFLOW },
      ];

  for (const candidate of candidates) {
    try {
      const instance = await candidate.workflow.get(id);
      const details = await instance.status();
      const normalized =
        typeof details === "object" && details !== null
          ? (details as Record<string, unknown>)
          : { status: details };
      const status = normalized.status;
      if (!eventName && status === "unknown") continue;
      return {
        id,
        name: candidate.name,
        ...normalized,
      };
    } catch (error) {
      if (/not\s*found|does\s*not\s*exist|unknown\s*instance/i.test(String(error))) {
        continue;
      }
      throw error;
    }
  }

  return null;
}
