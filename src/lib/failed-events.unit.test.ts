import { describe, expect, it } from "bun:test";

import type {
  BackendCallbackFailedEvent,
  BackendCallbackStep,
} from "@abshahin/workflows-sdk";
import type { Env } from "../env.js";
import { createFailedEventPointerMessage } from "./failed-event-pointer.js";
import { callbackStepsPolicy } from "./workflow-policy.js";
import {
  MAX_FAILED_EVENT_RECORD_BYTES,
  type FailedEventQueueMessage,
  processFailedEventBatch,
  storeFailedEvent,
} from "./failed-events.js";

interface StoredRow {
  id: string;
  event_id: string | null;
  event_name: string;
  event_data: string;
  idempotency_key: string | null;
  metadata: string | null;
  error: string;
  status: "PENDING" | "PROCESSING" | "COMPLETED" | "DEAD";
  attempts: number;
  max_attempts: number;
  next_retry_at: number;
  resolved_at: number | null;
  updated_at: number;
}

interface DeliveryReceiptRow {
  failed_event_id: string;
  channel: "RETRY" | "DLQ";
  state: "PENDING" | "CONFIRMED";
  owner: string | null;
  fence: number;
  lease_expires_at: number | null;
  next_attempt_at: number;
  delivery_delay_seconds: number;
  source_attempt: number;
  failure_count: number;
  confirmed_at: number | null;
  refresh_at: number | null;
  last_error: string | null;
  created_at: number;
  updated_at: number;
  expires_at: number;
}

function failedEvent(
  payload: Record<string, unknown> = { tenantId: "tenant_1" },
): BackendCallbackFailedEvent {
  return {
    eventId: "wf_1",
    workflowName: "email/verification",
    backendPath: "email/verification",
    backendEventId: "wf_1",
    payload,
    idempotencyKey: "idem_1",
    error: "backend unavailable",
  };
}

function queueCapture(options?: { fail?: boolean }) {
  const sent: Array<{ body: unknown; options?: QueueSendOptions }> = [];
  const queue = {
    async send(body: unknown, sendOptions?: QueueSendOptions) {
      if (options?.fail) throw new Error("DLQ unavailable");
      sent.push({ body, options: sendOptions });
    },
  } as unknown as Queue;
  return { queue, sent };
}

function controlDb() {
  const rows = new Map<string, StoredRow>();
  const receipts = new Map<string, DeliveryReceiptRow>();
  const receiptKey = (id: string, channel: string) => `${id}:${channel}`;
  const db = {
    __rows: rows,
    __receipts: receipts,
    withSession() {
      return db;
    },
    prepare(query: string) {
      return {
        bind(...values: unknown[]) {
          return {
            async run() {
              if (query.includes("INSERT INTO workflow_failed_events")) {
                const id = String(values[0]);
                if (rows.has(id)) return { success: true, meta: { changes: 0 } };
                rows.set(id, {
                  id,
                  event_id: values[1] === null ? null : String(values[1]),
                  event_name: String(values[2]),
                  event_data: String(values[3]),
                  idempotency_key:
                    values[4] === null ? null : String(values[4]),
                  metadata: values[5] === null ? null : String(values[5]),
                  error: String(values[6]),
                  status: "PENDING",
                  attempts: 0,
                  max_attempts: 10,
                  next_retry_at: Number(values[7]),
                  resolved_at: null,
                  updated_at: Number(values[9]),
                });
                return { success: true, meta: { changes: 1 } };
              }

              if (
                query.includes(
                  "INSERT OR IGNORE INTO failed_event_queue_receipts",
                )
              ) {
                const failedEventId = String(values[0]);
                const channel = String(values[1]) as "RETRY" | "DLQ";
                const key = receiptKey(failedEventId, channel);
                if (receipts.has(key)) {
                  return { success: true, meta: { changes: 0 } };
                }
                receipts.set(key, {
                  failed_event_id: failedEventId,
                  channel,
                  state: "PENDING",
                  owner: null,
                  fence: 0,
                  lease_expires_at: null,
                  next_attempt_at: Number(values[2]),
                  delivery_delay_seconds: Number(values[3]),
                  source_attempt: -1,
                  failure_count: 0,
                  confirmed_at: null,
                  refresh_at: null,
                  last_error: null,
                  created_at: Number(values[4]),
                  updated_at: Number(values[5]),
                  expires_at: Number(values[6]),
                });
                return { success: true, meta: { changes: 1 } };
              }

              if (
                query.includes(
                  "source_attempt < coalesce((",
                )
              ) {
                const failedEventId = String(values[4]);
                const receipt = receipts.get(
                  receiptKey(failedEventId, "RETRY"),
                );
                const failedRow = rows.get(failedEventId);
                if (
                  receipt === undefined ||
                  failedRow === undefined ||
                  leaseMetadata(failedRow).recoveryOwner !== "callback-queue" ||
                  (failedRow.status !== "PENDING" &&
                    failedRow.status !== "PROCESSING") ||
                  receipt.source_attempt >= failedRow.attempts
                ) {
                  return { success: true, meta: { changes: 0 } };
                }
                receipt.state = "PENDING";
                receipt.owner = null;
                receipt.fence += 1;
                receipt.lease_expires_at = null;
                receipt.next_attempt_at = Number(values[0]);
                receipt.delivery_delay_seconds = Number(values[1]);
                receipt.source_attempt = failedRow.attempts;
                receipt.failure_count = 0;
                receipt.confirmed_at = null;
                receipt.refresh_at = null;
                receipt.last_error = null;
                receipt.updated_at = Number(values[2]);
                receipt.expires_at = Number(values[3]);
                return { success: true, meta: { changes: 1 } };
              }

              if (
                query.includes("SET state='CONFIRMED', owner=NULL")
              ) {
                const row = receipts.get(
                  receiptKey(String(values[5]), String(values[6])),
                );
                if (
                  row?.state !== "PENDING" ||
                  row.owner !== values[7] ||
                  row.fence !== Number(values[8])
                ) {
                  return { success: true, meta: { changes: 0 } };
                }
                row.state = "CONFIRMED";
                row.owner = null;
                row.lease_expires_at = null;
                row.next_attempt_at = Number(values[0]);
                row.delivery_delay_seconds = 0;
                row.failure_count = 0;
                row.confirmed_at = Number(values[1]);
                row.refresh_at =
                  values[2] === null ? null : Number(values[2]);
                row.last_error = null;
                row.updated_at = Number(values[3]);
                row.expires_at = Number(values[4]);
                return { success: true, meta: { changes: 1 } };
              }

              if (
                query.includes("next_attempt_at=?, delivery_delay_seconds=0")
              ) {
                const row = receipts.get(
                  receiptKey(String(values[5]), String(values[6])),
                );
                if (
                  row?.state !== "PENDING" ||
                  row.owner !== values[7] ||
                  row.fence !== Number(values[8])
                ) {
                  return { success: true, meta: { changes: 0 } };
                }
                row.owner = null;
                row.lease_expires_at = null;
                row.next_attempt_at = Number(values[0]);
                row.delivery_delay_seconds = 0;
                row.failure_count = Number(values[1]);
                row.last_error = String(values[2]);
                row.updated_at = Number(values[3]);
                row.expires_at = Number(values[4]);
                return { success: true, meta: { changes: 1 } };
              }

              if (query.includes("SET metadata=json_set(metadata")) {
                const row = rows.get(String(values[2]));
                if (
                  row?.status !== "PROCESSING" ||
                  leaseMetadata(row).queueLeaseOwner !== values[3]
                ) {
                  return { success: true, meta: { changes: 0 } };
                }
                row.metadata = JSON.stringify({
                  ...leaseMetadata(row),
                  queueLeaseExpiresAt: Number(values[0]),
                });
                row.updated_at = Number(values[1]);
                return { success: true, meta: { changes: 1 } };
              } else if (query.includes("status='COMPLETED'")) {
                const row = rows.get(String(values[2]));
                if (
                  row?.status !== "PROCESSING" ||
                  leaseMetadata(row).queueLeaseOwner !== values[3]
                ) {
                  return { success: true, meta: { changes: 0 } };
                }
                row.status = "COMPLETED";
                row.metadata = withoutLeaseMetadata(row, true);
                row.resolved_at = Number(values[0]);
                row.updated_at = Number(values[1]);
                return { success: true, meta: { changes: 1 } };
              } else if (query.includes("status='DEAD'")) {
                const row = rows.get(String(values[3]));
                if (
                  row?.status !== "PROCESSING" ||
                  leaseMetadata(row).queueLeaseOwner !== values[4]
                ) {
                  return { success: true, meta: { changes: 0 } };
                }
                row.status = "DEAD";
                row.error = String(values[0]);
                row.metadata = withoutLeaseMetadata(row, true);
                row.resolved_at = Number(values[1]);
                row.updated_at = Number(values[2]);
                return { success: true, meta: { changes: 1 } };
              } else if (query.includes("status='PENDING'")) {
                const row = rows.get(String(values[4]));
                if (
                  row?.status !== "PROCESSING" ||
                  leaseMetadata(row).queueLeaseOwner !== values[5]
                ) {
                  return { success: true, meta: { changes: 0 } };
                }
                row.status = "PENDING";
                row.error = String(values[0]);
                row.next_retry_at = Number(values[1]);
                row.metadata = JSON.stringify({
                  ...leaseMetadata(row),
                  queueRetryNotBefore: Number(values[2]),
                });
                row.metadata = withoutLeaseMetadata(row);
                row.updated_at = Number(values[3]);
                return { success: true, meta: { changes: 1 } };
              } else {
                throw new Error(`Unexpected D1 run: ${query}`);
              }
            },
            async first<T>() {
              if (
                query.includes(
                  "UPDATE failed_event_queue_receipts\n       SET state='PENDING'",
                )
              ) {
                const owner = String(values[0]);
                const failedEventId = String(values[3]);
                const channel = String(values[4]) as "RETRY" | "DLQ";
                const now = Number(values[5]);
                const receipt = receipts.get(
                  receiptKey(failedEventId, channel),
                );
                const failedRow = rows.get(failedEventId);
                const recoveryOwner = failedRow
                  ? leaseMetadata(failedRow).recoveryOwner
                  : undefined;
                const rowIsEligible =
                  recoveryOwner === "callback-queue" &&
                  ((channel === "RETRY" &&
                    (failedRow?.status === "PENDING" ||
                      failedRow?.status === "PROCESSING")) ||
                    (channel === "DLQ" && failedRow?.status === "DEAD"));
                const receiptIsEligible =
                  receipt !== undefined &&
                  receipt.next_attempt_at <= now &&
                  ((receipt.state === "PENDING" &&
                    (receipt.lease_expires_at ?? 0) <= Number(values[6])) ||
                    (channel === "RETRY" &&
                      receipt.state === "CONFIRMED" &&
                      (receipt.refresh_at ?? Number.POSITIVE_INFINITY) <=
                        Number(values[7])));
                if (!rowIsEligible || !receiptIsEligible) return null;
                receipt.state = "PENDING";
                receipt.owner = owner;
                receipt.fence += 1;
                receipt.lease_expires_at = Number(values[1]);
                receipt.updated_at = Number(values[2]);
                return {
                  fence: receipt.fence,
                  delivery_delay_seconds: receipt.delivery_delay_seconds,
                  failure_count: receipt.failure_count,
                } as T;
              }
              if (query.includes("Failed-event durable retry attempts exhausted")) {
                const row = rows.get(String(values[2]));
                if (!row || row.attempts < row.max_attempts) return null;
                const metadata = leaseMetadata(row);
                const leaseExpired =
                  row.status === "PROCESSING" &&
                  (typeof metadata.queueLeaseExpiresAt === "number"
                    ? metadata.queueLeaseExpiresAt
                    : row.updated_at + Number(values[3])) <= Number(values[4]);
                if (row.status !== "PENDING" && !leaseExpired) return null;
                row.status = "DEAD";
                row.error = "Failed-event durable retry attempts exhausted";
                row.metadata = withoutLeaseMetadata(row, true);
                row.resolved_at = Number(values[0]);
                row.updated_at = Number(values[1]);
                return { ...row } as T;
              }
              if (query.includes("SET status='PROCESSING'")) {
                const owner = String(values[0]);
                const expiresAt = Number(values[1]);
                const now = Number(values[2]);
                const row = rows.get(String(values[3]));
                if (!row) return null;
                const metadata = leaseMetadata(row);
                const retryDue =
                  typeof metadata.queueRetryNotBefore !== "number" ||
                  metadata.queueRetryNotBefore <= Number(values[4]);
                const existingExpiry =
                  typeof metadata.queueLeaseExpiresAt === "number"
                    ? metadata.queueLeaseExpiresAt
                    : row.updated_at + Number(values[5]);
                if (
                  row.attempts >= row.max_attempts ||
                  !(
                    (row.status === "PENDING" && retryDue) ||
                    (row.status === "PROCESSING" &&
                      existingExpiry <= Number(values[6]))
                  )
                ) {
                  return null;
                }
                row.status = "PROCESSING";
                row.attempts += 1;
                row.metadata = JSON.stringify({
                  ...metadata,
                  queueLeaseOwner: owner,
                  queueLeaseExpiresAt: expiresAt,
                });
                row.updated_at = now;
                return { ...row } as T;
              }
              if (!query.includes("FROM workflow_failed_events")) {
                throw new Error(`Unexpected D1 first: ${query}`);
              }
              const row = rows.get(String(values[0]));
              return (row === undefined ? null : { ...row }) as T | null;
            },
          };
        },
      };
    },
  } as unknown as D1Database;
  return { db, rows, receipts };
}

function leaseMetadata(row: StoredRow): Record<string, unknown> {
  if (row.metadata === null) return {};
  try {
    const parsed: unknown = JSON.parse(row.metadata);
    return typeof parsed === "object" && parsed !== null && !Array.isArray(parsed)
      ? (parsed as Record<string, unknown>)
      : {};
  } catch {
    return { corruptRecoveryMetadata: row.metadata };
  }
}

function withoutLeaseMetadata(
  row: StoredRow,
  removeRetryNotBefore = false,
): string {
  const metadata = leaseMetadata(row);
  delete metadata.queueLeaseOwner;
  delete metadata.queueLeaseExpiresAt;
  if (removeRetryNotBefore) delete metadata.queueRetryNotBefore;
  return JSON.stringify(metadata);
}

function queueMessage(body: FailedEventQueueMessage, attempts = 1) {
  let acked = false;
  const retries: QueueRetryOptions[] = [];
  const message = {
    id: "message_1",
    timestamp: new Date("2026-08-27T00:00:00.000Z"),
    body,
    attempts,
    ack() {
      acked = true;
    },
    retry(options?: QueueRetryOptions) {
      retries.push(options ?? {});
    },
  } as Message<FailedEventQueueMessage>;
  return { message, wasAcked: () => acked, retries };
}

function messageBatch(
  message: Message<FailedEventQueueMessage>,
): MessageBatch<FailedEventQueueMessage> {
  return {
    queue: "manhali-failed-events",
    messages: [message],
    metadata: { metrics: { backlogCount: 1, backlogBytes: 1 } },
    ackAll() {},
    retryAll() {},
  };
}

function callbackEnv(options: {
  status: number;
  db: D1Database;
  retryQueue?: Queue;
  deadLetterQueue: Queue;
  backendFetch?: () => Promise<Response>;
}): Env {
  return {
    AUTH_TOKEN: "secret",
    BACKEND_CALLBACK_TOKEN: "callback-secret",
    BACKEND_ORIGIN: "https://api.manhali.com",
    BACKEND: {
      async fetch() {
        if (options.backendFetch) return options.backendFetch();
        return new Response("callback failed", { status: options.status });
      },
    },
    CONTROL_DB: options.db,
    FAILED_EVENTS_QUEUE: options.retryQueue ?? queueCapture().queue,
    FAILED_EVENTS_DLQ: options.deadLetterQueue,
  } as unknown as Env;
}

async function storedPointer(
  db: D1Database,
  queue: ReturnType<typeof queueCapture>,
) {
  await storeFailedEvent(
    { CONTROL_DB: db, FAILED_EVENTS_QUEUE: queue.queue },
    failedEvent(),
  );
  const pointer = queue.sent[0]?.body as FailedEventQueueMessage;
  if ("failedEventId" in pointer) {
    const database = db as unknown as { __rows?: Map<string, StoredRow> };
    const row = database.__rows?.get(pointer.failedEventId);
    if (row) {
      row.metadata = JSON.stringify({
        ...leaseMetadata(row),
        queueRetryNotBefore: Date.now() - 1,
      });
    }
  }
  return pointer;
}

describe("failed-event Queue recovery", () => {
  it("stores the bounded record in D1 and queues only its opaque ID", async () => {
    const retryQueue = queueCapture();
    const database = controlDb();

    const pointer = await storedPointer(database.db, retryQueue);

    expect(retryQueue.sent).toHaveLength(1);
    expect(retryQueue.sent[0]?.options).toEqual({ delaySeconds: 60 });
    expect(pointer).toEqual({ v: 3, failedEventId: "callback_wf_1" });
    expect(JSON.stringify(pointer)).not.toContain("tenant_1");
    expect(JSON.stringify(pointer)).not.toContain("payload");
    const stored = database.rows.get("callback_wf_1");
    expect(stored).toMatchObject({
      event_id: "wf_1",
      event_name: "email/verification",
      event_data: JSON.stringify({ tenantId: "tenant_1" }),
      status: "PENDING",
    });
    const metadata = JSON.parse(String(stored?.metadata)) as Record<
      string,
      unknown
    >;
    expect(metadata).not.toHaveProperty("callbackSteps");
    expect(
      callbackStepsPolicy(
        String(stored?.event_name),
        (metadata.callbackSteps as []) ?? [],
      ),
    ).toBe(true);
  });

  for (const terminalStatus of ["COMPLETED", "DEAD"] as const) {
    it(`does not revive or re-enqueue a ${terminalStatus} record on duplicate receipt`, async () => {
      const retryQueue = queueCapture();
      const database = controlDb();
      await storeFailedEvent(
        { CONTROL_DB: database.db, FAILED_EVENTS_QUEUE: retryQueue.queue },
        failedEvent(),
      );
      const row = database.rows.get("callback_wf_1");
      if (!row) throw new Error("test record was not stored");
      row.status = terminalStatus;
      row.attempts = 7;

      await storeFailedEvent(
        { CONTROL_DB: database.db, FAILED_EVENTS_QUEUE: retryQueue.queue },
        failedEvent(),
      );

      expect(row.status).toBe(terminalStatus);
      expect(row.attempts).toBe(7);
      expect(retryQueue.sent).toHaveLength(1);
    });
  }

  it("rejects a conflicting duplicate ID without changing the durable record", async () => {
    const retryQueue = queueCapture();
    const database = controlDb();
    await storeFailedEvent(
      { CONTROL_DB: database.db, FAILED_EVENTS_QUEUE: retryQueue.queue },
      failedEvent(),
    );

    await expect(
      storeFailedEvent(
        { CONTROL_DB: database.db, FAILED_EVENTS_QUEUE: retryQueue.queue },
        failedEvent({ tenantId: "different_tenant" }),
      ),
    ).rejects.toThrow("already belongs to a different envelope");

    expect(database.rows.get("callback_wf_1")?.event_data).toBe(
      JSON.stringify({ tenantId: "tenant_1" }),
    );
    expect(retryQueue.sent).toHaveLength(1);
  });

  it("stores a strict-policy-compatible payout fallback plan", async () => {
    const retryQueue = queueCapture();
    const database = controlDb();
    const payoutFailure: BackendCallbackFailedEvent = {
      eventId: "payout_tenant_1_tx_1",
      workflowName: "payment/process-payout",
      backendPath: "payment/process-payout",
      backendEventId: "payout_tenant_1_tx_1:process-payout",
      backendSteps: [
        {
          backendPath: "payment/process-payout",
          backendEventId: "payout_tenant_1_tx_1:process-payout",
        },
        {
          backendPath: "payment/notify-payout-status",
          backendEventId: "payout_tenant_1_tx_1:notify-payout-status",
        },
      ],
      payload: { tenantId: "tenant_1", transactionId: "tx_1" },
      idempotencyKey: "payout:tenant_1:tx_1",
      error: "payment provider unavailable",
    };

    await storeFailedEvent(
      { CONTROL_DB: database.db, FAILED_EVENTS_QUEUE: retryQueue.queue },
      payoutFailure,
    );

    const stored = database.rows.get("callback_payout_tenant_1_tx_1");
    const metadata = JSON.parse(String(stored?.metadata)) as {
      callbackSteps: BackendCallbackStep[];
    };
    expect(stored?.event_id).toBe("payout_tenant_1_tx_1");
    expect(metadata.callbackSteps).toEqual([
      {
        stepName: "validate-payout",
        backendPath: "payment/validate-payout",
        backendEventIdSuffix: "validate-payout",
      },
      {
        stepName: "process-payout",
        backendPath: "payment/process-payout",
        backendEventIdSuffix: "process-payout",
      },
      {
        stepName: "notify-payout-status",
        backendPath: "payment/notify-payout-status",
        backendEventIdSuffix: "notify-payout-status",
      },
    ]);
    expect(
      callbackStepsPolicy("payment/process-payout", metadata.callbackSteps),
    ).toBe(true);
  });

  it("accepts a near-limit D1 record and rejects an oversized one", async () => {
    const accepted = queueCapture();
    const database = controlDb();
    await storeFailedEvent(
      { CONTROL_DB: database.db, FAILED_EVENTS_QUEUE: accepted.queue },
      failedEvent({ value: "x".repeat(118_000) }),
    );
    expect(accepted.sent).toHaveLength(1);

    await expect(
      storeFailedEvent(
        {
          CONTROL_DB: database.db,
          FAILED_EVENTS_QUEUE: queueCapture().queue,
        },
        failedEvent({ value: "x".repeat(MAX_FAILED_EVENT_RECORD_BYTES) }),
      ),
    ).rejects.toThrow(`exceeds ${MAX_FAILED_EVENT_RECORD_BYTES} bytes`);
  });

  it("completes the D1 record before acknowledging a successful retry", async () => {
    const retryQueue = queueCapture();
    const deadLetters = queueCapture();
    const database = controlDb();
    const source = queueMessage(await storedPointer(database.db, retryQueue));

    await processFailedEventBatch(
      messageBatch(source.message),
      callbackEnv({
        status: 204,
        db: database.db,
        deadLetterQueue: deadLetters.queue,
      }),
    );

    expect(source.wasAcked()).toBe(true);
    expect(source.retries).toHaveLength(0);
    expect(database.rows.get("callback_wf_1")?.status).toBe("COMPLETED");
    expect(deadLetters.sent).toHaveLength(0);
  });

  it("allows only one concurrent delivery to call the backend", async () => {
    const retryQueue = queueCapture();
    const deadLetters = queueCapture();
    const database = controlDb();
    const pointer = await storedPointer(database.db, retryQueue);
    const first = queueMessage(pointer);
    const duplicate = queueMessage(pointer);
    let releaseBackend!: () => void;
    let backendEntered!: () => void;
    const backendGate = new Promise<void>((resolve) => {
      releaseBackend = resolve;
    });
    const entered = new Promise<void>((resolve) => {
      backendEntered = resolve;
    });
    let backendCalls = 0;
    const env = callbackEnv({
      status: 204,
      db: database.db,
      deadLetterQueue: deadLetters.queue,
      backendFetch: async () => {
        backendCalls += 1;
        backendEntered();
        await backendGate;
        return new Response(null, { status: 204 });
      },
    });

    const firstRun = processFailedEventBatch(messageBatch(first.message), env);
    await entered;
    await processFailedEventBatch(messageBatch(duplicate.message), env);

    expect(backendCalls).toBe(1);
    expect(database.rows.get("callback_wf_1")?.attempts).toBe(1);
    expect(duplicate.wasAcked()).toBe(false);
    expect(duplicate.retries).toHaveLength(1);

    releaseBackend();
    await firstRun;
    expect(first.wasAcked()).toBe(true);
    expect(database.rows.get("callback_wf_1")?.status).toBe("COMPLETED");
  });

  it("does not let a stale loser overwrite a winner's terminal state", async () => {
    const retryQueue = queueCapture();
    const deadLetters = queueCapture();
    const database = controlDb();
    const source = queueMessage(await storedPointer(database.db, retryQueue));
    let releaseBackend!: () => void;
    let backendEntered!: () => void;
    const backendGate = new Promise<void>((resolve) => {
      releaseBackend = resolve;
    });
    const entered = new Promise<void>((resolve) => {
      backendEntered = resolve;
    });
    const env = callbackEnv({
      status: 422,
      db: database.db,
      deadLetterQueue: deadLetters.queue,
      backendFetch: async () => {
        backendEntered();
        await backendGate;
        return new Response("invalid", { status: 422 });
      },
    });

    const staleRun = processFailedEventBatch(messageBatch(source.message), env);
    await entered;
    const row = database.rows.get("callback_wf_1");
    if (!row) throw new Error("test record was not stored");
    row.status = "COMPLETED";
    row.metadata = withoutLeaseMetadata(row);
    releaseBackend();
    await staleRun;

    expect(row.status).toBe("COMPLETED");
    expect(source.wasAcked()).toBe(true);
    expect(source.retries).toHaveLength(0);
    expect(deadLetters.sent).toHaveLength(0);
  });

  it("marks permanent failures dead and copies only the pointer to the DLQ", async () => {
    const retryQueue = queueCapture();
    const deadLetters = queueCapture();
    const database = controlDb();
    const source = queueMessage(await storedPointer(database.db, retryQueue));

    await processFailedEventBatch(
      messageBatch(source.message),
      callbackEnv({
        status: 422,
        db: database.db,
        deadLetterQueue: deadLetters.queue,
      }),
    );

    expect(deadLetters.sent).toEqual([{ body: source.message.body }]);
    expect(source.wasAcked()).toBe(true);
    expect(source.retries).toHaveLength(0);
    expect(database.rows.get("callback_wf_1")?.status).toBe("DEAD");
  });

  for (const status of [401, 403]) {
    it(`dead-letters callback auth HTTP ${status} without retrying`, async () => {
      const retryQueue = queueCapture();
      const deadLetters = queueCapture();
      const database = controlDb();
      const source = queueMessage(await storedPointer(database.db, retryQueue));

      await processFailedEventBatch(
        messageBatch(source.message),
        callbackEnv({
          status,
          db: database.db,
          deadLetterQueue: deadLetters.queue,
        }),
      );

      expect(source.wasAcked()).toBe(true);
      expect(source.retries).toHaveLength(0);
      expect(deadLetters.sent).toEqual([{ body: source.message.body }]);
      expect(database.rows.get("callback_wf_1")?.status).toBe("DEAD");
    });
  }

  it("retries the source when a permanent failure cannot reach the DLQ", async () => {
    const retryQueue = queueCapture();
    const database = controlDb();
    const source = queueMessage(await storedPointer(database.db, retryQueue));

    await processFailedEventBatch(
      messageBatch(source.message),
      callbackEnv({
        status: 422,
        db: database.db,
        deadLetterQueue: queueCapture({ fail: true }).queue,
      }),
    );

    expect(source.wasAcked()).toBe(false);
    expect(source.retries).toEqual([{ delaySeconds: 180 }]);
  });

  it("keeps transient callback failures on the retry queue", async () => {
    const retryQueue = queueCapture();
    const deadLetters = queueCapture();
    const database = controlDb();
    const source = queueMessage(await storedPointer(database.db, retryQueue));

    await processFailedEventBatch(
      messageBatch(source.message),
      callbackEnv({
        status: 503,
        db: database.db,
        deadLetterQueue: deadLetters.queue,
      }),
    );

    expect(deadLetters.sent).toHaveLength(0);
    expect(source.wasAcked()).toBe(false);
    expect(source.retries).toEqual([{ delaySeconds: 180 }]);
    expect(database.rows.get("callback_wf_1")?.status).toBe("PENDING");
    expect(database.rows.get("callback_wf_1")?.attempts).toBe(1);
  });

  it("does not let a duplicate pointer bypass durable backoff", async () => {
    const retryQueue = queueCapture();
    const deadLetters = queueCapture();
    const database = controlDb();
    const pointer = await storedPointer(database.db, retryQueue);
    const first = queueMessage(pointer);
    const duplicate = queueMessage(pointer);
    let backendCalls = 0;
    const env = callbackEnv({
      status: 503,
      db: database.db,
      retryQueue: retryQueue.queue,
      deadLetterQueue: deadLetters.queue,
      backendFetch: async () => {
        backendCalls += 1;
        return new Response("unavailable", { status: 503 });
      },
    });

    await processFailedEventBatch(messageBatch(first.message), env);
    await processFailedEventBatch(messageBatch(duplicate.message), env);

    expect(backendCalls).toBe(1);
    expect(database.rows.get("callback_wf_1")?.attempts).toBe(1);
    expect(database.rows.get("callback_wf_1")?.status).toBe("PENDING");
    expect(duplicate.wasAcked()).toBe(false);
    expect(duplicate.retries).toEqual([{ delaySeconds: 180 }]);
    expect(deadLetters.sent).toHaveLength(0);
  });

  it("rolls over a high transport attempt without exhausting durable retries", async () => {
    const retryQueue = queueCapture();
    const deadLetters = queueCapture();
    const database = controlDb();
    const pointer = await storedPointer(database.db, retryQueue);
    const source = queueMessage(pointer, 99);

    await processFailedEventBatch(
      messageBatch(source.message),
      callbackEnv({
        status: 503,
        db: database.db,
        retryQueue: retryQueue.queue,
        deadLetterQueue: deadLetters.queue,
      }),
    );

    expect(database.rows.get("callback_wf_1")?.attempts).toBe(1);
    expect(database.rows.get("callback_wf_1")?.status).toBe("PENDING");
    expect(source.wasAcked()).toBe(true);
    expect(source.retries).toHaveLength(0);
    expect(retryQueue.sent).toHaveLength(2);
    expect(retryQueue.sent[1]).toEqual({
      body: pointer,
      options: { delaySeconds: 180 },
    });
    expect(deadLetters.sent).toHaveLength(0);
  });

  it("marks D1 dead and directly dead-letters the final transient attempt", async () => {
    const retryQueue = queueCapture();
    const deadLetters = queueCapture();
    const database = controlDb();
    const pointer = await storedPointer(database.db, retryQueue);
    const row = database.rows.get("callback_wf_1");
    if (!row) throw new Error("test record was not stored");
    row.attempts = 9;
    const source = queueMessage(pointer);

    await processFailedEventBatch(
      messageBatch(source.message),
      callbackEnv({
        status: 503,
        db: database.db,
        deadLetterQueue: deadLetters.queue,
      }),
    );

    expect(source.wasAcked()).toBe(true);
    expect(source.retries).toHaveLength(0);
    expect(deadLetters.sent).toEqual([{ body: source.message.body }]);
    expect(database.rows.get("callback_wf_1")?.status).toBe("DEAD");
  });

  it("dead-letters legacy pointers whose Workflow is terminal", async () => {
    const database = controlDb();
    const deadLetters = queueCapture();
    const pointer = createFailedEventPointerMessage("legacy_wf_1");
    const source = queueMessage(pointer);
    const env = callbackEnv({
      status: 204,
      db: database.db,
      deadLetterQueue: deadLetters.queue,
    });
    (env as unknown as { WORKFLOW: unknown }).WORKFLOW = {
      async get() {
        return { async status() { return { status: "errored" }; } };
      },
    };

    await processFailedEventBatch(messageBatch(source.message), env);

    expect(source.wasAcked()).toBe(true);
    expect(source.retries).toHaveLength(0);
    expect(deadLetters.sent).toEqual([{ body: pointer }]);
  });
});
