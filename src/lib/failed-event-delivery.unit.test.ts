import { Database, type SQLQueryBindings } from "bun:sqlite";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";

import type { Env } from "../env.js";
import {
  enqueueFailedEventRetry,
  FAILED_EVENT_DELIVERY_LEASE_MS,
  FAILED_EVENT_DELIVERY_SWEEP_LIMIT,
  FAILED_EVENT_QUEUE_REFRESH_MS,
  FAILED_EVENT_RECEIPT_CLEANUP_LIMIT,
  FAILED_EVENT_RECEIPT_CLEANUP_MAX_PAGES,
  FAILED_EVENT_RECEIPT_DISCOVERY_LIMIT,
  FAILED_EVENT_RECEIPT_RETENTION_MS,
  FAILED_EVENT_TERMINAL_CLEANUP_GRACE_MS,
  publishFailedEventDlq,
  rearmFailedEventRetry,
  sweepFailedEventDeliveries,
} from "./failed-event-delivery.js";

interface AdapterControl {
  confirmFailures: number;
  expiredCleanupRuns: number;
  terminalCleanupRuns: number;
}

interface QueueCapture {
  queue: Queue;
  attempts: number;
  sent: Array<{ body: unknown; options?: QueueSendOptions }>;
}

interface ReceiptView {
  state: "PENDING" | "CONFIRMED";
  owner: string | null;
  fence: number;
  leaseExpiresAt: number | null;
  nextAttemptAt: number;
  delaySeconds: number;
  sourceAttempt: number;
  failureCount: number;
  refreshAt: number | null;
}

let sqlite: Database;
let control: AdapterControl;
let controlDb: D1Database;

function sqliteBinding(value: unknown): SQLQueryBindings {
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "bigint" ||
    typeof value === "boolean" ||
    value instanceof Uint8Array
  ) {
    return value;
  }
  throw new TypeError("Unsupported SQLite test binding");
}

function adaptDatabase(
  database: Database,
  adapterControl: AdapterControl,
): D1Database {
  const adapter = {
    prepare(query: string) {
      return {
        bind(...values: unknown[]) {
          const statement = database.query(query);
          const bindings = values.map(sqliteBinding);
          return {
            async first<T>() {
              return statement.get(...bindings) as T | null;
            },
            async all<T>() {
              return {
                success: true,
                results: statement.all(...bindings) as T[],
                meta: {},
              };
            },
            async run() {
              if (
                query.includes("FROM failed_event_queue_receipts") &&
                query.includes("WHERE expires_at <= ?")
              ) {
                adapterControl.expiredCleanupRuns += 1;
              }
              if (
                query.includes("FROM failed_event_queue_receipts AS r") &&
                query.includes("WHERE r.channel='RETRY'")
              ) {
                adapterControl.terminalCleanupRuns += 1;
              }
              if (
                query.includes("SET state='CONFIRMED', owner=NULL") &&
                adapterControl.confirmFailures > 0
              ) {
                adapterControl.confirmFailures -= 1;
                throw new Error("confirmation unavailable");
              }
              const result = statement.run(...bindings);
              return {
                success: true,
                results: [],
                meta: { changes: result.changes },
              };
            },
          };
        },
      };
    },
    withSession() {
      return adapter;
    },
  };
  return adapter as unknown as D1Database;
}

function queueCapture(input: {
  failures?: number;
  ambiguousFailures?: number;
  firstSendGate?: Promise<void>;
  firstSendStarted?: () => void;
} = {}): QueueCapture {
  let failures = input.failures ?? 0;
  let ambiguousFailures = input.ambiguousFailures ?? 0;
  let attempts = 0;
  const sent: Array<{ body: unknown; options?: QueueSendOptions }> = [];
  const queue = {
    async send(body: unknown, options?: QueueSendOptions) {
      attempts += 1;
      if (failures > 0) {
        failures -= 1;
        throw new Error("queue unavailable");
      }
      sent.push({ body, options });
      if (ambiguousFailures > 0) {
        ambiguousFailures -= 1;
        throw new Error("queue outcome unknown");
      }
      if (attempts === 1 && input.firstSendGate !== undefined) {
        input.firstSendStarted?.();
        await input.firstSendGate;
      }
    },
  } as unknown as Queue;
  return {
    queue,
    get attempts() {
      return attempts;
    },
    sent,
  };
}

function deliveryEnv(
  retry: QueueCapture,
  dlq: QueueCapture = queueCapture(),
): Pick<
  Env,
  "CONTROL_DB" | "FAILED_EVENTS_QUEUE" | "FAILED_EVENTS_DLQ"
> {
  return {
    CONTROL_DB: controlDb,
    FAILED_EVENTS_QUEUE: retry.queue,
    FAILED_EVENTS_DLQ: dlq.queue,
  };
}

function insertFailedRow(
  id: string,
  input: {
    status?: "PENDING" | "PROCESSING" | "COMPLETED" | "DEAD";
    recoveryOwner?: string;
    metadata?: Record<string, unknown>;
    updatedAt?: number;
    attempts?: number;
  } = {},
): void {
  const metadata = {
    recoveryOwner: input.recoveryOwner ?? "callback-queue",
    ...input.metadata,
  };
  sqlite
    .query(
      `INSERT INTO workflow_failed_events
       (id, status, metadata, attempts, next_retry_at, updated_at)
       VALUES (?, ?, ?, ?, 0, ?)`,
    )
    .run(
      id,
      input.status ?? "PENDING",
      JSON.stringify(metadata),
      input.attempts ?? 0,
      input.updatedAt ?? 0,
    );
}

function insertReceipt(
  id: string,
  input: {
    channel?: "RETRY" | "DLQ";
    state?: "PENDING" | "CONFIRMED";
    nextAttemptAt?: number;
    refreshAt?: number | null;
    updatedAt?: number;
    expiresAt?: number;
  } = {},
): void {
  const now = input.updatedAt ?? 0;
  sqlite
    .query(
      `INSERT INTO failed_event_queue_receipts
       (failed_event_id, channel, state, owner, fence, lease_expires_at,
        next_attempt_at, delivery_delay_seconds, source_attempt, failure_count,
        confirmed_at, refresh_at, last_error, created_at, updated_at, expires_at)
       VALUES (?, ?, ?, NULL, 0, NULL, ?, 0, -1, 0, ?, ?, NULL, ?, ?, ?)`,
    )
    .run(
      id,
      input.channel ?? "RETRY",
      input.state ?? "PENDING",
      input.nextAttemptAt ?? 0,
      input.state === "CONFIRMED" ? now : null,
      input.refreshAt ?? null,
      now,
      now,
      input.expiresAt ?? now + FAILED_EVENT_RECEIPT_RETENTION_MS,
    );
}

function receipt(id: string, channel = "RETRY"): ReceiptView | null {
  return sqlite
    .query(
      `SELECT state, owner, fence,
              lease_expires_at AS leaseExpiresAt,
              next_attempt_at AS nextAttemptAt,
              delivery_delay_seconds AS delaySeconds,
              source_attempt AS sourceAttempt,
              failure_count AS failureCount,
              refresh_at AS refreshAt
       FROM failed_event_queue_receipts
       WHERE failed_event_id=? AND channel=?`,
    )
    .get(id, channel) as ReceiptView | null;
}

beforeEach(async () => {
  sqlite = new Database(":memory:");
  sqlite.exec(`CREATE TABLE workflow_failed_events (
    id TEXT PRIMARY KEY NOT NULL,
    status TEXT NOT NULL,
    metadata TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_retry_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
  )`);
  // Model a pre-existing control database, including historical malformed
  // metadata, before applying the Workflows Worker's supplemental migrations.
  sqlite
    .query(
      `INSERT INTO workflow_failed_events
       (id, status, metadata, attempts, next_retry_at, updated_at)
       VALUES ('migration_fixture_invalid_metadata', 'PENDING', '{', 0, 0, 0)`,
    )
    .run();
  for (const migrationName of [
    "20260827_0001_workflow_dispatch_receipts.sql",
    "20260827_0002_failed_event_queue_receipts.sql",
  ]) {
    const migration = await Bun.file(
      new URL(`../../migrations/${migrationName}`, import.meta.url),
    ).text();
    sqlite.exec(migration);
  }
  control = {
    confirmFailures: 0,
    expiredCleanupRuns: 0,
    terminalCleanupRuns: 0,
  };
  controlDb = adaptDatabase(sqlite, control);
});

afterEach(() => sqlite.close());

describe("durable failed-event Queue delivery", () => {
  it("suppresses exact and concurrent replays after one confirmed send", async () => {
    const now = 1_000;
    const id = "callback_confirmed";
    insertFailedRow(id);
    const retry = queueCapture();
    const env = deliveryEnv(retry);

    expect(
      await enqueueFailedEventRetry(env, id, { delaySeconds: 60, now }),
    ).toBe("sent");
    expect(
      await enqueueFailedEventRetry(env, id, {
        delaySeconds: 60,
        now: now + 1,
      }),
    ).toBe("suppressed");

    expect(retry.attempts).toBe(1);
    expect(retry.sent).toEqual([
      {
        body: { v: 3, failedEventId: id },
        options: { delaySeconds: 60 },
      },
    ]);
    expect(receipt(id)).toMatchObject({
      state: "CONFIRMED",
      owner: null,
      fence: 1,
      failureCount: 0,
      refreshAt: now + FAILED_EVENT_QUEUE_REFRESH_MS,
    });
  });

  it("recovers a definite first-send failure only after durable backoff", async () => {
    const now = 10_000;
    const id = "callback_send_failure";
    insertFailedRow(id);
    const retry = queueCapture({ failures: 1 });
    const env = deliveryEnv(retry);

    await expect(
      enqueueFailedEventRetry(env, id, { delaySeconds: 60, now }),
    ).rejects.toThrow("queue unavailable");
    expect(receipt(id)).toMatchObject({
      state: "PENDING",
      owner: null,
      nextAttemptAt: now + 5 * 60_000,
      delaySeconds: 0,
      failureCount: 1,
    });

    const early = await sweepFailedEventDeliveries(env, {
      now: now + 5 * 60_000 - 1,
    });
    expect(early.attempted).toBe(0);
    expect(retry.attempts).toBe(1);

    const recovered = await sweepFailedEventDeliveries(env, {
      now: now + 5 * 60_000,
    });
    expect(recovered).toMatchObject({ attempted: 1, sent: 1 });
    expect(retry.attempts).toBe(2);
    expect(retry.sent).toEqual([
      { body: { v: 3, failedEventId: id }, options: undefined },
    ]);
  });

  it("bounds a possibly accepted send failure behind the durable backoff", async () => {
    const now = 15_000;
    const id = "callback_ambiguous_send";
    insertFailedRow(id);
    const retry = queueCapture({ ambiguousFailures: 1 });
    const env = deliveryEnv(retry);

    await expect(
      enqueueFailedEventRetry(env, id, { delaySeconds: 60, now }),
    ).rejects.toThrow("queue outcome unknown");
    expect(retry.sent).toHaveLength(1);
    expect(receipt(id)).toMatchObject({
      state: "PENDING",
      nextAttemptAt: now + 5 * 60_000,
      failureCount: 1,
    });

    expect(
      (
        await sweepFailedEventDeliveries(env, {
          now: now + 5 * 60_000 - 1,
        })
      ).attempted,
    ).toBe(0);
    expect(retry.attempts).toBe(1);

    expect(
      await sweepFailedEventDeliveries(env, {
        now: now + 5 * 60_000,
      }),
    ).toMatchObject({ attempted: 1, sent: 1 });
    expect(retry.attempts).toBe(2);
    // The first call may have committed at the platform boundary, so exactly
    // one later fenced repair is the unavoidable duplicate ceiling here.
    expect(retry.sent).toHaveLength(2);
  });

  it("allows only one live enqueue owner to call Queue.send", async () => {
    const now = 20_000;
    const id = "callback_concurrent";
    insertFailedRow(id);
    let releaseFirst!: () => void;
    let markStarted!: () => void;
    const gate = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    const started = new Promise<void>((resolve) => {
      markStarted = resolve;
    });
    const retry = queueCapture({
      firstSendGate: gate,
      firstSendStarted: markStarted,
    });
    const env = deliveryEnv(retry);

    const first = enqueueFailedEventRetry(env, id, {
      delaySeconds: 60,
      now,
    });
    await started;
    expect(
      await enqueueFailedEventRetry(env, id, { delaySeconds: 60, now }),
    ).toBe("suppressed");
    expect(retry.attempts).toBe(1);

    releaseFirst();
    expect(await first).toBe("sent");
    expect(receipt(id)).toMatchObject({ state: "CONFIRMED", fence: 1 });
  });

  it("rolls over at most one fresh pointer per durable processing attempt", async () => {
    const now = 25_000;
    const id = "callback_transport_rollover";
    insertFailedRow(id, { attempts: 1 });
    const retry = queueCapture();
    const env = deliveryEnv(retry);
    await enqueueFailedEventRetry(env, id, { delaySeconds: 60, now });

    expect(
      await rearmFailedEventRetry(env, id, {
        delaySeconds: 180,
        now: now + 1,
      }),
    ).toBe("sent");
    expect(
      await rearmFailedEventRetry(env, id, {
        delaySeconds: 180,
        now: now + 2,
      }),
    ).toBe("suppressed");

    expect(retry.attempts).toBe(2);
    expect(retry.sent[1]).toEqual({
      body: { v: 3, failedEventId: id },
      options: { delaySeconds: 180 },
    });
    expect(receipt(id)).toMatchObject({
      state: "CONFIRMED",
      sourceAttempt: 1,
      fence: 3,
    });
  });

  it("fences a stale sender after a newer lease confirms", async () => {
    const now = 30_000;
    const id = "callback_stale_fence";
    insertFailedRow(id);
    let releaseFirst!: () => void;
    let markStarted!: () => void;
    const gate = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    const started = new Promise<void>((resolve) => {
      markStarted = resolve;
    });
    const retry = queueCapture({
      firstSendGate: gate,
      firstSendStarted: markStarted,
    });
    const env = deliveryEnv(retry);

    const stale = enqueueFailedEventRetry(env, id, {
      delaySeconds: 60,
      now,
    });
    await started;
    expect(
      await enqueueFailedEventRetry(env, id, {
        delaySeconds: 60,
        now: now + FAILED_EVENT_DELIVERY_LEASE_MS,
      }),
    ).toBe("sent");

    releaseFirst();
    expect(await stale).toBe("ambiguous");
    expect(retry.attempts).toBe(2);
    expect(receipt(id)).toMatchObject({
      state: "CONFIRMED",
      owner: null,
      fence: 2,
    });
  });

  it("repairs one ambiguous confirmation after the lease without hot-looping", async () => {
    const now = 40_000;
    const id = "callback_ambiguous";
    insertFailedRow(id);
    const retry = queueCapture();
    const env = deliveryEnv(retry);
    control.confirmFailures = 1;

    expect(
      await enqueueFailedEventRetry(env, id, { delaySeconds: 60, now }),
    ).toBe("ambiguous");
    expect(retry.attempts).toBe(1);
    expect(receipt(id)).toMatchObject({
      state: "PENDING",
      fence: 1,
      leaseExpiresAt: now + FAILED_EVENT_DELIVERY_LEASE_MS,
    });

    expect(
      (
        await sweepFailedEventDeliveries(env, {
          now: now + FAILED_EVENT_DELIVERY_LEASE_MS - 1,
        })
      ).attempted,
    ).toBe(0);
    expect(retry.attempts).toBe(1);

    expect(
      await sweepFailedEventDeliveries(env, {
        now: now + FAILED_EVENT_DELIVERY_LEASE_MS,
      }),
    ).toMatchObject({ attempted: 1, sent: 1 });
    expect(retry.attempts).toBe(2);
    expect(receipt(id)).toMatchObject({
      state: "CONFIRMED",
      fence: 2,
    });
  });

  it("bounds missing-receipt discovery and due sends per sweep", async () => {
    const now = 50_000;
    const retry = queueCapture();
    const env = deliveryEnv(retry);
    for (let index = 0; index < FAILED_EVENT_RECEIPT_DISCOVERY_LIMIT + 5; index += 1) {
      insertFailedRow(`callback_missing_${index}`, { updatedAt: index });
    }

    const first = await sweepFailedEventDeliveries(env, { now });
    expect(first).toMatchObject({
      discovered: FAILED_EVENT_RECEIPT_DISCOVERY_LIMIT,
      attempted: FAILED_EVENT_RECEIPT_DISCOVERY_LIMIT,
      sent: FAILED_EVENT_RECEIPT_DISCOVERY_LIMIT,
    });
    expect(retry.attempts).toBe(FAILED_EVENT_RECEIPT_DISCOVERY_LIMIT);

    const second = await sweepFailedEventDeliveries(env, { now: now + 1 });
    expect(second).toMatchObject({ discovered: 5, attempted: 5, sent: 5 });

    for (let index = 0; index < FAILED_EVENT_DELIVERY_SWEEP_LIMIT + 5; index += 1) {
      const id = `callback_due_${index}`;
      insertFailedRow(id, { updatedAt: 100 + index });
      insertReceipt(id, { nextAttemptAt: now });
    }
    const bounded = await sweepFailedEventDeliveries(env, { now: now + 2 });
    expect(bounded.attempted).toBe(FAILED_EVENT_DELIVERY_SWEEP_LIMIT);
    expect(bounded.sent).toBe(FAILED_EVENT_DELIVERY_SWEEP_LIMIT);
  });

  it("delays a repaired PROCESSING pointer until its active Queue lease ends", async () => {
    const now = 55_000;
    const leaseExpiresAt = now + 5 * 60_000;
    const id = "callback_missing_processing";
    insertFailedRow(id, {
      status: "PROCESSING",
      attempts: 1,
      metadata: {
        queueRetryNotBefore: now - 1,
        queueLeaseExpiresAt: leaseExpiresAt,
      },
    });
    const retry = queueCapture();

    expect(
      await sweepFailedEventDeliveries(deliveryEnv(retry), { now }),
    ).toMatchObject({ discovered: 1, attempted: 1, sent: 1 });
    expect(retry.sent).toEqual([
      {
        body: { v: 3, failedEventId: id },
        options: { delaySeconds: 5 * 60 },
      },
    ]);
  });

  it("refreshes one confirmed pointer before default Queue retention", async () => {
    const now = 60_000;
    const id = "callback_long_outage";
    insertFailedRow(id);
    const retry = queueCapture();
    const env = deliveryEnv(retry);
    await enqueueFailedEventRetry(env, id, { delaySeconds: 60, now });

    expect(
      (
        await sweepFailedEventDeliveries(env, {
          now: now + FAILED_EVENT_QUEUE_REFRESH_MS - 1,
        })
      ).attempted,
    ).toBe(0);
    expect(retry.attempts).toBe(1);

    const refreshed = await sweepFailedEventDeliveries(env, {
      now: now + FAILED_EVENT_QUEUE_REFRESH_MS,
    });
    expect(refreshed).toMatchObject({ attempted: 1, sent: 1 });
    expect(retry.attempts).toBe(2);
    expect(receipt(id)).toMatchObject({
      state: "CONFIRMED",
      fence: 2,
      refreshAt: now + 2 * FAILED_EVENT_QUEUE_REFRESH_MS,
    });
  });

  it("continues cleanup across pages and stops after a short ordinary-sweep page", async () => {
    const now = 70_000 + FAILED_EVENT_TERMINAL_CLEANUP_GRACE_MS;
    const rowsPerCategory = 2 * FAILED_EVENT_RECEIPT_CLEANUP_LIMIT + 5;
    for (let index = 0; index < rowsPerCategory; index += 1) {
      insertReceipt(`callback_expired_${index}`, {
        channel: "DLQ",
        state: "CONFIRMED",
        nextAttemptAt: now + 1,
        updatedAt: now,
        expiresAt: now - 1,
      });
      insertReceipt(`callback_terminal_${index}`, {
        state: "CONFIRMED",
        nextAttemptAt: now + FAILED_EVENT_QUEUE_REFRESH_MS,
        refreshAt: now + FAILED_EVENT_QUEUE_REFRESH_MS,
        updatedAt: 0,
        expiresAt: now + FAILED_EVENT_RECEIPT_RETENTION_MS,
      });
    }
    const retry = queueCapture();
    const env = deliveryEnv(retry);

    const cleaned = await sweepFailedEventDeliveries(env, { now });
    expect(cleaned).toMatchObject({
      cleanedExpired: rowsPerCategory,
      cleanedTerminal: rowsPerCategory,
    });
    expect(control.expiredCleanupRuns).toBe(3);
    expect(control.terminalCleanupRuns).toBe(3);
    expect(
      sqlite
        .query(
          "SELECT count(*) AS count FROM failed_event_queue_receipts",
        )
        .get(),
    ).toEqual({ count: 0 });
    expect(retry.attempts).toBe(0);
  });

  it("hard-caps each cleanup category per sweep", async () => {
    const now = 75_000 + FAILED_EVENT_TERMINAL_CLEANUP_GRACE_MS;
    const cleanupCap =
      FAILED_EVENT_RECEIPT_CLEANUP_LIMIT *
      FAILED_EVENT_RECEIPT_CLEANUP_MAX_PAGES;
    for (let index = 0; index < cleanupCap + 5; index += 1) {
      insertReceipt(`callback_expired_bound_${index}`, {
        channel: "DLQ",
        state: "CONFIRMED",
        nextAttemptAt: now + 1,
        updatedAt: now,
        expiresAt: now - 1,
      });
      insertReceipt(`callback_terminal_bound_${index}`, {
        state: "CONFIRMED",
        nextAttemptAt: now + FAILED_EVENT_QUEUE_REFRESH_MS,
        refreshAt: now + FAILED_EVENT_QUEUE_REFRESH_MS,
        updatedAt: 0,
        expiresAt: now + FAILED_EVENT_RECEIPT_RETENTION_MS,
      });
    }

    const cleaned = await sweepFailedEventDeliveries(
      deliveryEnv(queueCapture()),
      { now },
    );
    expect(cleaned).toMatchObject({
      cleanedExpired: cleanupCap,
      cleanedTerminal: cleanupCap,
    });
    expect(control.expiredCleanupRuns).toBe(
      FAILED_EVENT_RECEIPT_CLEANUP_MAX_PAGES,
    );
    expect(control.terminalCleanupRuns).toBe(
      FAILED_EVENT_RECEIPT_CLEANUP_MAX_PAGES,
    );
    expect(
      sqlite
        .query(
          "SELECT count(*) AS count FROM failed_event_queue_receipts WHERE failed_event_id LIKE 'callback_expired_bound_%'",
        )
        .get(),
    ).toEqual({ count: 5 });
    expect(
      sqlite
        .query(
          "SELECT count(*) AS count FROM failed_event_queue_receipts WHERE failed_event_id LIKE 'callback_terminal_bound_%'",
        )
        .get(),
    ).toEqual({ count: 5 });
  });

  it("publishes one DLQ pointer for duplicate DEAD deliveries", async () => {
    const now = 80_000;
    const id = "callback_dead";
    insertFailedRow(id, { status: "DEAD" });
    const retry = queueCapture();
    const dlq = queueCapture();
    const env = deliveryEnv(retry, dlq);

    expect(await publishFailedEventDlq(env, id, { now })).toBe("sent");
    expect(await publishFailedEventDlq(env, id, { now: now + 1 })).toBe(
      "suppressed",
    );
    expect(dlq.attempts).toBe(1);
    expect(dlq.sent).toEqual([
      { body: { v: 3, failedEventId: id }, options: undefined },
    ]);
  });

  it("keeps backend ownership predicates indexed and safe for invalid JSON", () => {
    insertFailedRow("callback_indexed");

    const genericPlan = sqlite
      .query(
        `EXPLAIN QUERY PLAN
         SELECT id FROM workflow_failed_events
         WHERE status='PENDING' AND next_retry_at <= ?
           AND coalesce(
             CASE WHEN json_valid(metadata)
               THEN json_extract(metadata, '$.recoveryOwner')
             END,
             ''
           ) <> 'callback-queue'
         ORDER BY next_retry_at, id LIMIT ?`,
      )
      .all(1, 10) as Array<{ detail: string }>;
    expect(genericPlan.map((row) => row.detail).join("\n")).toContain(
      "workflow_failed_events_non_callback_retry_idx",
    );
    const leasePlan = sqlite
      .query(
        `EXPLAIN QUERY PLAN
         SELECT id FROM workflow_failed_events
         WHERE status='PROCESSING' AND updated_at <= ?
           AND coalesce(
             CASE WHEN json_valid(metadata)
               THEN json_extract(metadata, '$.recoveryOwner')
             END,
             ''
           ) <> 'callback-queue'
         ORDER BY updated_at, id LIMIT ?`,
      )
      .all(1, 10) as Array<{ detail: string }>;
    expect(leasePlan.map((row) => row.detail).join("\n")).toContain(
      "workflow_failed_events_non_callback_lease_idx",
    );
    const callbackPlan = sqlite
      .query(
        `EXPLAIN QUERY PLAN
         SELECT id FROM workflow_failed_events
         WHERE status IN ('PENDING','PROCESSING')
           AND CASE WHEN json_valid(metadata)
             THEN json_extract(metadata, '$.recoveryOwner')
           END = 'callback-queue'
         ORDER BY updated_at, id LIMIT ?`,
      )
      .all(10) as Array<{ detail: string }>;
    expect(callbackPlan.map((row) => row.detail).join("\n")).toContain(
      "workflow_failed_events_callback_recovery_idx",
    );
    expect(
      sqlite
        .query(
          `SELECT id FROM workflow_failed_events
           WHERE coalesce(
             CASE WHEN json_valid(metadata)
               THEN json_extract(metadata, '$.recoveryOwner')
             END,
             ''
           ) <> 'callback-queue'
           ORDER BY id`,
        )
        .all(),
    ).toEqual([{ id: "migration_fixture_invalid_metadata" }]);
    expect(
      sqlite
        .query(
          `SELECT name FROM sqlite_master
           WHERE type='table' AND name IN (
             'workflow_dispatch_receipts', 'failed_event_queue_receipts'
           ) ORDER BY name`,
        )
        .all(),
    ).toEqual([
      { name: "failed_event_queue_receipts" },
      { name: "workflow_dispatch_receipts" },
    ]);
  });
});
