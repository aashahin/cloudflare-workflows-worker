import { describe, expect, it } from "bun:test";
import { RECOMMENDED_CLOUDFLARE_WORKFLOW_RECEIPT_CLEANUP_CRON } from "@abshahin/workflows-sdk/cloudflare";
import { FAILED_EVENT_ENQUEUE_SWEEP_CRON } from "./lib/failed-event-delivery.js";

interface WorkerConfigView {
  limits?: { cpu_ms?: number };
  observability?: {
    logs?: {
      enabled?: boolean;
      head_sampling_rate?: number;
      persist?: boolean;
      invocation_logs?: boolean;
    };
  };
  queues?: {
    producers?: Array<{
      binding?: string;
      queue?: string;
    }>;
    consumers?: Array<{
      max_retries?: number;
      max_concurrency?: number;
    }>;
  };
  services?: Array<{ binding?: string; service?: string }>;
  d1_databases?: Array<{
    binding?: string;
    database_name?: string;
    database_id?: string;
    migrations_dir?: string;
  }>;
  vars?: Record<string, string>;
  triggers?: { crons?: string[] };
  env?: { sandbox?: WorkerConfigView };
}

describe("wrangler runtime cost controls", () => {
  it("keeps generated binding types aligned with the final manifest", async () => {
    const generated = await Bun.file(
      new URL("./worker-configuration.d.ts", import.meta.url),
    ).text();

    for (const binding of [
      "CONTROL_DB",
      "FAILED_EVENTS_QUEUE",
      "FAILED_EVENTS_DLQ",
      "AUTH_TOKEN",
      "BACKEND_CALLBACK_TOKEN",
      "BACKEND_ORIGIN",
      "WORKFLOW_BINDING_NAME",
      "BACKEND",
      "WORKFLOW",
    ]) {
      expect(generated).toContain(`\t${binding}:`);
    }
  });

  it("caps paid-production CPU while keeping the Workers Free sandbox deployable", async () => {
    const source = await Bun.file(
      new URL("../wrangler.jsonc", import.meta.url),
    ).text();
    const config = Bun.JSONC.parse(source) as WorkerConfigView;

    expect(config.limits?.cpu_ms).toBe(30_000);
    expect(config.env?.sandbox?.limits).toEqual({});

    for (const environment of [config, config.env?.sandbox]) {
      expect(environment?.observability?.logs).toMatchObject({
        enabled: true,
        persist: true,
        invocation_logs: false,
      });
      expect(environment?.observability?.logs?.head_sampling_rate).toBeGreaterThan(
        0,
      );
      expect(environment?.observability?.logs?.head_sampling_rate).toBeLessThanOrEqual(
        0.1,
      );
      expect(environment?.queues?.consumers?.[0]).toMatchObject({
        max_retries: 9,
        max_concurrency: 4,
      });
      expect(environment?.queues?.producers).toContainEqual({
        binding: "FAILED_EVENTS_DLQ",
        queue: expect.stringContaining("manhali-failed-events"),
      });
      expect(environment?.services).toContainEqual({
        binding: "BACKEND",
        service: expect.any(String),
      });
      expect(environment?.d1_databases).toContainEqual({
        binding: "CONTROL_DB",
        database_name: expect.stringContaining("manhali-control"),
        database_id: expect.stringMatching(/^[0-9a-f-]{36}$/),
        migrations_dir: "migrations",
      });
      expect(environment?.vars?.WORKFLOW_BINDING_NAME).toMatch(
        /^manhali-workflow(?:-sandbox)?$/,
      );
      expect(environment?.triggers?.crons).toEqual([
        FAILED_EVENT_ENQUEUE_SWEEP_CRON,
        RECOMMENDED_CLOUDFLARE_WORKFLOW_RECEIPT_CLEANUP_CRON,
      ]);
    }
  });
});
