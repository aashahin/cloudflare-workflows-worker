import { describe, expect, it } from "bun:test";

import type { Env } from "../env.js";
import {
  BACKEND_CALLBACK_TIMEOUT_MS,
  callBackendService,
} from "./backend.js";
import {
  backendExecuteUrl,
  fetchBackendExecute,
} from "./backend-transport.js";

function envWithOrigin(origin = "https://api.manhali.com"): Env {
  return {
    BACKEND_ORIGIN: origin,
    BACKEND_CALLBACK_TOKEN: "callback-secret",
    BACKEND: { fetch: async () => new Response(null, { status: 204 }) },
  } as unknown as Env;
}

describe("backend execute transport", () => {
  it("uses the private BACKEND service binding origin", () => {
    expect(backendExecuteUrl(envWithOrigin(), "course/rebuild-index")).toBe(
      "https://api.manhali.com/workflows/execute/course/rebuild-index",
    );
  });

  it("rejects callback traversal and non-origin authorities", () => {
    expect(() =>
      backendExecuteUrl(envWithOrigin(), "payment/../../admin"),
    ).toThrow(/slash-separated/);
    expect(() =>
      backendExecuteUrl(
        envWithOrigin("https://api.manhali.com/internal"),
        "payment/process-payout",
      ),
    ).toThrow("bare HTTPS origin");
  });

  it("fails closed when the required BACKEND binding is missing", async () => {
    const env = {
      AUTH_TOKEN: "secret",
      BACKEND_CALLBACK_TOKEN: "callback-secret",
      BACKEND_ORIGIN: "https://api.manhali.com",
    } as unknown as Env;

    await expect(
      fetchBackendExecute(env, "course/rebuild-index", {
        method: "POST",
        body: "{}",
      }),
    ).rejects.toThrow("BACKEND service binding is required");
  });

  it("posts through the binding and keeps bearer plus tenant headers", async () => {
    const requests: Request[] = [];
    const env = {
      AUTH_TOKEN: "secret",
      BACKEND_CALLBACK_TOKEN: "callback-secret",
      BACKEND_ORIGIN: "https://api.manhali.com",
      BACKEND: {
        async fetch(input: RequestInfo | URL, init?: RequestInit) {
          const request = new Request(input, init);
          requests.push(request);
          return new Response(null, { status: 204 });
        },
      },
    } as unknown as Env;

    await fetchBackendExecute(env, "course/rebuild-index", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: "Bearer callback-secret",
        "x-tenant-id": "tenant_1",
      },
      body: "{}",
    });

    expect(requests).toHaveLength(1);
    expect(requests[0]?.url).toBe(
      "https://api.manhali.com/workflows/execute/course/rebuild-index",
    );
    expect(requests[0]?.headers.get("Authorization")).toBe(
      "Bearer callback-secret",
    );
    expect(requests[0]?.headers.get("x-tenant-id")).toBe("tenant_1");
  });

  it("uses the callback-only token and a bounded service-call deadline", async () => {
    const requests: Request[] = [];
    const env = {
      AUTH_TOKEN: "public-dispatch-secret",
      BACKEND_CALLBACK_TOKEN: "private-callback-secret",
      BACKEND_ORIGIN: "https://api.manhali.com",
      BACKEND: {
        async fetch(input: RequestInfo | URL, init?: RequestInit) {
          requests.push(new Request(input, init));
          return new Response(null, { status: 204 });
        },
      },
    } as unknown as Env;

    await callBackendService(
      env,
      "course/rebuild-index",
      { tenantId: "tenant_1" },
      "trace_1",
      "wf_1",
    );

    expect(BACKEND_CALLBACK_TIMEOUT_MS).toBe(15_000);
    expect(requests[0]?.headers.get("Authorization")).toBe(
      "Bearer private-callback-secret",
    );
    expect(requests[0]?.signal).toBeInstanceOf(AbortSignal);
    expect(requests[0]?.signal.aborted).toBe(false);
  });

  it("fails closed when callback auth is missing", async () => {
    const env = {
      AUTH_TOKEN: "public-dispatch-secret",
      BACKEND_CALLBACK_TOKEN: "",
      BACKEND_ORIGIN: "https://api.manhali.com",
      BACKEND: { fetch: async () => new Response(null, { status: 204 }) },
    } as unknown as Env;

    try {
      await callBackendService(env, "course/rebuild-index", {});
      throw new Error("callBackendService should have failed");
    } catch (error) {
      expect(error).toBeInstanceOf(Error);
      expect((error as Error).message).toContain(
        "BACKEND_CALLBACK_TOKEN is required",
      );
      expect((error as Error).name).toBe("NonRetryableError");
    }
  });
});
