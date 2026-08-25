import { afterEach, describe, expect, it } from "bun:test";

import type { Env } from "../env.js";
import {
  BACKEND_BINDING_ORIGIN,
  backendExecuteUrl,
  fetchBackendExecute,
} from "./backend-transport.js";

const originalFetch = globalThis.fetch;

afterEach(() => {
  globalThis.fetch = originalFetch;
});

describe("backend execute transport", () => {
  it("uses the BACKEND service binding origin when present", () => {
    const env = {
      AUTH_TOKEN: "secret",
      BACKEND_URL: "https://api.public.test",
      BACKEND: { fetch: async () => new Response(null, { status: 204 }) },
    } as unknown as Env;

    expect(backendExecuteUrl(env, "email/send")).toBe(
      `${BACKEND_BINDING_ORIGIN}/workflows/execute/email/send`,
    );
  });

  it("falls back to BACKEND_URL when the binding is missing", () => {
    const env = {
      AUTH_TOKEN: "secret",
      BACKEND_URL: "https://api.public.test",
    } as unknown as Env;

    expect(backendExecuteUrl(env, "email/send")).toBe(
      "https://api.public.test/workflows/execute/email/send",
    );
  });

  it("posts through the binding and keeps bearer plus tenant headers", async () => {
    const requests: Request[] = [];
    const env = {
      AUTH_TOKEN: "secret",
      BACKEND_URL: "https://api.public.test",
      BACKEND: {
        async fetch(input: RequestInfo | URL, init?: RequestInit) {
          const request = new Request(input, init);
          requests.push(request);
          return new Response(null, { status: 204 });
        },
      },
    } as unknown as Env;

    globalThis.fetch = (async () => {
      throw new Error("public BACKEND_URL must not be used");
    }) as unknown as typeof fetch;

    await fetchBackendExecute(env, "email/send", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: "Bearer secret",
        "x-tenant-id": "tenant_1",
      },
      body: "{}",
    });

    expect(requests).toHaveLength(1);
    expect(requests[0]?.url).toBe(
      `${BACKEND_BINDING_ORIGIN}/workflows/execute/email/send`,
    );
    expect(requests[0]?.headers.get("Authorization")).toBe("Bearer secret");
    expect(requests[0]?.headers.get("x-tenant-id")).toBe("tenant_1");
  });
});
