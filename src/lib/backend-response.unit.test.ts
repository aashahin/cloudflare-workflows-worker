import { describe, expect, it } from "bun:test";

import {
  MAX_BACKEND_ERROR_BODY_BYTES,
  isPermanentBackendStatus,
  readBoundedBackendErrorBody,
} from "./backend-response.js";
import { isNonRetryableFailure } from "./backend.js";

describe("backend response classification", () => {
  it("treats auth and validation failures as permanent", () => {
    for (const status of [400, 401, 403, 404, 409, 413, 422]) {
      expect(isPermanentBackendStatus(status)).toBe(true);
    }
  });

  it("keeps timeout, early, rate-limit, and server failures retryable", () => {
    for (const status of [
      402,
      405,
      408,
      410,
      412,
      423,
      424,
      425,
      429,
      500,
      502,
      503,
      504,
    ]) {
      expect(isPermanentBackendStatus(status)).toBe(false);
    }
  });

  it("does not trust non-retryable text inside an upstream 5xx message", () => {
    expect(
      isNonRetryableFailure(
        new Error(
          "Backend course/rebuild failed (500): upstream NonRetryableError",
        ),
      ),
    ).toBe(false);
    expect(
      isNonRetryableFailure(
        new Error("NonRetryableError: Backend input failed (422)"),
      ),
    ).toBe(true);
  });

  it("bounds, sanitizes, and cancels large error bodies", async () => {
    const body = `backend\u0000 failed\n${"x".repeat(8_000)}`;
    const result = await readBoundedBackendErrorBody(
      new Response(body, { status: 500 }),
    );

    expect(result).toStartWith("backend failed ");
    expect(result).toEndWith("…");
    expect(result).not.toMatch(/[\u0000-\u001f\u007f]/);
    expect(new TextEncoder().encode(result).byteLength).toBeLessThanOrEqual(
      MAX_BACKEND_ERROR_BODY_BYTES + 3,
    );
  });
});
