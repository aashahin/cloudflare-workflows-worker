// ─── Shared Backend Utilities ────────────────────────────────────────────────
// Common helpers used by all workflow classes.

import type { Env } from "../env.js";
import {
  isPermanentBackendStatus,
  readBoundedBackendErrorBody,
} from "./backend-response.js";
import { fetchBackendExecute } from "./backend-transport.js";

export const BACKEND_CALLBACK_TIMEOUT_MS = 15_000;

export function isNonRetryableFailure(err: unknown): boolean {
  if (!(err instanceof Error)) return false;
  return (
    err.name === "NonRetryableError" ||
    err.message.startsWith("NonRetryableError:")
  );
}

/**
 * Call the backend's internal workflow handler endpoint.
 * The backend exposes POST /workflows/execute/:path that runs the
 * actual service logic within tenant context.
 */
export async function callBackendService(
  env: Env,
  path: string,
  data: object,
  traceId?: string,
  eventId?: string,
): Promise<void> {
  if (
    typeof env.BACKEND_CALLBACK_TOKEN !== "string" ||
    env.BACKEND_CALLBACK_TOKEN.trim().length === 0
  ) {
    const error = new Error(
      "NonRetryableError: BACKEND_CALLBACK_TOKEN is required",
    );
    error.name = "NonRetryableError";
    throw error;
  }
  const tenantId =
    "tenantId" in data && typeof data.tenantId === "string"
      ? data.tenantId.trim()
      : undefined;

  const response = await fetchBackendExecute(env, path, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${env.BACKEND_CALLBACK_TOKEN}`,
      ...(tenantId ? { "x-tenant-id": tenantId } : {}),
      ...(traceId ? { "X-Trace-Id": traceId } : {}),
      ...(eventId ? { "X-Workflow-Event-Id": eventId } : {}),
    },
    body: JSON.stringify(data),
    signal: AbortSignal.timeout(BACKEND_CALLBACK_TIMEOUT_MS),
  });

  if (!response.ok) {
    const body = await readBoundedBackendErrorBody(response);

    // Only truly permanent client errors are non-retryable:
    // - 400/413/422: malformed, oversized, or invalid workflow payload
    // - 401/403: callback-token/configuration failures require an operator
    //   deploy; repeating billed callbacks cannot repair them
    // - 404: resource doesn't exist
    // - 409: permanent conflict (transient/not-ready states should use 425)
    // - 422: validation failure (bad input shape)
    // These will never succeed without code or data changes.
    if (isPermanentBackendStatus(response.status)) {
      // Prefix the message with "NonRetryableError" so the failure is still
      // detectable after Cloudflare re-throws it across the step boundary,
      // where the error's prototype/name may not survive but the message does.
      const error = new Error(
        `NonRetryableError: Backend ${path} failed (${response.status})${body ? `: ${body}` : ""}`,
      );
      error.name = "NonRetryableError";
      throw error;
    }

    // 408/425/429 and 5xx remain retryable transient failures.
    throw new Error(
      `Backend ${path} failed (${response.status})${body ? `: ${body}` : ""}`,
    );
  }

  await response.body?.cancel();
}
