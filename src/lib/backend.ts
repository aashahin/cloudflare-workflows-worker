// ─── Shared Backend Utilities ────────────────────────────────────────────────
// Common helpers used by all workflow classes.

import { NonRetryableError } from "cloudflare:workflows";
import type { Env } from "../env.js";
import { fetchBackendExecute } from "./backend-transport.js";

export function isNonRetryableFailure(err: unknown): boolean {
  if (err instanceof NonRetryableError) return true;
  if (!(err instanceof Error)) return false;
  return (
    err.name === "NonRetryableError" ||
    err.message.includes("NonRetryableError")
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
  const tenantId =
    "tenantId" in data && typeof data.tenantId === "string"
      ? data.tenantId.trim()
      : undefined;

  const response = await fetchBackendExecute(env, path, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${env.AUTH_TOKEN}`,
      ...(tenantId ? { "x-tenant-id": tenantId } : {}),
      ...(traceId ? { "X-Trace-Id": traceId } : {}),
      ...(eventId ? { "X-Workflow-Event-Id": eventId } : {}),
    },
    body: JSON.stringify(data),
  });

  if (!response.ok) {
    const body = await response.text().catch(() => "");

    // Only truly permanent client errors are non-retryable:
    // - 400: malformed or incomplete workflow payload
    // - 404: resource doesn't exist
    // - 409: permanent conflict (transient/not-ready states should use 425)
    // - 422: validation failure (bad input shape)
    // These will never succeed without code or data changes.
    const NON_RETRYABLE_STATUSES = [400, 404, 409, 422];

    if (NON_RETRYABLE_STATUSES.includes(response.status)) {
      // Prefix the message with "NonRetryableError" so the failure is still
      // detectable after Cloudflare re-throws it across the step boundary,
      // where the error's prototype/name may not survive but the message does.
      throw new NonRetryableError(
        `NonRetryableError: Backend ${path} failed (${response.status}): ${body}`,
      );
    }

    // Everything else is retryable:
    // - 401/403: auth/config issues that can be fixed between retries
    // - 429: rate limiting
    // - 5xx: transient server errors
    throw new Error(`Backend ${path} failed (${response.status}): ${body}`);
  }

  await response.body?.cancel();
}
