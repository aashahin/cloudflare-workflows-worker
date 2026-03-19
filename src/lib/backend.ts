// ─── Shared Backend Utilities ────────────────────────────────────────────────
// Common helpers used by all workflow classes.

import type { WorkflowSleepDuration } from "cloudflare:workers";
import { NonRetryableError } from "cloudflare:workflows";
import type { Env } from "../env.js";

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

  const response = await fetch(`${env.BACKEND_URL}/workflows/execute/${path}`, {
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
    // - 404: resource doesn't exist
    // - 409: conflict (duplicate)
    // - 422: validation failure (bad input shape)
    // These will never succeed without code or data changes.
    const NON_RETRYABLE_STATUSES = [404, 409, 422];

    if (NON_RETRYABLE_STATUSES.includes(response.status)) {
      throw new NonRetryableError(
        `Backend ${path} failed (${response.status}): ${body}`,
      );
    }

    // Everything else is retryable:
    // - 400: often config issues (invalid API keys, credentials)
    // - 401/403: auth/config issues that can be fixed between retries
    // - 429: rate limiting
    // - 5xx: transient server errors
    throw new Error(`Backend ${path} failed (${response.status}): ${body}`);
  }
}

/**
 * Convert milliseconds to a Cloudflare Workflows Duration string.
 * CF `step.sleep()` expects strings like "30 seconds", "5 minutes".
 */
export function msToDuration(ms: number): WorkflowSleepDuration {
  const totalSeconds = Math.ceil(ms / 1000);
  if (totalSeconds <= 0) return "1 second" as WorkflowSleepDuration;
  if (totalSeconds === 1) return "1 second" as WorkflowSleepDuration;
  // Use seconds for all values under 1 hour to preserve precision
  if (totalSeconds < 3600)
    return `${totalSeconds} seconds` as WorkflowSleepDuration;
  const minutes = Math.round(totalSeconds / 60);
  if (minutes === 1) return "1 minute" as WorkflowSleepDuration;
  return `${minutes} minutes` as WorkflowSleepDuration;
}
