// ─── Shared Backend Utilities ────────────────────────────────────────────────
// Common helpers used by all workflow classes.

import { NonRetryableError } from "cloudflare:workflows";
import type { Env } from "../env.js";

// Default fetch timeout for a single backend call. Overridable per deployment
// via env.BACKEND_TIMEOUT_MS. Mirrors the SDK REST adapter's bounded-abort
// approach so a hung backend fails fast instead of stalling the queue batch.
const DEFAULT_BACKEND_TIMEOUT_MS = 60_000;

// Cap the amount of backend-controlled response text embedded in error
// messages, so a large/hostile body can't bloat logs.
const MAX_BODY_CHARS = 500;

export function isNonRetryableFailure(err: unknown): boolean {
  if (err instanceof NonRetryableError) return true;
  if (!(err instanceof Error)) return false;
  // Match only the trusted non-retryable signal: the real class name, or our
  // own deliberate "NonRetryableError:" message prefix. A substring check would
  // let untrusted backend response text (which we embed in retryable errors)
  // spoof a permanent failure and silently drop a retryable event.
  return (
    err.name === "NonRetryableError" ||
    err.message.startsWith("NonRetryableError")
  );
}

/** Resolve the per-call backend timeout, tolerating an undeclared env var. */
function resolveBackendTimeoutMs(env: Env): number {
  const raw = (env as unknown as { BACKEND_TIMEOUT_MS?: string })
    .BACKEND_TIMEOUT_MS;
  const parsed = raw != null ? Number.parseInt(String(raw), 10) : NaN;
  return Number.isFinite(parsed) && parsed > 0
    ? parsed
    : DEFAULT_BACKEND_TIMEOUT_MS;
}

/**
 * Truncate and sanitize backend-controlled response text before embedding it in
 * a retryable error message. Strips any leading "NonRetryableError" token so an
 * untrusted body cannot masquerade as our permanent-failure prefix.
 */
function sanitizeBody(body: string): string {
  const stripped = body.replace(/^\s*NonRetryableError:?\s*/i, "");
  return stripped.length > MAX_BODY_CHARS
    ? `${stripped.slice(0, MAX_BODY_CHARS)}…`
    : stripped;
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
  // Fail fast and loudly on misconfiguration rather than building an
  // "undefined/workflows/execute/..." URL and burning the whole retry budget.
  if (!env.BACKEND_URL) {
    throw new Error(
      `Backend ${path} failed: BACKEND_URL is not configured`,
    );
  }

  const tenantId =
    "tenantId" in data && typeof data.tenantId === "string"
      ? data.tenantId.trim()
      : undefined;

  const timeoutMs = resolveBackendTimeoutMs(env);

  let response: Response;
  try {
    response = await fetch(`${env.BACKEND_URL}/workflows/execute/${path}`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${env.AUTH_TOKEN}`,
        ...(tenantId ? { "x-tenant-id": tenantId } : {}),
        ...(traceId ? { "X-Trace-Id": traceId } : {}),
        ...(eventId ? { "X-Workflow-Event-Id": eventId } : {}),
      },
      body: JSON.stringify(data),
      signal: AbortSignal.timeout(timeoutMs),
    });
  } catch (err) {
    // Timeout/abort and network errors are transient — surface a clear,
    // retryable error so the queue/Workflow retry machinery advances promptly.
    const isAbort =
      err instanceof Error &&
      (err.name === "TimeoutError" || err.name === "AbortError");
    if (isAbort) {
      throw new Error(
        `Backend ${path} failed: request timed out after ${timeoutMs}ms`,
      );
    }
    throw new Error(
      `Backend ${path} failed: ${err instanceof Error ? err.message : String(err)}`,
    );
  }

  if (!response.ok) {
    const body = sanitizeBody(await response.text().catch(() => ""));

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
}
