export const MAX_BACKEND_ERROR_BODY_BYTES = 4_096;

const PERMANENT_BACKEND_STATUSES = new Set([
  400,
  401,
  403,
  404,
  409,
  413,
  422,
]);

/** Input/domain failures that cannot recover through bounded transport retries. */
export function isPermanentBackendStatus(status: number): boolean {
  return PERMANENT_BACKEND_STATUSES.has(status);
}

/** Read only a bounded prefix so an upstream error cannot inflate CPU or logs. */
export async function readBoundedBackendErrorBody(
  response: Response,
): Promise<string> {
  if (!response.body) return "";

  const reader = response.body.getReader();
  const bytes = new Uint8Array(MAX_BACKEND_ERROR_BODY_BYTES);
  let written = 0;
  let truncated = false;

  try {
    while (written < bytes.byteLength) {
      const { done, value } = await reader.read();
      if (done) break;
      const remaining = bytes.byteLength - written;
      const copied = Math.min(remaining, value.byteLength);
      bytes.set(value.subarray(0, copied), written);
      written += copied;
      if (copied < value.byteLength) {
        truncated = true;
        break;
      }
    }
    if (written === bytes.byteLength) truncated = true;
  } catch {
    return "unreadable backend error response";
  } finally {
    await reader.cancel().catch(() => undefined);
  }

  const decoded = new TextDecoder()
    .decode(bytes.subarray(0, written))
    .replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (!decoded) return "";
  return truncated ? `${decoded}…` : decoded;
}
