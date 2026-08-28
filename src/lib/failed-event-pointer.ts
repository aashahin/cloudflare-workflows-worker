import { assertCloudflareJsonSerializable } from "@abshahin/workflows-sdk/cloudflare";

export const CLOUDFLARE_QUEUE_MESSAGE_LIMIT_BYTES = 128_000;

/** Legacy compact envelope accepted during rollout from Workflow-output recovery. */
export interface FailedEventPointerMessage {
  v: 2;
  workflowInstanceId: string;
}

/** Current Queue envelope; the recovery record lives in control D1. */
export interface FailedEventRecordPointerMessage {
  v: 3;
  failedEventId: string;
}

export function createFailedEventPointerMessage(
  workflowInstanceId: string,
): FailedEventPointerMessage {
  if (
    workflowInstanceId.length === 0 ||
    workflowInstanceId.length > 100 ||
    /[\u0000-\u001f\u007f]/.test(workflowInstanceId)
  ) {
    throw new Error(
      "Failed-event Workflow instance id must contain 1-100 printable characters",
    );
  }

  const message: FailedEventPointerMessage = {
    v: 2,
    workflowInstanceId,
  };
  const bytes = assertCloudflareJsonSerializable(
    message,
    "Failed-event Queue pointer",
  );
  if (bytes >= CLOUDFLARE_QUEUE_MESSAGE_LIMIT_BYTES) {
    throw new Error("Failed-event Queue pointer exceeds the 128 KB message limit");
  }
  return message;
}

export function createFailedEventRecordPointerMessage(
  failedEventId: string,
): FailedEventRecordPointerMessage {
  if (
    failedEventId.length === 0 ||
    failedEventId.length > 128 ||
    /[\u0000-\u001f\u007f]/.test(failedEventId)
  ) {
    throw new Error(
      "Failed-event record id must contain 1-128 printable characters",
    );
  }

  const message: FailedEventRecordPointerMessage = { v: 3, failedEventId };
  const bytes = assertCloudflareJsonSerializable(
    message,
    "Failed-event Queue record pointer",
  );
  if (bytes >= CLOUDFLARE_QUEUE_MESSAGE_LIMIT_BYTES) {
    throw new Error(
      "Failed-event Queue record pointer exceeds the 128 KB message limit",
    );
  }
  return message;
}
