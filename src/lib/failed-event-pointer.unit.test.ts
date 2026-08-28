import { describe, expect, it } from "bun:test";

import {
  CLOUDFLARE_QUEUE_MESSAGE_LIMIT_BYTES,
  createFailedEventRecordPointerMessage,
  createFailedEventPointerMessage,
} from "./failed-event-pointer.js";

describe("failed-event Queue pointer", () => {
  it("contains only a bounded Workflow instance reference", () => {
    const pointer = createFailedEventPointerMessage("w".repeat(100));
    const encoded = new TextEncoder().encode(JSON.stringify(pointer));

    expect(pointer).toEqual({ v: 2, workflowInstanceId: "w".repeat(100) });
    expect(encoded.byteLength).toBeLessThan(CLOUDFLARE_QUEUE_MESSAGE_LIMIT_BYTES);
    expect(JSON.stringify(pointer)).not.toContain("payload");
  });

  it("rejects invalid instance ids before Queue send", () => {
    expect(() => createFailedEventPointerMessage("")).toThrow("1-100");
    expect(() => createFailedEventPointerMessage("x".repeat(101))).toThrow(
      "1-100",
    );
    expect(() => createFailedEventPointerMessage("wf_1\nforged")).toThrow(
      "printable",
    );
  });

  it("creates a payload-free control D1 record pointer", () => {
    const pointer = createFailedEventRecordPointerMessage("callback_wf_1");

    expect(pointer).toEqual({ v: 3, failedEventId: "callback_wf_1" });
    expect(JSON.stringify(pointer)).not.toContain("payload");
    expect(() => createFailedEventRecordPointerMessage("")).toThrow("1-128");
  });
});
