import { describe, expect, it } from "bun:test";

import {
  callbackStepsPolicy,
  SUPPORTED_WORKFLOW_CALLBACK_NAMES,
  workflowNamePolicy,
} from "./workflow-policy.js";

describe("Workflow callback admission", () => {
  it("accepts exactly the 16 top-level Worker-native producer names", () => {
    expect(SUPPORTED_WORKFLOW_CALLBACK_NAMES).toHaveLength(16);
    for (const name of SUPPORTED_WORKFLOW_CALLBACK_NAMES) {
      expect(workflowNamePolicy(name)).toBe(true);
    }
  });

  it("rejects arbitrary and unimplemented WhatsApp callbacks", () => {
    expect(workflowNamePolicy("whatsapp/send-template")).toContain(
      "Unsupported",
    );
    expect(workflowNamePolicy("admin/delete-all")).toContain("Unsupported");
    expect(workflowNamePolicy("payment/validate-payout")).toContain(
      "Unsupported",
    );
    expect(workflowNamePolicy("payment/notify-payout-status")).toContain(
      "Unsupported",
    );
  });

  it("allows only the canonical callback plan for each admitted workflow", () => {
    expect(callbackStepsPolicy("email/invitation", [])).toBe(true);
    expect(
      callbackStepsPolicy("email/invitation", [
        { stepName: "extra", backendPath: "notification/create" },
      ]),
    ).toContain("does not accept");

    const payout = [
      {
        stepName: "validate-payout",
        backendPath: "payment/validate-payout",
        backendEventIdSuffix: "validate-payout",
      },
      {
        stepName: "process-payout",
        backendPath: "payment/process-payout",
        backendEventIdSuffix: "process-payout",
      },
      {
        stepName: "notify-payout-status",
        backendPath: "payment/notify-payout-status",
        backendEventIdSuffix: "notify-payout-status",
      },
    ];
    expect(callbackStepsPolicy("payment/process-payout", payout)).toBe(true);
    expect(
      callbackStepsPolicy("payment/process-payout", payout.slice(0, 2)),
    ).toContain("canonical three-step");
    expect(
      callbackStepsPolicy("payment/process-payout", [
        ...payout.slice(0, 2),
        { ...payout[2]!, backendPath: "email/reset-password" },
      ]),
    ).toContain("canonical three-step");

    const craftedRecoverySuffix = [
      {
        stepName: "callback-recovery-1",
        backendPath: "payment/process-payout",
        backendEventId: "wf_original:process-payout",
      },
      {
        stepName: "callback-recovery-2",
        backendPath: "payment/notify-payout-status",
        backendEventId: "wf_original:notify-payout-status",
      },
    ];
    expect(
      callbackStepsPolicy("payment/process-payout", craftedRecoverySuffix),
    ).toContain("canonical three-step");
  });
});
