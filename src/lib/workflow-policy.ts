import type { BackendCallbackStep } from "@abshahin/workflows-sdk";

/**
 * Closed top-level producer admission list. The backend exposes two additional
 * payout callback paths, but those are subordinate steps and must never create
 * standalone Workflow instances.
 */
export const SUPPORTED_WORKFLOW_CALLBACK_NAMES = [
  "email/reset-password",
  "email/new-account-credentials",
  "email/change-email-verification",
  "email/verification",
  "email/cart-recovery",
  "email/invitation",
  "email/enrollment-confirmation",
  "email/trial-reminder",
  "email/payment-receipt",
  "email/withdrawal-status",
  "email/failed-payment-alert",
  "email/refund-confirmation",
  "notification/create",
  "notification/create-for-customer",
  "notification/bulk-create",
  "payment/process-payout",
] as const;

const SUPPORTED_NAMES: ReadonlySet<string> = new Set(
  SUPPORTED_WORKFLOW_CALLBACK_NAMES,
);

export function workflowNamePolicy(name: string): true | string {
  return SUPPORTED_NAMES.has(name)
    ? true
    : `Unsupported Workflow callback name: ${name}`;
}

const PAYOUT_CALLBACK_PLAN: readonly BackendCallbackStep[] = [
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
] as const;

/**
 * The delayed D1 fallback must pass the same public admission policy as an
 * ordinary dispatch. Replaying the full payout plan is safe because every
 * backend step retains its original event ID and callback claims deduplicate
 * already-completed work.
 */
export function createControlPlaneRecoveryCallbackSteps(
  workflowName: string,
): BackendCallbackStep[] | undefined {
  return workflowName === "payment/process-payout"
    ? PAYOUT_CALLBACK_PLAN.map((step) => ({ ...step }))
    : undefined;
}

function sameStep(actual: BackendCallbackStep, expected: BackendCallbackStep): boolean {
  return (
    actual.stepName === expected.stepName &&
    actual.backendPath === expected.backendPath &&
    actual.backendEventId === undefined &&
    actual.backendEventIdSuffix === expected.backendEventIdSuffix
  );
}

/** One admitted event can execute only its canonical callback plan. */
export function callbackStepsPolicy(
  workflowName: string,
  steps: readonly BackendCallbackStep[],
): true | string {
  if (workflowName !== "payment/process-payout") {
    return steps.length === 0
      ? true
      : `Workflow ${workflowName} does not accept explicit callback steps`;
  }

  return steps.length === PAYOUT_CALLBACK_PLAN.length &&
    steps.every((step, index) => sameStep(step, PAYOUT_CALLBACK_PLAN[index]!))
    ? true
    : "Workflow payment/process-payout requires its canonical three-step callback plan";
}
