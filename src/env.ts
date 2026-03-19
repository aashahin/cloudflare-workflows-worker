// ─── Environment Bindings ────────────────────────────────────────────────────
// Typed Cloudflare env bindings for the workflows worker.

export interface Env {
  // Workflow bindings (defined in wrangler.toml)
  EMAIL_WORKFLOW: Workflow;
  NOTIFICATION_WORKFLOW: Workflow;
  PAYMENT_WORKFLOW: Workflow;

  // Queue for failed event retries (replaces KV)
  FAILED_EVENTS_QUEUE: Queue;

  // Secrets (set via `wrangler secret put`)
  AUTH_TOKEN: string;
  BACKEND_URL: string;

  // Vars
  ENVIRONMENT: string;
}
