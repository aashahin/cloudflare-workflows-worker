export type Env = Cloudflare.Env & {
  /** `services.BACKEND` in wrangler.jsonc. Optional until `wrangler types` is regenerated. */
  BACKEND?: Fetcher;
};
