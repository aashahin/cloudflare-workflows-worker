import type { Env } from "../env.js";

export const BACKEND_BINDING_ORIGIN = "https://backend.internal";

export function backendBinding(env: Env): Fetcher | undefined {
  return env.BACKEND;
}

export function backendExecuteUrl(env: Env, path: string): string {
  return backendBinding(env)
    ? `${BACKEND_BINDING_ORIGIN}/workflows/execute/${path}`
    : `${env.BACKEND_URL}/workflows/execute/${path}`;
}

export async function fetchBackendExecute(
  env: Env,
  path: string,
  init: RequestInit,
): Promise<Response> {
  const url = backendExecuteUrl(env, path);
  const binding = backendBinding(env);
  return binding ? binding.fetch(new Request(url, init)) : fetch(url, init);
}
