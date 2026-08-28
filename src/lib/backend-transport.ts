import { assertBackendCallbackPath } from "@abshahin/workflows-sdk";
import type { Env } from "../env.js";

export function backendBinding(env: Env): Fetcher {
  if (!env.BACKEND || typeof env.BACKEND.fetch !== "function") {
    throw new Error("BACKEND service binding is required");
  }
  return env.BACKEND;
}

export function backendExecuteUrl(env: Env, path: string): string {
  assertBackendCallbackPath(path);
  if (typeof env.BACKEND_ORIGIN !== "string") {
    throw new Error("BACKEND_ORIGIN must be a valid HTTPS origin");
  }
  const rawOrigin = env.BACKEND_ORIGIN.trim();
  let origin: URL;
  try {
    origin = new URL(rawOrigin);
  } catch {
    throw new Error("BACKEND_ORIGIN must be a valid HTTPS origin");
  }
  if (
    origin.protocol !== "https:" ||
    origin.username !== "" ||
    origin.password !== "" ||
    origin.pathname !== "/" ||
    origin.search !== "" ||
    origin.hash !== ""
  ) {
    throw new Error("BACKEND_ORIGIN must be a bare HTTPS origin");
  }
  return new URL(`/workflows/execute/${path}`, origin).toString();
}

export async function fetchBackendExecute(
  env: Env,
  path: string,
  init: RequestInit,
): Promise<Response> {
  const binding = backendBinding(env);
  return binding.fetch(new Request(backendExecuteUrl(env, path), init));
}
