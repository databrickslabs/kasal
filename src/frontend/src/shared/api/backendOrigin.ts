/**
 * Where the local backend listens during development.
 *
 * `run.sh` starts the backend on `KASAL_PORT` (default 8000). The dev UI talks
 * to it directly (the Axios client and the SSE streams bypass the Vite proxy),
 * so it has to know that port too. `vite.config.ts` exposes it to the browser
 * as `VITE_KASAL_PORT`, filled from `VITE_KASAL_PORT` or `KASAL_PORT`, so
 * `KASAL_PORT=8001 npm start` and `KASAL_PORT=8001 ./run.sh` line up.
 *
 * Production never uses this: there the UI is served by the backend and every
 * call is relative (`/api/v1`).
 */

/** The backend port when none is configured; matches run.sh. */
export const DEFAULT_BACKEND_PORT = '8000';

interface BackendPortEnv {
  VITE_KASAL_PORT?: string;
}

/** The configured backend port, or the default when unset or not a port. */
export function devBackendPort(env: BackendPortEnv = import.meta.env): string {
  const raw = (env.VITE_KASAL_PORT ?? '').trim();
  const port = Number(raw);
  if (/^\d+$/.test(raw) && port > 0 && port <= 65535) return raw;
  return DEFAULT_BACKEND_PORT;
}

/** `http://localhost:<port>` for the local backend. */
export function devBackendOrigin(env?: BackendPortEnv): string {
  return `http://localhost:${devBackendPort(env)}`;
}
