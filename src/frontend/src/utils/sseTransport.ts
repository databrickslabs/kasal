/**
 * Whether to use SSE (EventSource) for live updates.
 *
 * DEFAULT: SSE is enabled only on localhost (local dev). On Databricks Apps —
 * our default, recommended deployment — the HTTP/2 proxy was observed dropping
 * or refusing long-lived SSE streams (ERR_HTTP2_PROTOCOL_ERROR /
 * ERR_CONNECTION_REFUSED), so off-localhost we fall back to REST polling
 * (useTracePolling + the HITL pending-approval poll).
 *
 * OVERRIDES (for testing SSE on a real deploy — the backend now emits
 * keep-alive comment frames every SSE_HEARTBEAT_SECONDS, which may keep the
 * proxy from dropping idle streams):
 *
 *   1. Runtime, no rebuild needed (preferred): in the browser console run
 *          localStorage.setItem('kasal.sse.enabled', 'true')
 *      then reload the page. Use 'false' to force SSE OFF (even on
 *      localhost), and localStorage.removeItem('kasal.sse.enabled') to
 *      return to the default hostname-based behavior.
 *
 *   2. Build time: set VITE_FORCE_SSE=true when building the frontend.
 *      The localStorage key, when present, wins over the build flag.
 *
 * The flag is evaluated once at module load (startup); changing the
 * localStorage key requires a page reload to take effect — which is exactly
 * how you'd test it on a deployed app.
 *
 * Detection is by hostname so it's correct at runtime regardless of build mode
 * (and so unit tests, which run on jsdom's `localhost`, keep their SSE paths).
 */

/** localStorage key that force-enables ('true'/'1') or force-disables
 *  ('false'/'0') SSE at runtime. Absent → default hostname detection. */
export const SSE_OVERRIDE_STORAGE_KEY = 'kasal.sse.enabled';

/** Why the gate resolved the way it did — for the startup line below. */
let resolvedReason = 'no window (SSR/test)';

/** Resolve the SSE gate — exported for tests; call-sites use SSE_ENABLED. */
export function resolveSseEnabled(): boolean {
  if (typeof window === 'undefined' || !window.location) return false;

  // 1. Runtime override via localStorage (survives without a rebuild).
  try {
    const override = window.localStorage?.getItem(SSE_OVERRIDE_STORAGE_KEY);
    if (override === 'true' || override === '1') {
      resolvedReason = `forced ON by localStorage['${SSE_OVERRIDE_STORAGE_KEY}']`;
      return true;
    }
    if (override === 'false' || override === '0') {
      resolvedReason = `forced OFF by localStorage['${SSE_OVERRIDE_STORAGE_KEY}'] — remove the key and reload to restore the default`;
      return false;
    }
  } catch {
    // Storage can throw in privacy modes — fall through to defaults.
  }

  // 2. Build-time override.
  try {
    const forced = import.meta.env?.VITE_FORCE_SSE;
    if (forced === 'true' || forced === '1' || forced === true) {
      resolvedReason = 'forced ON by VITE_FORCE_SSE at build time';
      return true;
    }
  } catch {
    // import.meta.env absent in some non-Vite contexts — ignore.
  }

  // 3. Default: dev-only by hostname.
  const h = window.location.hostname;
  const local = h === 'localhost' || h === '127.0.0.1' || h === '0.0.0.0' || h === '';
  resolvedReason = local
    ? `hostname '${h || '(empty)'}' is local`
    : `hostname '${h}' is not local (Databricks Apps drops long-lived SSE)`;
  return local;
}

export const SSE_ENABLED: boolean = resolveSseEnabled();

/** The reason the gate resolved as it did. Exported for diagnostics/tests. */
export function sseTransportReason(): string {
  return resolvedReason;
}

// Which transport is live is otherwise invisible: polling and SSE produce the
// same chat, so a session silently running on the fallback looks identical to
// one on SSE until you go counting requests in a HAR. One line at startup makes
// "why is it polling?" answerable at a glance — and names the override, which is
// the usual cause of SSE being off on a machine where it should work.
if (typeof console !== 'undefined') {
  console.info(
    `[transport] live updates: ${SSE_ENABLED ? 'SSE' : 'REST polling'} (${resolvedReason})`,
  );
}
