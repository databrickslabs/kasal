/**
 * Whether to use SSE (EventSource) for live updates.
 *
 * DEFAULT: SSE is enabled everywhere, including Databricks Apps (our default,
 * recommended deployment). It used to default OFF off-localhost because the
 * HTTP/2 proxy was observed dropping long-lived SSE streams
 * (ERR_HTTP2_PROTOCOL_ERROR / ERR_CONNECTION_REFUSED). The backend now emits
 * keep-alive comment frames every SSE_HEARTBEAT_SECONDS, which keeps the proxy
 * from dropping idle streams, so the default flipped ON. If a specific proxy
 * still refuses SSE, force it off with the override below and we fall back to
 * REST polling (useTracePolling + the HITL pending-approval poll).
 *
 * OVERRIDES:
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

  // 3. Default: SSE ON everywhere. The backend now emits keep-alive comment
  //    frames every SSE_HEARTBEAT_SECONDS, which keeps the Databricks Apps
  //    HTTP/2 proxy from dropping idle long-lived streams — the reason this
  //    used to default OFF off-localhost. If a proxy still refuses SSE, force
  //    it off at runtime with localStorage['kasal.sse.enabled']='false' (no
  //    rebuild) and we fall back to REST polling.
  const h = window.location.hostname;
  resolvedReason = `default ON for hostname '${h || '(empty)'}' (backend SSE heartbeat keeps proxy streams alive)`;
  return true;
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
