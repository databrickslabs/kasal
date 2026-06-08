/**
 * Whether to use SSE (EventSource) for live updates.
 *
 * SSE is a DEV-ONLY transport. On Databricks Apps — our default, recommended
 * deployment — the HTTP/2 proxy drops or refuses long-lived SSE streams
 * (ERR_HTTP2_PROTOCOL_ERROR / ERR_CONNECTION_REFUSED). So we never open SSE
 * connections there; live execution updates come entirely from REST polling
 * (useTracePolling). SSE is enabled only on localhost (local dev), where it
 * works and gives instant updates.
 *
 * Detection is by hostname so it's correct at runtime regardless of build mode
 * (and so unit tests, which run on jsdom's `localhost`, keep their SSE paths).
 */
export const SSE_ENABLED: boolean = (() => {
  if (typeof window === 'undefined' || !window.location) return false;
  const h = window.location.hostname;
  return h === 'localhost' || h === '127.0.0.1' || h === '0.0.0.0' || h === '';
})();
