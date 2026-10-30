/**
 * A flow/crew run that TIMED OUT while finalizing — the benign long-run case.
 *
 * The crews did their work (e.g. the UC Metric Views were generated and
 * deployed) and the timeout hit afterwards, during finalization. It is NOT the
 * crew logic failing, so it is presented as a WARNING rather than a hard
 * failure: an orange notice in the chat (see `useExecutionMonitoring`) and an
 * orange `warning` state on the canvas nodes (instead of the red `failed`
 * error icon).
 *
 * This is the single source of truth for that classification — the chat hook
 * and both execution stores (`taskExecutionStore`, `flowExecutionStore`) read
 * the same `jobFailed` event detail, so they must agree on what counts as a
 * late timeout.
 */
export function isLateTimeoutError(error: unknown): boolean {
  return typeof error === 'string' && /timed out/i.test(error);
}
