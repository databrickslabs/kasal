/**
 * Shared duration formatting for the execution trace timeline.
 *
 * Formats a millisecond value for human display:
 *   <1ms    → "<1 ms"
 *   <1000ms → "342 ms"  (integer)
 *   <60s    → "1.4s"    (one decimal)
 *   >=60s   → "2.5m"    (one decimal)
 */
export function formatDurationMs(ms: number): string {
  if (ms < 1) return '<1 ms';
  const rounded = Math.round(ms);
  if (rounded < 1000) return `${rounded} ms`;
  const seconds = ms / 1000;
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  return `${(seconds / 60).toFixed(1)}m`;
}
