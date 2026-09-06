import type { Trace } from '../types/execution/trace';

/** Coalesce store writes while allowing lifecycle window events to stay immediate. */
export function createTraceBatcher(
  write: (jobId: string, traces: Trace[]) => void,
  selectedGroup: () => string | null,
  intervalMs = 50,
) {
  const pending = new Map<string, { group: string | null; traces: Trace[] }>();
  let timer: ReturnType<typeof setTimeout> | undefined;

  const flush = () => {
    clearTimeout(timer);
    timer = undefined;
    const batches = [...pending];
    pending.clear();
    for (const [jobId, batch] of batches) {
      // Recheck at delivery: a workspace switch must not replay queued traces
      // from the previous selection into the newly selected workspace.
      if (batch.group === selectedGroup()) write(jobId, batch.traces);
    }
  };

  return {
    add(jobId: string, trace: Trace) {
      const group = selectedGroup();
      if (trace.group_id && trace.group_id !== group) return;
      let batch = pending.get(jobId);
      if (!batch || batch.group !== group) {
        batch = { group, traces: [] };
        pending.set(jobId, batch);
      }
      batch.traces.push(trace);
      if (timer === undefined) timer = setTimeout(flush, intervalMs);
    },
    flush,
  };
}
