/**
 * A chat run's activity, as the SAME model the Execution Trace Timeline renders.
 *
 * The chat used to build its own step list from the messages the SSE stream
 * dropped into the transcript, and narrate each one in prose. That put a second
 * derivation of "what happened in this run" next to the trace timeline's, and
 * the two disagreed: an event the chat's labeller did not recognise fell through
 * to its raw JSON frame, which is what the user saw in the activity panel.
 *
 * So there is one derivation now. Traces come from the trace API by job id — the
 * durable record, the same endpoint the timeline reads — and go through the same
 * `processTraces`. While a run is live we re-fetch on an interval, because the
 * rows are written as the run goes and a poll is what the pane already does for
 * status.
 */
import { useEffect, useRef, useState } from 'react';
import { getJobTraces } from '../api/executions';
import { processTraces } from '../../../hooks/global/useTraceData';
import type { ProcessedTraces, Trace } from '../../../types/execution/trace';

/** Matches the status poll's cadence: a run writes traces continuously, and a
 *  tighter loop would re-walk the whole array for a row or two. */
const LIVE_POLL_MS = 3000;

export interface UseRunTimelineResult {
  processed: ProcessedTraces | null;
  /** True only for the FIRST load of a job — a live re-poll must not blank the
   *  timeline that is already on screen. */
  loading: boolean;
}

export function useRunTimeline(
  jobId: string | undefined,
  live: boolean,
  /** Fetch only once the panel is actually open — the activity section is
   *  collapsed by default, and a transcript can hold many runs. */
  enabled = true,
): UseRunTimelineResult {
  const [processed, setProcessed] = useState<ProcessedTraces | null>(null);
  const [loading, setLoading] = useState(false);
  // Which job the state on screen belongs to, so switching runs clears it
  // rather than showing the previous run's timeline under the new heading.
  const shownJobRef = useRef<string | undefined>(undefined);

  useEffect(() => {
    if (!enabled || !jobId) {
      if (shownJobRef.current !== undefined) {
        shownJobRef.current = undefined;
        setProcessed(null);
      }
      return;
    }

    let cancelled = false;
    const firstLoad = shownJobRef.current !== jobId;
    if (firstLoad) {
      shownJobRef.current = jobId;
      setProcessed(null);
      setLoading(true);
    }

    const load = async () => {
      try {
        const traces = await getJobTraces(jobId);
        if (cancelled) return;
        setProcessed(processTraces(traces as unknown as Trace[]));
      } catch {
        // Best-effort: a failed fetch leaves whatever is already on screen.
        // The activity panel is a view of the run, never the run itself.
      } finally {
        if (!cancelled) setLoading(false);
      }
    };

    void load();
    if (!live) return () => { cancelled = true; };

    const timer = window.setInterval(load, LIVE_POLL_MS);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [jobId, live, enabled]);

  return { processed, loading };
}
