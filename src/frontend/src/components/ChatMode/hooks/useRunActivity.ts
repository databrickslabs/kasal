/**
 * The run-activity timeline the chat shows beside a message.
 *
 * Steps come from two places: the live trace of the run in flight, and the
 * traces re-fetched for a finished run when the user scrolls back to it. This
 * resolves which of the two a given message should display, and owns the
 * focus state the preview pane reads.
 */
import { useState, useCallback, useEffect, useRef, useMemo } from 'react';
import { getJobTraces } from '../api/executions';
import { useExecutionStore } from '../store/executionStore';
import { PreviewContent } from '../components/Preview/PreviewPanel';
import type { RunStep } from '../components/Preview/RunTimeline';
import { tracesToRunSteps, deriveMessageActivitySteps, pickRunActivitySteps } from '../utils/traceActivity';
import { useSessionStore } from '../store/sessionStore';

interface UseRunActivityArgs {
  viewIsExecuting: boolean;
}

export function useRunActivity({ viewIsExecuting }: UseRunActivityArgs) {

  const messages = useSessionStore((s) => s.messages);
  const currentSessionId = useSessionStore((s) => s.currentSessionId);

  // When the user opens a SPECIFIC run in the pane via its "Show in panel" icon,
  // these are that run's steps — shown in the pane instead of the latest run's, so
  // a historical run's pane shows ITS OWN activity. Cleared on close / session
  // switch / when a new live run starts (so the pane tracks the live run again).
  const [focusedRunSteps, setFocusedRunSteps] = useState<RunStep[] | null>(null);

  // …and when the user clicks an individual step ROW in a run's expanded
  // timeline, the pane opens directly on THAT step's content (master→detail
  // pre-selected). Cleared together with focusedRunSteps.
  const [focusedRunStep, setFocusedRunStep] = useState<RunStep | null>(null);

  // Drop the focused-run pin when the viewed session changes (the pinned steps
  // belong to the other session's run).
  useEffect(() => {
    setFocusedRunSteps(null);
    setFocusedRunStep(null);
  }, [currentSessionId]);

  // …and when a NEW live run starts (rising edge), so the pane stops pinning a
  // past run and tracks the fresh one. A run finishing does NOT clear the pin.
  const prevExecutingRef = useRef(false);

  useEffect(() => {
    if (viewIsExecuting && !prevExecutingRef.current) {
      setFocusedRunSteps(null);
      setFocusedRunStep(null);
    }
    prevExecutingRef.current = viewIsExecuting;
  }, [viewIsExecuting]);

  // Open a specific run in the side preview pane: its deliverable (A2UI surface or
  // the plain-text answer) with that run's activity. The pane is opt-in — this is
  // the ONLY way it opens for a chat run, and it fires only on the user's click.
  const handleShowRunInPane = useCallback((deliverable: PreviewContent | undefined, steps: RunStep[], focusStep?: RunStep) => {
    const st = useExecutionStore.getState();
    setFocusedRunSteps(steps.length ? steps : null);
    // A step ROW click opens the pane directly on that step's content; the
    // per-run pane icon (no focusStep) opens the run normally.
    setFocusedRunStep(focusStep ?? null);
    st.setActivityPlacement('preview');
    if (deliverable) {
      st.openPreviewPane(deliverable);
    } else {
      // No previewable deliverable (a run that only has activity): show the
      // activity alone — clear any stale content so the skeleton, not a prior
      // run's deliverable, fills the pane.
      st.setPreviewContent(null);
      st.openPreviewPane();
    }
  }, []);

  // The run-activity timeline shown in the preview pane (live skeleton AND
  // collapsed above the finished result). Sourced from the PERSISTENT chat trace
  // messages — the latest run's steps — so it survives the run finishing (unlike
  // the ephemeral live feed). Each trace message's resultData carries the
  // label / query / context the step pulled in.
  const messageActivitySteps = useMemo(() => deriveMessageActivitySteps(messages), [messages]);

  // The latest run's job id (a crew_actions / result message carries `executionId`)
  // — used to restore the activity from the durable execution traces on refresh.
  const latestRunJobId = useMemo(() => {
    for (let i = messages.length - 1; i >= 0; i--) {
      const e = messages[i].executionId;
      if (e) return e;
    }
    return undefined;
  }, [messages]);

  // Run activity restored from the PERSISTED execution traces, keyed by job id —
  // the durable, complete source (a refresh can lose the per-message copy).
  const [restoredStepsByJob, setRestoredStepsByJob] = useState<Record<string, ReturnType<typeof tracesToRunSteps>>>({});

  const fetchedTraceJobsRef = useRef<Set<string>>(new Set());

  useEffect(() => {
    // Only restore for a FINISHED run we're viewing — a live run streams its own
    // steps into the messages; the traces are fetched once it has settled.
    if (!latestRunJobId || viewIsExecuting) return;
    if (fetchedTraceJobsRef.current.has(latestRunJobId)) return;
    fetchedTraceJobsRef.current.add(latestRunJobId);
    let cancelled = false;
    (async () => {
      try {
        const traces = await getJobTraces(latestRunJobId);
        if (cancelled) return;
        const steps = tracesToRunSteps(traces);
        if (steps.length) setRestoredStepsByJob((prev) => ({ ...prev, [latestRunJobId]: steps }));
      } catch {
        /* best-effort: fall back to the per-message steps */
      }
    })();
    return () => { cancelled = true; };
  }, [latestRunJobId, viewIsExecuting]);

  // Durable restored steps vs the per-message live source — see
  // pickRunActivitySteps (the swap may never shrink the visible list).
  const runActivitySteps = useMemo(
    () => pickRunActivitySteps(latestRunJobId ? restoredStepsByJob[latestRunJobId] : undefined, messageActivitySteps),
    [restoredStepsByJob, latestRunJobId, messageActivitySteps],
  );

  return {
    handleShowRunInPane,
    runActivitySteps,
    latestRunJobId,
    focusedRunSteps,
    setFocusedRunSteps,
    focusedRunStep,
    setFocusedRunStep,
  };
}
