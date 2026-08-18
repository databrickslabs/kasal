/**
 * Which run's activity the preview pane is showing.
 *
 * A run is identified by its JOB ID and nothing else. The activity itself is
 * read from the trace API by that id (see {@link useRunTimeline}) — the same
 * record the Execution Trace Timeline renders — so this hook owns only the
 * question "which run, and which step of it, is the pane focused on?".
 *
 * It used to also derive a parallel step list from the chat's trace messages and
 * hand that to the pane. That was a second answer to "what happened in this
 * run", and it disagreed with the timeline's: events its labeller did not know
 * were rendered as their raw JSON frame. There is one answer now.
 */
import { useState, useCallback, useEffect, useRef, useMemo } from 'react';
import { useExecutionStore } from '../store/executionStore';
import { PreviewContent } from '../components/Preview/PreviewPanel';
import type { RunStep } from '../components/Preview/traceEventStep';
import { useSessionStore } from '../store/sessionStore';

interface UseRunActivityArgs {
  viewIsExecuting: boolean;
}

export function useRunActivity({ viewIsExecuting }: UseRunActivityArgs) {

  const messages = useSessionStore((s) => s.messages);
  const currentSessionId = useSessionStore((s) => s.currentSessionId);

  // When the user opens a SPECIFIC run in the pane via its "Show in panel" icon,
  // this is that run's job — shown in the pane instead of the latest run's, so a
  // historical run's pane shows ITS OWN activity. Cleared on close / session
  // switch / when a new live run starts (so the pane tracks the live run again).
  const [focusedRunJobId, setFocusedRunJobId] = useState<string | null>(null);

  // …and when the user clicks an individual step ROW in a run's expanded
  // timeline, the pane opens directly on THAT step's content (master→detail
  // pre-selected). Cleared together with focusedRunJobId.
  const [focusedRunStep, setFocusedRunStep] = useState<RunStep | null>(null);

  // Drop the focused-run pin when the viewed session changes (the pinned run
  // belongs to the other session).
  useEffect(() => {
    setFocusedRunJobId(null);
    setFocusedRunStep(null);
  }, [currentSessionId]);

  // …and when a NEW live run starts (rising edge), so the pane stops pinning a
  // past run and tracks the fresh one. A run finishing does NOT clear the pin.
  const prevExecutingRef = useRef(false);

  useEffect(() => {
    if (viewIsExecuting && !prevExecutingRef.current) {
      setFocusedRunJobId(null);
      setFocusedRunStep(null);
    }
    prevExecutingRef.current = viewIsExecuting;
  }, [viewIsExecuting]);

  // Open a specific run in the side preview pane: its deliverable (A2UI surface or
  // the plain-text answer) with that run's activity. The pane is opt-in — this is
  // the ONLY way it opens for a chat run, and it fires only on the user's click.
  const handleShowRunInPane = useCallback((
    deliverable: PreviewContent | undefined,
    jobId?: string,
    focusStep?: RunStep,
  ) => {
    const st = useExecutionStore.getState();
    setFocusedRunJobId(jobId ?? null);
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

  // The latest run's job id (a crew_actions / result message carries
  // `executionId`) — what the pane tracks when no specific run is pinned.
  const latestRunJobId = useMemo(() => {
    for (let i = messages.length - 1; i >= 0; i--) {
      const e = messages[i].executionId;
      if (e) return e;
    }
    return undefined;
  }, [messages]);

  return {
    handleShowRunInPane,
    latestRunJobId,
    focusedRunJobId,
    setFocusedRunJobId,
    focusedRunStep,
    setFocusedRunStep,
  };
}
