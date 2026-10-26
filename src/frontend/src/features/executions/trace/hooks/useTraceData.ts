import { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import type { ProcessedTraces, SelectedTraceEvent, Trace } from '../../../../types/execution/trace';
import { extractOutputForDisplay, extractExtraData } from '../lib/traceEventProcessors';
import TraceService from '../../../../api/execution/TraceService';
import { useRunStatusStore } from '../../../../store/runStatus';
import { processTraces, formatTraceDuration, formatTraceOffset } from '../lib/processTraces';

interface UseTraceDataParams {
  runId: string;
  jobId?: string;
  runStatus?: string;
  isActive: boolean;
}

interface UseTraceDataReturn {
  processedTraces: ProcessedTraces | null;
  loading: boolean;
  error: string | null;
  viewMode: 'summary' | 'timeline';
  setViewMode: (mode: 'summary' | 'timeline') => void;
  expandedAgents: Set<number>;
  expandedTasks: Set<string>;
  toggleAgent: (index: number) => void;
  toggleTask: (taskKey: string) => void;
  selectedEvent: SelectedTraceEvent | null;
  setSelectedEvent: (event: SelectedTraceEvent | null) => void;
  handleEventClick: (event: SelectedTraceEvent) => void;
  selectedTaskDescription: {
    taskName: string;
    taskId?: string;
    fullDescription?: string;
    isLoading: boolean;
  } | null;
  setSelectedTaskDescription: (desc: {
    taskName: string;
    taskId?: string;
    fullDescription?: string;
    isLoading: boolean;
  } | null) => void;
  handleTaskDescriptionClick: (task: {
    taskName: string;
    taskId?: string;
    configTaskId?: string;
    fullDescription?: string;
  }, e?: React.MouseEvent) => void;
  formatDuration: (ms: number) => string;
  formatTimeDelta: (start: Date, timestamp: Date) => string;
  truncateTaskName: (name: string, maxLength?: number) => string;
}

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

const truncateTaskName = (name: string, maxLength = 80): string => {
  if (name.length <= maxLength) return name;
  return name.substring(0, maxLength) + '...';
};

// A live run streams trace events in bursts of tens per second, and each one
// used to trigger a full `processTraces` pass (several O(n) walks + a sort) over
// the ENTIRE accumulated array, plus a rebuild of the expanded-agents/tasks
// sets. As the array grew into the thousands the main thread could not keep up
// and the tab froze — reloading (which drops the in-memory array) was the only
// way out. We coalesce reprocessing to at most one pass per this interval.
const REPROCESS_THROTTLE_MS = 400;

/** All agent indices, for expand-all. */
const allAgentIndices = (processed: ProcessedTraces): number[] =>
  processed.agents.map((_, idx) => idx);

/** All `${agentIdx}-${taskIdx}` task keys, for expand-all. */
const allTaskKeys = (processed: ProcessedTraces): string[] => {
  const keys: string[] = [];
  processed.agents.forEach((agent, agentIdx) => {
    agent.tasks.forEach((_, taskIdx) => keys.push(`${agentIdx}-${taskIdx}`));
  });
  return keys;
};

export function useTraceData({
  runId,
  jobId,
  runStatus,
  isActive,
}: UseTraceDataParams): UseTraceDataReturn {
  const setTracesForJob = useRunStatusStore(state => state.setTracesForJob);

  // Subscribe to traces from Zustand store
  const storeTraces = useRunStatusStore(state =>
    jobId ? state.traces.get(jobId) : undefined
  );
  const _traces = useMemo(() => storeTraces ?? [], [storeTraces]);

  const [processedData, setProcessedData] = useState<ProcessedTraces | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const [expandedAgents, setExpandedAgents] = useState<Set<number>>(new Set());
  const [expandedTasks, setExpandedTasks] = useState<Set<string>>(new Set());
  const [viewMode, setViewMode] = useState<'summary' | 'timeline'>('summary');
  const [selectedEvent, setSelectedEvent] = useState<SelectedTraceEvent | null>(null);
  const [selectedTaskDescription, setSelectedTaskDescription] = useState<{
    taskName: string;
    taskId?: string;
    fullDescription?: string;
    isLoading: boolean;
  } | null>(null);

  // Throttle bookkeeping for the reprocess effect below.
  const lastReprocessRef = useRef<number>(0);
  const reprocessTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  // Whether we've done the one-time "expand everything" for this run. After it,
  // new agents/tasks are added to the expanded sets but existing entries are
  // left as the user set them — a live update must not silently re-open panels
  // the user collapsed, and must not rebuild the sets on every event.
  const didInitialExpandRef = useRef<boolean>(false);
  // Which agent indices / task keys we've ALREADY auto-expanded. Needed to tell
  // "genuinely new, auto-expand it" apart from "the user just collapsed it":
  // both look absent from expandedAgents/expandedTasks, so absence alone can't
  // decide, and re-adding a collapsed entry is exactly the silent re-open we're
  // avoiding.
  const seenAgentIdxRef = useRef<Set<number>>(new Set());
  const seenTaskKeyRef = useRef<Set<string>>(new Set());

  const lastProcessedRef = useRef<{ traces: Trace[]; data: ProcessedTraces } | null>(null);

  const fetchTraceData = useCallback(async (isInitialLoad = true) => {
    if (!runId) return;

    try {
      if (isInitialLoad) {
        setLoading(true);
      }

      // The trace endpoint performs the existence/authorization check itself.
      // Resolve numeric execution IDs only when a job ID was not supplied.
      let traceId = jobId || runId;
      if (!jobId && /^\d+$/.test(runId)) {
        try {
          const runData = await TraceService.getRunDetails(runId);
          if (runData.job_id) traceId = runData.job_id;
        } catch {
          // The numeric trace endpoint remains a valid fallback.
        }
      }

      const traces = await TraceService.getTraces(traceId);

      if (!traces || !Array.isArray(traces) || traces.length === 0) {
        const isRunning = runStatus && ['running', 'queued', 'pending'].includes(runStatus.toLowerCase());
        if (!isRunning) {
          setError('No trace data is available for this run.');
        } else {
          setError(null);
          setLoading(false);
        }
      } else {
        if (jobId) {
          setTracesForJob(jobId, traces);
        }
        const processed = processTraces(traces);
        lastProcessedRef.current = { traces, data: processed };
        setProcessedData(processed);

        if (isInitialLoad) {
          const agentIdxs = allAgentIndices(processed);
          const taskKeys = allTaskKeys(processed);
          setExpandedAgents(new Set(agentIdxs));
          setExpandedTasks(new Set(taskKeys));
          seenAgentIdxRef.current = new Set(agentIdxs);
          seenTaskKeyRef.current = new Set(taskKeys);
          // The reprocess effect below shares this latch, so it won't redo the
          // expand-all when the store update it just triggered flows back in.
          didInitialExpandRef.current = true;
        }
        setError(null);
      }
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : 'Unknown error';
      setError(`Failed to load traces: ${errorMessage}`);
    } finally {
      setLoading(false);
    }
  }, [runId, runStatus, jobId, setTracesForJob]);

  // Fetch on activation
  useEffect(() => {
    if (isActive) {
      fetchTraceData(true);
    }
  }, [isActive, fetchTraceData]);

  // Reprocess when store traces change — coalesced to at most one pass per
  // REPROCESS_THROTTLE_MS. A trailing run always fires, so the final state is
  // correct even if the last burst arrived inside the throttle window.
  useEffect(() => {
    if (!isActive) return;

    const runReprocess = () => {
      lastReprocessRef.current = Date.now();

      if (!_traces || _traces.length === 0) {
        setProcessedData(processTraces([]));
        return;
      }

      const processed = lastProcessedRef.current?.traces === _traces
        ? lastProcessedRef.current.data : processTraces(_traces);
      lastProcessedRef.current = { traces: _traces, data: processed };
      setProcessedData(processed);
      setError(null);
      setLoading(false);

      const agentIdxs = allAgentIndices(processed);
      const taskKeys = allTaskKeys(processed);

      if (!didInitialExpandRef.current) {
        // First data for this run: expand all, once.
        setExpandedAgents(new Set(agentIdxs));
        setExpandedTasks(new Set(taskKeys));
        seenAgentIdxRef.current = new Set(agentIdxs);
        seenTaskKeyRef.current = new Set(taskKeys);
        didInitialExpandRef.current = true;
      } else {
        // Subsequent updates: only auto-expand entries we have NEVER seen before
        // (genuinely new agents/tasks). An entry that is absent but already seen
        // was collapsed by the user — leave it alone.
        const newAgents = agentIdxs.filter(idx => !seenAgentIdxRef.current.has(idx));
        const newTasks = taskKeys.filter(key => !seenTaskKeyRef.current.has(key));
        if (newAgents.length > 0) {
          newAgents.forEach(idx => seenAgentIdxRef.current.add(idx));
          setExpandedAgents(prev => {
            const next = new Set(prev);
            newAgents.forEach(idx => next.add(idx));
            return next;
          });
        }
        if (newTasks.length > 0) {
          newTasks.forEach(key => seenTaskKeyRef.current.add(key));
          setExpandedTasks(prev => {
            const next = new Set(prev);
            newTasks.forEach(key => next.add(key));
            return next;
          });
        }
      }
    };

    const elapsed = Date.now() - lastReprocessRef.current;
    if (elapsed >= REPROCESS_THROTTLE_MS) {
      // Enough time since the last pass — run immediately (also the first call).
      if (reprocessTimerRef.current) {
        clearTimeout(reprocessTimerRef.current);
        reprocessTimerRef.current = null;
      }
      runReprocess();
    } else {
      // Inside the window — schedule/replace a trailing pass with the freshest
      // data so a burst collapses into a single reprocess.
      if (reprocessTimerRef.current) clearTimeout(reprocessTimerRef.current);
      reprocessTimerRef.current = setTimeout(() => {
        reprocessTimerRef.current = null;
        runReprocess();
      }, REPROCESS_THROTTLE_MS - elapsed);
    }

    return () => {
      if (reprocessTimerRef.current) {
        clearTimeout(reprocessTimerRef.current);
        reprocessTimerRef.current = null;
      }
    };
  }, [_traces, isActive]);

  // Reset the one-time-expand latch (and the seen-entry tracking) when the run
  // changes, so a new run gets its own expand-all rather than inheriting the
  // previous run's collapsed state.
  useEffect(() => {
    didInitialExpandRef.current = false;
    seenAgentIdxRef.current = new Set();
    seenTaskKeyRef.current = new Set();
  }, [jobId, runId]);

  const toggleAgent = useCallback((index: number) => {
    setExpandedAgents(prev => {
      const next = new Set(prev);
      if (next.has(index)) next.delete(index);
      else next.add(index);
      return next;
    });
  }, []);

  const toggleTask = useCallback((taskKey: string) => {
    setExpandedTasks(prev => {
      const next = new Set(prev);
      if (next.has(taskKey)) next.delete(taskKey);
      else next.add(taskKey);
      return next;
    });
  }, []);

  /**
   * Open an event's output dialog, then pull the stored row for it.
   *
   * The copy held in the browser is whatever the live transport delivered, and
   * for subprocess runs that is the pipe frame — `output` truncated to 500
   * chars, no prompt metadata. The database row is the authoritative one, and
   * it is fetched here, on click, rather than shipping every row's full output
   * to the browser up front.
   */
  const handleEventClick = useCallback(async (event: SelectedTraceEvent) => {
    const { traceId } = event;
    setSelectedEvent({ ...event, isLoadingOutput: !!traceId });

    if (!traceId) return;

    try {
      const stored = await TraceService.getTraceById(traceId);
      setSelectedEvent(prev => {
        // The user may have moved on to another row while this was in flight.
        if (!prev || prev.traceId !== traceId) return prev;
        return {
          ...prev,
          output: extractOutputForDisplay(stored.output) ?? prev.output,
          extraData: extractExtraData(stored) ?? prev.extraData,
          isLoadingOutput: false,
        };
      });
    } catch {
      // Keep whatever the live view had; it is abridged but not wrong.
      setSelectedEvent(prev =>
        prev && prev.traceId === traceId ? { ...prev, isLoadingOutput: false } : prev
      );
    }
  }, []);

  const handleTaskDescriptionClick = useCallback(async (task: {
    taskName: string;
    taskId?: string;
    configTaskId?: string;
    fullDescription?: string;
  }, e?: React.MouseEvent) => {
    if (e) e.stopPropagation();

    const { taskName, taskId, configTaskId } = task;
    // What the run actually saw. Already complete when the crew config was
    // captured; otherwise it is the per-event copy, which the backend caps.
    const runtimeText = task.fullDescription || taskName;
    const mayBeCapped = !task.fullDescription;

    // Only the tasks-table id is fetchable. The trace's own task id is the
    // engine's per-run uuid and always 404s — asking for it just logged errors.
    // Dynamic paths stamp non-uuid placeholders (e.g. "call"); those 404 too.
    const shouldFetch = mayBeCapped && !!configTaskId && UUID_RE.test(configTaskId);

    setSelectedTaskDescription({
      taskName,
      taskId,
      fullDescription: runtimeText,
      isLoading: shouldFetch
    });

    if (!shouldFetch) return;

    try {
      const taskDetails = await TraceService.getTaskDetails(configTaskId as string);
      const stored = taskDetails.description || '';
      setSelectedTaskDescription(prev => prev ? {
        ...prev,
        // The stored description is the pre-interpolation template, so it only
        // wins when it carries text the capped runtime copy lost.
        fullDescription: stored.length > runtimeText.length ? stored : runtimeText,
        isLoading: false
      } : null);
    } catch {
      setSelectedTaskDescription(prev => prev ? { ...prev, isLoading: false } : null);
    }
  }, []);

  return {
    processedTraces: processedData,
    loading,
    error,
    viewMode,
    setViewMode,
    expandedAgents,
    expandedTasks,
    toggleAgent,
    toggleTask,
    selectedEvent,
    setSelectedEvent,
    handleEventClick,
    selectedTaskDescription,
    setSelectedTaskDescription,
    handleTaskDescriptionClick,
    formatDuration: formatTraceDuration,
    formatTimeDelta: formatTraceOffset,
    truncateTaskName,
  };
}
