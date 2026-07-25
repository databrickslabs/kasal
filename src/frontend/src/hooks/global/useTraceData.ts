import { useState, useEffect, useCallback, useMemo } from 'react';
import { ProcessedTraces, RunConfig, RunConfigAgent, RunConfigTask } from '../../types/trace';
import {
  processTraceEvent,
  extractOutputForDisplay,
  extractExtraData,
} from '../../components/Jobs/traceEventProcessors';
import TraceService from '../../api/TraceService';
import { useRunStatusStore, Trace } from '../../store/runStatus';
import { formatDurationMs } from '../../utils/formatDuration';

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
  selectedEvent: {
    type: string;
    description: string;
    intrinsicMs?: number;
    output?: string | Record<string, unknown>;
    extraData?: Record<string, unknown>;
  } | null;
  setSelectedEvent: (event: {
    type: string;
    description: string;
    intrinsicMs?: number;
    output?: string | Record<string, unknown>;
    extraData?: Record<string, unknown>;
  } | null) => void;
  handleEventClick: (event: {
    type: string;
    description: string;
    intrinsicMs?: number;
    output?: string | Record<string, unknown>;
    extraData?: Record<string, unknown>;
  }) => void;
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
  handleTaskDescriptionClick: (taskName: string, taskId?: string, e?: React.MouseEvent) => void;
  formatDuration: (ms: number) => string;
  formatTimeDelta: (start: Date, timestamp: Date) => string;
  truncateTaskName: (name: string, maxLength?: number) => string;
}

// Helper function to extract task_id from trace
const getTaskId = (trace: Trace): string | null => {
  if (trace.task_id) return trace.task_id;
  if (trace.trace_metadata && typeof trace.trace_metadata === 'object') {
    const metadata = trace.trace_metadata as Record<string, unknown>;
    if (metadata.task_id) return metadata.task_id as string;
  }
  if (trace.extra_data && typeof trace.extra_data === 'object') {
    const extraData = trace.extra_data as Record<string, unknown>;
    if (extraData.task_id) return extraData.task_id as string;
  }
  return null;
};

/**
 * Process raw traces into hierarchical structure for display.
 *
 * Exported for unit testing (the agent/task grouping, including the task-less
 * light-agent path, is non-trivial enough to test in isolation).
 */
export function processTraces(rawTraces: Trace[]): ProcessedTraces {
  const filteredTraces = rawTraces.filter(trace =>
    trace.event_source !== 'Task Orchestrator' &&
    trace.event_context !== 'task_management'
  );

  const sorted = [...filteredTraces].sort((a, b) =>
    new Date(a.created_at).getTime() - new Date(b.created_at).getTime()
  );

  if (sorted.length === 0) {
    return { agents: [], globalEvents: { start: [], end: [] } };
  }

  const globalStart = new Date(sorted[0].created_at);
  const globalEnd = new Date(sorted[sorted.length - 1].created_at);
  const totalDuration = globalEnd.getTime() - globalStart.getTime();

  const globalEvents = {
    start: sorted.filter(t =>
      ((t.event_source === 'crew' && (t.event_type === 'crew_started' || t.event_type === 'execution_started')) ||
       (t.event_source === 'flow' && t.event_type === 'flow_started'))
    ),
    end: sorted.filter(t =>
      (t.event_source === 'crew' && (t.event_type === 'crew_completed' || t.event_type === 'execution_completed')) ||
      (t.event_source === 'flow' && t.event_type === 'flow_completed')
    )
  };

  // Group by agent
  const agentMap = new Map<string, Trace[]>();

  // OTel span hierarchy maps
  const spanIdToTaskId = new Map<string, string>();
  const spanIdToAgent = new Map<string, string>();
  const taskIdToName = new Map<string, string>();
  const taskIdToAgent = new Map<string, string>();

  // First pass: build span hierarchy and task name index
  sorted.forEach(trace => {
    const taskId = getTaskId(trace);

    if (trace.span_id && taskId) {
      spanIdToTaskId.set(trace.span_id, taskId);
    }
    if (trace.span_id && trace.event_source && trace.event_source !== 'Unknown Agent') {
      spanIdToAgent.set(trace.span_id, trace.event_source);
    }

    if (taskId && !taskIdToName.has(taskId)) {
      const meta = trace.trace_metadata && typeof trace.trace_metadata === 'object'
        ? trace.trace_metadata as Record<string, unknown>
        : null;
      const name = (meta?.task_name as string)
        || (trace.event_type === 'task_started' && trace.event_context ? trace.event_context : null);
      if (name) {
        taskIdToName.set(taskId, name.length > 80 ? name.substring(0, 77) + '...' : name);
      }
    }

    if (taskId && !taskIdToAgent.has(taskId)) {
      const agent = trace.event_source;
      if (agent && agent !== 'Unknown Agent' && agent !== 'task' && agent !== 'crew') {
        taskIdToAgent.set(taskId, agent);
      }
    }
  });

  // Second pass: group traces by agent
  sorted.forEach(trace => {
    const isErrorEvent = trace.event_type?.includes('failed') || trace.event_type?.includes('error');
    const src = trace.event_source?.toLowerCase();
    if (!isErrorEvent && (
        src === 'crew' ||
        src === 'flow' ||
        src === 'task' ||
        src === 'system' ||
        trace.event_source === 'Task Orchestrator' ||
        trace.event_context === 'task_management')) {
      return;
    }

    let agent = trace.event_source || 'Unknown Agent';

    if (trace.parent_span_id && spanIdToAgent.has(trace.parent_span_id)) {
      agent = spanIdToAgent.get(trace.parent_span_id)!;
    }
    if (trace.span_id && agent !== 'Unknown Agent') {
      spanIdToAgent.set(trace.span_id, agent);
    }

    const traceTaskId = getTaskId(trace);
    if (traceTaskId && taskIdToAgent.has(traceTaskId)) {
      agent = taskIdToAgent.get(traceTaskId)!;
    }

    if ((trace.event_type === 'llm_call' || isErrorEvent) && trace.extra_data && typeof trace.extra_data === 'object') {
      const extraData = trace.extra_data as Record<string, unknown>;
      const agentRole = extraData.agent_role as string;
      if (agentRole && agentRole !== 'UnknownAgent-str' && agentRole !== 'Unknown Agent') {
        agent = agentRole;
      }
    }
    if (isErrorEvent && (agent === 'crew' || agent === 'task' || agent === 'system' || agent === 'Unknown Agent')) {
      const meta = trace.trace_metadata && typeof trace.trace_metadata === 'object'
        ? trace.trace_metadata as Record<string, unknown>
        : null;
      const metaRole = meta?.agent_role as string;
      if (metaRole && metaRole !== 'Unknown Agent') {
        agent = metaRole;
      }
    }

    if (!agentMap.has(agent)) {
      agentMap.set(agent, []);
    }
    agentMap.get(agent)!.push(trace);
  });

  // Process each agent's traces
  const agents: ProcessedTraces['agents'] = [];

  // A run with NO task ids anywhere is the single light/chat agent path
  // (Agent.kickoff_async) — there is no crew task, so its traces must not be
  // framed as an "Unassigned" task in the timeline. Crew runs always carry task
  // ids, so a stray unattributed trace there still shows under "Unassigned".
  const runHasTaskIds = sorted.some(t => !!getTaskId(t));

  agentMap.forEach((agentTraces, agentName) => {
    if (agentTraces.length === 0) return;

    const agentStart = new Date(agentTraces[0].created_at);
    const agentEnd = new Date(agentTraces[agentTraces.length - 1].created_at);

    const taskMap = new Map<string, Trace[]>();
    const taskIdToUniqueKey = new Map<string, string>();
    let taskCounter = 0;

    agentTraces.forEach(trace => {
      const traceTaskId = getTaskId(trace)
        || (trace.parent_span_id ? spanIdToTaskId.get(trace.parent_span_id) : undefined)
        || undefined;

      let taskKey = 'Unassigned';
      if (traceTaskId) {
        if (taskIdToUniqueKey.has(traceTaskId)) {
          taskKey = taskIdToUniqueKey.get(traceTaskId)!;
        } else {
          const baseName = taskIdToName.get(traceTaskId)
            || (trace.event_context && trace.event_context !== trace.event_type
                ? (trace.event_context.length > 80 ? trace.event_context.substring(0, 77) + '...' : trace.event_context)
                : 'Task');
          taskKey = taskMap.has(baseName) ? `${baseName} (${++taskCounter})` : baseName;
          taskIdToUniqueKey.set(traceTaskId, taskKey);
          if (!taskIdToName.has(traceTaskId)) {
            taskIdToName.set(traceTaskId, baseName);
          }
        }
      }

      if (!taskMap.has(taskKey)) {
        taskMap.set(taskKey, []);
      }
      taskMap.get(taskKey)!.push(trace);
    });

    const tasks = Array.from(taskMap.entries()).map(([taskName, taskTraces]) => {
      // The light/chat agent produces only the synthetic "Unassigned" bucket (no
      // crew task). Relabel it to the user's request (the trace's event_context)
      // — falling back to the agent name — and flag it so the UI drops the crew
      // "task" framing. Left as "Unassigned" for crew runs (runHasTaskIds), where
      // a stray bucket really is an unattributed task.
      const isUnassignedRun = taskName === 'Unassigned' && !runHasTaskIds;
      let displayName = taskName;
      if (isUnassignedRun) {
        const ctx = taskTraces.find(
          t => t.event_context && t.event_context !== t.event_type
        )?.event_context;
        const trimmedCtx = ctx?.trim();
        displayName = trimmedCtx
          ? (trimmedCtx.length > 80 ? trimmedCtx.substring(0, 77) + '...' : trimmedCtx)
          : agentName;
      }
      const taskStart = new Date(taskTraces[0].created_at);
      const taskEnd = new Date(taskTraces[taskTraces.length - 1].created_at);

      // Exhaustive wall-time accounting: build the VISIBLE rows first, then
      // give each row the slice from its own timestamp to the NEXT visible
      // row's (raw traces filtered out by the processors donate their time to
      // the preceding visible row); the last row runs to the task end. By
      // construction the row durations sum to the task span — no time is
      // swallowed invisibly. Intrinsic op times (memory query/save, MCP call)
      // are carried separately as detail — never as the column value.
      const visibleEvents = taskTraces.map((trace) => {
        const timestamp = new Date(trace.created_at);
        const processed = processTraceEvent(trace);
        if (!processed) return null;

        return {
          type: processed.type,
          description: processed.description,
          timestamp,
          intrinsicMs: processed.durationMs,
          output: extractOutputForDisplay(trace.output),
          extraData: extractExtraData(trace)
        };
      }).filter((event): event is NonNullable<typeof event> => event !== null);

      const events = visibleEvents.map((event, idx) => ({
        ...event,
        duration: idx + 1 < visibleEvents.length
          ? visibleEvents[idx + 1].timestamp.getTime() - event.timestamp.getTime()
          : taskEnd.getTime() - event.timestamp.getTime(),
      }));

      return {
        taskName: displayName,
        taskId: getTaskId(taskTraces[0]) || undefined,
        startTime: taskStart,
        endTime: taskEnd,
        duration: taskEnd.getTime() - taskStart.getTime(),
        events,
        unassigned: isUnassignedRun
      };
    });

    agents.push({
      agent: agentName,
      startTime: agentStart,
      endTime: agentEnd,
      duration: agentEnd.getTime() - agentStart.getTime(),
      tasks
    });
  });

  // Extract run configuration from crew_started/execution_started trace metadata
  let runConfig: RunConfig | undefined;
  for (const trace of sorted) {
    if (trace.trace_metadata && typeof trace.trace_metadata === 'object') {
      const meta = trace.trace_metadata as Record<string, unknown>;
      if (meta.crew_agents && Array.isArray(meta.crew_agents)) {
        runConfig = {
          crew_key: meta.crew_key as string | undefined,
          crew_id: meta.crew_id as string | undefined,
          crew_agents: meta.crew_agents as RunConfigAgent[],
          crew_tasks: (meta.crew_tasks || []) as RunConfigTask[],
          crew_inputs: meta.crew_inputs as Record<string, unknown> | undefined,
        };
        break;
      }
    }
  }

  return {
    globalStart,
    globalEnd,
    totalDuration,
    agents,
    globalEvents,
    runConfig,
  };
}

const formatDuration = (ms: number): string => {
  // Offsets/chips deal in whole-ms timestamp deltas — show a literal zero
  // instead of the helper's "<1 ms" (which is meant for measured sub-ms times).
  if (ms <= 0) return '0 ms';
  return formatDurationMs(ms);
};

// Offset column: ONE format everywhere — seconds with one decimal ("+0.0s",
// "+7.0s", "+11.8s") so offsets line up and read as a single system.
const formatTimeDelta = (start: Date, timestamp: Date): string => {
  const deltaSec = Math.max(0, timestamp.getTime() - start.getTime()) / 1000;
  return `+${deltaSec.toFixed(1)}s`;
};

const truncateTaskName = (name: string, maxLength = 80): string => {
  if (name.length <= maxLength) return name;
  return name.substring(0, maxLength) + '...';
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
  const [selectedEvent, setSelectedEvent] = useState<{
    type: string;
    description: string;
    intrinsicMs?: number;
    output?: string | Record<string, unknown>;
    extraData?: Record<string, unknown>;
  } | null>(null);
  const [selectedTaskDescription, setSelectedTaskDescription] = useState<{
    taskName: string;
    taskId?: string;
    fullDescription?: string;
    isLoading: boolean;
  } | null>(null);

  const fetchTraceData = useCallback(async (isInitialLoad = true) => {
    if (!runId) return;

    try {
      if (isInitialLoad) {
        setLoading(true);
      }

      const runExists = await TraceService.checkRunExists(runId);
      if (!runExists) {
        setError(`Run ID ${runId} does not exist or is no longer available.`);
        setLoading(false);
        return;
      }

      // Try to get run details for the job_id, but don't block trace loading if it fails
      let traceId = runId;
      try {
        const runData = await TraceService.getRunDetails(runId);
        if (runData.job_id && runData.job_id.includes('-')) {
          traceId = runData.job_id;
        }
      } catch {
        // getRunDetails may fail with 404 due to session routing differences;
        // fall back to using runId directly for trace fetch
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
        setProcessedData(processed);

        if (isInitialLoad) {
          setExpandedAgents(new Set(processed.agents.map((_, idx) => idx)));
          const allTaskKeys = new Set<string>();
          processed.agents.forEach((agent, agentIdx) => {
            agent.tasks.forEach((_, taskIdx) => {
              allTaskKeys.add(`${agentIdx}-${taskIdx}`);
            });
          });
          setExpandedTasks(allTaskKeys);
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

  // Reprocess when store traces change
  useEffect(() => {
    if (!isActive) return;

    if (_traces && _traces.length > 0) {
      const processed = processTraces(_traces);
      setProcessedData(processed);
      setError(null);
      setLoading(false);

      setExpandedAgents(new Set(processed.agents.map((_, idx) => idx)));
      const allTaskKeys = new Set<string>();
      processed.agents.forEach((agent, agentIdx) => {
        agent.tasks.forEach((_, taskIdx) => {
          allTaskKeys.add(`${agentIdx}-${taskIdx}`);
        });
      });
      setExpandedTasks(allTaskKeys);
    } else {
      const processed = processTraces([]);
      setProcessedData(processed);
    }
  }, [_traces, isActive]);

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

  const handleEventClick = useCallback((event: {
    type: string;
    description: string;
    intrinsicMs?: number;
    output?: string | Record<string, unknown>;
    extraData?: Record<string, unknown>;
  }) => {
    setSelectedEvent(event);
  }, []);

  const handleTaskDescriptionClick = useCallback(async (taskName: string, taskId?: string, e?: React.MouseEvent) => {
    if (e) e.stopPropagation();

    setSelectedTaskDescription({
      taskName,
      taskId,
      fullDescription: undefined,
      isLoading: !!taskId
    });

    if (taskId) {
      try {
        const taskDetails = await TraceService.getTaskDetails(taskId);
        setSelectedTaskDescription(prev => prev ? {
          ...prev,
          fullDescription: taskDetails.description || taskName,
          isLoading: false
        } : null);
      } catch {
        setSelectedTaskDescription(prev => prev ? {
          ...prev,
          fullDescription: taskName,
          isLoading: false
        } : null);
      }
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
    formatDuration,
    formatTimeDelta,
    truncateTaskName,
  };
}
