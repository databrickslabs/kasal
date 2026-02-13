import { useState, useEffect, useCallback, useMemo } from 'react';
import { ProcessedTraces, TraceEvent } from '../../types/trace';
import {
  processTraceEvent,
  extractOutputForDisplay,
  extractExtraData,
} from '../../components/Jobs/traceEventProcessors';
import TraceService from '../../api/TraceService';
import { useRunStatusStore, Trace } from '../../store/runStatus';

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
    output?: string | Record<string, unknown>;
    extraData?: Record<string, unknown>;
  } | null;
  setSelectedEvent: (event: {
    type: string;
    description: string;
    output?: string | Record<string, unknown>;
    extraData?: Record<string, unknown>;
  } | null) => void;
  handleEventClick: (event: {
    type: string;
    description: string;
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
 */
function processTraces(rawTraces: Trace[]): ProcessedTraces {
  const filteredTraces = rawTraces.filter(trace =>
    trace.event_source !== 'Task Orchestrator' &&
    trace.event_context !== 'task_management'
  );

  const sorted = [...filteredTraces].sort((a, b) =>
    new Date(a.created_at).getTime() - new Date(b.created_at).getTime()
  );

  if (sorted.length === 0) {
    return { agents: [], globalEvents: { start: [], end: [] }, crewPlanningEvents: [] };
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

  // Crew-level planning events
  const crewPlannerTraces = sorted.filter(t =>
    t.event_source === 'Task Execution Planner'
  );

  const crewPlanningEvents: TraceEvent[] = crewPlannerTraces
    .filter(trace =>
      trace.event_type === 'llm_response' || trace.event_type === 'task_completed'
    )
    .map((trace, idx, arr) => {
      const timestamp = new Date(trace.created_at);
      const nextTrace = arr[idx + 1];
      const duration = nextTrace
        ? new Date(nextTrace.created_at).getTime() - timestamp.getTime()
        : undefined;

      let outputContent: string | Record<string, unknown> | undefined = trace.output;
      if (trace.output && typeof trace.output === 'object' && 'content' in trace.output) {
        const content = (trace.output as Record<string, unknown>).content;
        if (typeof content === 'string' || (typeof content === 'object' && content !== null)) {
          outputContent = content as string | Record<string, unknown>;
        }
      }

      let description = 'Crew Planning';
      if (trace.event_type === 'llm_response') {
        const outputLen = typeof outputContent === 'string'
          ? outputContent.length
          : JSON.stringify(outputContent || '').length;
        description = `Execution Plan (${outputLen.toLocaleString()} chars)`;
      } else if (trace.event_type === 'task_completed') {
        description = 'Planning Complete';
      }

      return {
        type: 'crew_planning',
        description,
        timestamp,
        duration,
        output: outputContent,
        extraData: trace.extra_data as Record<string, unknown> | undefined
      };
    });

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
        trace.event_source === 'Task Execution Planner' ||
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

  agentMap.forEach((agentTraces, agentName) => {
    if (agentTraces.length === 0) return;

    const agentStart = new Date(agentTraces[0].created_at);
    const agentEnd = new Date(agentTraces[agentTraces.length - 1].created_at);

    const taskMap = new Map<string, Trace[]>();
    const taskIdToUniqueKey = new Map<string, string>();
    let taskCounter = 0;
    const agentLevelTraces: Trace[] = [];

    agentTraces.forEach(trace => {
      if (trace.event_type === 'agent_reasoning' || trace.event_type === 'agent_reasoning_error') {
        let metadata: Record<string, unknown> | null = null;
        if (trace.trace_metadata) {
          if (typeof trace.trace_metadata === 'string') {
            try {
              metadata = JSON.parse(trace.trace_metadata);
            } catch {
              metadata = null;
            }
          } else if (typeof trace.trace_metadata === 'object') {
            metadata = trace.trace_metadata as Record<string, unknown>;
          }
        }
        let extraData: Record<string, unknown> | null = null;
        if (trace.output && typeof trace.output === 'object' && 'extra_data' in trace.output) {
          extraData = (trace.output as Record<string, unknown>).extra_data as Record<string, unknown>;
        }

        const operation = metadata?.operation || extraData?.operation;
        if (operation !== 'reasoning_started') {
          agentLevelTraces.push(trace);
        }
        return;
      }

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
      const taskStart = new Date(taskTraces[0].created_at);
      const taskEnd = new Date(taskTraces[taskTraces.length - 1].created_at);

      const events = taskTraces.map((trace, idx) => {
        const timestamp = new Date(trace.created_at);
        const nextTrace = taskTraces[idx + 1];
        const duration = nextTrace
          ? new Date(nextTrace.created_at).getTime() - timestamp.getTime()
          : undefined;

        const processed = processTraceEvent(trace);
        if (!processed) return null;

        return {
          type: processed.type,
          description: processed.description,
          timestamp,
          duration,
          output: extractOutputForDisplay(trace.output),
          extraData: extractExtraData(trace)
        };
      }).filter((event): event is NonNullable<typeof event> => event !== null);

      return {
        taskName,
        taskId: getTaskId(taskTraces[0]) || undefined,
        startTime: taskStart,
        endTime: taskEnd,
        duration: taskEnd.getTime() - taskStart.getTime(),
        events
      };
    });

    const agentEvents: TraceEvent[] = agentLevelTraces.map((trace, idx) => {
      const timestamp = new Date(trace.created_at);
      const nextTrace = agentLevelTraces[idx + 1];
      const duration = nextTrace
        ? new Date(nextTrace.created_at).getTime() - timestamp.getTime()
        : undefined;

      const processed = processTraceEvent(trace);
      return {
        type: processed?.type ?? 'agent_reasoning',
        description: processed?.description ?? 'Agent Reasoning',
        timestamp,
        duration,
        output: extractOutputForDisplay(trace.output),
        extraData: extractExtraData(trace)
      };
    });

    agents.push({
      agent: agentName,
      startTime: agentStart,
      endTime: agentEnd,
      duration: agentEnd.getTime() - agentStart.getTime(),
      agentEvents,
      tasks
    });
  });

  return {
    globalStart,
    globalEnd,
    totalDuration,
    agents,
    globalEvents,
    crewPlanningEvents
  };
}

const formatDuration = (ms: number): string => {
  if (ms < 1000) return `${ms}ms`;
  const seconds = ms / 1000;
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  const minutes = seconds / 60;
  return `${minutes.toFixed(1)}m`;
};

const formatTimeDelta = (start: Date, timestamp: Date): string => {
  const delta = timestamp.getTime() - start.getTime();
  return `+${formatDuration(delta)}`;
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

      const runData = await TraceService.getRunDetails(runId);
      const traceId = (runData.job_id && runData.job_id.includes('-'))
                      ? runData.job_id
                      : runId;

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
