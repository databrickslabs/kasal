import React, { useState, useEffect, useCallback } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  IconButton,
  Typography,
  Box,
  Paper,
  CircularProgress,
  Theme,
  Collapse,
  Chip,
  Button,
  Tooltip,
  Divider,
} from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import ExpandLessIcon from '@mui/icons-material/ExpandLess';
import AccessTimeIcon from '@mui/icons-material/AccessTime';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import PlayCircleIcon from '@mui/icons-material/PlayCircle';
import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import PreviewIcon from '@mui/icons-material/Preview';
import TimelineIcon from '@mui/icons-material/Timeline';
import TerminalIcon from '@mui/icons-material/Terminal';
import RefreshIcon from '@mui/icons-material/Refresh';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import { ShowTraceProps, Trace } from '../../types/trace';

import apiClient from '../../config/api/ApiConfig';
import TraceService from '../../api/TraceService';
import { useUserPreferencesStore } from '../../store/userPreferencesStore';
import { useTranslation } from 'react-i18next';
import ShowLogs from './ShowLogs';
import { executionLogService, LogEntry } from '../../api/ExecutionLogs';
import { useRunResult } from '../../hooks/global/useExecutionResult';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

interface GroupedTrace {
  agent: string;
  startTime: Date;
  endTime: Date;
  duration: number;
  tasks: {
    taskName: string;
    taskId?: string;
    startTime: Date;
    endTime: Date;
    duration: number;
    events: Array<{
      type: string;
      description: string;
      timestamp: Date;
      duration?: number;
      output?: string | Record<string, unknown>;
    }>;
  }[];
}

interface ProcessedTraces {
  globalStart?: Date;
  globalEnd?: Date;
  totalDuration?: number;
  agents: GroupedTrace[];
  globalEvents: {
    start: Trace[];
    end: Trace[];
  };
}

const ShowTraceTimeline: React.FC<ShowTraceProps> = ({
  open,
  onClose,
  runId,
  run,
  onViewResult,
  onShowLogs,

}) => {
  const { t } = useTranslation();
  const { useNewExecutionUI } = useUserPreferencesStore();
  const { showRunResult } = useRunResult();
  const [_traces, setTraces] = useState<Trace[]>([]);
  const [processedTraces, setProcessedTraces] = useState<ProcessedTraces | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const [expandedAgents, setExpandedAgents] = useState<Set<number>>(new Set());
  const [expandedTasks, setExpandedTasks] = useState<Set<string>>(new Set());
  const [selectedEvent, setSelectedEvent] = useState<{
    type: string;
    description: string;
    output?: string | Record<string, unknown>;
  } | null>(null);
  const [showLogsDialog, setShowLogsDialog] = useState(false);
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [isLoadingLogs, setIsLoadingLogs] = useState(false);
  const [logsError, setLogsError] = useState<string | null>(null);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [evaluationEnabled, setEvaluationEnabled] = useState<boolean>(false);
  const [isEvaluationRunning, setIsEvaluationRunning] = useState(false);

  // Handle opening logs dialog
  const handleOpenLogs = async () => {
    if (!run) return;

    setShowLogsDialog(true);
    setIsLoadingLogs(true);
    setLogsError(null);

    try {
      const fetchedLogs = await executionLogService.getHistoricalLogs(run.job_id);
      // Convert LogMessage to LogEntry format
      const logEntries: LogEntry[] = fetchedLogs.map(log => ({
        id: log.id,
        output: log.output,
        content: log.content,
        timestamp: log.timestamp,
        logType: log.type
      }));
      setLogs(logEntries);
    } catch (error) {
      console.error('Error fetching logs:', error);
      setLogsError('Failed to fetch logs');
    } finally {
      setIsLoadingLogs(false);
    }
  };

  // Compute MLflow Traces deep link for this workspace (experiment + workspace id)
  const getMlflowTracesUrl = async (): Promise<string> => {
    try {
      // 1) Resolve workspace URL (avoid hitting /databricks/config to prevent schema mismatch issues)
      let workspaceUrl = '';

      // 2) Get environment to obtain workspace_id (for the 'o' query param)
      let workspaceId: string | undefined;
      try {
        const envResp = await apiClient.get('/databricks/environment');
        workspaceId = envResp?.data?.workspace_id || undefined;
        // If workspaceUrl still empty, try env host
        const envHost = envResp?.data?.databricks_host;
        if (!workspaceUrl && envHost) {
          workspaceUrl = envHost.startsWith('http') ? envHost : `https://${envHost}`;
        }
      } catch (e) {
        console.warn('Unable to get Databricks environment info');
      }

      // 3) If not configured, try to infer from current host when running in Databricks
      if (!workspaceUrl || workspaceUrl === 'compute' || !workspaceUrl.includes('.')) {
        if (typeof window !== 'undefined' && window.location.hostname.includes('databricks')) {
          workspaceUrl = `https://${window.location.hostname}`;
        } else {
          // 4) Fallback to backend helper if available
          try {
            const resp = await apiClient.get('/databricks/workspace-url');
            workspaceUrl = resp.data?.workspace_url || '';
          } catch (e) {
            console.warn('Unable to determine Databricks workspace URL');
          }
        }
      }

      if (!workspaceUrl) return '#';

      // Normalize
      if (workspaceUrl.endsWith('/')) workspaceUrl = workspaceUrl.slice(0, -1);
      if (!workspaceUrl.startsWith('http')) workspaceUrl = `https://${workspaceUrl}`;

      // 5) Fetch experiment id for crew traces
      let experimentId: string | undefined;
      try {
        const expResp = await apiClient.get('/mlflow/experiment-info');
        experimentId = expResp?.data?.experiment_id || undefined;
      } catch (e) {
        console.warn('Unable to resolve MLflow experiment id');
      }

      if (experimentId) {
        // Build deep link to Traces tab. 'o' param (workspace id) improves routing if available.
        const o = workspaceId ? `?o=${encodeURIComponent(workspaceId)}` : '';
        return `${workspaceUrl}/ml/experiments/${encodeURIComponent(experimentId)}/traces${o}`;
      }

      // Fallback to experiments root if we cannot resolve experiment id
      return `${workspaceUrl}/mlflow/experiments`;
    } catch (err) {
      console.error('Failed to build MLflow traces URL:', err);
      return '#';
    }
  };

  // Get specific trace URL with trace ID if available
  const getSpecificTraceUrl = async (): Promise<string> => {
    if (!run) return '#';

    // Prefer backend-built deep link (handles workspace url/id and selectedEvaluationId)
    try {
      const resp = await apiClient.get('/mlflow/trace-link', { params: { job_id: run.job_id } });
      const url = resp?.data?.url as string | undefined;
      if (url) return url;
    } catch (e) {
      // ignore and fallback to frontend construction below
    }

    try {
      // 1) Get workspace URL and ID
      let workspaceUrl = '';
      let workspaceId: string | undefined;
      try {
        const envResp = await apiClient.get('/databricks/environment');
        workspaceId = envResp?.data?.workspace_id || undefined;
        const envHost = envResp?.data?.databricks_host;
        if (!workspaceUrl && envHost) {
          workspaceUrl = envHost.startsWith('http') ? envHost : `https://${envHost}`;
        }
      } catch (e) {
        console.warn('Unable to get Databricks environment info');
      }

      if (!workspaceUrl || workspaceUrl === 'compute' || !workspaceUrl.includes('.')) {
        if (typeof window !== 'undefined' && window.location.hostname.includes('databricks')) {
          workspaceUrl = `https://${window.location.hostname}`;
        } else {
          try {
            const resp = await apiClient.get('/databricks/workspace-url');
            workspaceUrl = resp.data?.workspace_url || '';
          } catch (e) {
            console.warn('Unable to determine Databricks workspace URL');
          }
        }
      }

      if (!workspaceUrl) return '#';

      // Normalize
      if (workspaceUrl.endsWith('/')) workspaceUrl = workspaceUrl.slice(0, -1);
      if (!workspaceUrl.startsWith('http')) workspaceUrl = `https://${workspaceUrl}`;

      // 2) Get experiment ID
      let experimentId: string | undefined;
      try {
        const expResp = await apiClient.get('/mlflow/experiment-info');
        experimentId = expResp?.data?.experiment_id || undefined;
      } catch (e) {
        console.warn('Unable to resolve MLflow experiment id');
      }

      if (!experimentId) {
        // Fallback to general traces URL
        return getMlflowTracesUrl();
      }

      // 3) Check if the execution has a stored MLflow trace ID
      let traceId: string | undefined;

      // Check if there's a mlflow_trace_id field directly on the run
      if (run.mlflow_trace_id) {
        traceId = run.mlflow_trace_id;
      }

      if (traceId) {
        // Build URL with specific trace ID
        const o = workspaceId ? `?o=${encodeURIComponent(workspaceId)}` : '';
        const selectedParam = `&selectedEvaluationId=${encodeURIComponent(traceId)}`;
        const url = `${workspaceUrl}/ml/experiments/${encodeURIComponent(experimentId)}/traces${o}${selectedParam}`;
        return url;
      }

      // Fallback to general traces URL if no trace ID
      return getMlflowTracesUrl();
    } catch (err) {
      console.error('Failed to build specific trace URL:', err);
      return getMlflowTracesUrl();
    }
  };

  // Process traces into hierarchical structure
  const processTraces = useCallback((rawTraces: Trace[]): ProcessedTraces => {
    // Filter out Task Orchestrator events before processing
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

    // Separate global events - support both crew and flow events
    const globalEvents = {
      start: sorted.filter(t =>
        ((t.event_source === 'crew' && (t.event_type === 'crew_started' || t.event_type === 'execution_started')) ||
         (t.event_source === 'flow' && t.event_type === 'flow_started'))
      ),
      end: sorted.filter(t =>
        ((t.event_source === 'crew' && (t.event_type === 'crew_completed' || t.event_type === 'execution_completed')) ||
         (t.event_source === 'flow' && t.event_type === 'flow_completed'))
      )
    };

    // Group by agent
    const agentMap = new Map<string, Trace[]>();
    const taskDescriptions = new Map<string, string>(); // Map to store task descriptions by context
    const agentTaskMap = new Map<string, string>(); // Map tasks to agents
    let currentTaskContext: string | null = null;

    // First pass: collect task descriptions and agent associations
    sorted.forEach(trace => {
      // Track task completions and descriptions
      if (trace.event_source === 'task' && trace.event_type === 'task_completed' && trace.event_context) {
        taskDescriptions.set(trace.event_context, trace.event_context);
        currentTaskContext = trace.event_context;
      }

      // Extract agent info from traces
      if (trace.event_source && trace.event_source !== 'crew' && trace.event_source !== 'task' && trace.event_source !== 'Unknown Agent') {
        // Map current task context to this agent
        if (currentTaskContext) {
          agentTaskMap.set(currentTaskContext, trace.event_source);
        }
      }

      // For agent_step events, extract agent name from extra_data
      if (trace.event_type === 'agent_execution' && trace.extra_data && typeof trace.extra_data === 'object') {
        const extraData = trace.extra_data as Record<string, unknown>;
        const agentRole = extraData.agent_role as string;
        if (agentRole && agentRole !== 'UnknownAgent-str' && agentRole !== 'Unknown Agent') {
          if (currentTaskContext) {
            agentTaskMap.set(currentTaskContext, agentRole);
          }
        }
      }
    });

    // Second pass: group traces by agent
    sorted.forEach(trace => {
      // Skip global events (crew/flow), task orchestration events, and task events for agent grouping
      if (trace.event_source === 'crew' ||
          trace.event_source === 'flow' ||
          trace.event_source === 'task' ||
          trace.event_source === 'Task Orchestrator' ||
          trace.event_context === 'task_management') {
        return;
      }

      let agent = trace.event_source || 'Unknown Agent';

      // For LLM calls, extract agent from extra_data if available
      if (trace.event_type === 'llm_call' && trace.extra_data && typeof trace.extra_data === 'object') {
        const extraData = trace.extra_data as Record<string, unknown>;
        const agentRole = extraData.agent_role as string;
        if (agentRole && agentRole !== 'UnknownAgent-str' && agentRole !== 'Unknown Agent') {
          agent = agentRole;
        }
      }

      // If still unknown, try to infer from current task context
      if ((agent === 'Unknown Agent' || agent.startsWith('Unknown')) && currentTaskContext) {
        const mappedAgent = agentTaskMap.get(currentTaskContext);
        if (mappedAgent) {
          agent = mappedAgent;
        }
      }

      if (!agentMap.has(agent)) {
        agentMap.set(agent, []);
      }
      const agentTraces = agentMap.get(agent);
      if (agentTraces) {
        agentTraces.push(trace);
      }
    });

    // Process each agent's traces
    const agents: GroupedTrace[] = [];

    agentMap.forEach((agentTraces, agentName) => {
      if (agentTraces.length === 0) return;

      const agentStart = new Date(agentTraces[0].created_at);
      const agentEnd = new Date(agentTraces[agentTraces.length - 1].created_at);

      // Group agent traces by task - using task_started events to determine task boundaries
      const taskMap = new Map<string, Trace[]>();
      let currentTask: string | null = null;
      let taskCounter = 0;

      agentTraces.forEach(trace => {
        // Check if this is a task_started event - if so, start a new task section
        if (trace.event_type === 'task_started') {
          // Extract task name from the task_started event
          let newTaskName: string | null = null;

          // First try trace_metadata (where task_name is typically stored)
          if (trace.trace_metadata) {
            const metadata = trace.trace_metadata as Record<string, unknown>;
            const taskName = metadata.task_name as string;
            if (taskName) {
              newTaskName = taskName;
            }
          }

          // Fallback to extra_data if trace_metadata doesn't have task_name
          if (!newTaskName && trace.extra_data) {
            const extraData = trace.extra_data as Record<string, unknown>;
            const taskName = extraData.task_name as string;
            if (taskName) {
              newTaskName = taskName;
            }
          }

          // If we found a task name, use it as the new current task
          if (newTaskName) {
            // Make task name unique if it already exists
            let uniqueTaskName = newTaskName;
            if (taskMap.has(newTaskName)) {
              taskCounter++;
              uniqueTaskName = `${newTaskName} (${taskCounter})`;
            }
            currentTask = uniqueTaskName;
          }
        }

        // If we still don't have a current task, try to find it from task descriptions
        if (!currentTask) {
          const traceTime = new Date(trace.created_at).getTime();

          // Find matching task by checking task completions
          const taskEntries = Array.from(taskDescriptions.entries());
          for (const [taskContext, taskDesc] of taskEntries) {
            // Find task completion trace
            const taskCompletion = sorted.find(t =>
              t.event_source === 'task' &&
              t.event_type === 'task_completed' &&
              t.event_context === taskContext
            );

            if (taskCompletion) {
              const taskEndTime = new Date(taskCompletion.created_at).getTime();
              // If trace is before task completion and agent matches, it belongs to this task
              if (traceTime <= taskEndTime + 1000) { // Within 1 second after task completion
                const taskAgent = agentTaskMap.get(taskContext);
                if (!taskAgent || taskAgent === agentName) {
                  currentTask = taskDesc;
                  break;
                }
              }
            }
          }
        }

        // Fallback to a generic task name if no match found
        if (!currentTask) {
          currentTask = 'Processing Task';
        }

        if (!taskMap.has(currentTask)) {
          taskMap.set(currentTask, []);
        }
        const taskTraces = taskMap.get(currentTask);
        if (taskTraces) {
          taskTraces.push(trace);
        }
      });

      // Process tasks
      const tasks = Array.from(taskMap.entries()).map(([taskName, taskTraces]) => {
        const taskStart = new Date(taskTraces[0].created_at);
        const taskEnd = new Date(taskTraces[taskTraces.length - 1].created_at);

        // Process events within task
        const events = taskTraces.map((trace, idx) => {
          const timestamp = new Date(trace.created_at);
          const nextTrace = taskTraces[idx + 1];
          const duration = nextTrace
            ? new Date(nextTrace.created_at).getTime() - timestamp.getTime()
            : undefined;

          // Determine event type and description
          let eventType = 'info';
          let description = '';

          if (trace.event_type === 'llm_call') {
            // LLM call event - extract agent name and model from extra_data
            eventType = 'llm';
            let agentName = '';
            let modelName = '';

            if (trace.extra_data && typeof trace.extra_data === 'object') {
              const extraData = trace.extra_data as Record<string, unknown>;
              agentName = (extraData.agent_role as string) || '';
              modelName = (extraData.model as string) || '';
            }

            if (agentName && agentName !== 'Unknown Agent') {
              description = `LLM call (${agentName})`;
            } else {
              description = 'LLM call';
            }

            if (modelName) {
              // Extract just the model name (e.g., "deepseek-chat" from "deepseek/deepseek-chat")
              const modelParts = modelName.split('/');
              const shortModelName = modelParts[modelParts.length - 1];
              description += ` - ${shortModelName}`;
            }
          } else if (trace.event_type === 'tool_usage') {
            // Tool usage event - tool name is now in event_context
            eventType = 'tool';
            const toolName = trace.event_context || 'Tool';
            description = toolName;
          } else if (trace.event_type === 'agent_execution' || trace.event_type === 'agent_step') {
            // Extract the actual content from the output JSON structure
            let outputStr = '';
            if (trace.output) {
              if (typeof trace.output === 'string') {
                outputStr = trace.output;
              } else if (typeof trace.output === 'object' && 'content' in trace.output) {
                outputStr = String((trace.output as Record<string, unknown>).content || '');
              }
            }
            const output = outputStr;

            // Check extra_data for step type information
            let stepType = '';
            if (trace.extra_data && typeof trace.extra_data === 'object') {
              const extraData = trace.extra_data as Record<string, unknown>;
              stepType = (extraData.step_type as string) || '';
            }

            if (output.includes('Tool:')) {
              // Tool usage
              const toolMatch = output.match(/Tool: ([^|]+)/);
              eventType = 'tool';
              description = toolMatch ? toolMatch[1].trim() : 'Tool Usage';
            } else if (output.includes('ToolResult')) {
              // Tool result
              eventType = 'tool_result';
              description = 'Tool Result';
            } else if (output.toLowerCase().includes('llm')) {
              eventType = 'llm';
              description = 'LLM call';
            } else if (stepType === 'AgentFinish') {
              eventType = 'agent_complete';
              description = 'Final Answer';
            } else if (stepType === 'AgentStart') {
              eventType = 'agent_start';
              description = 'Task Started';
            } else if (output && output.length > 100) {
              // Long output usually means the agent is providing results
              eventType = 'agent_output';
              description = 'Task Output';
            } else {
              eventType = 'agent_processing';
              description = 'Processing';
            }
          } else if (trace.event_type === 'task_started') {
            eventType = 'task_start';
            // Extract task name from trace_metadata first, then extra_data
            let taskName = 'Task Started';
            if (trace.trace_metadata && typeof trace.trace_metadata === 'object') {
              const metadata = trace.trace_metadata as Record<string, unknown>;
              const name = metadata.task_name as string;
              if (name) {
                // Truncate long task names for display
                taskName = name.length > 50 ? name.substring(0, 47) + '...' : name;
              }
            } else if (trace.extra_data && typeof trace.extra_data === 'object') {
              const extraData = trace.extra_data as Record<string, unknown>;
              const name = extraData.task_name as string;
              if (name) {
                // Truncate long task names for display
                taskName = name.length > 50 ? name.substring(0, 47) + '...' : name;
              }
            }
            description = `Starting: ${taskName}`;
          } else if (trace.event_type === 'task_completed') {
            eventType = 'task_complete';
            // Extract task name from trace_metadata first, then extra_data
            let taskName = 'Task Completed';
            if (trace.trace_metadata && typeof trace.trace_metadata === 'object') {
              const metadata = trace.trace_metadata as Record<string, unknown>;
              const name = metadata.task_name as string;
              if (name) {
                // Truncate long task names for display
                taskName = name.length > 50 ? name.substring(0, 47) + '...' : name;
              }
            } else if (trace.extra_data && typeof trace.extra_data === 'object') {
              const extraData = trace.extra_data as Record<string, unknown>;
              const name = extraData.task_name as string;
              if (name) {
                // Truncate long task names for display
                taskName = name.length > 50 ? name.substring(0, 47) + '...' : name;
              }
            }
            description = `Completed: ${taskName}`;
          } else if (trace.event_type === 'memory_operation') {
            eventType = 'memory_operation';
            // Extract operation type from context or output
            if (trace.event_context) {
              if (trace.event_context.includes('query')) {
                description = 'Memory Query';
              } else if (trace.event_context.includes('sav')) {
                description = 'Memory Save';
              } else {
                description = 'Memory Operation';
              }
            } else {
              description = 'Memory Operation';
            }
          } else if (trace.event_type === 'knowledge_operation') {
            eventType = 'knowledge_operation';
            description = 'Knowledge Operation';
          } else if (trace.event_type === 'agent_reasoning' || trace.event_type === 'agent_reasoning_error') {
            eventType = 'reasoning';
            // Extract reasoning details
            if (trace.event_type === 'agent_reasoning_error') {
              description = 'Reasoning Failed';
            } else {
              description = 'Agent Reasoning';
              // Check if there's a plan in the output
              if (trace.output && typeof trace.output === 'string' && trace.output.includes('plan')) {
                description = 'Agent Planning';
              }
            }
          } else if (trace.event_type === 'llm_guardrail') {
            eventType = 'guardrail';
            description = 'LLM Guardrail Check';
            // Check if it passed or failed from extra_data
            if (trace.extra_data && typeof trace.extra_data === 'object') {
              const extraData = trace.extra_data as Record<string, unknown>;
              if (extraData.success === true) {
                description = 'Guardrail Passed';
              } else if (extraData.success === false) {
                description = 'Guardrail Failed';
              }
            }
          } else {
            eventType = trace.event_type;
            // Make the description more readable
            const readableDesc = trace.event_type
              .replace(/_/g, ' ')
              .replace(/\b\w/g, (l) => l.toUpperCase());
            description = readableDesc;
          }

          // Extract the actual content from the output JSON structure
          let outputContent: string | Record<string, unknown> | undefined = trace.output;
          if (trace.output && typeof trace.output === 'object' && 'content' in trace.output) {
            const content = (trace.output as Record<string, unknown>).content;
            // Ensure content is of the right type
            if (typeof content === 'string' || (typeof content === 'object' && content !== null)) {
              outputContent = content as string | Record<string, unknown>;
            }
          }

          return {
            type: eventType,
            description,
            timestamp,
            duration,
            output: outputContent
          };
        });

        return {
          taskName,
          taskId: taskTraces[0].task_id,
          startTime: taskStart,
          endTime: taskEnd,
          duration: taskEnd.getTime() - taskStart.getTime(),
          events
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

    return {
      globalStart,
      globalEnd,
      totalDuration,
      agents,
      globalEvents
    };
  }, []);

  const fetchTraceData = useCallback(async (isInitialLoad = true) => {
    if (!runId) return;

    try {
      // Use different loading states for initial load vs refresh
      if (isInitialLoad) {
        setLoading(true);
      } else {
        setIsRefreshing(true);
      }

      const runExists = await TraceService.checkRunExists(runId);
      if (!runExists) {
        setError(`Run ID ${runId} does not exist or is no longer available.`);
        setLoading(false);
        setIsRefreshing(false);
        return;
      }

      const runData = await TraceService.getRunDetails(runId);
      const traceId = (runData.job_id && runData.job_id.includes('-'))
                      ? runData.job_id
                      : runId;

      const traces = await TraceService.getTraces(traceId);

      if (!traces || !Array.isArray(traces) || traces.length === 0) {
        setError('No trace data is available for this run.');
        setTraces([]);
      } else {
        setTraces(traces);
        const processed = processTraces(traces);
        setProcessedTraces(processed);

        if (isInitialLoad) {
          // Only expand all agents on initial load
          setExpandedAgents(new Set(processed.agents.map((_, idx) => idx)));
        } else {
          // During refresh, preserve expanded state but auto-expand new tasks for expanded agents
          // We'll handle this outside the callback to avoid dependency issues
          setExpandedAgents(prev => {
            // Just preserve the current expanded agents
            return prev;
          });
        }
        // Don't reset selectedEvent during refresh
        // This preserves any open event detail dialog
        setError(null);
      }
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : 'Unknown error';
      setError(`Failed to load traces: ${errorMessage}`);
      setTraces([]);
    } finally {
      setLoading(false);
      setIsRefreshing(false);
    }
  }, [runId, processTraces]);

  useEffect(() => {
    if (open) {
      fetchTraceData(true); // Initial load
    }
  }, [open, fetchTraceData]);

  // Load evaluation toggle to enable/disable button
  useEffect(() => {
    if (!open) return;
    (async () => {
      try {
        const resp = await apiClient.get('/mlflow/evaluation-status');
        setEvaluationEnabled(!!resp?.data?.enabled);
      } catch (e) {
        setEvaluationEnabled(false);
      }
    })();
  }, [open]);

  // Trigger evaluation and open the MLflow run page
  const triggerEvaluationAndOpen = async () => {
    if (!run || isEvaluationRunning) return;

    setIsEvaluationRunning(true);
    try {
      // 1) Trigger evaluation
      const ev = await apiClient.post('/mlflow/evaluate', { job_id: run.job_id });
      const runId = ev?.data?.run_id as string | undefined;
      const experimentId = ev?.data?.experiment_id as string | undefined;

      if (!runId || !experimentId) {
        console.error('Evaluation did not return run_id/experiment_id', ev?.data);
        setError('MLflow evaluation did not start or returned no run information. Please check backend logs.');
        return;
      }

      // 2) Resolve workspace URL and ID similar to traces
      let workspaceUrl = '';
      let workspaceId: string | undefined;
      try {
        const envResp = await apiClient.get('/databricks/environment');
        workspaceId = envResp?.data?.workspace_id || undefined;
        const envHost = envResp?.data?.databricks_host;
        if (!workspaceUrl && envHost) {
          workspaceUrl = envHost.startsWith('http') ? envHost : `https://${envHost}`;
        }
      } catch (e) { console.warn('Unable to get Databricks environment info for evaluation'); }
      if (!workspaceUrl || workspaceUrl === 'compute' || !workspaceUrl.includes('.')) {
        if (typeof window !== 'undefined' && window.location.hostname.includes('databricks')) {
          workspaceUrl = `https://${window.location.hostname}`;
        } else {
          try {
            const resp = await apiClient.get('/databricks/workspace-url');
            workspaceUrl = resp.data?.workspace_url || '';
          } catch (e) { console.warn('Unable to determine Databricks workspace URL'); }
        }
      }
      if (!workspaceUrl) {
        setError('Could not determine Databricks workspace URL to open evaluation run.');
        return;
      }
      if (workspaceUrl.endsWith('/')) workspaceUrl = workspaceUrl.slice(0, -1);
      if (!workspaceUrl.startsWith('http')) workspaceUrl = `https://${workspaceUrl}`;

      // 3) Build URL with workspace param
      const o = workspaceId ? `?o=${encodeURIComponent(workspaceId)}` : '';
      const url = `${workspaceUrl}/ml/experiments/${encodeURIComponent(experimentId)}/runs/${encodeURIComponent(runId)}${o}`;
      window.open(url, '_blank', 'noopener');
    } catch (e) {
      console.error('Failed to trigger evaluation:', e);
      setError('Failed to trigger MLflow evaluation. Please check backend logs.');
    } finally {
      setIsEvaluationRunning(false);
    }
  };

  // Auto-expand tasks for expanded agents when traces update
  useEffect(() => {
    if (!processedTraces || !isRefreshing) return;

    setExpandedTasks(prevExpandedTasks => {
      const newExpandedTasks = new Set(prevExpandedTasks);

      // For each expanded agent, ensure all its tasks are in the expanded set
      expandedAgents.forEach(agentIdx => {
        if (processedTraces.agents[agentIdx]) {
          processedTraces.agents[agentIdx].tasks.forEach((_, taskIdx) => {
            const taskKey = `${agentIdx}-${taskIdx}`;
            // Add new tasks to expanded set if the agent is expanded
            newExpandedTasks.add(taskKey);
          });
        }
      });

      return newExpandedTasks;
    });
  }, [processedTraces, expandedAgents, isRefreshing]);

  // Auto-refresh every 5 seconds while dialog is open and execution is not completed
  useEffect(() => {
    if (!open) return;

    // Check if execution is in a terminal state (don't refresh if completed/failed/cancelled/stopped)
    const isTerminalState = run?.status && [
      'completed',
      'failed',
      'cancelled',
      'stopped',
      'error'
    ].includes(run.status.toLowerCase());

    if (isTerminalState) {
      return; // Don't set up refresh interval if execution is done
    }

    const refreshInterval = setInterval(() => {
      fetchTraceData(false); // Refresh (not initial load)
    }, 5000); // Refresh every 5 seconds

    return () => {
      clearInterval(refreshInterval);
    };
  }, [open, run?.status, fetchTraceData]);

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

  const toggleAgent = (index: number) => {
    const newExpanded = new Set(expandedAgents);
    if (newExpanded.has(index)) {
      newExpanded.delete(index);
    } else {
      newExpanded.add(index);
    }
    setExpandedAgents(newExpanded);
  };

  const toggleTask = (taskKey: string) => {
    const newExpanded = new Set(expandedTasks);
    if (newExpanded.has(taskKey)) {
      newExpanded.delete(taskKey);
    } else {
      newExpanded.add(taskKey);
    }
    setExpandedTasks(newExpanded);
  };

  const getEventIcon = (type: string): JSX.Element => {
    const iconProps = { fontSize: 'small' as const, sx: { fontSize: 16 } };

    switch (type) {
      case 'tool':
      case 'tool_result':
      case 'tool_usage':
        return <TerminalIcon {...iconProps} color="action" />;
      case 'llm':
        return <PlayCircleIcon {...iconProps} color="primary" />;
      case 'agent_start':
      case 'task_start':
      case 'started':
        return <PlayArrowIcon {...iconProps} color="primary" />;
      case 'agent_complete':
      case 'task_complete':
      case 'completed':
        return <CheckCircleIcon {...iconProps} color="success" />;
      case 'agent_output':
      case 'agent_execution':
        return <PreviewIcon {...iconProps} color="action" />;
      case 'agent_processing':
        return <RefreshIcon {...iconProps} color="action" />;
      case 'memory_operation':
        return <AccessTimeIcon {...iconProps} color="action" />;
      case 'knowledge_operation':
        return <TimelineIcon {...iconProps} color="action" />;
      case 'crew_started':
        return <PlayCircleIcon {...iconProps} color="primary" />;
      case 'crew_completed':
        return <CheckCircleIcon {...iconProps} color="success" />;
      case 'flow_started':
        return <PlayCircleIcon {...iconProps} color="primary" />;
      case 'flow_completed':
        return <CheckCircleIcon {...iconProps} color="success" />;
      default:
        return <span style={{ fontSize: 16 }}>•</span>;
    }
  };

  const formatOutput = (output: string | Record<string, unknown> | undefined): string => {
    if (!output) return 'No output available';

    if (typeof output === 'string') {
      // Clean up tool results and other formatted strings
      if (output.includes('ToolResult')) {
        const match = output.match(/result="([^"]+)"/);
        if (match) {
          try {
            const parsed = JSON.parse(match[1].replace(/'/g, '"'));
            return JSON.stringify(parsed, null, 2);
          } catch {
            return output;
          }
        }
      }
      return output;
    }

    return JSON.stringify(output, null, 2);
  };

  const handleEventClick = (event: { type: string; description: string; output?: string | Record<string, unknown> }) => {
    setSelectedEvent(event);
  };

  if (!open) return null;

  return (
    <>
    <Dialog
      open={open}
      onClose={onClose}
      maxWidth="lg"
      fullWidth
      PaperProps={{
        sx: {
          minHeight: '80vh',
          maxHeight: '90vh'
        }
      }}
    >
      <DialogTitle sx={{ m: 0, p: 2 }}>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <Typography variant="h6">Execution Trace Timeline</Typography>
            {isRefreshing && (
              <Tooltip title="Refreshing...">
                <RefreshIcon
                  sx={{
                    fontSize: 20,


                    color: 'primary.main',
                    animation: 'spin 1s linear infinite',
                    '@keyframes spin': {
                      '0%': { transform: 'rotate(0deg)' },
                      '100%': { transform: 'rotate(360deg)' }
                    }
                  }}
                />
              </Tooltip>
            )}
            <Tooltip title={
              run?.status && ['completed', 'failed', 'cancelled', 'stopped', 'error'].includes(run.status.toLowerCase())
                ? "Refresh now (auto-refresh disabled - execution completed)"
                : "Refresh now (auto-refreshes every 5s)"
            }>
              <IconButton
                size="small"
                onClick={() => fetchTraceData(false)}
                disabled={isRefreshing || loading}
                sx={{ ml: 0.5 }}
              >
                <RefreshIcon fontSize="small" />
              </IconButton>
            </Tooltip>
          </Box>
          <Box sx={{ display: 'flex', gap: 1, alignItems: 'center' }}>
            {/* Show action buttons only in new UI mode and when we have the run data */}
            {useNewExecutionUI && run && (
              <>
                <Tooltip title={t('runHistory.actions.viewResult')}>
                  <span>
                    <IconButton
                      size="small"
                      onClick={() => {
                        if (onViewResult && run) {
                          // Use showRunResult from hook to ensure proper z-index handling
                          showRunResult(run);
                        }
                      }}
                      color="primary"
                      disabled={['running', 'pending', 'queued', 'in_progress'].includes(run?.status?.toLowerCase() || '')}
                    >
                      <PreviewIcon fontSize="small" />
                    </IconButton>
                  </span>
                </Tooltip>
                <Tooltip title="MLflow Trace">
                  <span>
                    <IconButton
                      size="small"
                      onClick={async () => {
                        const url = await getSpecificTraceUrl();
                        if (url && url !== '#') {
                          window.open(url, '_blank', 'noopener');
                        }
                      }}
                      color="primary"
                      disabled={['running', 'pending', 'queued', 'in_progress'].includes(run?.status?.toLowerCase() || '')}
                    >
                      <TimelineIcon fontSize="small" />
                    </IconButton>
                  </span>
                </Tooltip>

                {/* Grouped result/logs/trace actions */}
                <Tooltip title={t('runHistory.actions.viewLogs')}>
                  <span>
                    <IconButton
                      size="small"
                      onClick={handleOpenLogs}
                      color="primary"
                      disabled={['running', 'pending', 'queued', 'in_progress'].includes(run?.status?.toLowerCase() || '')}
                    >
                      <TerminalIcon fontSize="small" />
                    </IconButton>
                  </span>
                </Tooltip>

                {/* MLflow Evaluation trigger/view (separated, with play icon + text) */}
                <Divider orientation="vertical" flexItem sx={{ mx: 1 }} />
                <Button
                  size="small"
                  variant="outlined"
                  startIcon={isEvaluationRunning ? <CircularProgress size={16} /> : <PlayArrowIcon fontSize="small" />}
                  onClick={triggerEvaluationAndOpen}
                  color="primary"
                  disabled={
                    isEvaluationRunning ||
                    !evaluationEnabled ||
                    ['running', 'pending', 'queued', 'in_progress'].includes(run?.status?.toLowerCase() || '')
                  }
                >
                  {isEvaluationRunning ? 'Running Evaluation...' : 'Run MLflow Evaluation'}
                </Button>
                <Divider orientation="vertical" flexItem sx={{ mx: 1 }} />
              </>
            )}
            <IconButton
              aria-label="close"
              onClick={onClose}
              sx={{
                color: (theme: Theme) => theme.palette.grey[500],
              }}
            >
              <CloseIcon />
            </IconButton>
          </Box>
        </Box>
      </DialogTitle>

      <DialogContent dividers sx={{ p: 0 }}>
        {loading ? (
          <Box display="flex" justifyContent="center" alignItems="center" minHeight="400px">
            <CircularProgress />
          </Box>
        ) : error ? (
          <Box sx={{ p: 3, textAlign: 'center' }}>
            <Typography color="error">{error}</Typography>
          </Box>
        ) : processedTraces && processedTraces.agents.length > 0 ? (
          <Box sx={{ p: 2 }}>
            {/* Global Start Events */}
            {processedTraces.globalEvents.start.map((event, idx) => (
              <Box key={idx} sx={{ mb: 2, display: 'flex', alignItems: 'center', gap: 1 }}>
                <PlayCircleIcon color="primary" />
                <Typography variant="body2" color="text.secondary">
                  {event.event_type.replace(/_/g, ' ').toUpperCase()}
                </Typography>
                <Typography variant="caption" color="text.secondary">
                  {new Date(event.created_at).toLocaleTimeString()}
                </Typography>
              </Box>
            ))}

            {/* Agents and Tasks */}
            {processedTraces.agents.map((agent, agentIdx) => (
              <Paper key={agentIdx} sx={{ mb: 2, overflow: 'hidden' }}>
                <Box
                  sx={{
                    p: 2,
                    bgcolor: 'grey.100',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'space-between',
                    cursor: 'pointer',
                    '&:hover': { bgcolor: 'grey.200' }
                  }}
                  onClick={() => toggleAgent(agentIdx)}
                >
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
                    <IconButton size="small">
                      {expandedAgents.has(agentIdx) ? <ExpandLessIcon /> : <ExpandMoreIcon />}
                    </IconButton>
                    <Typography variant="subtitle1" fontWeight="bold">
                      {agent.agent}
                    </Typography>
                    <Chip
                      size="small"
                      label={formatDuration(agent.duration)}
                      icon={<AccessTimeIcon />}
                    />
                    {processedTraces.globalStart && (
                      <Typography variant="caption" color="text.secondary">
                        ({formatTimeDelta(processedTraces.globalStart, agent.endTime)})
                      </Typography>
                    )}
                  </Box>
                  <Typography variant="body2" color="text.secondary">
                    {agent.tasks.length} task{agent.tasks.length !== 1 ? 's' : ''}
                  </Typography>
                </Box>

                <Collapse in={expandedAgents.has(agentIdx)}>
                  <Box sx={{ pl: 6, pr: 2, py: 1 }}>
                    {agent.tasks.map((task, taskIdx) => {
                      const taskKey = `${agentIdx}-${taskIdx}`;
                      return (
                        <Box key={taskIdx} sx={{ mb: 2 }}>
                          <Box
                            sx={{
                              display: 'flex',
                              alignItems: 'center',
                              gap: 1,
                              p: 1,
                              bgcolor: 'grey.50',
                              borderRadius: 1,
                              cursor: 'pointer',
                              '&:hover': { bgcolor: 'grey.100' }
                            }}
                            onClick={() => toggleTask(taskKey)}
                          >
                            <IconButton size="small">
                              {expandedTasks.has(taskKey) ? <ExpandLessIcon /> : <ExpandMoreIcon />}
                            </IconButton>
                            <Typography variant="body2" fontWeight="medium">
                              {task.taskName}
                            </Typography>
                            <Chip
                              size="small"
                              label={formatDuration(task.duration)}
                              variant="outlined"
                            />
                          </Box>

                          <Collapse in={expandedTasks.has(taskKey)}>
                            <Box sx={{ pl: 4, mt: 1 }}>
                              {task.events.map((event, eventIdx) => {
                                const hasOutput = !!event.output;
                                // Make all events with output clickable, including memory operations
                                const isClickable = hasOutput && (
                                  event.type === 'llm' ||
                                  event.type === 'agent_complete' ||
                                  event.type === 'agent_output' ||
                                  event.type === 'tool_result' ||
                                  event.type === 'task_complete' ||
                                  event.type === 'memory_operation' ||
                                  event.type === 'tool_usage' ||
                                  event.type === 'knowledge_operation' ||
                                  event.type === 'agent_execution' ||
                                  // Also check for underscore versions
                                  event.type.includes('memory') ||
                                  event.type.includes('tool') ||
                                  event.type.includes('knowledge')
                                );

                                return (
                                  <Box
                                    key={eventIdx}
                                    sx={{
                                      display: 'flex',
                                      alignItems: 'center',
                                      gap: 1,
                                      py: 0.5,
                                      borderLeft: '2px solid',
                                      borderColor: 'grey.300',
                                      pl: 2,
                                      ml: 1,
                                      position: 'relative',
                                      cursor: isClickable ? 'pointer' : 'default',
                                      '&:hover': {
                                        bgcolor: isClickable ? 'action.hover' : 'transparent',
                                        '& .output-hint': {
                                          opacity: 1
                                        },
                                        '& .click-hint': {
                                          visibility: 'visible'
                                        }
                                      }
                                    }}
                                    onClick={() => isClickable && handleEventClick(event)}
                                  >
                                    <Typography variant="caption" sx={{ minWidth: 60 }}>
                                      {processedTraces.globalStart &&
                                        formatTimeDelta(processedTraces.globalStart, event.timestamp)}
                                    </Typography>
                                    <Box sx={{ minWidth: 20, display: 'flex', alignItems: 'center' }}>
                                      {getEventIcon(event.type)}
                                    </Box>
                                    <Typography
                                      variant="body2"
                                      sx={{
                                        flex: 1,
                                        color: isClickable ? 'primary.main' : 'text.primary',
                                        textDecoration: isClickable ? 'underline dotted' : 'none',
                                        textUnderlineOffset: '3px'
                                      }}
                                    >
                                      {event.description}
                                    </Typography>
                                    {event.duration && (
                                      <Chip
                                        size="small"
                                        label={formatDuration(event.duration)}
                                        sx={{ height: 20 }}
                                      />
                                    )}
                                    {isClickable && (
                                      <>
                                        <Chip
                                          className="output-hint"
                                          size="small"
                                          label="View"
                                          sx={{
                                            height: 18,
                                            fontSize: '0.65rem',
                                            bgcolor: 'primary.main',
                                            color: 'white',
                                            opacity: 0.7,
                                            transition: 'opacity 0.2s',
                                            '& .MuiChip-label': {
                                              px: 0.5
                                            }
                                          }}
                                        />
                                        <Typography
                                          className="click-hint"
                                          variant="caption"
                                          sx={{
                                            position: 'absolute',
                                            right: -10,
                                            top: '50%',
                                            transform: 'translateY(-50%)',
                                            bgcolor: 'grey.900',
                                            color: 'white',
                                            px: 1,
                                            py: 0.5,
                                            borderRadius: 1,
                                            fontSize: '0.7rem',
                                            visibility: 'hidden',
                                            zIndex: 1000,
                                            whiteSpace: 'nowrap',
                                            '&::before': {
                                              content: '""',
                                              position: 'absolute',
                                              left: -4,
                                              top: '50%',
                                              transform: 'translateY(-50%)',
                                              width: 0,
                                              height: 0,
                                              borderTop: '4px solid transparent',
                                              borderBottom: '4px solid transparent',
                                              borderRight: '4px solid',
                                              borderRightColor: 'grey.900'
                                            }
                                          }}
                                        >
                                          Click to view output
                                        </Typography>
                                      </>
                                    )}
                                  </Box>
                                );
                              })}
                            </Box>
                          </Collapse>
                        </Box>
                      );
                    })}
                  </Box>
                </Collapse>
              </Paper>
            ))}

            {/* Global End Events */}
            {processedTraces.globalEvents.end.map((event, idx) => (
              <Box key={idx} sx={{ mt: 2, display: 'flex', alignItems: 'center', gap: 1 }}>
                <CheckCircleIcon color="success" />
                <Typography variant="body2" color="text.secondary">
                  {event.event_type.replace(/_/g, ' ').toUpperCase()}
                </Typography>
                <Typography variant="caption" color="text.secondary">
                  {new Date(event.created_at).toLocaleTimeString()}
                </Typography>
                {processedTraces.totalDuration && (
                  <Chip
                    size="small"
                    label={`Total: ${formatDuration(processedTraces.totalDuration)}`}
                    color="primary"
                  />
                )}
              </Box>
            ))}
          </Box>
        ) : (
          <Box sx={{ p: 3, textAlign: 'center' }}>
            <Typography>No trace data available</Typography>
          </Box>
        )}
      </DialogContent>

      {/* Output Details Dialog */}
      <Dialog
        open={!!selectedEvent}
        onClose={() => setSelectedEvent(null)}
        maxWidth="md"
        fullWidth
      >
        {selectedEvent && (
          <>
            <DialogTitle sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <Box>
                <Typography variant="h6">{selectedEvent.description}</Typography>
                <Typography variant="caption" color="text.secondary">
                  Event Type: {selectedEvent.type}
                </Typography>
              </Box>
              <IconButton
                onClick={() => setSelectedEvent(null)}
                size="small"
              >
                <CloseIcon />
              </IconButton>
            </DialogTitle>
            <DialogContent dividers>
              <Box sx={{ position: 'relative' }}>
                {/* Special formatting for memory operations */}
                {selectedEvent.type === 'memory_operation' || selectedEvent.type.includes('memory') ? (
                  <Box>
                    <Typography variant="subtitle2" gutterBottom>
                      Memory Operation Details
                    </Typography>
                    {typeof selectedEvent.output === 'object' && selectedEvent.output && (
                      <Box sx={{ mb: 2 }}>
                        {('operation' in selectedEvent.output) && (
                          <Chip
                            label={`Operation: ${selectedEvent.output.operation as string}`}
                            sx={{ mr: 1, mb: 1 }}
                            size="small"
                            color="primary"
                          />
                        )}
                        {('memory_type' in selectedEvent.output) && (
                          <Chip
                            label={`Type: ${selectedEvent.output.memory_type as string}`}
                            sx={{ mr: 1, mb: 1 }}
                            size="small"
                            color="secondary"
                          />
                        )}
                      </Box>
                    )}
                  </Box>
                ) : null}

                {/* Special formatting for tool usage */}
                {selectedEvent.type === 'tool_usage' || selectedEvent.type === 'tool_result' ? (
                  <Box>
                    <Typography variant="subtitle2" gutterBottom>
                      Tool Usage Details
                    </Typography>
                    {typeof selectedEvent.output === 'object' && selectedEvent.output && (
                      <Box sx={{ mb: 2 }}>
                        {('tool_name' in selectedEvent.output) && (
                          <Chip
                            label={`Tool: ${selectedEvent.output.tool_name as string}`}
                            sx={{ mr: 1, mb: 1 }}
                            size="small"
                            color="info"
                          />
                        )}
                      </Box>
                    )}
                  </Box>
                ) : null}

                <Paper
                  sx={{
                    p: 2,
                    mt: 1,
                    backgroundColor: (theme) => theme.palette.mode === 'dark' ? 'grey.900' : 'grey.50',
                    maxHeight: '60vh',
                    overflow: 'auto',
                    '& pre': {
                      overflowX: 'auto',
                      padding: 1,
                      borderRadius: 1,
                      backgroundColor: (theme) => theme.palette.mode === 'dark' ? 'grey.800' : 'grey.100',
                      fontFamily: 'monospace',
                      fontSize: '0.875rem',
                    },
                    '& code': {
                      fontFamily: 'monospace',
                      fontSize: '0.875rem',
                      backgroundColor: (theme) => theme.palette.mode === 'dark' ? 'grey.800' : 'grey.200',
                      padding: '2px 4px',
                      borderRadius: '3px',
                    },
                    '& p': {
                      marginTop: 1,
                      marginBottom: 1,
                    },
                    '& ul, & ol': {
                      paddingLeft: 3,
                      marginTop: 1,
                      marginBottom: 1,
                    },
                    '& li': {
                      marginTop: 0.5,
                      marginBottom: 0.5,
                    },
                    '& blockquote': {
                      borderLeft: '4px solid',
                      borderColor: 'primary.main',
                      paddingLeft: 2,
                      marginLeft: 0,
                      marginTop: 1,
                      marginBottom: 1,
                      fontStyle: 'italic',
                      color: 'text.secondary',
                    },
                    '& h1, & h2, & h3, & h4, & h5, & h6': {
                      marginTop: 2,
                      marginBottom: 1,
                      fontWeight: 'bold',
                    },
                    '& h1': { fontSize: '1.5rem' },
                    '& h2': { fontSize: '1.3rem' },
                    '& h3': { fontSize: '1.1rem' },
                    '& h4': { fontSize: '1rem' },
                    '& h5': { fontSize: '0.9rem' },
                    '& h6': { fontSize: '0.85rem' },
                    '& table': {
                      width: '100%',
                      borderCollapse: 'collapse',
                      marginTop: 1,
                      marginBottom: 1,
                    },
                    '& th, & td': {
                      border: '1px solid',
                      borderColor: 'divider',
                      padding: 1,
                      textAlign: 'left',
                    },
                    '& th': {
                      backgroundColor: (theme) => theme.palette.mode === 'dark' ? 'grey.800' : 'grey.200',
                      fontWeight: 'bold',
                    },
                    '& hr': {
                      marginTop: 2,
                      marginBottom: 2,
                      border: 'none',
                      borderTop: '1px solid',
                      borderColor: 'divider',
                    },
                    '& a': {
                      color: 'primary.main',
                      textDecoration: 'none',
                      '&:hover': {
                        textDecoration: 'underline',
                      },
                    },
                  }}
                >
                  <ReactMarkdown remarkPlugins={[remarkGfm]}>
                    {formatOutput(selectedEvent.output)}
                  </ReactMarkdown>
                </Paper>
              </Box>
            </DialogContent>
            <DialogActions>
              <Button
                onClick={() => {
                  navigator.clipboard.writeText(formatOutput(selectedEvent.output));
                }}
                startIcon={<ContentCopyIcon />}
                size="small"
              >
                Copy Output
              </Button>
              <Button onClick={() => setSelectedEvent(null)} size="small">
                Close
              </Button>
            </DialogActions>
          </>
        )}
      </Dialog>
    </Dialog>

    {/* Render dialogs outside of main dialog to ensure proper z-index stacking */}
    {/* Show Logs Dialog - only in new UI mode */}
    {useNewExecutionUI && run && showLogsDialog && (
      <ShowLogs
        open={showLogsDialog}
        onClose={() => setShowLogsDialog(false)}
        logs={logs}
        jobId={run.job_id}
        isConnecting={isLoadingLogs}
        connectionError={logsError}
      />
    )}
  </>
  );
};

export default ShowTraceTimeline;