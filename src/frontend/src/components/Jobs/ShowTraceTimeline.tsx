import React, { useState, useEffect, useCallback, useMemo } from 'react';
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
  Card,
  CardContent,
  Stack,
  Alert,
  ToggleButton,
  ToggleButtonGroup,
} from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';
import SummarizeIcon from '@mui/icons-material/Summarize';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import ExpandLessIcon from '@mui/icons-material/ExpandLess';
import AccessTimeIcon from '@mui/icons-material/AccessTime';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import PlayCircleIcon from '@mui/icons-material/PlayCircle';
import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import PreviewIcon from '@mui/icons-material/Preview';
import TimelineIcon from '@mui/icons-material/Timeline';
import TerminalIcon from '@mui/icons-material/Terminal';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import StorageIcon from '@mui/icons-material/Storage';
import AssignmentIcon from '@mui/icons-material/Assignment';
import PersonIcon from '@mui/icons-material/Person';
import BuildIcon from '@mui/icons-material/Build';
import TargetIcon from '@mui/icons-material/TrackChanges';
import { ShowTraceProps } from '../../types/trace';
import {
  processTraceEvent,
  extractOutputForDisplay,
  extractExtraData,
  isEventClickable,
  getEventIcon as getEventIconConfig,
} from './traceEventProcessors';

import apiClient from '../../config/api/ApiConfig';
import TraceService from '../../api/TraceService';
import { useUserPreferencesStore } from '../../store/userPreferencesStore';
import { useRunStatusStore, Trace } from '../../store/runStatus';
import { useTranslation } from 'react-i18next';
import ShowLogs from './ShowLogs';
import { executionLogService, LogEntry } from '../../api/ExecutionLogs';
import { useRunResult } from '../../hooks/global/useExecutionResult';
import { PaginatedOutput } from '../Common';

interface TraceEvent {
  type: string;
  description: string;
  timestamp: Date;
  duration?: number;
  output?: string | Record<string, unknown>;
  extraData?: Record<string, unknown>;
}

interface GroupedTrace {
  agent: string;
  startTime: Date;
  endTime: Date;
  duration: number;
  // Agent-level events (like reasoning/planning) that happen outside of specific tasks
  agentEvents: TraceEvent[];
  tasks: {
    taskName: string;
    taskId?: string;
    startTime: Date;
    endTime: Date;
    duration: number;
    events: TraceEvent[];
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
  // Crew-level planning events (from "Task Execution Planner" agent)
  crewPlanningEvents: TraceEvent[];
}

// Interface for parsed task data
interface ParsedTask {
  taskNumber: number;
  taskTitle: string;
  taskDescription: string;
  expectedOutput: string;
  agent: string;
  agentGoal: string;
  taskTools: string;
  agentTools: string;
}

// Helper function to parse task description into structured data
const parseTaskDescription = (description: string): { header: string; tasks: ParsedTask[]; footer: string } | null => {
  if (!description) return null;

  // Check if this is a structured task description
  if (!description.includes('Task Number') && !description.includes('task_description')) {
    return null;
  }

  const result: { header: string; tasks: ParsedTask[]; footer: string } = {
    header: '',
    tasks: [],
    footer: ''
  };

  // Extract header (text before first Task Number)
  const headerMatch = description.match(/^(.*?)(?=Task Number \d)/s);
  if (headerMatch) {
    result.header = headerMatch[1].trim();
  }

  // Extract footer (text after last task, typically "Create the most descriptive plan...")
  const footerMatch = description.match(/Create the most descriptive plan.*$/s);
  if (footerMatch) {
    result.footer = footerMatch[0].trim();
  }

  // Split by Task Number pattern
  const taskBlocks = description.split(/(?=Task Number \d+)/);

  for (const block of taskBlocks) {
    if (!block.trim() || !block.includes('Task Number')) continue;

    const task: ParsedTask = {
      taskNumber: 0,
      taskTitle: '',
      taskDescription: '',
      expectedOutput: '',
      agent: '',
      agentGoal: '',
      taskTools: '',
      agentTools: ''
    };

    // Extract task number and title
    const titleMatch = block.match(/Task Number (\d+)\s*-\s*([^\n"]+)/);
    if (titleMatch) {
      task.taskNumber = parseInt(titleMatch[1], 10);
      task.taskTitle = titleMatch[2].trim();
    }

    // Extract task_description
    const descMatch = block.match(/"task_description":\s*([^\n]*(?:\n(?!"task_expected_output")[^\n]*)*)/);
    if (descMatch) {
      task.taskDescription = descMatch[1].trim().replace(/^["']|["']$/g, '');
    }

    // Extract task_expected_output
    const outputMatch = block.match(/"task_expected_output":\s*([^\n]*(?:\n(?!"agent":)[^\n]*)*)/);
    if (outputMatch) {
      task.expectedOutput = outputMatch[1].trim().replace(/^["']|["']$/g, '');
    }

    // Extract agent
    const agentMatch = block.match(/"agent":\s*([^\n]+)/);
    if (agentMatch) {
      task.agent = agentMatch[1].trim().replace(/^["']|["']$/g, '');
    }

    // Extract agent_goal
    const goalMatch = block.match(/"agent_goal":\s*([^\n]+)/);
    if (goalMatch) {
      task.agentGoal = goalMatch[1].trim().replace(/^["']|["']$/g, '');
    }

    // Extract task_tools - handle complex tool definitions
    const toolsMatch = block.match(/"task_tools":\s*\[([^\]]*)\]/s);
    if (toolsMatch) {
      const toolsContent = toolsMatch[1].trim();
      if (toolsContent) {
        // Extract tool names from PerplexitySearchTool(name='ToolName', ...) pattern
        const toolNameMatches = toolsContent.match(/name='([^']+)'/g);
        if (toolNameMatches) {
          task.taskTools = toolNameMatches.map(m => m.replace(/name='|'/g, '')).join(', ');
        } else {
          task.taskTools = toolsContent.length > 100 ? 'Custom Tools' : toolsContent;
        }
      } else {
        task.taskTools = 'None';
      }
    }

    // Extract agent_tools
    const agentToolsMatch = block.match(/"agent_tools":\s*"?([^"\n]+)"?/);
    if (agentToolsMatch) {
      task.agentTools = agentToolsMatch[1].trim();
    }

    if (task.taskNumber > 0) {
      result.tasks.push(task);
    }
  }

  return result.tasks.length > 0 ? result : null;
};

// Component to render formatted task description
const FormattedTaskDescription: React.FC<{ description: string }> = ({ description }) => {
  const parsed = parseTaskDescription(description);

  if (!parsed) {
    // If parsing fails, show raw description
    return (
      <Typography
        variant="body1"
        sx={{
          whiteSpace: 'pre-wrap',
          wordBreak: 'break-word',
          lineHeight: 1.6
        }}
      >
        {description}
      </Typography>
    );
  }

  return (
    <Box>
      {/* Header */}
      {parsed.header && (
        <Alert severity="info" sx={{ mb: 2 }}>
          <Typography variant="body2">{parsed.header}</Typography>
        </Alert>
      )}

      {/* Tasks */}
      <Stack spacing={2}>
        {parsed.tasks.map((task) => (
          <Card key={task.taskNumber} variant="outlined" sx={{ borderRadius: 2 }}>
            <CardContent>
              {/* Task Header */}
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
                <Chip
                  label={`Task ${task.taskNumber}`}
                  color="primary"
                  size="small"
                  icon={<AssignmentIcon />}
                />
                <Typography variant="subtitle1" fontWeight="bold" sx={{ flex: 1 }}>
                  {task.taskTitle}
                </Typography>
              </Box>

              {/* Task Description */}
              {task.taskDescription && (
                <Box sx={{ mb: 2 }}>
                  <Typography variant="caption" color="text.secondary" sx={{ display: 'flex', alignItems: 'center', gap: 0.5, mb: 0.5 }}>
                    <AssignmentIcon fontSize="inherit" /> Description
                  </Typography>
                  <Paper sx={{ p: 1.5, bgcolor: 'action.hover', borderRadius: 1 }}>
                    <Typography variant="body2" sx={{ whiteSpace: 'pre-wrap' }}>
                      {task.taskDescription}
                    </Typography>
                  </Paper>
                </Box>
              )}

              {/* Expected Output */}
              {task.expectedOutput && (
                <Box sx={{ mb: 2 }}>
                  <Typography variant="caption" color="text.secondary" sx={{ display: 'flex', alignItems: 'center', gap: 0.5, mb: 0.5 }}>
                    <CheckCircleIcon fontSize="inherit" /> Expected Output
                  </Typography>
                  <Paper sx={{ p: 1.5, bgcolor: 'success.main', color: 'success.contrastText', borderRadius: 1, opacity: 0.9 }}>
                    <Typography variant="body2" sx={{ whiteSpace: 'pre-wrap' }}>
                      {task.expectedOutput}
                    </Typography>
                  </Paper>
                </Box>
              )}

              {/* Agent & Tools Row */}
              <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 2 }}>
                {/* Agent Info */}
                <Box sx={{ flex: '1 1 200px', minWidth: 0 }}>
                  <Typography variant="caption" color="text.secondary" sx={{ display: 'flex', alignItems: 'center', gap: 0.5, mb: 0.5 }}>
                    <PersonIcon fontSize="inherit" /> Agent
                  </Typography>
                  <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.5 }}>
                    <Chip
                      label={task.agent}
                      size="small"
                      color="secondary"
                      variant="outlined"
                      icon={<PersonIcon />}
                    />
                    {task.agentGoal && (
                      <Typography variant="caption" color="text.secondary" sx={{ display: 'flex', alignItems: 'center', gap: 0.5, mt: 0.5 }}>
                        <TargetIcon fontSize="inherit" /> {task.agentGoal}
                      </Typography>
                    )}
                  </Box>
                </Box>

                {/* Tools Info */}
                <Box sx={{ flex: '1 1 200px', minWidth: 0 }}>
                  <Typography variant="caption" color="text.secondary" sx={{ display: 'flex', alignItems: 'center', gap: 0.5, mb: 0.5 }}>
                    <BuildIcon fontSize="inherit" /> Tools
                  </Typography>
                  <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                    {task.taskTools && task.taskTools !== 'None' ? (
                      task.taskTools.split(', ').map((tool, idx) => (
                        <Chip
                          key={idx}
                          label={tool}
                          size="small"
                          color="info"
                          variant="outlined"
                          icon={<BuildIcon />}
                        />
                      ))
                    ) : (
                      <Chip label="No tools" size="small" variant="outlined" />
                    )}
                  </Box>
                </Box>
              </Box>
            </CardContent>
          </Card>
        ))}
      </Stack>

      {/* Footer */}
      {parsed.footer && (
        <Alert severity="success" sx={{ mt: 2 }}>
          <Typography variant="body2" fontWeight="medium">{parsed.footer}</Typography>
        </Alert>
      )}
    </Box>
  );
};

const ShowTraceTimeline: React.FC<ShowTraceProps> = ({
  open,
  onClose,
  runId,
  run,
  onViewResult,
  onShowLogs: _onShowLogs,
}) => {
  const { t } = useTranslation();
  const { useNewExecutionUI } = useUserPreferencesStore();
  const { showRunResult } = useRunResult();
  const setTracesForJob = useRunStatusStore(state => state.setTracesForJob);
  const getTracesForJob = useRunStatusStore(state => state.getTracesForJob);

  // Subscribe to traces from Zustand store
  // Use getTracesForJob which returns empty array from store (stable reference)
  const storeTraces = useRunStatusStore(state =>
    run?.job_id ? state.traces.get(run.job_id) : undefined
  );

  // CRITICAL: Use a stable empty array reference to prevent infinite re-renders
  // Creating a new [] on each render causes reference inequality ([] !== [])
  const _traces = useMemo(() => storeTraces ?? [], [storeTraces]);
  const [processedTraces, setProcessedTraces] = useState<ProcessedTraces | null>(null);

  // Debug: Log when dialog opens and what job_id we're using
  useEffect(() => {
    if (open) {
      console.log('[ShowTraceTimeline] Dialog opened');
      console.log('[ShowTraceTimeline] runId:', runId);
      console.log('[ShowTraceTimeline] run?.job_id:', run?.job_id);
      console.log('[ShowTraceTimeline] run?.status:', run?.status);
      console.log('[ShowTraceTimeline] Initial store traces count:', _traces.length);

      // Also check what's in the store directly
      if (run?.job_id) {
        const storeTraces = getTracesForJob(run.job_id);
        console.log('[ShowTraceTimeline] Direct store query traces count:', storeTraces.length);
      }
    }
  }, [open, runId, run?.job_id, run?.status, _traces.length, getTracesForJob]);
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
  const [showLogsDialog, setShowLogsDialog] = useState(false);
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [isLoadingLogs, setIsLoadingLogs] = useState(false);
  const [logsError, setLogsError] = useState<string | null>(null);
  const [evaluationEnabled, setEvaluationEnabled] = useState<boolean>(false);
  const [isEvaluationRunning, setIsEvaluationRunning] = useState(false);
  const [selectedTaskDescription, setSelectedTaskDescription] = useState<{
    taskName: string;
    taskId?: string;
    fullDescription?: string;
    isLoading: boolean;
  } | null>(null);

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
    // Note: agent_execution and task source events are now prevented at backend source
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

    // Separate global events - support both crew and flow events
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

    // Extract crew-level planning events (from "Task Execution Planner" agent)
    // These should be displayed as a special section, not as a regular agent
    const crewPlannerTraces = sorted.filter(t =>
      t.event_source === 'Task Execution Planner'
    );

    // Process crew planner traces into events
    const crewPlanningEvents: TraceEvent[] = crewPlannerTraces
      .filter(trace => {
        // Only show meaningful events (LLM response with the plan, or task completion)
        // Skip task_started and llm_request as they're less informative
        return trace.event_type === 'llm_response' ||
               trace.event_type === 'task_completed';
      })
      .map((trace, idx, arr) => {
        const timestamp = new Date(trace.created_at);
        const nextTrace = arr[idx + 1];
        const duration = nextTrace
          ? new Date(nextTrace.created_at).getTime() - timestamp.getTime()
          : undefined;

        // Extract the plan content
        let outputContent: string | Record<string, unknown> | undefined = trace.output;
        if (trace.output && typeof trace.output === 'object' && 'content' in trace.output) {
          const content = (trace.output as Record<string, unknown>).content;
          if (typeof content === 'string' || (typeof content === 'object' && content !== null)) {
            outputContent = content as string | Record<string, unknown>;
          }
        }

        // Determine description
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

    // Group by agent (excluding Task Execution Planner which is handled separately)
    const agentMap = new Map<string, Trace[]>();

    // Helper function to extract task_id from trace (could be direct field or in metadata)
    const getTaskId = (trace: Trace): string | null => {
      // First check direct field
      if (trace.task_id) return trace.task_id;
      // Then check trace_metadata
      if (trace.trace_metadata && typeof trace.trace_metadata === 'object') {
        const metadata = trace.trace_metadata as Record<string, unknown>;
        if (metadata.task_id) return metadata.task_id as string;
      }
      // Finally check extra_data
      if (trace.extra_data && typeof trace.extra_data === 'object') {
        const extraData = trace.extra_data as Record<string, unknown>;
        if (extraData.task_id) return extraData.task_id as string;
      }
      return null;
    };

    // OTel span hierarchy maps — the source of truth for task grouping
    const spanIdToTaskId = new Map<string, string>();  // span_id -> task_id
    const spanIdToAgent = new Map<string, string>();   // span_id -> agent name
    const taskIdToName = new Map<string, string>();    // task_id -> display name
    const taskIdToAgent = new Map<string, string>();   // task_id -> agent name

    // First pass: build span hierarchy and task name index
    sorted.forEach(trace => {
      const taskId = getTaskId(trace);

      // Index every span that carries a task_id (instrumentor spans + bridge spans with task_id)
      if (trace.span_id && taskId) {
        spanIdToTaskId.set(trace.span_id, taskId);
      }
      // Index span -> agent for parent-child agent attribution
      if (trace.span_id && trace.event_source && trace.event_source !== 'Unknown Agent') {
        spanIdToAgent.set(trace.span_id, trace.event_source);
      }

      // Build task_id -> name from task_started events or event_context
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

      // Build task_id -> agent
      if (taskId && !taskIdToAgent.has(taskId)) {
        const agent = trace.event_source;
        if (agent && agent !== 'Unknown Agent' && agent !== 'task' && agent !== 'crew') {
          taskIdToAgent.set(taskId, agent);
        }
      }
    });

    // Second pass: group traces by agent
    sorted.forEach(trace => {
      // Skip global events (crew/flow/system), task orchestration events, task events, and Task Execution Planner for agent grouping
      // Task Execution Planner is handled separately as crew-level planning
      // "system"/"System" events are crew-level internal calls (e.g., final output synthesis)
      // EXCEPTION: error/failure events are never skipped — they must appear in the timeline
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

      // OTel span hierarchy: use parent_span_id for accurate agent attribution
      if (trace.parent_span_id && spanIdToAgent.has(trace.parent_span_id)) {
        agent = spanIdToAgent.get(trace.parent_span_id)!;
      }
      // Also index this span's agent for its children
      if (trace.span_id && agent !== 'Unknown Agent') {
        spanIdToAgent.set(trace.span_id, agent);
      }

      // Use task_id to determine correct agent (fixes misattributed events)
      const traceTaskId = getTaskId(trace);
      if (traceTaskId && taskIdToAgent.has(traceTaskId)) {
        agent = taskIdToAgent.get(traceTaskId)!;
      }

      // For LLM calls or error events, extract agent from extra_data/metadata if available
      if ((trace.event_type === 'llm_call' || isErrorEvent) && trace.extra_data && typeof trace.extra_data === 'object') {
        const extraData = trace.extra_data as Record<string, unknown>;
        const agentRole = extraData.agent_role as string;
        if (agentRole && agentRole !== 'UnknownAgent-str' && agentRole !== 'Unknown Agent') {
          agent = agentRole;
        }
      }
      // For error events, also try trace_metadata.agent_role
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

      // Group agent traces by task - using task_id for parallel task support
      const taskMap = new Map<string, Trace[]>();
      const taskIdToUniqueKey = new Map<string, string>(); // Map task_id to unique task key
      let taskCounter = 0;
      // Agent-level traces (reasoning, planning) that happen at the agent level, not task level
      const agentLevelTraces: Trace[] = [];

      agentTraces.forEach(trace => {
        // Check if this is an agent-level event (reasoning/planning)
        // These events should be shown at the agent level, not nested under tasks
        if (trace.event_type === 'agent_reasoning' || trace.event_type === 'agent_reasoning_error') {
          // Check if this is a "reasoning_started" event (not very informative) vs "reasoning_completed" (valuable)
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
          // Also check extra_data inside output
          let extraData: Record<string, unknown> | null = null;
          if (trace.output && typeof trace.output === 'object' && 'extra_data' in trace.output) {
            extraData = (trace.output as Record<string, unknown>).extra_data as Record<string, unknown>;
          }

          const operation = metadata?.operation || extraData?.operation;

          // Skip "reasoning_started" events - they just say "Agent X starting reasoning process"
          // Only show "reasoning_completed" events which have the actual plan
          if (operation !== 'reasoning_started') {
            agentLevelTraces.push(trace);
          }
          return; // Don't add to task map
        }

        // Resolve task via span hierarchy: direct task_id → parent_span_id → task_id
        let traceTaskId = getTaskId(trace)
          || (trace.parent_span_id ? spanIdToTaskId.get(trace.parent_span_id) : undefined)
          || undefined;

        let taskKey = 'Unassigned';
        if (traceTaskId) {
          // Already mapped this task_id to a display key?
          if (taskIdToUniqueKey.has(traceTaskId)) {
            taskKey = taskIdToUniqueKey.get(traceTaskId)!;
          } else {
            // Resolve name: from index, or event_context as last resort
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
        const taskTraces = taskMap.get(taskKey);
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

          // Use registry-based event processing
          const processed = processTraceEvent(trace);
          if (!processed) {
            return null; // Filter out events the registry says to skip
          }
          eventType = processed.type;
          description = processed.description;

          // Extract display content from output
          const outputContent = extractOutputForDisplay(trace.output);

          // Extract extra_data for detailed event information
          const extraDataObj = extractExtraData(trace);

          return {
            type: eventType,
            description,
            timestamp,
            duration,
            output: outputContent,
            extraData: extraDataObj
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

      // Process agent-level traces into agentEvents using registry
      const agentEvents: TraceEvent[] = agentLevelTraces.map((trace, idx) => {
        const timestamp = new Date(trace.created_at);
        const nextTrace = agentLevelTraces[idx + 1];
        const duration = nextTrace
          ? new Date(nextTrace.created_at).getTime() - timestamp.getTime()
          : undefined;

        const processed = processTraceEvent(trace);
        const eventType = processed?.type ?? 'agent_reasoning';
        const description = processed?.description ?? 'Agent Reasoning';

        return {
          type: eventType,
          description,
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
  }, []);

  const fetchTraceData = useCallback(async (isInitialLoad = true) => {
    if (!runId) return;

    console.log('[ShowTraceTimeline] fetchTraceData called, isInitialLoad:', isInitialLoad);

    try {
      // Set loading state for initial load
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

      console.log('[ShowTraceTimeline] Fetching traces for traceId:', traceId);
      const traces = await TraceService.getTraces(traceId);
      console.log('[ShowTraceTimeline] API returned traces count:', traces?.length || 0);

      if (!traces || !Array.isArray(traces) || traces.length === 0) {
        // Only show error if job is in terminal state (not running)
        const isRunning = run?.status && ['running', 'queued', 'pending'].includes(run.status.toLowerCase());
        console.log('[ShowTraceTimeline] No traces from API. Job status:', run?.status, 'isRunning:', isRunning);
        if (!isRunning) {
          setError('No trace data is available for this run.');
        } else {
          // Clear error for running jobs - traces will come via SSE
          setError(null);
          setLoading(false);
        }
        // DON'T overwrite store with empty array - SSE might have already added traces
        // The store subscription will handle displaying any existing traces
      } else {
        // Store traces in Zustand store (only if we got data from API)
        if (run?.job_id) {
          console.log('[ShowTraceTimeline] Storing', traces.length, 'traces in store for job_id:', run.job_id);
          setTracesForJob(run.job_id, traces);
        }
        const processed = processTraces(traces);
        setProcessedTraces(processed);

        if (isInitialLoad) {
          // Expand all agents and tasks on initial load
          setExpandedAgents(new Set(processed.agents.map((_, idx) => idx)));
          // Generate all task keys and expand them
          const allTaskKeys = new Set<string>();
          processed.agents.forEach((agent, agentIdx) => {
            agent.tasks.forEach((_, taskIdx) => {
              allTaskKeys.add(`${agentIdx}-${taskIdx}`);
            });
          });
          setExpandedTasks(allTaskKeys);
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
      console.error('[ShowTraceTimeline] Error fetching traces:', errorMessage);
      setError(`Failed to load traces: ${errorMessage}`);
      // DON'T overwrite store on error - SSE traces might already be there
    } finally {
      setLoading(false);
    }
  }, [runId, run?.status, run?.job_id, processTraces, setTracesForJob]);

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

  // Automatically reprocess traces when they change in the store
  useEffect(() => {
    if (!open) return;

    console.log('[ShowTraceTimeline] Store traces updated:', _traces.length, 'traces for job', run?.job_id);

    if (_traces && _traces.length > 0) {
      const processed = processTraces(_traces);
      setProcessedTraces(processed);

      // Clear any error when traces arrive
      setError(null);
      setLoading(false);

      // Auto-expand all agents and tasks when traces update
      setExpandedAgents(new Set(processed.agents.map((_, idx) => idx)));
      // Generate all task keys and expand them
      const allTaskKeys = new Set<string>();
      processed.agents.forEach((agent, agentIdx) => {
        agent.tasks.forEach((_, taskIdx) => {
          allTaskKeys.add(`${agentIdx}-${taskIdx}`);
        });
      });
      setExpandedTasks(allTaskKeys);
    } else {
      // No traces yet - only show loading if we haven't finished initial fetch
      // If initial fetch is done and still no traces, rely on error message
      const processed = processTraces([]);
      setProcessedTraces(processed);
    }
  }, [_traces, open, processTraces, run?.job_id]);

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

  // Truncate task name for display
  const truncateTaskName = (name: string, maxLength = 80): string => {
    if (name.length <= maxLength) return name;
    return name.substring(0, maxLength) + '...';
  };

  // Handle clicking on a task name to show full description
  const handleTaskDescriptionClick = async (taskName: string, taskId?: string, e?: React.MouseEvent) => {
    // Stop propagation to prevent toggling the task expansion
    if (e) {
      e.stopPropagation();
    }

    // Set initial state with task name
    setSelectedTaskDescription({
      taskName,
      taskId,
      fullDescription: undefined,
      isLoading: !!taskId
    });

    // If we have a taskId, fetch the full task details
    if (taskId) {
      try {
        const taskDetails = await TraceService.getTaskDetails(taskId);
        setSelectedTaskDescription(prev => prev ? {
          ...prev,
          fullDescription: taskDetails.description || taskName,
          isLoading: false
        } : null);
      } catch (error) {
        console.error('Failed to fetch task details:', error);
        // Use the task name as fallback
        setSelectedTaskDescription(prev => prev ? {
          ...prev,
          fullDescription: taskName,
          isLoading: false
        } : null);
      }
    }
  };

  const getEventIcon = (type: string): JSX.Element => {
    const iconProps = { fontSize: 'small' as const, sx: { fontSize: 16 } };
    const config = getEventIconConfig(type);
    if (config.Component) {
      const IconComponent = config.Component;
      return <IconComponent {...iconProps} color={config.color} />;
    }
    return <span style={{ fontSize: 16 }}>•</span>;
  };

  const handleEventClick = (event: { type: string; description: string; output?: string | Record<string, unknown>; extraData?: Record<string, unknown> }) => {
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
          <Typography variant="h6">Execution Trace Timeline</Typography>
          <Box sx={{ display: 'flex', gap: 1, alignItems: 'center' }}>
            <ToggleButtonGroup
              value={viewMode}
              exclusive
              onChange={(_, newMode) => newMode && setViewMode(newMode)}
              size="small"
              sx={{ mr: 2 }}
            >
              <ToggleButton value="summary">
                <SummarizeIcon fontSize="small" sx={{ mr: 0.5 }} />
                Summary
              </ToggleButton>
              <ToggleButton value="timeline">
                <TimelineIcon fontSize="small" sx={{ mr: 0.5 }} />
                Timeline
              </ToggleButton>
            </ToggleButtonGroup>
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
            {/* Summary View */}
            {viewMode === 'summary' && (
              <Stack spacing={2}>
                {processedTraces.totalDuration && (
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
                    <AccessTimeIcon fontSize="small" color="action" />
                    <Typography variant="body2" color="text.secondary">
                      Total Duration: {formatDuration(processedTraces.totalDuration)}
                    </Typography>
                  </Box>
                )}
                {processedTraces.agents.map((agent, agentIdx) => (
                  <Paper key={agentIdx} variant="outlined" sx={{ overflow: 'hidden' }}>
                    <Box
                      sx={{
                        p: 2,
                        bgcolor: 'primary.50',
                        borderBottom: '1px solid',
                        borderColor: 'divider',
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'space-between',
                      }}
                    >
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
                        <PersonIcon color="primary" />
                        <Typography variant="subtitle1" fontWeight="bold">
                          {agent.agent}
                        </Typography>
                        <Chip
                          size="small"
                          label={`${agent.tasks.length} task${agent.tasks.length !== 1 ? 's' : ''}`}
                          variant="outlined"
                        />
                      </Box>
                      <Chip
                        size="small"
                        icon={<AccessTimeIcon />}
                        label={formatDuration(agent.duration)}
                        color="default"
                      />
                    </Box>
                    <Stack spacing={0} divider={<Divider />}>
                      {agent.tasks.map((task, taskIdx) => {
                        // Try task_complete event first, then fall back to last event with output
                        const completionEvent = task.events.find(
                          (e) => e.type === 'task_complete' || e.type === 'task_completed'
                        );
                        const taskOutput = completionEvent?.output
                          || [...task.events].reverse().find((e) => e.output)?.output;
                        return (
                          <Box key={taskIdx} sx={{ p: 2 }}>
                            <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 1 }}>
                              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, minWidth: 0, flex: 1 }}>
                                <AssignmentIcon fontSize="small" color="action" />
                                <Typography
                                  variant="subtitle2"
                                  fontWeight="medium"
                                  sx={{ wordBreak: 'break-word' }}
                                >
                                  {task.taskName}
                                </Typography>
                              </Box>
                              <Chip
                                size="small"
                                label={formatDuration(task.duration)}
                                sx={{ ml: 1, flexShrink: 0 }}
                              />
                            </Box>
                            {taskOutput ? (
                              <Box sx={{ mt: 1 }}>
                                <PaginatedOutput
                                  content={taskOutput}
                                  pageSize={10000}
                                  enableMarkdown={true}
                                  showCopyButton={true}
                                  maxHeight="300px"
                                  eventType="task_complete"
                                />
                              </Box>
                            ) : (
                              <Typography variant="body2" color="text.secondary" sx={{ fontStyle: 'italic', mt: 0.5 }}>
                                No output captured
                              </Typography>
                            )}
                          </Box>
                        );
                      })}
                    </Stack>
                  </Paper>
                ))}
              </Stack>
            )}

            {/* Timeline View */}
            {viewMode === 'timeline' && (<>
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

            {/* Crew-level Planning Section */}
            {processedTraces.crewPlanningEvents && processedTraces.crewPlanningEvents.length > 0 && (
              <Paper sx={{ mb: 2, overflow: 'hidden', borderLeft: '4px solid', borderLeftColor: 'secondary.main' }}>
                <Box
                  sx={{
                    p: 2,
                    bgcolor: 'secondary.50',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'space-between',
                  }}
                >
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
                    <AssignmentIcon sx={{ color: 'secondary.main' }} />
                    <Typography variant="subtitle1" fontWeight="bold" color="secondary.main">
                      Crew Planning
                    </Typography>
                    <Chip
                      size="small"
                      label={formatDuration(
                        processedTraces.crewPlanningEvents.reduce((total, e) => total + (e.duration || 0), 0) ||
                        (processedTraces.crewPlanningEvents.length > 0
                          ? new Date(processedTraces.crewPlanningEvents[processedTraces.crewPlanningEvents.length - 1].timestamp).getTime() -
                            new Date(processedTraces.crewPlanningEvents[0].timestamp).getTime()
                          : 0)
                      )}
                      color="secondary"
                    />
                  </Box>
                  <Typography variant="body2" color="text.secondary">
                    Task Execution Planner
                  </Typography>
                </Box>

                <Box sx={{ pl: 6, pr: 2, py: 1 }}>
                  {processedTraces.crewPlanningEvents.map((event, eventIdx) => {
                    const hasOutput = !!event.output;
                    const isClickable = hasOutput;

                    return (
                      <Box
                        key={eventIdx}
                        sx={{
                          display: 'flex',
                          alignItems: 'center',
                          gap: 1,
                          py: 0.5,
                          borderLeft: '2px solid',
                          borderColor: 'secondary.200',
                          pl: 2,
                          ml: 1,
                          position: 'relative',
                          cursor: isClickable ? 'pointer' : 'default',
                          '&:hover': {
                            bgcolor: isClickable ? 'action.hover' : 'transparent',
                            '& .output-hint': {
                              opacity: 1
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
                          <AssignmentIcon sx={{ fontSize: 16, color: 'secondary.main' }} />
                        </Box>
                        <Typography
                          variant="body2"
                          sx={{
                            flex: 1,
                            color: isClickable ? 'secondary.main' : 'text.primary',
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
                          <Chip
                            className="output-hint"
                            size="small"
                            label="View Plan"
                            sx={{
                              height: 18,
                              fontSize: '0.65rem',
                              bgcolor: 'secondary.main',
                              color: 'white',
                              opacity: 0.7,
                              transition: 'opacity 0.2s',
                              '& .MuiChip-label': {
                                px: 0.5
                              }
                            }}
                          />
                        )}
                      </Box>
                    );
                  })}
                </Box>
              </Paper>
            )}

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
                    {/* Agent-level events (reasoning/planning) shown before tasks */}
                    {agent.agentEvents && agent.agentEvents.length > 0 && (
                      <Box sx={{ mb: 2 }}>
                        <Box
                          sx={{
                            display: 'flex',
                            alignItems: 'center',
                            gap: 1,
                            p: 1,
                            bgcolor: 'primary.50',
                            borderRadius: 1,
                            borderLeft: '3px solid',
                            borderLeftColor: 'primary.main',
                          }}
                        >
                          <TargetIcon sx={{ color: 'primary.main', fontSize: 20 }} />
                          <Typography
                            variant="body2"
                            fontWeight="medium"
                            sx={{ color: 'primary.main' }}
                          >
                            Agent Planning & Reasoning
                          </Typography>
                          <Chip
                            size="small"
                            label={`${agent.agentEvents.length} event${agent.agentEvents.length !== 1 ? 's' : ''}`}
                            variant="outlined"
                            sx={{ borderColor: 'primary.main', color: 'primary.main' }}
                          />
                        </Box>
                        <Box sx={{ pl: 4, mt: 1 }}>
                          {agent.agentEvents.map((event, eventIdx) => {
                            const hasOutput = !!event.output;
                            const isClickable = isEventClickable(event.type, hasOutput);

                            return (
                              <Box
                                key={eventIdx}
                                sx={{
                                  display: 'flex',
                                  alignItems: 'center',
                                  gap: 1,
                                  py: 0.5,
                                  borderLeft: '2px solid',
                                  borderColor: 'primary.200',
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
                                      Click to view reasoning
                                    </Typography>
                                  </>
                                )}
                              </Box>
                            );
                          })}
                        </Box>
                      </Box>
                    )}

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
                            <Tooltip
                              title={task.taskName.length > 80 ? "Click to view full description" : ""}
                              arrow
                              placement="top"
                            >
                              <Typography
                                variant="body2"
                                fontWeight="medium"
                                onClick={(e) => handleTaskDescriptionClick(task.taskName, task.taskId, e)}
                                sx={{
                                  maxWidth: '500px',
                                  overflow: 'hidden',
                                  textOverflow: 'ellipsis',
                                  whiteSpace: 'nowrap',
                                  cursor: 'pointer',
                                  '&:hover': {
                                    color: 'primary.main',
                                    textDecoration: 'underline'
                                  }
                                }}
                              >
                                {truncateTaskName(task.taskName)}
                              </Typography>
                            </Tooltip>
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
                                const isClickable = isEventClickable(event.type, hasOutput);

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
              <Box key={idx} sx={{ mt: 2 }}>
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
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
              </Box>
            ))}
            </>)}
          </Box>
        ) : (
          <Box sx={{ p: 3, textAlign: 'center' }}>
            <Typography>No trace data available</Typography>
          </Box>
        )}
      </DialogContent>

      {/* Task Description Dialog */}
      <Dialog
        open={!!selectedTaskDescription}
        onClose={() => setSelectedTaskDescription(null)}
        maxWidth="md"
        fullWidth
      >
        {selectedTaskDescription && (
          <>
            <DialogTitle sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <Box>
                <Typography variant="h6">Task Description</Typography>
                {selectedTaskDescription.taskId && (
                  <Typography variant="caption" color="text.secondary">
                    Task ID: {selectedTaskDescription.taskId}
                  </Typography>
                )}
              </Box>
              <IconButton
                onClick={() => setSelectedTaskDescription(null)}
                size="small"
              >
                <CloseIcon />
              </IconButton>
            </DialogTitle>
            <DialogContent dividers>
              {selectedTaskDescription.isLoading ? (
                <Box display="flex" justifyContent="center" alignItems="center" minHeight="100px">
                  <CircularProgress size={24} />
                  <Typography sx={{ ml: 2 }} color="text.secondary">
                    Loading task details...
                  </Typography>
                </Box>
              ) : (
                <Box
                  sx={{
                    maxHeight: '60vh',
                    overflow: 'auto',
                  }}
                >
                  <FormattedTaskDescription
                    description={selectedTaskDescription.fullDescription || selectedTaskDescription.taskName}
                  />
                </Box>
              )}
            </DialogContent>
            <DialogActions>
              <Button
                onClick={() => {
                  navigator.clipboard.writeText(
                    selectedTaskDescription.fullDescription || selectedTaskDescription.taskName
                  );
                }}
                startIcon={<ContentCopyIcon />}
                size="small"
              >
                Copy Description
              </Button>
              <Button onClick={() => setSelectedTaskDescription(null)} size="small">
                Close
              </Button>
            </DialogActions>
          </>
        )}
      </Dialog>

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
                {selectedEvent.type === 'memory_operation' || selectedEvent.type === 'memory_write' || selectedEvent.type === 'memory_retrieval' || selectedEvent.type.includes('memory') ? (
                  <Box sx={{ mb: 2 }}>
                    <Typography variant="subtitle2" gutterBottom sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      <StorageIcon fontSize="small" />
                      Memory Operation Details
                    </Typography>
                    <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1, mb: 1 }}>
                      {/* Show operation type based on event type */}
                      {selectedEvent.type === 'memory_write' && (
                        <Chip
                          icon={<StorageIcon />}
                          label="Write"
                          size="small"
                          color="primary"
                          variant="filled"
                        />
                      )}
                      {selectedEvent.type === 'memory_retrieval' && (
                        <Chip
                          icon={<StorageIcon />}
                          label="Read"
                          size="small"
                          color="success"
                          variant="filled"
                        />
                      )}
                      {/* Extract memory type from description if present (format: "Memory Write (long_term)") */}
                      {(() => {
                        const memTypeMatch = selectedEvent.description.match(/\(([^)]+)\)/);
                        if (memTypeMatch) {
                          const memType = memTypeMatch[1];
                          return (
                            <Chip
                              label={`Type: ${memType}`}
                              size="small"
                              color="secondary"
                              variant="outlined"
                            />
                          );
                        }
                        return null;
                      })()}
                      {/* Extract extra_data from output object for additional details */}
                      {(() => {
                        // The trace output structure is: { content: ..., extra_data: {...} }
                        // Memory info is in extra_data
                        const output = selectedEvent.output;
                        if (typeof output === 'object' && output !== null) {
                          const outputObj = output as Record<string, unknown>;
                          const extraData = outputObj.extra_data as Record<string, unknown> | undefined;

                          const chips: JSX.Element[] = [];

                          // Check extra_data for operation, memory_type, etc.
                          if (extraData) {
                            if (extraData.operation && !selectedEvent.description.includes('Write') && !selectedEvent.description.includes('Read')) {
                              chips.push(
                                <Chip
                                  key="operation"
                                  label={`Operation: ${extraData.operation as string}`}
                                  size="small"
                                  color="info"
                                  variant="outlined"
                                />
                              );
                            }
                            if (extraData.memory_type && !selectedEvent.description.includes('(')) {
                              chips.push(
                                <Chip
                                  key="memory_type"
                                  label={`Type: ${extraData.memory_type as string}`}
                                  size="small"
                                  color="secondary"
                                  variant="outlined"
                                />
                              );
                            }
                            if (extraData.results_count !== undefined) {
                              chips.push(
                                <Chip
                                  key="results_count"
                                  label={`Results: ${extraData.results_count as number}`}
                                  size="small"
                                  color="default"
                                  variant="outlined"
                                />
                              );
                            }
                            if (extraData.query) {
                              chips.push(
                                <Chip
                                  key="query"
                                  label="Query included"
                                  size="small"
                                  color="default"
                                  variant="outlined"
                                />
                              );
                            }
                            if (extraData.backend) {
                              chips.push(
                                <Chip
                                  key="backend"
                                  label={`Backend: ${extraData.backend as string}`}
                                  size="small"
                                  color="default"
                                  variant="outlined"
                                />
                              );
                            }
                          }

                          // Also check top-level fields on output if no extra_data
                          if (chips.length === 0) {
                            if ('operation' in outputObj && !selectedEvent.description.includes('Write') && !selectedEvent.description.includes('Read')) {
                              chips.push(
                                <Chip
                                  key="operation"
                                  label={`Operation: ${outputObj.operation as string}`}
                                  size="small"
                                  color="info"
                                  variant="outlined"
                                />
                              );
                            }
                            if ('memory_type' in outputObj && !selectedEvent.description.includes('(')) {
                              chips.push(
                                <Chip
                                  key="memory_type"
                                  label={`Type: ${outputObj.memory_type as string}`}
                                  size="small"
                                  color="secondary"
                                  variant="outlined"
                                />
                              );
                            }
                          }

                          return chips.length > 0 ? <>{chips}</> : null;
                        }
                        return null;
                      })()}
                    </Box>
                    {/* Show query if available for memory reads */}
                    {(() => {
                      const output = selectedEvent.output;
                      if (typeof output === 'object' && output !== null) {
                        const outputObj = output as Record<string, unknown>;
                        const extraData = outputObj.extra_data as Record<string, unknown> | undefined;
                        const query = extraData?.query || outputObj.query;
                        if (query) {
                          return (
                            <Box sx={{ mb: 1, p: 1, bgcolor: 'action.hover', borderRadius: 1 }}>
                              <Typography variant="caption" color="text.secondary" display="block">
                                Query:
                              </Typography>
                              <Typography variant="body2" sx={{ fontFamily: 'monospace', fontSize: '0.85rem' }}>
                                {String(query).substring(0, 200)}{String(query).length > 200 ? '...' : ''}
                              </Typography>
                            </Box>
                          );
                        }
                      }
                      return null;
                    })()}
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

                {/* Special formatting for guardrail events */}
                {selectedEvent.type === 'guardrail' || selectedEvent.type.includes('guardrail') ? (
                  <Box>
                    <Typography variant="subtitle2" gutterBottom>
                      Guardrail Validation Details
                    </Typography>
                    {selectedEvent.extraData && (
                      <Box sx={{ mb: 2 }}>
                        {(() => {
                          const extraData = selectedEvent.extraData as Record<string, unknown>;
                          const success = extraData.success;
                          const validationValid = extraData.validation_valid;
                          const validationMessage = extraData.validation_message;
                          const guardrailDescription = extraData.guardrail_description;
                          const taskName = extraData.task_name;
                          const retryCount = extraData.retry_count;

                          return (
                            <>
                              {/* Status chip */}
                              <Chip
                                label={success === true || validationValid === true ? 'Passed' : success === false || validationValid === false ? 'Failed' : 'Unknown'}
                                sx={{ mr: 1, mb: 1 }}
                                size="small"
                                color={success === true || validationValid === true ? 'success' : success === false || validationValid === false ? 'error' : 'default'}
                              />
                              {taskName && (
                                <Chip
                                  label={`Task: ${taskName}`}
                                  sx={{ mr: 1, mb: 1 }}
                                  size="small"
                                  color="info"
                                />
                              )}
                              {retryCount !== undefined && Number(retryCount) > 0 && (
                                <Chip
                                  label={`Retries: ${retryCount}`}
                                  sx={{ mr: 1, mb: 1 }}
                                  size="small"
                                  color="warning"
                                />
                              )}
                              {/* Validation criteria */}
                              {guardrailDescription && (
                                <Box sx={{ mt: 2, p: 2, bgcolor: 'grey.100', borderRadius: 1 }}>
                                  <Typography variant="caption" color="text.secondary" display="block" gutterBottom>
                                    Validation Criteria:
                                  </Typography>
                                  <Typography variant="body2" sx={{ whiteSpace: 'pre-wrap' }}>
                                    {String(guardrailDescription)}
                                  </Typography>
                                </Box>
                              )}
                              {/* Validation result message */}
                              {validationMessage && (
                                <Box sx={{ mt: 2, p: 2, bgcolor: validationValid === true ? 'success.light' : validationValid === false ? 'error.light' : 'grey.100', borderRadius: 1 }}>
                                  <Typography variant="caption" color="text.secondary" display="block" gutterBottom>
                                    Validation Result:
                                  </Typography>
                                  <Typography variant="body2" sx={{ whiteSpace: 'pre-wrap' }}>
                                    {String(validationMessage)}
                                  </Typography>
                                </Box>
                              )}
                            </>
                          );
                        })()}
                      </Box>
                    )}
                  </Box>
                ) : null}

                {/* Paginated output display - prevents browser crash on large content */}
                <PaginatedOutput
                  content={selectedEvent.output}
                  pageSize={10000}
                  enableMarkdown={true}
                  showCopyButton={true}
                  maxHeight="55vh"
                  eventType={selectedEvent.type}
                />
              </Box>
            </DialogContent>
            <DialogActions>
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