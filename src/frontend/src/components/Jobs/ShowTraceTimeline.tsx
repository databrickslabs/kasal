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
import WarningAmberIcon from '@mui/icons-material/WarningAmber';
import ErrorOutlineIcon from '@mui/icons-material/ErrorOutline';
import StorageIcon from '@mui/icons-material/Storage';
import AssignmentIcon from '@mui/icons-material/Assignment';
import PersonIcon from '@mui/icons-material/Person';
import BuildIcon from '@mui/icons-material/Build';
import TargetIcon from '@mui/icons-material/TrackChanges';
import { ShowTraceProps } from '../../types/trace';

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
        ((t.event_source === 'crew' && (t.event_type === 'crew_completed' || t.event_type === 'execution_completed')) ||
         (t.event_source === 'flow' && t.event_type === 'flow_completed'))
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

    // Build task_id -> task_name mapping for parallel task support
    const taskIdToName = new Map<string, string>();
    // Build task_id -> agent mapping to fix agent attribution for events
    const taskIdToAgent = new Map<string, string>();
    // Also build task time ranges for temporal matching (task_id -> {start, end})
    const taskTimeRanges = new Map<string, { start: number; end: number; name: string }>();

    sorted.forEach(trace => {
      const taskId = getTaskId(trace);
      if (trace.event_type === 'task_started' && taskId) {
        let taskName: string | null = null;
        let agentRole: string | null = null;

        // Extract task name and agent from trace_metadata first
        if (trace.trace_metadata && typeof trace.trace_metadata === 'object') {
          const metadata = trace.trace_metadata as Record<string, unknown>;
          if (metadata.task_name) {
            taskName = metadata.task_name as string;
          }
          if (metadata.agent_role) {
            agentRole = metadata.agent_role as string;
          }
        }
        // Fallback to extra_data
        if (!taskName && trace.extra_data && typeof trace.extra_data === 'object') {
          const extraData = trace.extra_data as Record<string, unknown>;
          if (extraData.task_name) {
            taskName = extraData.task_name as string;
          }
          if (!agentRole && extraData.agent_role) {
            agentRole = extraData.agent_role as string;
          }
        }
        // Also use event_source as agent (task_started events have correct agent)
        if (!agentRole && trace.event_source && trace.event_source !== 'task' && trace.event_source !== 'Unknown Agent') {
          agentRole = trace.event_source;
        }

        if (taskName) {
          taskIdToName.set(taskId, taskName);
          // Initialize time range with start time
          taskTimeRanges.set(taskId, {
            start: new Date(trace.created_at).getTime(),
            end: Infinity, // Will be set when task_completed is found
            name: taskName
          });
        }
        if (agentRole) {
          taskIdToAgent.set(taskId, agentRole);
        }
      } else if (trace.event_type === 'task_completed' && taskId) {
        // Update end time for task time range
        const range = taskTimeRanges.get(taskId);
        if (range) {
          range.end = new Date(trace.created_at).getTime();
        }
      }
    });

    // Second pass: group traces by agent
    sorted.forEach(trace => {
      // Skip global events (crew/flow), task orchestration events, task events, and Task Execution Planner for agent grouping
      // Task Execution Planner is handled separately as crew-level planning
      if (trace.event_source === 'crew' ||
          trace.event_source === 'flow' ||
          trace.event_source === 'task' ||
          trace.event_source === 'Task Orchestrator' ||
          trace.event_source === 'Task Execution Planner' ||
          trace.event_context === 'task_management') {
        return;
      }

      let agent = trace.event_source || 'Unknown Agent';

      // Use task_id to determine correct agent (fixes misattributed events)
      const traceTaskId = getTaskId(trace);
      if (traceTaskId && taskIdToAgent.has(traceTaskId)) {
        agent = taskIdToAgent.get(traceTaskId)!;
      }

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

        let taskKey = 'Processing Task'; // Default fallback
        const traceTaskId = getTaskId(trace);

        // Primary method: Use task_id to determine which task this trace belongs to
        if (traceTaskId && taskIdToName.has(traceTaskId)) {
          // Check if we already have a unique key for this task_id
          if (taskIdToUniqueKey.has(traceTaskId)) {
            taskKey = taskIdToUniqueKey.get(traceTaskId) ?? taskKey;
          } else {
            // Create a unique key for this task
            const baseName = taskIdToName.get(traceTaskId) ?? 'Processing Task';
            // Check if this base name already exists (same task name running in parallel)
            if (taskMap.has(baseName)) {
              taskCounter++;
              taskKey = `${baseName} (${taskCounter})`;
            } else {
              taskKey = baseName;
            }
            taskIdToUniqueKey.set(traceTaskId, taskKey);
          }
        } else if (trace.event_type === 'task_started') {
          // For task_started events without task_id in the map, extract name and create entry
          let newTaskName: string | null = null;

          if (trace.trace_metadata) {
            const metadata = trace.trace_metadata as Record<string, unknown>;
            const taskName = metadata.task_name as string;
            if (taskName) {
              newTaskName = taskName;
            }
          }

          if (!newTaskName && trace.extra_data) {
            const extraData = trace.extra_data as Record<string, unknown>;
            const taskName = extraData.task_name as string;
            if (taskName) {
              newTaskName = taskName;
            }
          }

          if (newTaskName) {
            if (taskMap.has(newTaskName)) {
              taskCounter++;
              taskKey = `${newTaskName} (${taskCounter})`;
            } else {
              taskKey = newTaskName;
            }
            if (traceTaskId) {
              taskIdToUniqueKey.set(traceTaskId, taskKey);
            }
          } else {
            taskKey = 'Processing Task';
          }
        } else {
          // Fallback: try to find task using temporal matching with task time ranges
          const traceTime = new Date(trace.created_at).getTime();

          // First, try matching using taskTimeRanges (most accurate)
          let foundMatch = false;
          for (const [taskId, range] of taskTimeRanges.entries()) {
            // Check if trace time falls within this task's time range (with 1s buffer)
            if (traceTime >= range.start - 1000 && traceTime <= range.end + 1000) {
              // Check if we already have a unique key for this task_id
              if (taskIdToUniqueKey.has(taskId)) {
                taskKey = taskIdToUniqueKey.get(taskId) ?? taskKey;
              } else {
                // Create a unique key for this task
                const baseName = range.name;
                if (taskMap.has(baseName)) {
                  taskCounter++;
                  taskKey = `${baseName} (${taskCounter})`;
                } else {
                  taskKey = baseName;
                }
                taskIdToUniqueKey.set(taskId, taskKey);
              }
              foundMatch = true;
              break;
            }
          }

          // If no match from time ranges, try using taskDescriptions as fallback
          if (!foundMatch) {
            const taskEntries = Array.from(taskDescriptions.entries());
            for (const [taskContext, taskDesc] of taskEntries) {
              const taskCompletion = sorted.find(t =>
                t.event_source === 'task' &&
                t.event_type === 'task_completed' &&
                t.event_context === taskContext
              );

              if (taskCompletion) {
                const taskEndTime = new Date(taskCompletion.created_at).getTime();
                if (traceTime <= taskEndTime + 1000) {
                  const taskAgent = agentTaskMap.get(taskContext);
                  if (!taskAgent || taskAgent === agentName) {
                    taskKey = taskDesc;
                    foundMatch = true;
                    break;
                  }
                }
              }
            }
          }

          // If still no match but we have a most recent task, use it as fallback
          // This handles events that occur just after a task starts
          if (!foundMatch && taskTimeRanges.size > 0) {
            // Find the most recent task that started before this trace
            let mostRecentTask: { taskId: string; range: { start: number; end: number; name: string } } | null = null;
            for (const [taskId, range] of taskTimeRanges.entries()) {
              if (range.start <= traceTime) {
                if (!mostRecentTask || range.start > mostRecentTask.range.start) {
                  mostRecentTask = { taskId, range };
                }
              }
            }
            if (mostRecentTask) {
              // Use the most recent task
              if (taskIdToUniqueKey.has(mostRecentTask.taskId)) {
                taskKey = taskIdToUniqueKey.get(mostRecentTask.taskId) ?? taskKey;
              } else {
                taskKey = mostRecentTask.range.name;
                taskIdToUniqueKey.set(mostRecentTask.taskId, taskKey);
              }
            }
          }
          // If no match found after all attempts, taskKey remains 'Processing Task' (default)
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
            // Tool usage event - backend provides proper operation type and tool name
            // Extract tool name from event_context (format: "tool:{tool_name}")
            const toolName = trace.event_context?.replace('tool:', '') || 'Tool';

            // Check operation type from trace_metadata (backend saves extra_data as trace_metadata)
            const operation = trace.trace_metadata && typeof trace.trace_metadata === 'object'
              ? (trace.trace_metadata as Record<string, unknown>).operation as string
              : undefined;

            if (operation === 'tool_started') {
              eventType = 'tool';
              description = `${toolName} (input)`;
            } else if (operation === 'tool_finished') {
              eventType = 'tool_result';
              description = `${toolName} (output)`;
            } else {
              // Fallback if operation not specified
              eventType = 'tool';
              description = toolName;
            }
          } else if (trace.event_type === 'llm_response') {
            // LLM response from agent execution - show with output length
            eventType = 'llm_response';
            let outputLen = 0;
            if (trace.output) {
              if (typeof trace.output === 'string') {
                outputLen = trace.output.length;
              } else if (typeof trace.output === 'object' && 'content' in trace.output) {
                outputLen = String((trace.output as Record<string, unknown>).content || '').length;
              }
            }
            // Also check trace_metadata for output_length from backend
            if (trace.trace_metadata && typeof trace.trace_metadata === 'object') {
              const metadata = trace.trace_metadata as Record<string, unknown>;
              if (metadata.output_length) {
                outputLen = metadata.output_length as number;
              }
            }
            description = outputLen > 0 ? `LLM Response (${outputLen.toLocaleString()} chars)` : 'LLM Response';
          } else if (trace.event_type === 'agent_reasoning' || trace.event_type === 'agent_reasoning_error') {
            // Agent reasoning/thinking process - make clickable to show reasoning details
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
            if (operation === 'reasoning_started') {
              return null; // Filter out started events
            }

            eventType = 'agent_reasoning';
            if (trace.event_type === 'agent_reasoning_error') {
              description = 'Reasoning Failed';
            } else {
              // Get the actual content to determine description
              const outputStr = typeof trace.output === 'string'
                ? trace.output
                : (typeof trace.output === 'object' && 'content' in trace.output)
                  ? (trace.output as Record<string, unknown>).content as string
                  : '';

              // Check if it's a planning event
              if (outputStr && outputStr.toLowerCase().includes('plan')) {
                description = 'Agent Planning';
              } else if (outputStr && outputStr.length > 100) {
                // It's a detailed reasoning with content
                description = 'Agent Reasoning';
              } else {
                // Fallback - show as reasoning
                description = 'Agent Reasoning';
              }
            }
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

            // Check trace_metadata (extra_data) for step type information
            let stepType = '';
            if (trace.trace_metadata && typeof trace.trace_metadata === 'object') {
              const metadata = trace.trace_metadata as Record<string, unknown>;
              stepType = (metadata.step_type as string) || '';
            }
            // Fallback to extra_data
            if (!stepType && trace.extra_data && typeof trace.extra_data === 'object') {
              const extraData = trace.extra_data as Record<string, unknown>;
              stepType = (extraData.step_type as string) || '';
            }

            // Removed text parsing for "Tool:" and "ToolResult" - backend now sends proper tool_usage events
            // This prevents duplicate/synthetic events with incorrect timestamps
            if (stepType === 'AgentFinish') {
              eventType = 'agent_complete';
              description = 'Final Answer';
            } else if (stepType === 'AgentStart') {
              eventType = 'agent_start';
              description = 'Task Started';
            } else if (output && output.length > 100) {
              // Long output usually means the agent is providing results
              eventType = 'agent_output';
              description = 'Agent Output';
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
          } else if (trace.event_type === 'memory_write_started' || trace.event_type === 'memory_retrieval_started') {
            // Skip started events - we'll show the completed events instead
            // This prevents duplicate entries (started + completed)
            return null;
          } else if (trace.event_type === 'memory_write') {
            // Memory write completed - extract memory type for display
            eventType = 'memory_write';
            // Helper to extract memory type from multiple sources
            const extractMemoryType = (): string => {
              // 1. Check trace_metadata
              if (trace.trace_metadata && typeof trace.trace_metadata === 'object') {
                const metadata = trace.trace_metadata as Record<string, unknown>;
                if (metadata.memory_type && metadata.memory_type !== 'memory') {
                  return metadata.memory_type as string;
                }
              }
              // 2. Check extra_data
              if (trace.extra_data && typeof trace.extra_data === 'object') {
                const extraData = trace.extra_data as Record<string, unknown>;
                if (extraData.memory_type && extraData.memory_type !== 'memory') {
                  return extraData.memory_type as string;
                }
              }
              // 3. Extract from event_context (e.g., "saved_long_term", "saving_short_term")
              if (trace.event_context) {
                const contextMatch = trace.event_context.match(/(?:saved_|saving_|retrieved_|memory_query\[)(\w+)/);
                if (contextMatch) {
                  return contextMatch[1];
                }
              }
              return 'memory';
            };
            const memoryType = extractMemoryType();
            // Format memory type for display (short_term -> Short-Term Memory)
            const formatMemoryType = (type: string): string => {
              if (type === 'short_term') return 'Short-Term Memory';
              if (type === 'long_term') return 'Long-Term Memory';
              if (type === 'entity') return 'Entity Memory';
              return type;
            };
            description = `Memory Write (${formatMemoryType(memoryType)})`;
          } else if (trace.event_type === 'memory_retrieval') {
            // Memory retrieval completed - extract memory type and results count for display
            eventType = 'memory_retrieval';
            // Helper to extract memory type from multiple sources
            const extractMemoryType = (): string => {
              // 1. Check trace_metadata
              if (trace.trace_metadata && typeof trace.trace_metadata === 'object') {
                const metadata = trace.trace_metadata as Record<string, unknown>;
                if (metadata.memory_type && metadata.memory_type !== 'memory') {
                  return metadata.memory_type as string;
                }
              }
              // 2. Check extra_data
              if (trace.extra_data && typeof trace.extra_data === 'object') {
                const extraData = trace.extra_data as Record<string, unknown>;
                if (extraData.memory_type && extraData.memory_type !== 'memory') {
                  return extraData.memory_type as string;
                }
              }
              // 3. Extract from event_context (e.g., "memory_query[long_term]")
              if (trace.event_context) {
                const contextMatch = trace.event_context.match(/(?:saved_|saving_|retrieved_|memory_query\[)(\w+)/);
                if (contextMatch) {
                  return contextMatch[1];
                }
              }
              return 'memory';
            };
            const memoryType = extractMemoryType();
            // Format memory type for display
            const formatMemoryType = (type: string): string => {
              if (type === 'short_term') return 'Short-Term Memory';
              if (type === 'long_term') return 'Long-Term Memory';
              if (type === 'entity') return 'Entity Memory';
              return type;
            };
            let resultsCount = 0;
            if (trace.trace_metadata && typeof trace.trace_metadata === 'object') {
              const metadata = trace.trace_metadata as Record<string, unknown>;
              if (metadata.results_count) resultsCount = metadata.results_count as number;
            } else if (trace.extra_data && typeof trace.extra_data === 'object') {
              const extraData = trace.extra_data as Record<string, unknown>;
              if (extraData.results_count) resultsCount = extraData.results_count as number;
            }
            description = resultsCount > 0
              ? `Memory Read (${formatMemoryType(memoryType)}) - ${resultsCount} results`
              : `Memory Read (${formatMemoryType(memoryType)})`;
          } else if (trace.event_type === 'memory_retrieval_completed') {
            // Skip this event - memory_retrieval already shows the read completed
            // This prevents duplicate entries
            return null;
          } else if (trace.event_type === 'memory_context_retrieved') {
            // Aggregated memory context - this is the ACTUAL memory content from all sources
            eventType = 'memory_context';
            let contentLength = 0;
            if (trace.extra_data && typeof trace.extra_data === 'object') {
              const extraData = trace.extra_data as Record<string, unknown>;
              if (extraData.content_length) contentLength = extraData.content_length as number;
            }
            description = contentLength > 0
              ? `Memory Context Retrieved (${contentLength} chars)`
              : 'Memory Context Retrieved';
          } else if (trace.event_type === 'memory_operation') {
            // Legacy/generic memory operation - try to extract details
            eventType = 'memory_operation';
            let memoryType = '';
            let operation = '';
            if (trace.trace_metadata && typeof trace.trace_metadata === 'object') {
              const metadata = trace.trace_metadata as Record<string, unknown>;
              if (metadata.memory_type) memoryType = metadata.memory_type as string;
              if (metadata.operation) operation = metadata.operation as string;
            } else if (trace.extra_data && typeof trace.extra_data === 'object') {
              const extraData = trace.extra_data as Record<string, unknown>;
              if (extraData.memory_type) memoryType = extraData.memory_type as string;
              if (extraData.operation) operation = extraData.operation as string;
            }
            // Build description from available info
            if (operation && memoryType) {
              const opLabel = operation.includes('query') || operation.includes('retriev') ? 'Read' : 'Write';
              description = `Memory ${opLabel} (${memoryType})`;
            } else if (trace.event_context) {
              if (trace.event_context.includes('query')) {
                description = memoryType ? `Memory Query (${memoryType})` : 'Memory Query';
              } else if (trace.event_context.includes('sav')) {
                description = memoryType ? `Memory Save (${memoryType})` : 'Memory Save';
              } else {
                description = memoryType ? `Memory Operation (${memoryType})` : 'Memory Operation';
              }
            } else {
              description = memoryType ? `Memory Operation (${memoryType})` : 'Memory Operation';
            }
          } else if (trace.event_type === 'memory_backend_error') {
            // Databricks index validation error - indexes missing or provisioning
            eventType = 'memory_backend_error';
            let title = 'Memory Backend Error';
            let errorType = '';
            if (trace.trace_metadata && typeof trace.trace_metadata === 'object') {
              const metadata = trace.trace_metadata as Record<string, unknown>;
              if (metadata.title) title = metadata.title as string;
              if (metadata.error_type) errorType = metadata.error_type as string;
            } else if (trace.extra_data && typeof trace.extra_data === 'object') {
              const extraData = trace.extra_data as Record<string, unknown>;
              if (extraData.title) title = extraData.title as string;
              if (extraData.error_type) errorType = extraData.error_type as string;
            }
            // Provide descriptive message based on error type
            if (errorType === 'missing_indexes') {
              description = '⚠️ Databricks Indexes Not Found';
            } else if (errorType === 'provisioning_indexes') {
              description = '⏳ Databricks Indexes Still Provisioning';
            } else {
              description = title;
            }
          } else if (trace.event_type === 'knowledge_operation') {
            eventType = 'knowledge_operation';
            description = 'Knowledge Operation';
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
          } else if (trace.event_type === 'rate_limit') {
            eventType = 'rate_limit';
            // Extract rate limit details from trace_metadata or extra_data
            let model = '';
            let attempt = '';
            if (trace.trace_metadata && typeof trace.trace_metadata === 'object') {
              const metadata = trace.trace_metadata as Record<string, unknown>;
              model = (metadata.model as string) || '';
              attempt = metadata.attempt ? `(attempt ${metadata.attempt})` : '';
            }
            description = model
              ? `Rate Limit: ${model} ${attempt}`.trim()
              : `Rate Limit ${attempt}`.trim();
          } else if (trace.event_type === 'task_failed') {
            eventType = 'task_failed';
            // Extract error details from extra_data or output - show full message, no truncation
            let errorMsg = 'Task Failed';
            if (trace.extra_data && typeof trace.extra_data === 'object') {
              const extraData = trace.extra_data as Record<string, unknown>;
              const error = extraData.error as string;
              if (error) {
                errorMsg = error;
              }
            } else if (trace.output) {
              // Try to extract from output content
              const outputStr = typeof trace.output === 'string'
                ? trace.output
                : (trace.output as Record<string, unknown>).content as string || '';
              if (outputStr && outputStr.includes('failed:')) {
                const failedPart = outputStr.split('failed:')[1]?.trim();
                if (failedPart) {
                  errorMsg = failedPart;
                }
              }
            }
            description = `❌ ${errorMsg}`;
          } else if (trace.event_type === 'llm_request') {
            eventType = 'llm_request';
            // Extract prompt length for display
            let promptLength = 0;
            if (trace.output) {
              const outputStr = typeof trace.output === 'string'
                ? trace.output
                : JSON.stringify(trace.output);
              promptLength = outputStr.length;
            }
            // Check extra_data for prompt_length field
            if (trace.extra_data && typeof trace.extra_data === 'object') {
              const extraData = trace.extra_data as Record<string, unknown>;
              if (typeof extraData.prompt_length === 'number') {
                promptLength = extraData.prompt_length;
              }
            }
            description = `LLM Request (${promptLength.toLocaleString()} chars)`;
          } else if (trace.event_type === 'llm_response') {
            eventType = 'llm_response';
            // Extract response length for display
            let responseLength = 0;
            if (trace.output) {
              const outputStr = typeof trace.output === 'string'
                ? trace.output
                : JSON.stringify(trace.output);
              responseLength = outputStr.length;
            }
            // Check extra_data for output_length field
            if (trace.extra_data && typeof trace.extra_data === 'object') {
              const extraData = trace.extra_data as Record<string, unknown>;
              if (typeof extraData.output_length === 'number') {
                responseLength = extraData.output_length;
              }
            }
            description = `LLM Response (${responseLength.toLocaleString()} chars)`;
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

          // Extract extra_data for detailed event information (guardrails, etc.)
          const extraData = trace.extra_data && typeof trace.extra_data === 'object'
            ? trace.extra_data as Record<string, unknown>
            : undefined;

          return {
            type: eventType,
            description,
            timestamp,
            duration,
            output: outputContent,
            extraData
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

      // Process agent-level traces into agentEvents
      const agentEvents: TraceEvent[] = agentLevelTraces.map((trace, idx) => {
        const timestamp = new Date(trace.created_at);
        const nextTrace = agentLevelTraces[idx + 1];
        const duration = nextTrace
          ? new Date(nextTrace.created_at).getTime() - timestamp.getTime()
          : undefined;

        // Determine event type and description
        let eventType = 'agent_reasoning';
        let description = 'Agent Reasoning';

        if (trace.event_type === 'agent_reasoning_error') {
          description = 'Reasoning Failed';
        } else {
          // Get the actual content to determine description
          const outputStr = typeof trace.output === 'string'
            ? trace.output
            : (typeof trace.output === 'object' && trace.output !== null && 'content' in trace.output)
              ? (trace.output as Record<string, unknown>).content as string
              : '';

          // Check if it's a planning event
          if (outputStr && outputStr.toLowerCase().includes('plan')) {
            description = 'Agent Planning';
          } else if (outputStr && outputStr.length > 100) {
            description = 'Agent Reasoning';
          }
        }

        // Extract the actual content from the output JSON structure
        let outputContent: string | Record<string, unknown> | undefined = trace.output;
        if (trace.output && typeof trace.output === 'object' && 'content' in trace.output) {
          const content = (trace.output as Record<string, unknown>).content;
          if (typeof content === 'string' || (typeof content === 'object' && content !== null)) {
            outputContent = content as string | Record<string, unknown>;
          }
        }

        // Extract extra_data for detailed event information
        const extraData = trace.extra_data && typeof trace.extra_data === 'object'
          ? trace.extra_data as Record<string, unknown>
          : undefined;

        return {
          type: eventType,
          description,
          timestamp,
          duration,
          output: outputContent,
          extraData
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

    switch (type) {
      case 'tool':
        // Tool request (input) - blue to indicate outgoing call
        return <TerminalIcon {...iconProps} color="primary" />;
      case 'tool_result':
        // Tool response (output) - green to indicate received result
        return <TerminalIcon {...iconProps} color="success" />;
      case 'tool_usage':
        return <TerminalIcon {...iconProps} color="action" />;
      case 'llm':
        return <PlayCircleIcon {...iconProps} color="primary" />;
      case 'llm_response':
        return <PlayCircleIcon {...iconProps} color="success" />;
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
      case 'memory_write':
        // Memory write events - use storage icon with primary color for write operations
        return <StorageIcon {...iconProps} color="primary" />;
      case 'memory_retrieval':
        // Memory retrieval events - use storage icon with success color for read operations
        return <StorageIcon {...iconProps} color="success" />;
      case 'memory_context':
        // Aggregated memory context - use storage icon with info color
        return <StorageIcon {...iconProps} color="info" />;
      case 'memory_operation':
        // Legacy memory operation - use storage icon with action color
        return <StorageIcon {...iconProps} color="action" />;
      case 'memory_backend_error':
        // Memory backend error (e.g., Databricks indexes missing or provisioning)
        return <ErrorOutlineIcon {...iconProps} color="error" />;
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
      case 'rate_limit':
        return <WarningAmberIcon {...iconProps} color="warning" />;
      case 'task_failed':
      case 'error':
        return <ErrorOutlineIcon {...iconProps} color="error" />;
      default:
        return <span style={{ fontSize: 16 }}>•</span>;
    }
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
                            const isClickable = hasOutput && (
                              event.type === 'agent_reasoning' ||
                              event.type.includes('reasoning')
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
                                // Make all events with output clickable, including memory operations, guardrails, and reasoning
                                const isClickable = hasOutput && (
                                  event.type === 'llm' ||
                                  event.type === 'llm_request' ||
                                  event.type === 'llm_response' ||
                                  event.type === 'agent_complete' ||
                                  event.type === 'agent_output' ||
                                  event.type === 'tool_result' ||
                                  event.type === 'task_complete' ||
                                  event.type === 'memory_operation' ||
                                  event.type === 'memory_write' ||
                                  event.type === 'memory_retrieval' ||
                                  event.type === 'tool_usage' ||
                                  event.type === 'knowledge_operation' ||
                                  event.type === 'agent_execution' ||
                                  event.type === 'guardrail' ||
                                  event.type === 'agent_reasoning' ||
                                  // Also check for underscore versions and partial matches
                                  event.type.includes('memory') ||
                                  event.type.includes('tool') ||
                                  event.type.includes('knowledge') ||
                                  event.type.includes('guardrail') ||
                                  event.type.includes('reasoning')
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