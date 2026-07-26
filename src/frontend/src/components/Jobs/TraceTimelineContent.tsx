import React, { memo, useMemo, useState } from 'react';
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
import TimelineIcon from '@mui/icons-material/Timeline';
import StorageIcon from '@mui/icons-material/Storage';
import AssignmentIcon from '@mui/icons-material/Assignment';
import PersonIcon from '@mui/icons-material/Person';
import BuildIcon from '@mui/icons-material/Build';
import TargetIcon from '@mui/icons-material/TrackChanges';
import TuneIcon from '@mui/icons-material/Tune';
import ChevronRightIcon from '@mui/icons-material/ChevronRight';
import {
  isEventClickable,
  getEventIcon as getEventIconConfig,
} from './traceEventProcessors';
import { PaginatedOutput } from '../Common';
import { ProcessedTraces, RunConfig, TimelineItem } from '../../types/trace';

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

  if (!description.includes('Task Number') && !description.includes('task_description')) {
    return null;
  }

  const result: { header: string; tasks: ParsedTask[]; footer: string } = {
    header: '',
    tasks: [],
    footer: ''
  };

  const headerMatch = description.match(/^(.*?)(?=Task Number \d)/s);
  if (headerMatch) {
    result.header = headerMatch[1].trim();
  }

  const footerMatch = description.match(/Create the most descriptive plan.*$/s);
  if (footerMatch) {
    result.footer = footerMatch[0].trim();
  }

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

    const titleMatch = block.match(/Task Number (\d+)\s*-\s*([^\n"]+)/);
    if (titleMatch) {
      task.taskNumber = parseInt(titleMatch[1], 10);
      task.taskTitle = titleMatch[2].trim();
    }

    const descMatch = block.match(/"task_description":\s*([^\n]*(?:\n(?!"task_expected_output")[^\n]*)*)/);
    if (descMatch) {
      task.taskDescription = descMatch[1].trim().replace(/^["']|["']$/g, '');
    }

    const outputMatch = block.match(/"task_expected_output":\s*([^\n]*(?:\n(?!"agent":)[^\n]*)*)/);
    if (outputMatch) {
      task.expectedOutput = outputMatch[1].trim().replace(/^["']|["']$/g, '');
    }

    const agentMatch = block.match(/"agent":\s*([^\n]+)/);
    if (agentMatch) {
      task.agent = agentMatch[1].trim().replace(/^["']|["']$/g, '');
    }

    const goalMatch = block.match(/"agent_goal":\s*([^\n]+)/);
    if (goalMatch) {
      task.agentGoal = goalMatch[1].trim().replace(/^["']|["']$/g, '');
    }

    const toolsMatch = block.match(/"task_tools":\s*\[([^\]]*)\]/s);
    if (toolsMatch) {
      const toolsContent = toolsMatch[1].trim();
      if (toolsContent) {
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
    return (
      <Typography
        variant="body1"
        sx={{ whiteSpace: 'pre-wrap', wordBreak: 'break-word', lineHeight: 1.6 }}
      >
        {description}
      </Typography>
    );
  }

  return (
    <Box>
      {parsed.header && (
        <Alert severity="info" sx={{ mb: 2 }}>
          <Typography variant="body2">{parsed.header}</Typography>
        </Alert>
      )}

      <Stack spacing={2}>
        {parsed.tasks.map((task) => (
          <Card key={task.taskNumber} variant="outlined" sx={{ borderRadius: 2 }}>
            <CardContent>
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

              <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 2 }}>
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

      {parsed.footer && (
        <Alert severity="success" sx={{ mt: 2 }}>
          <Typography variant="body2" fontWeight="medium">{parsed.footer}</Typography>
        </Alert>
      )}
    </Box>
  );
};

export interface TraceTimelineContentProps {
  processedTraces: ProcessedTraces | null;
  runConfig?: RunConfig;
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

// Below this threshold the duration column renders blank — the time still
// belongs to that row conceptually (sub-50ms rounding noise is acceptable
// slack in the task-span accounting).
const MIN_ROW_DURATION_MS = 50;

// One level of timeline depth (MUI spacing units). The run reads as the
// execution DAG: FLOW STARTED (0) > CREW STARTED (one level in) > agent card
// (two levels in). Depth is applied relative to what is actually above a row,
// so a crew-only run starts at 0 instead of being indented under nothing.
const CREW_ROW_INDENT = 2;

// Shared duration column: ONE right-aligned muted slot on every event row.
// The value is the row's ADDITIVE wall-time slice (own timestamp → next
// visible row; last row → task end) so row durations sum to the task span.
// Intrinsic op times are detail in the row's output dialog, not column values.
const DURATION_COLUMN_SX = {
  minWidth: 56,
  textAlign: 'right',
  fontFamily: 'monospace',
  fontVariantNumeric: 'tabular-nums',
  color: 'text.secondary',
  flexShrink: 0,
} as const;

// Single hover affordance for clickable rows: a muted chevron that fades in.
const OPEN_ICON_SX = {
  fontSize: 16,
  color: 'text.secondary',
  opacity: 0,
  transition: 'opacity 0.15s',
  flexShrink: 0,
} as const;

const getEventIcon = (type: string): JSX.Element => {
  const iconProps = { fontSize: 'small' as const, sx: { fontSize: 16 } };
  const config = getEventIconConfig(type);
  if (config.Component) {
    const IconComponent = config.Component;
    return <IconComponent {...iconProps} color={config.color} />;
  }
  return <span style={{ fontSize: 16 }}>•</span>;
};

const TraceTimelineContent = memo<TraceTimelineContentProps>(({
  processedTraces,
  runConfig: runConfigProp,
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
}) => {
  const [runConfigOpen, setRunConfigOpen] = useState(false);

  // Use runConfig from prop or from processedTraces
  const runConfig = runConfigProp ?? processedTraces?.runConfig;

  // Render order for the timeline. `processTraces` always supplies this, but the
  // component also accepts a hand-built ProcessedTraces (tests, and any caller
  // that only fills in `agents`) — fall back to the pre-spine flat agent list
  // rather than rendering an empty timeline.
  const timelineItems: TimelineItem[] = useMemo(() => {
    if (!processedTraces) return [];
    if (processedTraces.timelineItems) return processedTraces.timelineItems;
    return processedTraces.agents.map((_, agentIdx) => ({
      kind: 'agent' as const, agentIdx, nested: false,
    }));
  }, [processedTraces]);

  // Depth is RELATIVE to what is actually above a row. A flow run nests crews
  // under FLOW STARTED; a plain crew run has no flow row, so its crew header is
  // itself the root and must not be indented under nothing.
  const crewIndent = (processedTraces?.globalEvents.start.length ?? 0) > 0
    ? CREW_ROW_INDENT
    : 0;
  const nestedAgentIndent = crewIndent + CREW_ROW_INDENT;

  // Compact run summary derived from the already-processed traces
  const summaryStats = useMemo(() => {
    if (!processedTraces || processedTraces.agents.length === 0) return null;

    let llmCalls = 0;
    let toolCalls = 0;
    let toolResults = 0;
    let memoryOps = 0;
    let taskCount = 0;

    const countEvent = (type: string) => {
      if (type === 'llm' || type === 'llm_request') llmCalls++;
      else if (type === 'tool' || type === 'mcp_tool') toolCalls++;
      else if (type === 'tool_result' || type === 'mcp_tool_result') toolResults++;
      else if (type.startsWith('memory')) memoryOps++;
    };

    processedTraces.agents.forEach((agent) => {
      agent.tasks.forEach((task) => {
        if (!task.unassigned) taskCount++;
        task.events.forEach((e) => countEvent(e.type));
      });
    });

    return {
      totalDuration: processedTraces.totalDuration,
      agentCount: processedTraces.agents.length,
      taskCount,
      llmCalls,
      // Tool "(input)"/"(output)" rows come in pairs — count calls, falling
      // back to result rows for paths that only emit results.
      toolCalls: toolCalls > 0 ? toolCalls : toolResults,
      memoryOps,
    };
  }, [processedTraces]);

  const summaryItems = useMemo(() => {
    if (!summaryStats) return [];
    const plural = (n: number, word: string) => `${n} ${word}${n !== 1 ? 's' : ''}`;
    const items: string[] = [];
    if (summaryStats.totalDuration != null) {
      items.push(`${formatDuration(summaryStats.totalDuration)} total`);
    }
    items.push(plural(summaryStats.agentCount, 'agent'));
    if (summaryStats.taskCount > 0) items.push(plural(summaryStats.taskCount, 'task'));
    if (summaryStats.llmCalls > 0) items.push(plural(summaryStats.llmCalls, 'LLM call'));
    if (summaryStats.toolCalls > 0) items.push(plural(summaryStats.toolCalls, 'tool call'));
    if (summaryStats.memoryOps > 0) items.push(plural(summaryStats.memoryOps, 'memory op'));
    return items;
  }, [summaryStats, formatDuration]);

  return (
    <Box sx={{ contain: 'content' }}>
      {/* View mode toggle + Run Config button */}
      <Box sx={{ px: 2, pt: 2, pb: 1, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <ToggleButtonGroup
          value={viewMode}
          exclusive
          onChange={(_, newMode) => newMode && setViewMode(newMode)}
          size="small"
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
        {runConfig && (
          <Button
            variant="outlined"
            size="small"
            startIcon={<TuneIcon />}
            onClick={() => setRunConfigOpen(true)}
            sx={{ textTransform: 'none', fontWeight: 500 }}
          >
            Run Config
          </Button>
        )}
      </Box>

      {/* Compact run summary strip */}
      {summaryItems.length > 0 && (
        <Box
          sx={{
            mx: 2,
            mt: 0.5,
            px: 1.5,
            py: 0.75,
            bgcolor: 'action.hover',
            borderRadius: 1,
            display: 'flex',
            alignItems: 'center',
            gap: 0.75,
          }}
        >
          <AccessTimeIcon sx={{ fontSize: 14, color: 'text.secondary' }} />
          <Typography variant="caption" color="text.secondary" sx={{ lineHeight: 1.4 }}>
            {summaryItems.join(' · ')}
          </Typography>
        </Box>
      )}

      {/* Content area */}
      <Box sx={{ p: 0 }}>
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
                        {/* A task-less light/chat run has a single "unassigned"
                            bucket — don't frame it as a crew task. */}
                        {!(agent.tasks.length === 1 && agent.tasks[0].unassigned) && (
                          <Chip
                            size="small"
                            label={`${agent.tasks.length} task${agent.tasks.length !== 1 ? 's' : ''}`}
                            variant="outlined"
                          />
                        )}
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

            {/* Crew spine: FLOW STARTED > (CREW STARTED > agents > CREW COMPLETED) x N.
                One ordered stream rather than a nested tree — depth is carried by
                `nested`, which indents a crew's agents under its banner. */}
            {timelineItems.map((item, itemIdx) => {
              if (item.kind === 'crew-start' || item.kind === 'crew-end') {
                // A crew is a CHILD of the flow, so it sits one level in from the
                // FLOW STARTED / FLOW COMPLETED rows. Start and end are rendered
                // by the same branch so they cannot drift apart in level or style
                // — only the icon and label differ.
                const isStart = item.kind === 'crew-start';
                return (
                  <Box
                    key={`${item.kind}-${itemIdx}`}
                    sx={{
                      ml: crewIndent,
                      mt: isStart && itemIdx > 0 ? 2 : 0,
                      mb: 1,
                      display: 'flex',
                      alignItems: 'center',
                      gap: 1,
                    }}
                  >
                    {isStart
                      ? <PlayCircleIcon color="primary" fontSize="small" />
                      : <CheckCircleIcon color="success" fontSize="small" />}
                    <Typography variant="body2" color="text.secondary">
                      {item.trace.event_type.replace(/_/g, ' ').toUpperCase()}
                    </Typography>
                    {isStart && item.crewName && (
                      <Typography variant="body2" fontWeight="bold">{item.crewName}</Typography>
                    )}
                    <Typography variant="caption" color="text.secondary">
                      {new Date(item.trace.created_at).toLocaleTimeString()}
                    </Typography>
                  </Box>
                );
              }
              const agentIdx = item.agentIdx;
              const agent = processedTraces.agents[agentIdx];
              if (!agent) return null;
              return (
              <Paper key={agentIdx} sx={{ mb: 2, overflow: 'hidden', ...(item.nested ? { ml: nestedAgentIndent } : {}) }}>
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
                  </Box>
                  {/* A task-less light/chat run has a single "unassigned"
                      bucket — don't frame it as a crew task. */}
                  {!(agent.tasks.length === 1 && agent.tasks[0].unassigned) && (
                    <Typography variant="body2" color="text.secondary">
                      {agent.tasks.length} task{agent.tasks.length !== 1 ? 's' : ''}
                    </Typography>
                  )}
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
                            {/* Global timing shown once here — event rows below
                                are offset from this task's own start. */}
                            {processedTraces.globalStart && !task.unassigned && (
                              <Typography variant="caption" color="text.secondary">
                                starts {formatTimeDelta(processedTraces.globalStart, task.startTime)}
                              </Typography>
                            )}
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
                                        '& .row-open-icon': { opacity: 0.6 }
                                      }
                                    }}
                                    onClick={() => isClickable && handleEventClick(event)}
                                  >
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
                                    <Typography variant="caption" sx={DURATION_COLUMN_SX}>
                                      {event.duration != null && event.duration >= MIN_ROW_DURATION_MS
                                        ? formatDuration(event.duration)
                                        : ''}
                                    </Typography>
                                    {/* Always rendered, hidden when the row does
                                        not open: dropping it from the tree let
                                        non-clickable rows reclaim its width, so
                                        their durations sat ~16px right of the
                                        clickable ones and the column looked
                                        ragged. visibility keeps the box. */}
                                    <ChevronRightIcon
                                      className="row-open-icon"
                                      sx={
                                        isClickable
                                          ? OPEN_ICON_SX
                                          : { ...OPEN_ICON_SX, visibility: 'hidden' }
                                      }
                                    />
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
              );
            })}

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
      </Box>

      {/* Run Configuration Dialog */}
      <Dialog
        open={runConfigOpen}
        onClose={() => setRunConfigOpen(false)}
        maxWidth="md"
        fullWidth
      >
        <DialogTitle sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <TuneIcon color="primary" />
            <Typography variant="h6">Run Configuration</Typography>
          </Box>
          <IconButton onClick={() => setRunConfigOpen(false)} size="small">
            <CloseIcon />
          </IconButton>
        </DialogTitle>
        <DialogContent dividers>
          {runConfig && (
            <Box sx={{ maxHeight: '60vh', overflow: 'auto' }}>
              {/* Run Name from crew_inputs */}
              {runConfig.crew_inputs?.run_name != null && (
                <Box sx={{ mb: 2 }}>
                  <Typography variant="caption" color="text.secondary">Run Name</Typography>
                  <Typography variant="subtitle1" fontWeight="bold">
                    {String(runConfig.crew_inputs.run_name)}
                  </Typography>
                </Box>
              )}

              {/* Crew Info */}
              {(runConfig.crew_key || runConfig.crew_id) && (
                <Box sx={{ display: 'flex', gap: 1, mb: 2, flexWrap: 'wrap' }}>
                  {runConfig.crew_key && (
                    <Chip label={`Crew: ${runConfig.crew_key}`} size="small" color="primary" variant="outlined" />
                  )}
                  {runConfig.crew_id && (
                    <Chip label={`ID: ${runConfig.crew_id.substring(0, 8)}...`} size="small" variant="outlined" />
                  )}
                </Box>
              )}

              {/* Agents Section */}
              {runConfig.crew_agents.length > 0 && (
                <Box sx={{ mb: 3 }}>
                  <Typography variant="subtitle1" fontWeight="bold" sx={{ mb: 1, display: 'flex', alignItems: 'center', gap: 0.5 }}>
                    <PersonIcon fontSize="small" color="primary" />
                    Agents ({runConfig.crew_agents.length})
                  </Typography>
                  <Stack spacing={1.5}>
                    {runConfig.crew_agents.map((agent, idx) => (
                      <Card key={agent.id || idx} variant="outlined" sx={{ borderRadius: 2 }}>
                        <CardContent sx={{ py: 1.5, '&:last-child': { pb: 1.5 } }}>
                          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
                            <Chip label={agent.role} size="small" color="secondary" icon={<PersonIcon />} />
                            {agent.delegation_enabled && (
                              <Chip label="Delegation" size="small" variant="outlined" color="info" />
                            )}
                            {agent.max_iter && (
                              <Chip label={`Max Iter: ${agent.max_iter}`} size="small" variant="outlined" />
                            )}
                            {agent.max_rpm && (
                              <Chip label={`Max RPM: ${agent.max_rpm}`} size="small" variant="outlined" />
                            )}
                          </Box>
                          {agent.goal && (
                            <Box sx={{ mb: 1 }}>
                              <Typography variant="caption" color="text.secondary" sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                                <TargetIcon fontSize="inherit" /> Goal
                              </Typography>
                              <Typography variant="body2" sx={{ whiteSpace: 'pre-wrap' }}>
                                {agent.goal}
                              </Typography>
                            </Box>
                          )}
                          {agent.backstory && (
                            <Box sx={{ mb: 1 }}>
                              <Typography variant="caption" color="text.secondary">Backstory</Typography>
                              <Paper sx={{ p: 1, bgcolor: 'action.hover', borderRadius: 1 }}>
                                <Typography variant="body2" sx={{ whiteSpace: 'pre-wrap', fontSize: '0.8rem' }}>
                                  {agent.backstory}
                                </Typography>
                              </Paper>
                            </Box>
                          )}
                          {agent.tools_names && agent.tools_names.length > 0 && (
                            <Box>
                              <Typography variant="caption" color="text.secondary" sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                                <BuildIcon fontSize="inherit" /> Tools
                              </Typography>
                              <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5, mt: 0.5 }}>
                                {agent.tools_names.map((tool, tIdx) => (
                                  <Chip key={tIdx} label={tool} size="small" color="info" variant="outlined" icon={<BuildIcon />} />
                                ))}
                              </Box>
                            </Box>
                          )}
                        </CardContent>
                      </Card>
                    ))}
                  </Stack>
                </Box>
              )}

              {/* Tasks Section */}
              {runConfig.crew_tasks.length > 0 && (
                <Box sx={{ mb: 3 }}>
                  <Typography variant="subtitle1" fontWeight="bold" sx={{ mb: 1, display: 'flex', alignItems: 'center', gap: 0.5 }}>
                    <AssignmentIcon fontSize="small" color="primary" />
                    Tasks ({runConfig.crew_tasks.length})
                  </Typography>
                  <Stack spacing={1.5}>
                    {runConfig.crew_tasks.map((task, idx) => (
                      <Card key={task.id || idx} variant="outlined" sx={{ borderRadius: 2 }}>
                        <CardContent sx={{ py: 1.5, '&:last-child': { pb: 1.5 } }}>
                          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1, flexWrap: 'wrap' }}>
                            <Chip label={`Task ${idx + 1}`} size="small" color="primary" icon={<AssignmentIcon />} />
                            <Chip label={task.agent_role} size="small" color="secondary" variant="outlined" icon={<PersonIcon />} />
                            {task.async_execution && (
                              <Chip label="Async" size="small" variant="outlined" color="warning" />
                            )}
                            {task.human_input && (
                              <Chip label="Human Input" size="small" variant="outlined" color="info" />
                            )}
                          </Box>
                          <Box sx={{ mb: 1 }}>
                            <Typography variant="caption" color="text.secondary" sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                              <AssignmentIcon fontSize="inherit" /> Description
                            </Typography>
                            <Paper sx={{ p: 1, bgcolor: 'action.hover', borderRadius: 1 }}>
                              <Typography variant="body2" sx={{ whiteSpace: 'pre-wrap', fontSize: '0.8rem' }}>
                                {task.description}
                              </Typography>
                            </Paper>
                          </Box>
                          {task.expected_output && (
                            <Box sx={{ mb: 1 }}>
                              <Typography variant="caption" color="text.secondary" sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                                <CheckCircleIcon fontSize="inherit" /> Expected Output
                              </Typography>
                              <Paper sx={{ p: 1, bgcolor: 'success.main', color: 'success.contrastText', borderRadius: 1, opacity: 0.9 }}>
                                <Typography variant="body2" sx={{ whiteSpace: 'pre-wrap', fontSize: '0.8rem' }}>
                                  {task.expected_output}
                                </Typography>
                              </Paper>
                            </Box>
                          )}
                          {task.tools_names && task.tools_names.length > 0 && (
                            <Box>
                              <Typography variant="caption" color="text.secondary" sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                                <BuildIcon fontSize="inherit" /> Tools
                              </Typography>
                              <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5, mt: 0.5 }}>
                                {task.tools_names.map((tool, tIdx) => (
                                  <Chip key={tIdx} label={tool} size="small" color="info" variant="outlined" icon={<BuildIcon />} />
                                ))}
                              </Box>
                            </Box>
                          )}
                          {task.context && task.context.length > 0 && (
                            <Box sx={{ mt: 0.5 }}>
                              <Typography variant="caption" color="text.secondary">Context Tasks</Typography>
                              <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5, mt: 0.5 }}>
                                {task.context.map((ctxId, cIdx) => (
                                  <Chip key={cIdx} label={ctxId} size="small" variant="outlined" />
                                ))}
                              </Box>
                            </Box>
                          )}
                        </CardContent>
                      </Card>
                    ))}
                  </Stack>
                </Box>
              )}

              {/* Additional Inputs Section */}
              {runConfig.crew_inputs && Object.keys(runConfig.crew_inputs).filter(k => k !== 'run_name').length > 0 && (
                <Box>
                  <Typography variant="subtitle1" fontWeight="bold" sx={{ mb: 1 }}>
                    Crew Inputs
                  </Typography>
                  <Paper sx={{ p: 1.5, bgcolor: 'action.hover', borderRadius: 1 }}>
                    <pre style={{ margin: 0, whiteSpace: 'pre-wrap', wordBreak: 'break-word', fontSize: '0.8rem' }}>
                      {JSON.stringify(
                        Object.fromEntries(
                          Object.entries(runConfig.crew_inputs).filter(([k]) => k !== 'run_name')
                        ),
                        null,
                        2
                      )}
                    </pre>
                  </Paper>
                </Box>
              )}
            </Box>
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setRunConfigOpen(false)} size="small">
            Close
          </Button>
        </DialogActions>
      </Dialog>

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
              <IconButton onClick={() => setSelectedTaskDescription(null)} size="small">
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
                <Box sx={{ maxHeight: '60vh', overflow: 'auto' }}>
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
                {selectedEvent.intrinsicMs != null && (
                  <Typography variant="caption" color="text.secondary" sx={{ display: 'block' }}>
                    Measured operation time: {formatDuration(selectedEvent.intrinsicMs)}
                  </Typography>
                )}
              </Box>
              <IconButton onClick={() => setSelectedEvent(null)} size="small">
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
                      {selectedEvent.type === 'memory_write' && (
                        <Chip icon={<StorageIcon />} label="Write" size="small" color="primary" variant="filled" />
                      )}
                      {selectedEvent.type === 'memory_retrieval' && (
                        <Chip icon={<StorageIcon />} label="Read" size="small" color="success" variant="filled" />
                      )}
                      {(() => {
                        const memTypeMatch = selectedEvent.description.match(/\(([^)]+)\)/);
                        if (memTypeMatch) {
                          return (
                            <Chip label={`Type: ${memTypeMatch[1]}`} size="small" color="secondary" variant="outlined" />
                          );
                        }
                        return null;
                      })()}
                      {(() => {
                        const output = selectedEvent.output;
                        if (typeof output === 'object' && output !== null) {
                          const outputObj = output as Record<string, unknown>;
                          const extraData = outputObj.extra_data as Record<string, unknown> | undefined;
                          const chips: JSX.Element[] = [];

                          if (extraData) {
                            if (extraData.operation && !selectedEvent.description.includes('Write') && !selectedEvent.description.includes('Read')) {
                              chips.push(<Chip key="operation" label={`Operation: ${extraData.operation as string}`} size="small" color="info" variant="outlined" />);
                            }
                            if (extraData.memory_type && !selectedEvent.description.includes('(')) {
                              chips.push(<Chip key="memory_type" label={`Type: ${extraData.memory_type as string}`} size="small" color="secondary" variant="outlined" />);
                            }
                            if (extraData.results_count !== undefined) {
                              chips.push(<Chip key="results_count" label={`Results: ${extraData.results_count as number}`} size="small" color="default" variant="outlined" />);
                            }
                            if (extraData.query) {
                              chips.push(<Chip key="query" label="Query included" size="small" color="default" variant="outlined" />);
                            }
                            if (extraData.backend) {
                              chips.push(<Chip key="backend" label={`Backend: ${extraData.backend as string}`} size="small" color="default" variant="outlined" />);
                            }
                          }

                          if (chips.length === 0) {
                            if ('operation' in outputObj && !selectedEvent.description.includes('Write') && !selectedEvent.description.includes('Read')) {
                              chips.push(<Chip key="operation" label={`Operation: ${outputObj.operation as string}`} size="small" color="info" variant="outlined" />);
                            }
                            if ('memory_type' in outputObj && !selectedEvent.description.includes('(')) {
                              chips.push(<Chip key="memory_type" label={`Type: ${outputObj.memory_type as string}`} size="small" color="secondary" variant="outlined" />);
                            }
                          }

                          return chips.length > 0 ? <>{chips}</> : null;
                        }
                        return null;
                      })()}
                    </Box>
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
                              <Chip
                                label={success === true || validationValid === true ? 'Passed' : success === false || validationValid === false ? 'Failed' : 'Unknown'}
                                sx={{ mr: 1, mb: 1 }}
                                size="small"
                                color={success === true || validationValid === true ? 'success' : success === false || validationValid === false ? 'error' : 'default'}
                              />
                              {taskName && (
                                <Chip label={`Task: ${taskName}`} sx={{ mr: 1, mb: 1 }} size="small" color="info" />
                              )}
                              {retryCount !== undefined && Number(retryCount) > 0 && (
                                <Chip label={`Retries: ${retryCount}`} sx={{ mr: 1, mb: 1 }} size="small" color="warning" />
                              )}
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

                {/* Paginated output display */}
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
    </Box>
  );
});

TraceTimelineContent.displayName = 'TraceTimelineContent';

export default TraceTimelineContent;
