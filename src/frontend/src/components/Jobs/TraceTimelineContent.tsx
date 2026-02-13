import React, { memo } from 'react';
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
import {
  isEventClickable,
  getEventIcon as getEventIconConfig,
} from './traceEventProcessors';
import { PaginatedOutput } from '../Common';
import { ProcessedTraces } from '../../types/trace';

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
  return (
    <Box sx={{ contain: 'content' }}>
      {/* View mode toggle */}
      <Box sx={{ px: 2, pt: 2, pb: 1, display: 'flex', justifyContent: 'flex-start' }}>
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
      </Box>

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
                            '& .output-hint': { opacity: 1 }
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
                          <Chip size="small" label={formatDuration(event.duration)} sx={{ height: 20 }} />
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
                              '& .MuiChip-label': { px: 0.5 }
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
                    {/* Agent-level events (reasoning/planning) */}
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
                          <Typography variant="body2" fontWeight="medium" sx={{ color: 'primary.main' }}>
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
                                    '& .output-hint': { opacity: 1 },
                                    '& .click-hint': { visibility: 'visible' }
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
                                  <Chip size="small" label={formatDuration(event.duration)} sx={{ height: 20 }} />
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
                                        '& .MuiChip-label': { px: 0.5 }
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
                                        '& .output-hint': { opacity: 1 },
                                        '& .click-hint': { visibility: 'visible' }
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
                                            '& .MuiChip-label': { px: 0.5 }
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
      </Box>

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
