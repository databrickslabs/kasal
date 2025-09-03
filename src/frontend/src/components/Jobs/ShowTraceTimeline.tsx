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
} from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import ExpandLessIcon from '@mui/icons-material/ExpandLess';
import AccessTimeIcon from '@mui/icons-material/AccessTime';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import PlayCircleIcon from '@mui/icons-material/PlayCircle';
import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import { ShowTraceProps, Trace } from '../../types/trace';
import TraceService from '../../api/TraceService';

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

const ShowTraceTimeline: React.FC<ShowTraceProps> = ({ open, onClose, runId }) => {
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

  // Process traces into hierarchical structure
  const processTraces = useCallback((rawTraces: Trace[]): ProcessedTraces => {
    const sorted = [...rawTraces].sort((a, b) => 
      new Date(a.created_at).getTime() - new Date(b.created_at).getTime()
    );

    if (sorted.length === 0) {
      return { agents: [], globalEvents: { start: [], end: [] } };
    }

    const globalStart = new Date(sorted[0].created_at);
    const globalEnd = new Date(sorted[sorted.length - 1].created_at);
    const totalDuration = globalEnd.getTime() - globalStart.getTime();

    // Separate global events
    const globalEvents = {
      start: sorted.filter(t => 
        t.event_source === 'crew' && 
        (t.event_type === 'crew_started' || t.event_type === 'execution_started')
      ),
      end: sorted.filter(t => 
        t.event_source === 'crew' && 
        (t.event_type === 'crew_completed' || t.event_type === 'execution_completed')
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
      // Skip global events and task events for agent grouping
      if (trace.event_source === 'crew' || trace.event_source === 'task') {
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

      // Group agent traces by task - using timestamps to determine task boundaries
      const taskMap = new Map<string, Trace[]>();
      let currentTask: string | null = null;

      agentTraces.forEach(trace => {
        // Look for a task that encompasses this trace's timestamp
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
            // Tool usage event
            eventType = 'tool';
            const toolName = trace.event_source || 'Tool';
            description = toolName;
          } else if (trace.event_type === 'agent_execution' || trace.event_type === 'agent_step') {
            const output = typeof trace.output === 'string' ? trace.output : '';
            
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
            } else if (trace.output && typeof trace.output === 'string' && trace.output.length > 100) {
              // Long output usually means the agent is providing results
              eventType = 'agent_output';
              description = 'Task Output';
            } else {
              eventType = 'agent_processing';
              description = 'Processing';
            }
          } else if (trace.event_type === 'task_started') {
            eventType = 'task_start';
            description = 'Task Started';
          } else if (trace.event_type === 'task_completed') {
            eventType = 'task_complete';
            description = 'Task Completed';
          } else {
            eventType = trace.event_type;
            // Make the description more readable
            const readableDesc = trace.event_type
              .replace(/_/g, ' ')
              .replace(/\b\w/g, (l) => l.toUpperCase());
            description = readableDesc;
          }

          return {
            type: eventType,
            description,
            timestamp,
            duration,
            output: trace.output
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

  const fetchTraceData = useCallback(async () => {
    if (!runId) return;
    
    try {
      setLoading(true);
      
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
        setError('No trace data is available for this run.');
        setTraces([]);
      } else {
        setTraces(traces);
        const processed = processTraces(traces);
        setProcessedTraces(processed);
        // Expand all agents by default
        setExpandedAgents(new Set(processed.agents.map((_, idx) => idx)));
        setError(null);
      }
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : 'Unknown error';
      setError(`Failed to load traces: ${errorMessage}`);
      setTraces([]);
    } finally {
      setLoading(false);
    }
  }, [runId, processTraces]);

  useEffect(() => {
    if (open) {
      fetchTraceData();
    }
  }, [open, fetchTraceData]);

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

  const getEventIcon = (type: string) => {
    switch (type) {
      case 'tool':
      case 'tool_result':
        return '🔧';
      case 'llm':
        return '🤖';
      case 'agent_start':
      case 'task_start':
      case 'started':
        return '▶️';
      case 'agent_complete':
      case 'task_complete':
      case 'completed':
        return '✅';
      case 'agent_output':
        return '📝';
      case 'agent_processing':
        return '⚙️';
      default:
        return '•';
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
      <DialogTitle sx={{ m: 0, p: 2, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <Typography variant="h6">Execution Trace Timeline</Typography>
        <IconButton
          aria-label="close"
          onClick={onClose}
          sx={{
            position: 'absolute',
            right: 8,
            top: 8,
            color: (theme: Theme) => theme.palette.grey[500],
          }}
        >
          <CloseIcon />
        </IconButton>
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
                                const isClickable = hasOutput && (
                                  event.type === 'llm' || 
                                  event.type === 'agent_complete' || 
                                  event.type === 'agent_output' ||
                                  event.type === 'tool_result'
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
                                    <Typography variant="body2" sx={{ minWidth: 20 }}>
                                      {getEventIcon(event.type)}
                                    </Typography>
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
              <Typography variant="h6">{selectedEvent.description}</Typography>
              <IconButton
                onClick={() => setSelectedEvent(null)}
                size="small"
              >
                <CloseIcon />
              </IconButton>
            </DialogTitle>
            <DialogContent dividers>
              <Box sx={{ position: 'relative' }}>
                <Typography variant="caption" color="text.secondary" gutterBottom>
                  Event Type: {selectedEvent.type}
                </Typography>
                <Paper 
                  sx={{ 
                    p: 2, 
                    mt: 1,
                    backgroundColor: 'grey.50',
                    maxHeight: '60vh',
                    overflow: 'auto',
                    fontFamily: 'monospace',
                    fontSize: '0.875rem',
                    whiteSpace: 'pre-wrap',
                    wordBreak: 'break-word'
                  }}
                >
                  {formatOutput(selectedEvent.output)}
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
  );
};

export default ShowTraceTimeline;