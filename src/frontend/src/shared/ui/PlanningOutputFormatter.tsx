import React from 'react';
import {
  Box,
  Paper,
  Typography,
  Chip,
  Divider,
  Card,
  CardContent,
  Stack,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  useTheme,
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import AssignmentIcon from '@mui/icons-material/Assignment';
import PersonIcon from '@mui/icons-material/Person';
import BuildIcon from '@mui/icons-material/Build';
import FlagIcon from '@mui/icons-material/Flag';
import DescriptionIcon from '@mui/icons-material/Description';

interface ParsedTask {
  taskNumber: number;
  taskTitle: string;
  taskDescription: string;
  taskExpectedOutput: string;
  agent: string;
  agentGoal: string;
  taskTools: string[];
  agentTools: string;
}

// New JSON format from Task Execution Planner
interface PlanTaskEntry {
  task: string;
  plan: string;
}

interface JsonPlanningOutput {
  list_of_plans_per_task: PlanTaskEntry[];
}

interface PlanningOutputFormatterProps {
  content: string;
}

/**
 * Tries to parse content as JSON planning output with list_of_plans_per_task
 */
export const parseJsonPlanningOutput = (content: string): JsonPlanningOutput | null => {
  if (!content || typeof content !== 'string') return null;

  try {
    const parsed = JSON.parse(content);
    if (
      parsed &&
      typeof parsed === 'object' &&
      Array.isArray(parsed.list_of_plans_per_task) &&
      parsed.list_of_plans_per_task.length > 0 &&
      parsed.list_of_plans_per_task.every(
        (item: unknown) =>
          item &&
          typeof item === 'object' &&
          'task' in (item as Record<string, unknown>) &&
          'plan' in (item as Record<string, unknown>)
      )
    ) {
      return parsed as JsonPlanningOutput;
    }
  } catch {
    // Not valid JSON, continue to check other formats
  }

  return null;
};

/**
 * Detects if content is CrewAI planning output
 */
export const isPlanningOutput = (content: string): boolean => {
  if (!content || typeof content !== 'string') return false;

  // Check for JSON format with list_of_plans_per_task
  if (parseJsonPlanningOutput(content)) {
    return true;
  }

  // Check for legacy planning output markers (Task Number X format)
  const hasTaskNumber = /Task Number \d+/i.test(content);
  const hasTaskDescription = /"task_description":/i.test(content);
  const hasAgent = /"agent":/i.test(content);
  const hasPlanSummary = /Based on these tasks summary/i.test(content);

  // Need at least task number + one other marker to be considered planning output
  return hasTaskNumber && (hasTaskDescription || hasAgent || hasPlanSummary);
};

/**
 * Parses CrewAI planning output into structured task objects
 */
const parsePlanningOutput = (content: string): ParsedTask[] => {
  const tasks: ParsedTask[] = [];

  // Split by "Task Number" to get individual tasks
  const taskRegex = /Task Number (\d+)\s*[-–—]\s*([^\n]+)/g;
  const taskMatches = [...content.matchAll(taskRegex)];

  if (taskMatches.length === 0) return tasks;

  // For each task, extract the content between this task and the next
  for (let i = 0; i < taskMatches.length; i++) {
    const match = taskMatches[i];
    const taskNumber = parseInt(match[1], 10);
    const taskTitle = match[2].trim();

    // Find the end index (start of next task or end of content)
    const startIdx = match.index! + match[0].length;
    const endIdx = i < taskMatches.length - 1
      ? taskMatches[i + 1].index!
      : content.length;

    const taskContent = content.slice(startIdx, endIdx);

    // Extract fields using regex
    const extractField = (fieldName: string): string => {
      // Match both quoted and unquoted field names
      const regex = new RegExp(`["']?${fieldName}["']?\\s*:\\s*([^\\n]+|[\\s\\S]*?(?=["']?(?:task_description|task_expected_output|agent|agent_goal|task_tools|agent_tools)["']?\\s*:|Task Number|$))`, 'i');
      const match = taskContent.match(regex);
      if (!match) return '';

      let value = match[1].trim();
      // Remove leading/trailing quotes
      value = value.replace(/^["']|["']$/g, '').trim();
      // Remove trailing commas
      value = value.replace(/,\s*$/, '').trim();
      return value;
    };

    const extractTools = (fieldName: string): string[] => {
      const regex = new RegExp(`["']?${fieldName}["']?\\s*:\\s*\\[([^\\]]+)\\]`, 'i');
      const match = taskContent.match(regex);
      if (!match) return [];

      // Parse the tools array - it may contain Tool objects
      const toolsContent = match[1];
      const tools: string[] = [];

      // Match tool patterns like PerplexitySearchTool(name='PerplexityTool', ...)
      const toolRegex = /(\w+Tool)\s*\(/g;
      let toolMatch;
      while ((toolMatch = toolRegex.exec(toolsContent)) !== null) {
        tools.push(toolMatch[1]);
      }

      // If no Tool pattern found, try to parse as simple list
      if (tools.length === 0) {
        const simpleTools = toolsContent.split(',').map(t => t.trim().replace(/["']/g, '')).filter(Boolean);
        tools.push(...simpleTools);
      }

      return tools;
    };

    const task: ParsedTask = {
      taskNumber,
      taskTitle,
      taskDescription: extractField('task_description'),
      taskExpectedOutput: extractField('task_expected_output'),
      agent: extractField('agent'),
      agentGoal: extractField('agent_goal'),
      taskTools: extractTools('task_tools'),
      agentTools: extractField('agent_tools'),
    };

    tasks.push(task);
  }

  return tasks;
};

/**
 * Formats a plan string with proper line breaks and styling
 */
const formatPlanSteps = (plan: string): React.ReactNode[] => {
  // Split by "Step X:" pattern but keep the delimiter
  const stepPattern = /(Step \d+:)/gi;
  const parts = plan.split(stepPattern).filter(Boolean);

  const steps: React.ReactNode[] = [];
  for (let i = 0; i < parts.length; i++) {
    if (stepPattern.test(parts[i])) {
      // This is a step header
      const stepHeader = parts[i];
      const stepContent = parts[i + 1] || '';
      i++; // Skip next item as we're processing it now

      steps.push(
        <Box key={i} sx={{ mb: 2 }}>
          <Typography
            variant="subtitle2"
            sx={{
              fontWeight: 'bold',
              color: 'primary.main',
              display: 'inline',
            }}
          >
            {stepHeader}
          </Typography>
          <Typography
            variant="body2"
            component="span"
            sx={{
              whiteSpace: 'pre-wrap',
              display: 'inline',
            }}
          >
            {stepContent.startsWith(' ') ? stepContent : ' ' + stepContent}
          </Typography>
        </Box>
      );
    } else if (i === 0) {
      // Content before first step (intro text)
      steps.push(
        <Typography
          key="intro"
          variant="body2"
          sx={{ mb: 2, whiteSpace: 'pre-wrap' }}
        >
          {parts[i].trim()}
        </Typography>
      );
    }
  }

  // If no steps were found, just render the whole plan
  if (steps.length === 0) {
    return [
      <Typography key="full" variant="body2" sx={{ whiteSpace: 'pre-wrap' }}>
        {plan}
      </Typography>
    ];
  }

  return steps;
};

/**
 * Renders JSON planning output with list_of_plans_per_task format
 */
const JsonPlanningFormatter: React.FC<{ planData: JsonPlanningOutput }> = ({ planData }) => {
  const theme = useTheme();
  const tasks = planData.list_of_plans_per_task;

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
      {/* Header */}
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
        <AssignmentIcon color="secondary" />
        <Typography variant="h6" color="secondary.main">
          Execution Plan
        </Typography>
        <Chip
          label={`${tasks.length} Task${tasks.length > 1 ? 's' : ''}`}
          size="small"
          color="secondary"
          variant="outlined"
        />
      </Box>

      <Divider />

      {/* Task Plan Cards */}
      <Stack spacing={2}>
        {tasks.map((taskEntry, index) => (
          <Card
            key={index}
            variant="outlined"
            sx={{
              borderLeft: `4px solid ${theme.palette.secondary.main}`,
              '&:hover': {
                boxShadow: 2,
              }
            }}
          >
            <CardContent sx={{ pb: 2 }}>
              {/* Task Header */}
              <Box sx={{ display: 'flex', alignItems: 'flex-start', gap: 1, mb: 2 }}>
                <Chip
                  label={`Task ${index + 1}`}
                  color="secondary"
                  size="small"
                  sx={{ fontWeight: 'bold' }}
                />
              </Box>

              {/* Task Description */}
              <Accordion
                defaultExpanded={index === 0}
                disableGutters
                elevation={0}
                sx={{
                  '&:before': { display: 'none' },
                  backgroundColor: 'transparent',
                }}
              >
                <AccordionSummary
                  expandIcon={<ExpandMoreIcon />}
                  sx={{
                    px: 0,
                    minHeight: 'auto',
                    '& .MuiAccordionSummary-content': { my: 0.5, alignItems: 'center', gap: 1 }
                  }}
                >
                  <DescriptionIcon fontSize="small" color="action" />
                  <Typography variant="body2" fontWeight="medium" color="text.secondary">
                    Task Description
                  </Typography>
                </AccordionSummary>
                <AccordionDetails sx={{ px: 0, pt: 0 }}>
                  <Paper
                    sx={{
                      p: 1.5,
                      backgroundColor: theme.palette.mode === 'dark' ? 'grey.900' : 'grey.50',
                    }}
                  >
                    <Typography variant="body2" sx={{ whiteSpace: 'pre-wrap' }}>
                      {taskEntry.task}
                    </Typography>
                  </Paper>
                </AccordionDetails>
              </Accordion>

              {/* Execution Plan */}
              <Accordion
                defaultExpanded
                disableGutters
                elevation={0}
                sx={{
                  '&:before': { display: 'none' },
                  backgroundColor: 'transparent',
                }}
              >
                <AccordionSummary
                  expandIcon={<ExpandMoreIcon />}
                  sx={{
                    px: 0,
                    minHeight: 'auto',
                    '& .MuiAccordionSummary-content': { my: 0.5, alignItems: 'center', gap: 1 }
                  }}
                >
                  <FlagIcon fontSize="small" color="secondary" />
                  <Typography variant="body2" fontWeight="medium" color="secondary.main">
                    Execution Plan
                  </Typography>
                </AccordionSummary>
                <AccordionDetails sx={{ px: 0, pt: 0 }}>
                  <Paper
                    sx={{
                      p: 2,
                      backgroundColor: theme.palette.mode === 'dark' ? 'grey.900' : 'secondary.50',
                      borderLeft: `3px solid ${theme.palette.secondary.main}`,
                    }}
                  >
                    {formatPlanSteps(taskEntry.plan)}
                  </Paper>
                </AccordionDetails>
              </Accordion>
            </CardContent>
          </Card>
        ))}
      </Stack>
    </Box>
  );
};

/**
 * Renders a formatted planning output with task cards
 */
const PlanningOutputFormatter: React.FC<PlanningOutputFormatterProps> = ({ content }) => {
  const theme = useTheme();

  // First, try to parse as JSON format
  const jsonPlanData = parseJsonPlanningOutput(content);
  if (jsonPlanData) {
    return <JsonPlanningFormatter planData={jsonPlanData} />;
  }

  // Fall back to legacy format parsing
  const tasks = parsePlanningOutput(content);

  if (tasks.length === 0) {
    // Fallback to raw content if parsing fails
    return (
      <Typography
        component="pre"
        sx={{
          whiteSpace: 'pre-wrap',
          wordBreak: 'break-word',
          fontFamily: 'monospace',
          fontSize: '0.875rem',
        }}
      >
        {content}
      </Typography>
    );
  }

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
      {/* Header */}
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
        <AssignmentIcon color="primary" />
        <Typography variant="h6" color="primary">
          Task Execution Plan
        </Typography>
        <Chip
          label={`${tasks.length} Task${tasks.length > 1 ? 's' : ''}`}
          size="small"
          color="primary"
          variant="outlined"
        />
      </Box>

      <Divider />

      {/* Task Cards */}
      <Stack spacing={2}>
        {tasks.map((task, index) => (
          <Card
            key={task.taskNumber}
            variant="outlined"
            sx={{
              borderLeft: `4px solid ${theme.palette.primary.main}`,
              '&:hover': {
                boxShadow: 2,
              }
            }}
          >
            <CardContent sx={{ pb: 1 }}>
              {/* Task Header */}
              <Box sx={{ display: 'flex', alignItems: 'flex-start', gap: 1, mb: 2 }}>
                <Chip
                  label={`Task ${task.taskNumber}`}
                  color="primary"
                  size="small"
                  sx={{ fontWeight: 'bold' }}
                />
                <Typography variant="subtitle1" fontWeight="medium" sx={{ flex: 1 }}>
                  {task.taskTitle}
                </Typography>
              </Box>

              {/* Task Description */}
              {task.taskDescription && (
                <Accordion
                  defaultExpanded={index === 0}
                  disableGutters
                  elevation={0}
                  sx={{
                    '&:before': { display: 'none' },
                    backgroundColor: 'transparent',
                  }}
                >
                  <AccordionSummary
                    expandIcon={<ExpandMoreIcon />}
                    sx={{
                      px: 0,
                      minHeight: 'auto',
                      '& .MuiAccordionSummary-content': { my: 0.5, alignItems: 'center', gap: 1 }
                    }}
                  >
                    <DescriptionIcon fontSize="small" color="action" />
                    <Typography variant="body2" fontWeight="medium" color="text.secondary">
                      Description
                    </Typography>
                  </AccordionSummary>
                  <AccordionDetails sx={{ px: 0, pt: 0 }}>
                    <Paper
                      sx={{
                        p: 1.5,
                        backgroundColor: theme.palette.mode === 'dark' ? 'grey.900' : 'grey.50',
                      }}
                    >
                      <Typography variant="body2" sx={{ whiteSpace: 'pre-wrap' }}>
                        {task.taskDescription}
                      </Typography>
                    </Paper>
                  </AccordionDetails>
                </Accordion>
              )}

              {/* Expected Output */}
              {task.taskExpectedOutput && (
                <Accordion
                  disableGutters
                  elevation={0}
                  sx={{
                    '&:before': { display: 'none' },
                    backgroundColor: 'transparent',
                  }}
                >
                  <AccordionSummary
                    expandIcon={<ExpandMoreIcon />}
                    sx={{
                      px: 0,
                      minHeight: 'auto',
                      '& .MuiAccordionSummary-content': { my: 0.5, alignItems: 'center', gap: 1 }
                    }}
                  >
                    <FlagIcon fontSize="small" color="action" />
                    <Typography variant="body2" fontWeight="medium" color="text.secondary">
                      Expected Output
                    </Typography>
                  </AccordionSummary>
                  <AccordionDetails sx={{ px: 0, pt: 0 }}>
                    <Paper
                      sx={{
                        p: 1.5,
                        backgroundColor: theme.palette.mode === 'dark' ? 'grey.900' : 'grey.50',
                      }}
                    >
                      <Typography variant="body2" sx={{ whiteSpace: 'pre-wrap' }}>
                        {task.taskExpectedOutput}
                      </Typography>
                    </Paper>
                  </AccordionDetails>
                </Accordion>
              )}

              {/* Agent Info */}
              <Box sx={{ mt: 2, display: 'flex', flexWrap: 'wrap', gap: 2 }}>
                {/* Agent Name & Goal */}
                {task.agent && (
                  <Box sx={{ flex: '1 1 300px' }}>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5, mb: 0.5 }}>
                      <PersonIcon fontSize="small" color="secondary" />
                      <Typography variant="caption" color="text.secondary" fontWeight="medium">
                        ASSIGNED AGENT
                      </Typography>
                    </Box>
                    <Typography variant="body2" fontWeight="medium" color="secondary.main">
                      {task.agent}
                    </Typography>
                    {task.agentGoal && (
                      <Typography variant="caption" color="text.secondary" sx={{ mt: 0.5, display: 'block' }}>
                        Goal: {task.agentGoal}
                      </Typography>
                    )}
                  </Box>
                )}

                {/* Tools */}
                {(task.taskTools.length > 0 || task.agentTools) && (
                  <Box sx={{ flex: '1 1 300px' }}>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5, mb: 0.5 }}>
                      <BuildIcon fontSize="small" color="info" />
                      <Typography variant="caption" color="text.secondary" fontWeight="medium">
                        TOOLS
                      </Typography>
                    </Box>
                    <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                      {task.taskTools.length > 0 ? (
                        task.taskTools.map((tool, idx) => (
                          <Chip
                            key={idx}
                            label={tool}
                            size="small"
                            variant="outlined"
                            color="info"
                            sx={{ fontSize: '0.7rem' }}
                          />
                        ))
                      ) : task.agentTools && task.agentTools !== 'agent has no tools' ? (
                        <Typography variant="caption" color="text.secondary">
                          {task.agentTools}
                        </Typography>
                      ) : (
                        <Typography variant="caption" color="text.disabled" fontStyle="italic">
                          No tools assigned
                        </Typography>
                      )}
                    </Box>
                  </Box>
                )}
              </Box>
            </CardContent>
          </Card>
        ))}
      </Stack>
    </Box>
  );
};

export default PlanningOutputFormatter;
