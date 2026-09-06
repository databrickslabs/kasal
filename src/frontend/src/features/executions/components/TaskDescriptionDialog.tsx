/**
 * The Task Description dialog from the Execution Trace Timeline, plus the
 * structured renderer it uses.
 *
 * Split out of TraceTimelineContent.tsx: the parser + card renderer + dialog
 * are one cohesive block that nothing else in the timeline touches.
 */
import React from 'react';
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
  Chip,
  Button,
  Card,
  CardContent,
  Stack,
  Alert,
} from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import AssignmentIcon from '@mui/icons-material/Assignment';
import PersonIcon from '@mui/icons-material/Person';
import BuildIcon from '@mui/icons-material/Build';
import TargetIcon from '@mui/icons-material/TrackChanges';

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

export interface SelectedTaskDescription {
  taskName: string;
  taskId?: string;
  fullDescription?: string;
  isLoading: boolean;
}

export interface TaskDescriptionDialogProps {
  value: SelectedTaskDescription | null;
  onClose: () => void;
}

/**
 * The task's instructions, in full. `fullDescription` is whatever complete copy
 * the run left behind; `taskName` is the fallback, and on runs recorded before
 * the engine started emitting the untruncated description it is capped at the
 * per-event limit.
 */
export const TaskDescriptionDialog: React.FC<TaskDescriptionDialogProps> = ({ value, onClose }) => {
  const description = value ? (value.fullDescription || value.taskName) : '';

  return (
    <Dialog open={!!value} onClose={onClose} maxWidth="md" fullWidth>
      {value && (
        <>
          <DialogTitle sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <Box>
              <Typography variant="h6">Task Description</Typography>
              {value.taskId && (
                <Typography variant="caption" color="text.secondary">
                  Task ID: {value.taskId}
                </Typography>
              )}
            </Box>
            <IconButton onClick={onClose} size="small">
              <CloseIcon />
            </IconButton>
          </DialogTitle>
          <DialogContent dividers>
            {value.isLoading ? (
              <Box display="flex" justifyContent="center" alignItems="center" minHeight="100px">
                <CircularProgress size={24} />
                <Typography sx={{ ml: 2 }} color="text.secondary">
                  Loading task details...
                </Typography>
              </Box>
            ) : (
              <Box sx={{ maxHeight: '60vh', overflow: 'auto' }}>
                <FormattedTaskDescription description={description} />
              </Box>
            )}
          </DialogContent>
          <DialogActions>
            <Button
              onClick={() => { navigator.clipboard.writeText(description); }}
              startIcon={<ContentCopyIcon />}
              size="small"
            >
              Copy Description
            </Button>
            <Button onClick={onClose} size="small">
              Close
            </Button>
          </DialogActions>
        </>
      )}
    </Dialog>
  );
};

export default TaskDescriptionDialog;
