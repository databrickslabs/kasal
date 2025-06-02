import React, { useState, useEffect, useCallback } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  TextField,
  List,
  ListItem,
  ListItemText,
  ListItemSecondaryAction,
  IconButton,
  Switch,
  Typography,
  Box,
  Tooltip,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  ToggleButton,
  ToggleButtonGroup,
  Checkbox,
  FormControlLabel,
  FormGroup,
  Grid,
  Snackbar,
  Alert,
} from '@mui/material';
import DeleteIcon from '@mui/icons-material/Delete';
import EditIcon from '@mui/icons-material/Edit';
import { ScheduleService } from '../../api/ScheduleService';
import { toast } from 'react-hot-toast';
import InfoIcon from '@mui/icons-material/Info';
import { Schedule, ScheduleDialogProps, ConfigViewerDialogProps } from '../../types/schedule';
import { AgentYaml, TaskYaml } from '../../types/crew';
import CloseIcon from '@mui/icons-material/Close';

type CronMode = 'manual' | 'visual';

type DialogMode = 'create' | 'edit';

const ConfigViewerDialog: React.FC<ConfigViewerDialogProps> = ({ open, onClose, schedule }) => {
  if (!schedule) return null;

  return (
    <Dialog open={open} onClose={onClose} maxWidth="md" fullWidth>
      <DialogTitle>Configuration for {schedule.name}</DialogTitle>
      <DialogContent>
        <Box sx={{ mt: 2 }}>
          <Typography variant="subtitle1" gutterBottom>Agents YAML</Typography>
          <Box sx={{ 
            whiteSpace: 'pre-wrap',
            fontFamily: 'monospace',
            bgcolor: 'grey.100',
            p: 2,
            borderRadius: 1,
            maxHeight: '300px',
            overflow: 'auto',
            mb: 3
          }}>
            {JSON.stringify(schedule.agents_yaml, null, 2)}
          </Box>

          <Typography variant="subtitle1" gutterBottom>Tasks YAML</Typography>
          <Box sx={{ 
            whiteSpace: 'pre-wrap',
            fontFamily: 'monospace',
            bgcolor: 'grey.100',
            p: 2,
            borderRadius: 1,
            maxHeight: '300px',
            overflow: 'auto',
            mb: 3
          }}>
            {JSON.stringify(schedule.tasks_yaml, null, 2)}
          </Box>

          {schedule.inputs && Object.keys(schedule.inputs).length > 0 && (
            <>
              <Typography variant="subtitle1" gutterBottom>Inputs</Typography>
              <Box sx={{ 
                whiteSpace: 'pre-wrap',
                fontFamily: 'monospace',
                bgcolor: 'grey.100',
                p: 2,
                borderRadius: 1,
                maxHeight: '300px',
                overflow: 'auto'
              }}>
                {JSON.stringify(schedule.inputs, null, 2)}
              </Box>
            </>
          )}
        </Box>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Close</Button>
      </DialogActions>
    </Dialog>
  );
};

const ScheduleDialog: React.FC<ScheduleDialogProps> = ({ 
  open, 
  onClose, 
  nodes, 
  edges,
  planningEnabled,
  selectedModel
}) => {
  const [schedules, setSchedules] = useState<Schedule[]>([]);
  const [name, setName] = useState('');
  const [cronExpression, setCronExpression] = useState('');
  const [loading, setLoading] = useState(false);
  const [cronMode, setCronMode] = useState<CronMode>('visual');
  const [dialogMode, setDialogMode] = useState<DialogMode>('create');
  const [editingScheduleId, setEditingScheduleId] = useState<number | null>(null);

  const [frequency, setFrequency] = useState('daily');
  const [selectedTime, setSelectedTime] = useState({ hour: '0', minute: '0' });
  const [selectedDays, setSelectedDays] = useState<string[]>([]);
  const [selectedMonths, setSelectedMonths] = useState<string[]>([]);
  const [selectedDaysOfMonth, setSelectedDaysOfMonth] = useState<string[]>([]);

  const [selectedSchedule, setSelectedSchedule] = useState<Schedule | null>(null);
  const [isConfigViewerOpen, setIsConfigViewerOpen] = useState(false);
  const [inputs, setInputs] = useState<Record<string, unknown>>({});
  const [error, setError] = useState<string | null>(null);

  const resetForm = useCallback(() => {
    setName('');
    setCronExpression('');
    setFrequency('daily');
    setSelectedTime({ hour: '0', minute: '0' });
    setSelectedDays([]);
    setSelectedMonths([]);
    setSelectedDaysOfMonth([]);
    setCronMode('visual');
    setDialogMode('create');
    setEditingScheduleId(null);
    setInputs({});
  }, []);

  const loadSchedules = useCallback(async () => {
    try {
      const fetchedSchedules = await ScheduleService.listSchedules();
      setSchedules(fetchedSchedules);
    } catch (error) {
      console.error('Error loading schedules:', error);
      toast.error('Failed to load schedules');
    }
  }, []);

  useEffect(() => {
    if (open) {
      loadSchedules();
      if (!editingScheduleId) {
        resetForm();
      }
    }
  }, [open, editingScheduleId, loadSchedules, resetForm]);

  const parseCronExpression = (expression: string) => {
    const parts = expression.split(' ');
    const [minute, hour, dayOfMonth, month, dayOfWeek] = parts;

    // Set time
    setSelectedTime({
      minute: minute === '*' ? '0' : minute,
      hour: hour === '*' ? '0' : hour,
    });

    // Determine frequency and set corresponding values
    if (hour === '*') {
      setFrequency('hourly');
    } else if (dayOfMonth === '*' && month === '*' && dayOfWeek === '*') {
      setFrequency('daily');
    } else if (dayOfMonth === '*' && month === '*' && dayOfWeek !== '*') {
      setFrequency('weekly');
      setSelectedDays(dayOfWeek.split(','));
    } else if (dayOfMonth !== '*' && month === '*') {
      setFrequency('monthly');
      setSelectedDaysOfMonth(dayOfMonth.split(','));
    } else if (month !== '*') {
      setFrequency('yearly');
      setSelectedMonths(month.split(','));
    }
  };

  const handleEditSchedule = (schedule: Schedule) => {
    setDialogMode('edit');
    setEditingScheduleId(schedule.id);
    setName(schedule.name);
    setCronExpression(schedule.cron_expression);
    setInputs(schedule.inputs || {});
    parseCronExpression(schedule.cron_expression);
  };

  const updateCronFromVisual = useCallback(() => {
    const { hour, minute } = selectedTime;
    let newCronExpression = '';

    const days = selectedDays.length > 0 ? selectedDays.join(',') : '*';
    const daysOfMonth = selectedDaysOfMonth.length > 0 ? selectedDaysOfMonth.join(',') : '*';
    const months = selectedMonths.length > 0 ? selectedMonths.join(',') : '*';

    switch (frequency) {
      case 'hourly':
        newCronExpression = `${minute} * * * *`;
        break;
      case 'daily':
        newCronExpression = `${minute} ${hour} * * *`;
        break;
      case 'weekly':
        newCronExpression = `${minute} ${hour} * * ${days}`;
        break;
      case 'monthly':
        newCronExpression = `${minute} ${hour} ${daysOfMonth} * *`;
        break;
      case 'yearly':
        newCronExpression = `${minute} ${hour} * ${months} *`;
        break;
      default:
        newCronExpression = '0 0 * * *';
    }

    setCronExpression(newCronExpression);
  }, [frequency, selectedTime, selectedDays, selectedMonths, selectedDaysOfMonth]);

  useEffect(() => {
    if (cronMode === 'visual') {
      updateCronFromVisual();
    }
  }, [cronMode, updateCronFromVisual]);

  const handleCloseError = () => {
    setError(null);
  };

  const handleSaveSchedule = async () => {
    if (!name || !cronExpression) {
      setError('Please fill in all required fields');
      return;
    }

    try {
      setLoading(true);
      
      let scheduleData;
      if (dialogMode === 'edit' && editingScheduleId) {
        // When editing, keep the existing configuration
        const existingSchedule = schedules.find(s => s.id === editingScheduleId);
        if (!existingSchedule) {
          toast.error('Schedule not found');
          return;
        }
        scheduleData = {
          name,
          cron_expression: cronExpression,
          agents_yaml: existingSchedule.agents_yaml,
          tasks_yaml: existingSchedule.tasks_yaml,
          inputs: existingSchedule.inputs,
          is_active: existingSchedule.is_active,
          planning: existingSchedule.planning,
          model: existingSchedule.model
        };
      } else {
        // When creating new, validate that there are agents and tasks on the canvas
        if (nodes.length === 0) {
          setError('Please build your agent configuration on the canvas before creating a schedule. Add agents and tasks to get started.');
          return;
        }

        const agentNodes = nodes.filter(node => node.type === 'agentNode');
        const taskNodes = nodes.filter(node => node.type === 'taskNode');

        if (agentNodes.length === 0 || taskNodes.length === 0) {
          setError('Your configuration must include at least one agent and one task. Please add both to the canvas.');
          return;
        }

        // When creating new, use the current flow state
        const agentsYaml: Record<string, AgentYaml> = {};
        const tasksYaml: Record<string, TaskYaml> = {};

        nodes.forEach(node => {
          if (node.type === 'agentNode') {
            const agentData = node.data as AgentYaml;
            agentsYaml[`agent_${node.id}`] = agentData;
          } else if (node.type === 'taskNode') {
            const taskData = node.data as TaskYaml;
            tasksYaml[`task_${node.id}`] = taskData;
          }
        });

        // Validate that all tasks are assigned to agents
        const unassignedTasks = taskNodes.filter(taskNode => {
          return !edges.some(edge => 
            edge.target === taskNode.id && 
            nodes.find(n => n.id === edge.source)?.type === 'agentNode'
          );
        });

        if (unassignedTasks.length > 0) {
          toast.error('All tasks must be assigned to an agent. Please connect your tasks to agents on the canvas.');
          return;
        }

        edges.forEach(edge => {
          if (edge.source && edge.target) {
            const sourceNode = nodes.find(n => n.id === edge.source);
            const targetNode = nodes.find(n => n.id === edge.target);

            if (sourceNode?.type === 'agentNode' && targetNode?.type === 'taskNode') {
              tasksYaml[`task_${edge.target}`].agent = `agent_${edge.source}`;
            }
          }
        });

        scheduleData = {
          name,
          cron_expression: cronExpression,
          agents_yaml: agentsYaml,
          tasks_yaml: tasksYaml,
          inputs: inputs,
          is_active: true,
          planning: planningEnabled,
          model: selectedModel
        };
      }

      if (dialogMode === 'edit' && editingScheduleId) {
        await ScheduleService.updateSchedule(editingScheduleId, scheduleData);
        toast.success('Schedule updated successfully');
      } else {
        await ScheduleService.createSchedule(scheduleData);
        toast.success('Schedule created successfully');
      }

      resetForm();
      loadSchedules();
    } catch (error) {
      console.error('Error saving schedule:', error);
      setError('Failed to save schedule');
    } finally {
      setLoading(false);
    }
  };

  const handleDeleteSchedule = async (id: number) => {
    try {
      await ScheduleService.deleteSchedule(id);
      toast.success('Schedule deleted successfully');
      loadSchedules();
    } catch (error) {
      console.error('Error deleting schedule:', error);
      toast.error('Failed to delete schedule');
    }
  };

  const handleToggleSchedule = async (id: number) => {
    try {
      await ScheduleService.toggleSchedule(id);
      toast.success('Schedule toggled successfully');
      loadSchedules();
    } catch (error) {
      console.error('Error toggling schedule:', error);
      toast.error('Failed to toggle schedule');
    }
  };

  const handleViewConfig = (schedule: Schedule) => {
    setSelectedSchedule(schedule);
    setIsConfigViewerOpen(true);
  };

  const renderVisualCronBuilder = () => {
    const hours = Array.from({ length: 24 }, (_, i) => i.toString());
    const minutes = Array.from({ length: 60 }, (_, i) => i.toString());
    const daysOfWeek = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
    const monthNames = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
    const daysInMonth = Array.from({ length: 31 }, (_, i) => (i + 1).toString());

    return (
      <Box sx={{ mt: 2 }}>
        <FormControl fullWidth sx={{ mb: 2 }}>
          <InputLabel>Frequency</InputLabel>
          <Select
            value={frequency}
            onChange={(e) => setFrequency(e.target.value)}
            label="Frequency"
          >
            <MenuItem value="hourly">Hourly</MenuItem>
            <MenuItem value="daily">Daily</MenuItem>
            <MenuItem value="weekly">Weekly</MenuItem>
            <MenuItem value="monthly">Monthly</MenuItem>
            <MenuItem value="yearly">Yearly</MenuItem>
          </Select>
        </FormControl>

        <Grid container spacing={2} sx={{ mb: 2 }}>
          <Grid item xs={6}>
            <FormControl fullWidth>
              <InputLabel>Hour</InputLabel>
              <Select
                value={selectedTime.hour}
                onChange={(e) => setSelectedTime(prev => ({ ...prev, hour: e.target.value }))}
                label="Hour"
                disabled={frequency === 'hourly'}
              >
                {hours.map(hour => (
                  <MenuItem key={hour} value={hour}>{hour.padStart(2, '0')}</MenuItem>
                ))}
              </Select>
            </FormControl>
          </Grid>
          <Grid item xs={6}>
            <FormControl fullWidth>
              <InputLabel>Minute</InputLabel>
              <Select
                value={selectedTime.minute}
                onChange={(e) => setSelectedTime(prev => ({ ...prev, minute: e.target.value }))}
                label="Minute"
              >
                {minutes.map(minute => (
                  <MenuItem key={minute} value={minute}>{minute.padStart(2, '0')}</MenuItem>
                ))}
              </Select>
            </FormControl>
          </Grid>
        </Grid>

        {frequency === 'weekly' && (
          <FormControl component="fieldset" fullWidth sx={{ mb: 2 }}>
            <Typography variant="subtitle2" gutterBottom>Select Days</Typography>
            <FormGroup row>
              {daysOfWeek.map((day, index) => (
                <FormControlLabel
                  key={day}
                  control={
                    <Checkbox
                      checked={selectedDays.includes(index.toString())}
                      onChange={(e) => {
                        if (e.target.checked) {
                          setSelectedDays([...selectedDays, index.toString()]);
                        } else {
                          setSelectedDays(selectedDays.filter(d => d !== index.toString()));
                        }
                      }}
                    />
                  }
                  label={day}
                />
              ))}
            </FormGroup>
          </FormControl>
        )}

        {frequency === 'monthly' && (
          <FormControl component="fieldset" fullWidth sx={{ mb: 2 }}>
            <Typography variant="subtitle2" gutterBottom>Select Days of Month</Typography>
            <FormGroup row>
              {daysInMonth.map((day) => (
                <FormControlLabel
                  key={day}
                  control={
                    <Checkbox
                      checked={selectedDaysOfMonth.includes(day)}
                      onChange={(e) => {
                        if (e.target.checked) {
                          setSelectedDaysOfMonth([...selectedDaysOfMonth, day]);
                        } else {
                          setSelectedDaysOfMonth(selectedDaysOfMonth.filter(d => d !== day));
                        }
                      }}
                    />
                  }
                  label={day}
                  sx={{ width: '70px' }}
                />
              ))}
            </FormGroup>
          </FormControl>
        )}

        {frequency === 'yearly' && (
          <FormControl component="fieldset" fullWidth sx={{ mb: 2 }}>
            <Typography variant="subtitle2" gutterBottom>Select Months</Typography>
            <FormGroup row>
              {monthNames.map((month, index) => (
                <FormControlLabel
                  key={month}
                  control={
                    <Checkbox
                      checked={selectedMonths.includes((index + 1).toString())}
                      onChange={(e) => {
                        if (e.target.checked) {
                          setSelectedMonths([...selectedMonths, (index + 1).toString()]);
                        } else {
                          setSelectedMonths(selectedMonths.filter(m => m !== (index + 1).toString()));
                        }
                      }}
                    />
                  }
                  label={month}
                />
              ))}
            </FormGroup>
          </FormControl>
        )}
      </Box>
    );
  };

  // Add a warning message when editing
  const renderEditWarning = () => {
    if (dialogMode === 'edit') {
      return (
        <Box sx={{ mt: 2, mb: 2, p: 2, bgcolor: 'warning.light', borderRadius: 1 }}>
          <Typography variant="body2" color="warning.dark">
            Note: When editing a schedule, the existing agent and task configuration will be preserved.
            To modify the configuration, create a new schedule instead.
          </Typography>
        </Box>
      );
    }
    return null;
  };

  return (
    <>
      <Dialog open={open} onClose={onClose} maxWidth="md" fullWidth>
        <DialogTitle sx={{ 
          display: 'flex', 
          alignItems: 'center', 
          justifyContent: 'space-between',
          pb: 1.5,
          borderBottom: '1px solid',
          borderColor: 'divider'
        }}>
          <Typography variant="h6">Schedule Manager</Typography>
          <IconButton 
            onClick={onClose}
            size="small"
            sx={{ 
              color: 'text.secondary',
              '&:hover': {
                color: 'text.primary',
              }
            }}
          >
            <CloseIcon />
          </IconButton>
        </DialogTitle>
        <DialogContent>
          <Box sx={{ mb: 4 }}>
            <Typography variant="h6" gutterBottom>
              {dialogMode === 'create' ? 'Create New Schedule' : 'Edit Schedule'}
            </Typography>

            {renderEditWarning()}

            <TextField
              fullWidth
              label="Schedule Name"
              value={name}
              onChange={(e) => setName(e.target.value)}
              margin="normal"
            />

            <Box sx={{ mb: 2, mt: 2 }}>
              <ToggleButtonGroup
                value={cronMode}
                exclusive
                onChange={(e, newMode) => newMode && setCronMode(newMode)}
                sx={{ mb: 2 }}
              >
                <ToggleButton value="visual">Simple Schedule</ToggleButton>
                <ToggleButton value="manual">Advanced (Cron)</ToggleButton>
              </ToggleButtonGroup>

              {cronMode === 'manual' ? (
                <TextField
                  fullWidth
                  label="Cron Expression"
                  value={cronExpression}
                  onChange={(e) => setCronExpression(e.target.value)}
                  helperText="Example: '0 0 * * *' for daily at midnight"
                />
              ) : (
                renderVisualCronBuilder()
              )}
            </Box>

            <Button
              variant="contained"
              onClick={handleSaveSchedule}
              disabled={loading || !name || !cronExpression}
              sx={{ mt: 2 }}
            >
              {dialogMode === 'create' ? 'Create Schedule' : 'Update Schedule'}
            </Button>
            {dialogMode === 'edit' && (
              <Button
                variant="outlined"
                onClick={resetForm}
                sx={{ mt: 2, ml: 2 }}
              >
                Cancel Edit
              </Button>
            )}
          </Box>

          <Typography variant="h6" gutterBottom>Existing Schedules</Typography>
          <List>
            {schedules.map((schedule) => (
              <ListItem key={schedule.id}>
                <ListItemText
                  primary={schedule.name}
                  secondary={
                    <>
                      <Typography component="span" variant="body2" color="text.primary">
                        {schedule.cron_expression}
                      </Typography>
                      <br />
                      {schedule.next_run_at && (
                        <Typography component="span" variant="body2">
                          Next run: {new Date(schedule.next_run_at).toLocaleString()}
                        </Typography>
                      )}
                    </>
                  }
                />
                <ListItemSecondaryAction>
                  <Tooltip title="View Configuration">
                    <IconButton
                      edge="end"
                      onClick={() => handleViewConfig(schedule)}
                      sx={{ ml: 1 }}
                    >
                      <InfoIcon />
                    </IconButton>
                  </Tooltip>
                  <Tooltip title="Toggle Schedule">
                    <Switch
                      edge="end"
                      checked={schedule.is_active}
                      onChange={() => handleToggleSchedule(schedule.id)}
                    />
                  </Tooltip>
                  <Tooltip title="Edit Schedule">
                    <IconButton
                      edge="end"
                      onClick={() => handleEditSchedule(schedule)}
                      sx={{ ml: 1 }}
                    >
                      <EditIcon />
                    </IconButton>
                  </Tooltip>
                  <Tooltip title="Delete Schedule">
                    <IconButton
                      edge="end"
                      onClick={() => handleDeleteSchedule(schedule.id)}
                      sx={{ ml: 1 }}
                    >
                      <DeleteIcon />
                    </IconButton>
                  </Tooltip>
                </ListItemSecondaryAction>
              </ListItem>
            ))}
          </List>
        </DialogContent>
        <DialogActions>
          <Button onClick={onClose}>Close</Button>
        </DialogActions>
      </Dialog>

      <ConfigViewerDialog
        open={isConfigViewerOpen}
        onClose={() => {
          setIsConfigViewerOpen(false);
          setSelectedSchedule(null);
        }}
        schedule={selectedSchedule}
      />

      <Snackbar
        open={!!error}
        autoHideDuration={6000}
        onClose={handleCloseError}
        anchorOrigin={{ vertical: 'top', horizontal: 'center' }}
      >
        <Alert
          onClose={handleCloseError}
          severity="error"
          variant="filled"
          sx={{ width: '100%', whiteSpace: 'pre-line' }}
        >
          {error}
        </Alert>
      </Snackbar>
    </>
  );
};

export default ScheduleDialog;
