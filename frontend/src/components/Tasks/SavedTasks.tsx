import React, { useState, useEffect, useCallback } from 'react';
import {
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  IconButton,
  Stack,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  TextField,
  Snackbar,
  Alert,
  FormControlLabel,
  Switch,
  Select,
  MenuItem,
  FormControl,
  InputLabel,
  Box,
  Chip,
  OutlinedInput,
  SelectChangeEvent,
} from '@mui/material';
import EditIcon from '@mui/icons-material/Edit';
import DeleteIcon from '@mui/icons-material/Delete';
import BuildIcon from '@mui/icons-material/Build';
import { NotificationState } from '../../types/common';
import { SavedTasksProps, TaskFormData } from '../../types/task';
import { TaskService } from '../../api/TaskService';
import { AgentService } from '../../api/AgentService';
import { ToolService, Tool as ServiceTool } from '../../api/ToolService';
import { Agent } from '../../types/agent';

function SavedTasks({ refreshTrigger }: SavedTasksProps): JSX.Element {
  const [tasks, setTasks] = useState<TaskFormData[]>([]);
  const [editDialog, setEditDialog] = useState<boolean>(false);
  const [editedTask, setEditedTask] = useState<TaskFormData>({} as TaskFormData);
  const [tools, setTools] = useState<ServiceTool[]>([]);
  const [availableTasks, setAvailableTasks] = useState<TaskFormData[]>([]);
  const [agents, setAgents] = useState<Agent[]>([]);
  const [notification, setNotification] = useState<NotificationState>({
    open: false,
    message: '',
    severity: 'success'
  });

  const loadTasks = useCallback(async () => {
    try {
      const response = await TaskService.listTasks();
      setTasks(response);
      setAvailableTasks(response);
    } catch (error) {
      console.error('Error loading tasks:', error);
      showNotification('Error loading tasks', 'error');
    }
  }, []);

  useEffect(() => {
    loadTasks();
  }, [refreshTrigger, loadTasks]);

  useEffect(() => {
    const fetchData = async () => {
      try {
        const [toolsResponse, agentsResponse] = await Promise.all([
          ToolService.listTools(),
          AgentService.listAgents()
        ]);
        setTools(toolsResponse);
        setAgents(agentsResponse.filter((agent): agent is Agent => agent !== null));
      } catch (error) {
        console.error('Error fetching data:', error);
      }
    };
    fetchData();
  }, []);

  const showNotification = (message: string, severity: NotificationState['severity'] = 'success') => {
    setNotification({
      open: true,
      message,
      severity
    });
  };

  const handleEdit = (task: TaskFormData) => {
    setEditedTask({
      ...task,
      tools: task.tools || [],
      context: task.context || [],
    });
    setEditDialog(true);
  };

  const handleSaveEdit = async () => {
    try {
      if (!editedTask.id) {
        throw new Error('Task ID is missing');
      }
      const savedTask = await TaskService.updateTask(editedTask.id, editedTask);
      setEditDialog(false);
      showNotification('Task updated successfully');
      await loadTasks();
      setEditedTask(savedTask);
    } catch (error) {
      console.error('Error updating task:', error);
      showNotification(`Error updating task: ${error instanceof Error ? error.message : String(error)}`, 'error');
    }
  };

  const handleDeleteTask = async (taskId: string) => {
    if (window.confirm('Are you sure you want to delete this task?')) {
      try {
        await TaskService.deleteTask(taskId);
        showNotification('Task deleted successfully');
        loadTasks();
      } catch (error) {
        console.error('Error deleting task:', error);
        showNotification(`Error deleting task: ${error instanceof Error ? error.message : String(error)}`, 'error');
      }
    }
  };

  const handleChange = (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement> | SelectChangeEvent<string | string[]>) => {
    const { name, value } = e.target;
    setEditedTask(prev => ({
      ...prev,
      [name]: value
    }));
  };

  const handleSwitchChange = (name: string) => (e: React.ChangeEvent<HTMLInputElement>) => {
    setEditedTask(prev => ({
      ...prev,
      [name]: e.target.checked
    }));
  };

  return (
    <>
      <TableContainer component={Paper}>
        <Table>
          <TableHead>
            <TableRow>
              <TableCell>Name</TableCell>
              <TableCell>Description</TableCell>
              <TableCell>Expected Output</TableCell>
              <TableCell>Agent</TableCell>
              <TableCell>Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {tasks.map((task) => (
              <TableRow key={task.id}>
                <TableCell>{task.name}</TableCell>
                <TableCell>{task.description}</TableCell>
                <TableCell>{task.expected_output}</TableCell>
                <TableCell>
                  {agents.find(a => a.id === String(task.agent_id))?.name || 'No Agent'}
                </TableCell>
                <TableCell>
                  <Stack direction="row" spacing={1}>
                    <IconButton 
                      size="small" 
                      onClick={() => handleEdit(task)}
                      color="primary"
                    >
                      <EditIcon />
                    </IconButton>
                    <IconButton 
                      size="small" 
                      onClick={() => task.id && handleDeleteTask(task.id.toString())}
                      color="error"
                    >
                      <DeleteIcon />
                    </IconButton>
                  </Stack>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>

      <Dialog open={editDialog} onClose={() => setEditDialog(false)} maxWidth="md" fullWidth>
        <DialogTitle>Edit Task</DialogTitle>
        <DialogContent>
          <Stack spacing={2} sx={{ mt: 2 }}>
            <TextField
              fullWidth
              label="Name"
              name="name"
              value={editedTask.name || ''}
              onChange={handleChange}
            />
            <TextField
              fullWidth
              label="Description"
              name="description"
              value={editedTask.description || ''}
              onChange={handleChange}
              multiline
              rows={3}
            />
            <TextField
              fullWidth
              label="Expected Output"
              name="expected_output"
              value={editedTask.expected_output || ''}
              onChange={handleChange}
              multiline
              rows={2}
            />

            <FormControl fullWidth>
              <InputLabel>Agent</InputLabel>
              <Select
                value={String(editedTask.agent_id || '')}
                onChange={handleChange}
                name="agent_id"
                label="Agent"
              >
                <MenuItem value="">
                  <em>No Agent</em>
                </MenuItem>
                {agents.map((agent) => (
                  <MenuItem key={agent.id} value={agent.id}>
                    {agent.name || agent.role}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>

            <FormControl fullWidth>
              <InputLabel>Tools</InputLabel>
              <Select
                multiple
                value={editedTask.tools || []}
                onChange={handleChange}
                name="tools"
                input={<OutlinedInput label="Tools" />}
                renderValue={(selected) => (
                  <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                    {(selected as string[]).map((toolId) => {
                      const tool = tools.find(t => String(t.id) === toolId);
                      return (
                        <Chip 
                          key={toolId}
                          label={tool ? tool.title : toolId}
                          size="small"
                          icon={<BuildIcon />}
                        />
                      );
                    })}
                  </Box>
                )}
              >
                {tools.map((tool) => (
                  <MenuItem key={tool.id} value={String(tool.id)}>
                    {tool.title}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>

            <FormControl fullWidth>
              <InputLabel>Context Tasks</InputLabel>
              <Select
                multiple
                value={editedTask.context || []}
                onChange={handleChange}
                name="context"
                input={<OutlinedInput label="Context Tasks" />}
              >
                {availableTasks.map((task) => (
                  <MenuItem key={task.id} value={task.id}>
                    {task.name}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>

            <FormControlLabel
              control={
                <Switch
                  checked={editedTask.async_execution || false}
                  onChange={handleSwitchChange('async_execution')}
                />
              }
              label="Async Execution"
            />

            <TextField
              fullWidth
              label="Output JSON Schema"
              name="output_json"
              value={editedTask.output_json || ''}
              onChange={handleChange}
              multiline
              rows={3}
            />

            <TextField
              fullWidth
              label="Output Pydantic Model"
              name="output_pydantic"
              value={editedTask.output_pydantic || ''}
              onChange={handleChange}
              multiline
              rows={3}
            />

            <TextField
              fullWidth
              label="Output File Path"
              name="output_file"
              value={editedTask.output_file || ''}
              onChange={handleChange}
            />

            <FormControlLabel
              control={
                <Switch
                  checked={editedTask.human_input || false}
                  onChange={handleSwitchChange('human_input')}
                />
              }
              label="Human Input"
            />

            <FormControlLabel
              control={
                <Switch
                  checked={editedTask.retry_on_fail !== false}
                  onChange={handleSwitchChange('retry_on_fail')}
                />
              }
              label="Retry on Fail"
            />

            <TextField
              fullWidth
              type="number"
              label="Max Retries"
              name="max_retries"
              value={editedTask.max_retries || 3}
              onChange={handleChange}
            />

            <TextField
              fullWidth
              type="number"
              label="Timeout (seconds)"
              name="timeout"
              value={editedTask.timeout || ''}
              onChange={handleChange}
            />

            <TextField
              fullWidth
              type="number"
              label="Priority"
              name="priority"
              value={editedTask.priority || 1}
              onChange={handleChange}
            />

            <FormControl fullWidth>
              <InputLabel>Error Handling</InputLabel>
              <Select
                value={editedTask.error_handling || 'default'}
                onChange={handleChange}
                name="error_handling"
                label="Error Handling"
              >
                <MenuItem value="default">Default</MenuItem>
                <MenuItem value="ignore">Ignore</MenuItem>
                <MenuItem value="retry">Retry</MenuItem>
                <MenuItem value="fail">Fail</MenuItem>
              </Select>
            </FormControl>

            <FormControlLabel
              control={
                <Switch
                  checked={editedTask.cache_response !== false}
                  onChange={handleSwitchChange('cache_response')}
                />
              }
              label="Cache Response"
            />

            <TextField
              fullWidth
              type="number"
              label="Cache TTL (seconds)"
              name="cache_ttl"
              value={editedTask.cache_ttl || 3600}
              onChange={handleChange}
            />
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setEditDialog(false)}>Cancel</Button>
          <Button onClick={handleSaveEdit} variant="contained">Save Changes</Button>
        </DialogActions>
      </Dialog>

      <Snackbar 
        open={notification.open} 
        autoHideDuration={6000} 
        onClose={() => setNotification(prev => ({ ...prev, open: false }))}
        anchorOrigin={{ vertical: 'top', horizontal: 'right' }}
      >
        <Alert 
          onClose={() => setNotification(prev => ({ ...prev, open: false }))} 
          severity={notification.severity}
        >
          {notification.message}
        </Alert>
      </Snackbar>
    </>
  );
}

export default SavedTasks; 