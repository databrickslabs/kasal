import React, { useState, useEffect } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  IconButton,
  List,
  ListItemButton,
  ListItemIcon,
  ListItemText,
  Divider,
  Box,
  Button,
  Tooltip,
} from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';
import TaskIcon from '@mui/icons-material/Assignment';
import DeleteIcon from '@mui/icons-material/Delete';
import AddIcon from '@mui/icons-material/Add';
import { Task, TaskService } from '../../api/TaskService';
import { TaskSelectionDialogProps } from '../../types/task';
import TaskForm from './TaskForm';
import { ToolService, Tool } from '../../api/ToolService';

const TaskSelectionDialog: React.FC<TaskSelectionDialogProps> = ({
  open,
  onClose,
  onTaskSelect,
  tasks,
  onShowTaskForm,
  fetchTasks,
}) => {
  const [selectedTask, setSelectedTask] = useState<Task | undefined>(undefined);
  const [showTaskForm, setShowTaskForm] = useState(false);
  const [selectedTasks, setSelectedTasks] = useState<Task[]>([]);
  const [tools, setTools] = useState<Tool[]>([]);
  const [isDeleting, setIsDeleting] = useState(false);

  useEffect(() => {
    let mounted = true;

    const fetchData = async () => {
      try {
        if (open) {
          const [_, toolsData] = await Promise.all([
            fetchTasks(),
            ToolService.listTools()
          ]);
          if (mounted) {
            setTools(toolsData);
          }
        }
      } catch (error) {
        console.error('Error fetching data:', error);
      }
    };

    void fetchData();

    return () => {
      mounted = false;
    };
  }, [open, fetchTasks]);

  const handleDeleteTask = async (task: Task) => {
    if (!task.id) return;
    
    try {
      setIsDeleting(true);
      await TaskService.deleteTask(task.id);
      await fetchTasks();
    } catch (error) {
      console.error('Error deleting task:', error);
    } finally {
      setIsDeleting(false);
    }
  };

  const handleCreateTask = () => {
    setSelectedTask(undefined);
    setSelectedTasks([]);
    setShowTaskForm(true);
  };

  const handleTaskSelect = (task: Task) => {
    const isSelected = selectedTasks.some(t => t.id === task.id);
    const newSelectedTasks = isSelected
      ? selectedTasks.filter(t => t.id !== task.id)
      : [...selectedTasks, task];
    
    setSelectedTasks(newSelectedTasks);
  };

  const handleConfirm = () => {
    const formattedTasks = selectedTasks.map(task => ({
      ...task,
      id: task.id,
      name: task.name,
      description: task.description,
      expected_output: task.expected_output,
      tools: task.tools,
      agent_id: task.agent_id,
      async_execution: task.async_execution,
      context: task.context,
      config: task.config
    }));

    onTaskSelect(formattedTasks);
    onClose();
  };

  const handleSelectAll = () => {
    if (selectedTasks.length === tasks.length) {
      setSelectedTasks([]);
    } else {
      setSelectedTasks([...tasks]);
    }
  };

  return (
    <>
      <Dialog open={open} onClose={onClose} maxWidth="md" fullWidth>
        <DialogTitle>
          Manage Tasks
          <IconButton
            aria-label="close"
            onClick={onClose}
            sx={{ position: 'absolute', right: 8, top: 8 }}
          >
            <CloseIcon />
          </IconButton>
        </DialogTitle>
        <DialogContent>
          <Box sx={{ mb: 2, display: 'flex', gap: 1 }}>
            <Button
              variant="contained"
              startIcon={<AddIcon />}
              onClick={handleCreateTask}
            >
              Create Task
            </Button>
            <Button
              variant="outlined"
              onClick={handleSelectAll}
            >
              {selectedTasks.length === tasks.length ? 'Deselect All' : 'Select All'}
            </Button>
            <Button
              variant="outlined"
              color="error"
              startIcon={<DeleteIcon />}
              onClick={async () => {
                try {
                  await TaskService.deleteAllTasks();
                  setSelectedTasks([]);
                  await fetchTasks();
                } catch (error) {
                  console.error('Error deleting all tasks:', error);
                }
              }}
            >
              Delete All
            </Button>
          </Box>

          <Divider sx={{ my: 2 }} />
          
          <List sx={{ maxHeight: '50vh', overflow: 'auto' }}>
            {[...tasks].reverse().map((task) => (
              <ListItemButton 
                key={task.id}
                onClick={() => handleTaskSelect(task)}
                selected={selectedTasks.some(t => t.id === task.id)}
              >
                <ListItemIcon>
                  <TaskIcon />
                </ListItemIcon>
                <ListItemText 
                  primary={task.name || task.description}
                  secondary={task.description}
                />
                <Tooltip title="Delete Task">
                  <IconButton
                    onClick={(e) => {
                      e.stopPropagation();
                      handleDeleteTask(task);
                    }}
                    size="small"
                    color="error"
                    disabled={isDeleting}
                  >
                    <DeleteIcon fontSize="small" />
                  </IconButton>
                </Tooltip>
              </ListItemButton>
            ))}
          </List>
        </DialogContent>
        <DialogActions sx={{ px: 3, py: 2 }}>
          <Button
            variant="contained"
            onClick={handleConfirm}
            disabled={selectedTasks.length === 0}
          >
            Place Selected ({selectedTasks.length})
          </Button>
        </DialogActions>
      </Dialog>

      {showTaskForm && (
        <Dialog 
          open={showTaskForm}
          onClose={() => {
            setShowTaskForm(false);
            setSelectedTask(undefined);
          }}
          maxWidth="md"
          fullWidth
          PaperProps={{
            sx: {
              position: 'relative'
            }
          }}
        >
          <DialogTitle>
            {selectedTask ? 'Edit Task' : 'Create New Task'}
            <IconButton
              aria-label="close"
              onClick={() => {
                setShowTaskForm(false);
                setSelectedTask(undefined);
              }}
              sx={{
                position: 'absolute',
                right: 8,
                top: 8
              }}
            >
              <CloseIcon />
            </IconButton>
          </DialogTitle>
          <DialogContent>
            <TaskForm
              key={selectedTask ? 'edit' : 'create'}
              initialData={selectedTask}
              onCancel={() => {
                setShowTaskForm(false);
                setSelectedTask(undefined);
              }}
              onTaskSaved={async (savedTask) => {
                await fetchTasks();
                if (!selectedTasks.some(t => t.id === savedTask.id)) {
                  setSelectedTasks(prev => [...prev, savedTask]);
                } else {
                  setSelectedTasks(prev => 
                    prev.map(t => t.id === savedTask.id ? savedTask : t)
                  );
                }
                setShowTaskForm(false);
                setSelectedTask(undefined);
              }}
              tools={tools}
              hideTitle
            />
          </DialogContent>
        </Dialog>
      )}
    </>
  );
};

export default TaskSelectionDialog; 