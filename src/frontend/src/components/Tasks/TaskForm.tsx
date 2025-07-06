import React, { useState, useEffect, useRef } from 'react';
import {
  TextField,
  Button,
  Box,
  FormControl,
  InputLabel,
  Select,
  Typography,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  SelectChangeEvent,
  MenuItem,
  Chip,
  Snackbar,
  Alert,
  Card,
  Divider,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  IconButton,
  InputAdornment,
} from '@mui/material';
import { type Task } from '../../api/TaskService';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import OpenInFullIcon from '@mui/icons-material/OpenInFull';
import CloseIcon from '@mui/icons-material/Close';
import { TaskAdvancedConfig } from './TaskAdvancedConfig';
import { TaskService } from '../../api/TaskService';
import useStableResize from '../../hooks/global/useStableResize';

interface TaskFormProps {
  initialData?: Task;
  onCancel?: () => void;
  onTaskSaved?: (task: Task) => void;
  onSubmit?: (task: Task) => Promise<void>;
  isEdit?: boolean;
  tools: Tool[];
  hideTitle?: boolean;
}

interface Tool {
  id: number;
  title: string;
  description: string;
  icon: string;
  enabled?: boolean;
}

const TaskForm: React.FC<TaskFormProps> = ({ initialData, onCancel, onTaskSaved, onSubmit, isEdit, tools, hideTitle }) => {
  const [expandedAccordion, setExpandedAccordion] = useState<boolean>(false);
  const [expandedDescription, setExpandedDescription] = useState<boolean>(false);
  const [expandedOutput, setExpandedOutput] = useState<boolean>(false);
  const accordionRef = useRef<HTMLDivElement>(null);
  const [formData, setFormData] = useState<Task>({
    id: initialData?.id ?? '',
    name: initialData?.name ?? '',
    description: initialData?.description ?? '',
    expected_output: initialData?.expected_output ?? '',
    tools: initialData?.tools ?? [],
    agent_id: initialData?.agent_id ?? null,
    async_execution: initialData?.async_execution !== undefined ? Boolean(initialData.async_execution) : false,
    context: initialData?.context ?? [],
    markdown: initialData?.markdown === true || String(initialData?.markdown) === 'true',
    config: !initialData?.config ? {
      cache_response: false,
      cache_ttl: 3600,
      retry_on_fail: true,
      max_retries: 3,
      timeout: null,
      priority: 1,
      error_handling: 'default',
      output_file: null,
      output_json: null,
      output_pydantic: null,
      callback: null,
      human_input: false,
      guardrail: null,
      markdown: false
    } : {
      cache_response: initialData.config.cache_response ?? false,
      cache_ttl: initialData.config.cache_ttl ?? 3600,
      retry_on_fail: initialData.config.retry_on_fail ?? true,
      max_retries: initialData.config.max_retries ?? 3,
      timeout: initialData.config.timeout ?? null,
      priority: initialData.config.priority ?? 1,
      error_handling: initialData.config.error_handling ?? 'default',
      output_file: initialData.config.output_file ?? null,
      output_json: initialData.config.output_json ?? null,
      output_pydantic: initialData.config.output_pydantic ?? null,
      callback: initialData.config.callback ?? null,
      human_input: initialData.config.human_input ?? false,
      condition: initialData.config.condition,
      guardrail: initialData.config.guardrail ?? null,
      markdown: initialData.config.markdown ?? false
    }
  });
  const [error, setError] = useState<string | null>(null);
  const [availableTasks, setAvailableTasks] = useState<Task[]>([]);

  useEffect(() => {
    if (initialData?.tools) {
      setFormData(prev => ({
        ...prev,
        tools: initialData.tools
      }));
    }
  }, [initialData?.tools]);

  useEffect(() => {
    // Fetch available tasks when component mounts
    const fetchTasks = async () => {
      try {
        const tasks = await TaskService.listTasks();
        setAvailableTasks(tasks);
      } catch (error) {
        console.error('Error fetching tasks:', error);
        setError('Error loading available tasks');
      }
    };

    void fetchTasks();
  }, []);

  const handleInputChange = (field: keyof Task, value: string) => {
    setFormData((prev: Task) => ({
      ...prev,
      [field]: value
    }));
  };

  const handleAdvancedConfigChange = (field: string, value: string | number | boolean | null) => {
    console.log(`Changing ${field} to:`, value);
    
    setFormData(prev => {
      // Handle special fields that exist at the top level of formData
      if (field === 'async_execution') {
        console.log(`Setting async_execution to ${value} (type: ${typeof value})`);
        return {
          ...prev,
          async_execution: value === undefined ? false : Boolean(value)
        };
      }
      
      // Create updated config for all other fields
      const updatedConfig = {
        ...prev.config,
        [field]: field === 'condition' ? (value ? 'is_data_missing' : undefined) : value,
      };
      
      console.log('Updated config will be:', updatedConfig);
      
      return {
        ...prev,
        config: updatedConfig
      };
    });
  };

  const handleToolsChange = (event: SelectChangeEvent<string[]>) => {
    const selectedTools = Array.isArray(event.target.value) 
      ? event.target.value 
      : [event.target.value];
    
    console.log('Tools selected:', selectedTools);
    
    setFormData(prev => ({
      ...prev,
      tools: selectedTools
    }));
  };

  const handleSave = async () => {
    try {
      // Clear any existing error
      setError(null);

      try {
        // Validate the form data
        if (!formData.name.trim()) {
          setError('Task name is required');
          return;
        }
        
        console.log('Current formData before saving:', formData);
        console.log('Current config values:', {
          output_pydantic: formData.config.output_pydantic,
          callback: formData.config.callback
        });

        // Create a cleaned version of the form data
        const cleanedFormData: Task = {
          ...formData,
          context: Array.from(formData.context),
          // Ensure top-level markdown is synchronized with config.markdown
          markdown: formData.config.markdown ?? formData.markdown,
          config: {
            ...formData.config,
            condition: formData.config.condition === 'is_data_missing' ? 'is_data_missing' : undefined,
            callback: formData.config.callback,
            // Ensure output_pydantic is properly set in config
            output_pydantic: formData.config.output_pydantic,
            // Ensure config.markdown is synchronized with top-level markdown
            markdown: formData.config.markdown ?? formData.markdown
          }
        };
        
        console.log('Cleaned formData to save:', cleanedFormData);
        console.log('Final config values to save:', {
          output_pydantic: cleanedFormData.config.output_pydantic,
          callback: cleanedFormData.config.callback
        });

        try {
          // Create or update the task in the database
          let savedTask;
          if (formData.id) {
            console.log('Updating existing task with ID:', formData.id);
            savedTask = await TaskService.updateTask(formData.id, cleanedFormData);
          } else {
            console.log('Creating new task');
            savedTask = await TaskService.createTask(cleanedFormData);
          }
          
          console.log('Task saved successfully:', savedTask);
          console.log('Saved config values:', {
            output_pydantic: savedTask.config.output_pydantic,
            callback: savedTask.config.callback
          });

          if (onTaskSaved) {
            onTaskSaved(savedTask);
          }
          
          // Close the form after successful save
          if (onCancel) {
            onCancel();
          }
        } catch (error) {
          console.error('Error saving task:', error);
          setError(error instanceof Error ? error.message : 'Error saving task');
        }
      } catch (error) {
        console.error('Error validating task:', error);
        setError('Error validating task configuration.');
      }
    } catch (error) {
      console.error('Error in handleSave:', error);
      setError('An unexpected error occurred.');
    }
  };



  // Handle accordion expansion with debouncing to prevent ResizeObserver loops
  const handleAccordionChange = (_event: React.SyntheticEvent, isExpanded: boolean) => {
    setExpandedAccordion(isExpanded);
  };
  
  // Use our custom resize hook to safely handle resizes
  useStableResize(
    () => {
      // This callback is called in a debounced manner to prevent loops
      // You can add any additional layout adjustments here if needed
    },
    accordionRef,
    150 // Debounce time in ms
  );

  const handleOpenDescriptionDialog = () => {
    setExpandedDescription(true);
  };

  const handleCloseDescriptionDialog = () => {
    setExpandedDescription(false);
  };

  const handleOpenOutputDialog = () => {
    setExpandedOutput(true);
  };

  const handleCloseOutputDialog = () => {
    setExpandedOutput(false);
  };

  return (
    <>
      <Card sx={{ 
        display: 'flex', 
        flexDirection: 'column', 
        height: '70vh',
        position: 'relative',
        overflow: 'hidden'
      }}>
        <Box sx={{ p: 3, pb: 2 }}>
          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
            {!hideTitle && (
              <Typography variant="h6">
                {initialData?.id ? 'Edit Task' : 'Create New Task'}
              </Typography>
            )}
          </Box>
          <Divider />
        </Box>

        <Box sx={{ 
          flex: '1 1 auto', 
          overflow: 'auto',
          px: 3, 
          pb: 2,
          height: 'calc(90vh - 170px)',
        }}>
          <Box 
            component="form" 
            onSubmit={(e: React.FormEvent<HTMLFormElement>) => {
              e.preventDefault();
              void handleSave();
            }}
            sx={{ 
              display: 'flex', 
              flexDirection: 'column', 
              gap: 2
            }}
          >
            <TextField
              fullWidth
              label="Name"
              value={formData.name}
              onChange={(e) => handleInputChange('name', e.target.value)}
              required
              margin="normal"
              sx={{
                '& .MuiOutlinedInput-root': {
                  '& fieldset': {
                    borderColor: 'rgba(0, 0, 0, 0.23)',
                  },
                },
                '& .MuiInputLabel-root': {
                  backgroundColor: 'white',
                  padding: '0 4px',
                },
              }}
            />
            <TextField
              fullWidth
              label="Description"
              value={formData.description}
              onChange={(e) => handleInputChange('description', e.target.value)}
              multiline
              rows={3}
              required
              InputProps={{
                endAdornment: (
                  <InputAdornment position="end">
                    <IconButton
                      edge="end"
                      onClick={handleOpenDescriptionDialog}
                      size="small"
                      sx={{ opacity: 0.7 }}
                      title="Expand description"
                    >
                      <OpenInFullIcon fontSize="small" />
                    </IconButton>
                  </InputAdornment>
                )
              }}
            />
            <TextField
              fullWidth
              label="Expected Output"
              value={formData.expected_output}
              onChange={(e) => handleInputChange('expected_output', e.target.value)}
              multiline
              rows={3}
              required
              InputProps={{
                endAdornment: (
                  <InputAdornment position="end">
                    <IconButton
                      edge="end"
                      onClick={handleOpenOutputDialog}
                      size="small"
                      sx={{ opacity: 0.7 }}
                      title="Expand expected output"
                    >
                      <OpenInFullIcon fontSize="small" />
                    </IconButton>
                  </InputAdornment>
                )
              }}
            />
            <FormControl fullWidth>
              <InputLabel id="tools-label">Tools</InputLabel>
              <Select
                labelId="tools-label"
                multiple
                value={formData.tools.map(String)}
                onChange={handleToolsChange}
                label="Tools"
                renderValue={(selected) => (
                  <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                    {(selected as string[]).map((toolId) => {
                      const tool = tools.find(t => t.id === Number(toolId));
                      return (
                        <Chip 
                          key={toolId}
                          label={tool ? tool.title : toolId}
                          size="small"
                        />
                      );
                    })}
                  </Box>
                )}
              >
                {tools
                  .filter(tool => tool.enabled !== false)
                  .map((tool) => (
                  <MenuItem key={tool.id} value={tool.id.toString()}>
                    <Box sx={{ display: 'flex', flexDirection: 'column' }}>
                      <Typography>{tool.title}</Typography>
                      <Typography variant="caption" color="text.secondary">
                        {tool.description}
                      </Typography>
                    </Box>
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
            <Accordion 
              expanded={expandedAccordion}
              onChange={handleAccordionChange}
              ref={accordionRef}
              TransitionProps={{ 
                unmountOnExit: false,
                timeout: { enter: 300, exit: 200 }
              }}
              sx={{
                '& .MuiAccordionDetails-root': {
                  padding: 2
                }
              }}
            >
              <AccordionSummary expandIcon={<ExpandMoreIcon />}>
                <Typography>Advanced Configuration</Typography>
              </AccordionSummary>
              <AccordionDetails>
                <TaskAdvancedConfig
                  advancedConfig={{
                    async_execution: formData.async_execution,
                    cache_response: formData.config?.cache_response || false,
                    cache_ttl: formData.config?.cache_ttl || 3600,
                    callback: formData.config?.callback || null,
                    context: formData.context || [],
                    dependencies: [],
                    error_handling: formData.config?.error_handling || 'default',
                    human_input: formData.config?.human_input || false,
                    max_retries: formData.config?.max_retries || 3,
                    output_file: formData.config?.output_file || null,
                    output_json: formData.config?.output_json || null,
                    output_parser: null,
                    output_pydantic: formData.config?.output_pydantic || null,
                    priority: formData.config?.priority || 1,
                    retry_on_fail: formData.config?.retry_on_fail || true,
                    timeout: formData.config?.timeout || null,
                    condition: formData.config?.condition,
                    guardrail: formData.config?.guardrail || null,
                    markdown: formData.config?.markdown || false
                  }}
                  onConfigChange={handleAdvancedConfigChange}
                  availableTasks={availableTasks}
                />
              </AccordionDetails>
            </Accordion>
          </Box>
        </Box>

        <Box 
          sx={{ 
            display: 'flex', 
            gap: 2, 
            justifyContent: 'flex-end', 
            p: 2,
            backgroundColor: 'white',
            borderTop: '1px solid rgba(0, 0, 0, 0.12)',
            position: 'static',
            width: '100%',
            zIndex: 1100
          }}
        >
          <Button onClick={onCancel}>Cancel</Button>
          <Button onClick={() => void handleSave()} variant="contained" color="primary">
            Save
          </Button>
        </Box>
      </Card>
      <Snackbar 
        open={!!error} 
        autoHideDuration={6000} 
        onClose={() => setError(null)}
        anchorOrigin={{ vertical: 'top', horizontal: 'center' }}
      >
        <Alert 
          onClose={() => setError(null)} 
          severity="error" 
          variant="filled"
          sx={{ width: '100%' }}
        >
          {error}
        </Alert>
      </Snackbar>
      <Dialog 
        open={expandedDescription} 
        onClose={handleCloseDescriptionDialog}
        fullWidth
        maxWidth="md"
      >
        <DialogTitle>
          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            Task Description
            <IconButton onClick={handleCloseDescriptionDialog}>
              <CloseIcon />
            </IconButton>
          </Box>
        </DialogTitle>
        <DialogContent>
          <TextField
            autoFocus
            fullWidth
            multiline
            rows={15}
            value={formData.description}
            onChange={(e) => handleInputChange('description', e.target.value)}
            variant="outlined"
            sx={{ mt: 2 }}
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={handleCloseDescriptionDialog} variant="contained">
            Done
          </Button>
        </DialogActions>
      </Dialog>
      <Dialog 
        open={expandedOutput} 
        onClose={handleCloseOutputDialog}
        fullWidth
        maxWidth="md"
      >
        <DialogTitle>
          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            Expected Output
            <IconButton onClick={handleCloseOutputDialog}>
              <CloseIcon />
            </IconButton>
          </Box>
        </DialogTitle>
        <DialogContent>
          <TextField
            autoFocus
            fullWidth
            multiline
            rows={15}
            value={formData.expected_output}
            onChange={(e) => handleInputChange('expected_output', e.target.value)}
            variant="outlined"
            sx={{ mt: 2 }}
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={handleCloseOutputDialog} variant="contained">
            Done
          </Button>
        </DialogActions>
      </Dialog>

    </>
  );
};

export default TaskForm;