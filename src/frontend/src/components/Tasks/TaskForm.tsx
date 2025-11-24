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
  Tooltip,
} from '@mui/material';
import { type Task } from '../../api/TaskService';
import { type Agent } from '../../types/agent';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import OpenInFullIcon from '@mui/icons-material/OpenInFull';
import CloseIcon from '@mui/icons-material/Close';
import DeleteIcon from '@mui/icons-material/Delete';
import HelpOutlineIcon from '@mui/icons-material/HelpOutline';
import AttachFileIcon from '@mui/icons-material/AttachFile';
import { TaskAdvancedConfig } from './TaskAdvancedConfig';
import { TaskService } from '../../api/TaskService';
import { DatabricksService } from '../../api/DatabricksService';
import useStableResize from '../../hooks/global/useStableResize';
import { GenieSpaceSelector } from '../Common/GenieSpaceSelector';
import { PerplexityConfigSelector } from '../Common/PerplexityConfigSelector';
import { SerperConfigSelector } from '../Common/SerperConfigSelector';
import { MCPServerSelector } from '../Common/MCPServerSelector';
import { PerplexityConfig, SerperConfig } from '../../types/config';
import TaskBestPractices from '../BestPractices/TaskBestPractices';

interface TaskFormProps {
  initialData?: Task;
  onCancel?: () => void;
  onTaskSaved?: (task: Task) => void;
  onSubmit?: (task: Task) => Promise<void>;
  isEdit?: boolean;
  tools: Tool[];
  hideTitle?: boolean;
  isCreateMode?: boolean;
  agent?: Agent;  // Agent associated with this task (for showing knowledge sources)
}

interface Tool {
  id: number;
  title: string;
  description: string;
  icon: string;
  enabled?: boolean;
}

const TaskForm: React.FC<TaskFormProps> = ({ initialData, onCancel, onTaskSaved, onSubmit, isEdit, tools, hideTitle, isCreateMode, agent }) => {
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
      callback_config: null,
      human_input: false,
      guardrail: null,
      llm_guardrail: null,
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
      callback_config: initialData.config.callback_config ?? null,
      human_input: initialData.config.human_input ?? false,
      condition: initialData.config.condition,
      guardrail: initialData.config.guardrail ?? null,
      llm_guardrail: initialData.config.llm_guardrail ?? null,
      markdown: initialData.config.markdown ?? false
    }
  });
  const [error, setError] = useState<string | null>(null);
  const [availableTasks, setAvailableTasks] = useState<Task[]>([]);
  const [selectedGenieSpace, setSelectedGenieSpace] = useState<{ id: string; name: string } | null>(null);
  const [perplexityConfig, setPerplexityConfig] = useState<PerplexityConfig>({});
  const [serperConfig, setSerperConfig] = useState<SerperConfig>({});
  const [selectedMcpServers, setSelectedMcpServers] = useState<string[]>([]);
  const [toolConfigs, setToolConfigs] = useState<Record<string, unknown>>(initialData?.tool_configs || {});
  const [showBestPractices, setShowBestPractices] = useState(false);
  const [workspaceUrlFromBackend, setWorkspaceUrlFromBackend] = useState<string>('');

  useEffect(() => {
    if (initialData?.tools) {
      setFormData(prev => ({
        ...prev,
        tools: initialData.tools
      }));
    }
    // Load tool_configs and set Genie space and Perplexity config if they exist
    if (initialData?.tool_configs) {
      setToolConfigs(initialData.tool_configs);

      // Check for GenieTool config - try both spaceId and space_id for compatibility
      const genieConfig = initialData.tool_configs.GenieTool as Record<string, unknown>;
      if (genieConfig) {
        const spaceId = genieConfig.spaceId || genieConfig.space_id;
        const spaceName = genieConfig.spaceName || genieConfig.space_name || spaceId; // Fallback to ID if name not stored
        if (spaceId && typeof spaceId === 'string') {
          setSelectedGenieSpace({
            id: spaceId as string,
            name: spaceName as string
          });
        }
      }

      if (initialData.tool_configs.PerplexityTool) {
        setPerplexityConfig(initialData.tool_configs.PerplexityTool as PerplexityConfig);
      }

      if (initialData.tool_configs.SerperDevTool) {
        setSerperConfig(initialData.tool_configs.SerperDevTool as SerperConfig);
      }

      // Check for MCP_SERVERS config
      if (initialData.tool_configs.MCP_SERVERS) {
        const mcpConfig = initialData.tool_configs.MCP_SERVERS as Record<string, unknown>;
        const mcpServers = Array.isArray(mcpConfig.servers)
          ? mcpConfig.servers
          : Array.isArray(initialData.tool_configs.MCP_SERVERS)
          ? initialData.tool_configs.MCP_SERVERS  // Fallback for old format
          : [];
        setSelectedMcpServers(mcpServers);
      }
    }
  }, [initialData?.tools, initialData?.tool_configs, tools]);

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

  useEffect(() => {
    // Fetch workspace URL from backend environment
    const fetchWorkspaceUrl = async () => {
      try {
        const databricksService = DatabricksService.getInstance();
        const envInfo = await databricksService.getDatabricksEnvironment();
        if (envInfo.databricks_host) {
          setWorkspaceUrlFromBackend(envInfo.databricks_host);
        }
      } catch (error) {
        console.error('Error fetching Databricks environment:', error);
      }
    };

    void fetchWorkspaceUrl();
  }, []);


  const handleInputChange = (field: keyof Task, value: string) => {
    setFormData((prev: Task) => ({
      ...prev,
      [field]: value
    }));
  };

  const handleAdvancedConfigChange = (field: string, value: string | number | boolean | null | Record<string, unknown>) => {

    setFormData(prev => {
      // Handle special fields that exist at the top level of formData
      if (field === 'async_execution') {
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

      // Debug logging for callback_config updates
      if (field === 'callback_config') {
        console.log('TaskForm - Updating callback_config:', value);
      }

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

        // If GenieTool is selected, ensure a Genie Space is specified
        const isGenieSelected = formData.tools.some(toolId => {
          const tool = tools.find(t =>
            String(t.id) === String(toolId) ||
            t.id === Number(toolId) ||
            t.title === toolId
          );
          return tool?.title === 'GenieTool';
        });
        if (isGenieSelected && !selectedGenieSpace) {
          setError('Please select a Genie Space when GenieTool is selected');
          return;
        }

        // Build tool_configs for tools that need configuration
        let updatedToolConfigs = { ...toolConfigs };

        // Handle GenieTool config
        if (selectedGenieSpace && formData.tools.some(toolId => {
          const tool = tools.find(t =>
            String(t.id) === String(toolId) ||
            t.id === Number(toolId) ||
            t.title === toolId
          );
          return tool?.title === 'GenieTool';
        })) {
          updatedToolConfigs = {
            ...updatedToolConfigs,
            GenieTool: {
              spaceId: selectedGenieSpace.id,
              spaceName: selectedGenieSpace.name
            }
          };
        } else if (!selectedGenieSpace) {
          // Remove GenieTool config if no space selected
          delete updatedToolConfigs.GenieTool;
        }

        // Handle PerplexityTool config
        if (perplexityConfig && Object.keys(perplexityConfig).length > 0 && formData.tools.some(toolId => {
          const tool = tools.find(t =>
            String(t.id) === String(toolId) ||
            t.id === Number(toolId) ||
            t.title === toolId
          );
          return tool?.title === 'PerplexityTool';
        })) {
          updatedToolConfigs = {
            ...updatedToolConfigs,
            PerplexityTool: perplexityConfig
          };
        } else if (!formData.tools.some(toolId => {
          const tool = tools.find(t =>
            String(t.id) === String(toolId) ||
            t.id === Number(toolId) ||
            t.title === toolId
          );
          return tool?.title === 'PerplexityTool';
        })) {
          // Remove PerplexityTool config if tool not selected
          delete updatedToolConfigs.PerplexityTool;
        }

        // Handle SerperDevTool config
        if (serperConfig && Object.keys(serperConfig).length > 0 && formData.tools.some(toolId => {
          const tool = tools.find(t =>
            String(t.id) === String(toolId) ||
            t.id === Number(toolId) ||
            t.title === toolId
          );
          return tool?.title === 'SerperDevTool';
        })) {
          updatedToolConfigs = {
            ...updatedToolConfigs,
            SerperDevTool: serperConfig
          };
        } else if (!formData.tools.some(toolId => {
          const tool = tools.find(t =>
            String(t.id) === String(toolId) ||
            t.id === Number(toolId) ||
            t.title === toolId
          );
          return tool?.title === 'SerperDevTool';
        })) {
          // Remove SerperDevTool config if tool not selected
          delete updatedToolConfigs.SerperDevTool;
        }

        // Handle MCP_SERVERS config - use dict format to match schema
        if (selectedMcpServers && selectedMcpServers.length > 0) {
          updatedToolConfigs = {
            ...updatedToolConfigs,
            MCP_SERVERS: {
              servers: selectedMcpServers
            }
          };
        } else {
          // Remove MCP_SERVERS config if none selected
          delete updatedToolConfigs.MCP_SERVERS;
        }

        // Create a cleaned version of the form data
        const cleanedFormData: Task = {
          ...formData,
          context: Array.from(formData.context),
          tool_configs: Object.keys(updatedToolConfigs).length > 0 ? updatedToolConfigs : undefined,
          // Ensure top-level markdown is synchronized with config.markdown
          markdown: formData.config.markdown ?? formData.markdown,
          // Sync llm_guardrail to top-level for database persistence
          llm_guardrail: formData.config.llm_guardrail ?? null,
          config: {
            ...formData.config,
            condition: formData.config.condition === 'is_data_missing' ? 'is_data_missing' : undefined,
            callback: formData.config.callback,
            callback_config: formData.config.callback_config,
            // Ensure output_pydantic is properly set in config
            output_pydantic: formData.config.output_pydantic,
            // Ensure config.markdown is synchronized with top-level markdown
            markdown: formData.config.markdown ?? formData.markdown,
            // Ensure llm_guardrail is properly set in config
            llm_guardrail: formData.config.llm_guardrail ?? null
          }
        };



        try {
          // Create or update the task in the database
          let savedTask;
          if (formData.id) {
            savedTask = await TaskService.updateTask(formData.id, cleanedFormData);
          } else {
            savedTask = await TaskService.createTask(cleanedFormData);
          }


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

  const handleGenieSpaceClick = async (event: React.MouseEvent) => {
    // Prevent any bubbling that might interfere
    event.stopPropagation();

    if (!selectedGenieSpace) {
      console.warn('No Genie space selected');
      return;
    }

    try {
      console.log('Fetching Databricks configuration...');
      const databricksService = DatabricksService.getInstance();
      const config = await databricksService.getDatabricksConfig();
      console.log('Databricks config:', config);

      // Use workspaceUrlFromBackend if available, otherwise fall back to config.workspace_url
      const workspaceUrlSource = workspaceUrlFromBackend || config?.workspace_url;

      if (workspaceUrlSource) {
        // Ensure the URL has https:// and remove trailing slash if present
        let workspaceUrl = workspaceUrlSource.startsWith('https://')
          ? workspaceUrlSource
          : `https://${workspaceUrlSource}`;

        // Remove trailing slash to avoid double slashes
        workspaceUrl = workspaceUrl.replace(/\/$/, '');

        // Construct the Genie room URL
        // Format: https://{workspace}/genie/rooms/{space_id}/monitoring
        const genieUrl = `${workspaceUrl}/genie/rooms/${selectedGenieSpace.id}/monitoring`;


        console.log('Opening Genie URL:', genieUrl);
        // Open in new tab
        window.open(genieUrl, '_blank', 'noopener,noreferrer');
      } else {
        console.warn('Databricks workspace URL not configured');
        alert('Databricks workspace URL is not configured. Please configure it in Settings > Configuration > Databricks.');
      }
    } catch (error) {
      console.error('Error opening Genie space:', error);
      alert('Error opening Genie space. Please check the console for details.');
    }
  };

  // Derived validation flags
  const isGenieToolSelected = formData.tools.some(toolId => {
    const tool = tools.find(t =>
      String(t.id) === String(toolId) ||
      t.id === Number(toolId) ||
      t.title === toolId
    );
    return tool?.title === 'GenieTool';
  });
  const isGenieSpaceMissing = isGenieToolSelected && !selectedGenieSpace;

  return (
    <>
      <Card sx={{
        display: 'flex',
        flexDirection: 'column',
        height: '70vh',
        position: 'relative',
        overflow: 'hidden'
      }}>

        {!isCreateMode && (
          <Box sx={{ p: 3, pb: 2 }}>
            <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
              {!hideTitle && (
                <Typography variant="h6">
                  {initialData?.id ? 'Edit Task' : 'Create New Task'}
                </Typography>
              )}
              <Button
                startIcon={<HelpOutlineIcon />}
                onClick={() => setShowBestPractices(true)}
                variant="outlined"
                size="small"
                sx={{ ml: 2 }}
              >
                Best Practices
              </Button>
            </Box>
            <Divider />
          </Box>
        )}

        <Box sx={{
          flex: '1 1 auto',
          overflow: 'auto',
          px: 3,
          pb: 2,
          pt: isCreateMode ? 3 : 0,
          height: isCreateMode ? 'calc(90vh - 120px)' : 'calc(90vh - 170px)',
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
                      // Try to find tool by comparing both string and number forms
                      const tool = tools.find(t =>
                        String(t.id) === String(toolId) ||
                        t.id === Number(toolId) ||
                        t.title === toolId  // Also check by title for backward compatibility
                      );
                      return (
                        <Chip
                          key={toolId}
                          label={tool ? tool.title : `Tool ${toolId}`}
                          size="small"
                          onDelete={() => {
                            const newTools = formData.tools.filter(id => String(id) !== String(toolId));
                            handleToolsChange({ target: { value: newTools } } as SelectChangeEvent<string[]>);
                          }}
                          onMouseDown={(event: React.MouseEvent) => {
                            event.stopPropagation(); // Prevent dropdown from opening when clicking delete icon
                          }}
                          deleteIcon={
                            <DeleteIcon
                              fontSize="small"
                              sx={{ fontSize: '16px !important' }}
                            />
                          }
                        />
                      );
                    })}
                  </Box>
                )}
              >
                {tools && tools.length > 0 ? (
                  tools
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
                  ))
                ) : (
                  <MenuItem disabled>
                    <Typography variant="body2" color="text.secondary">
                      No tools available
                    </Typography>
                  </MenuItem>
                )}
              </Select>
            </FormControl>

            {/* Knowledge Sources / File Attachments Display - Show when agent has uploaded files */}
            {agent && agent.knowledge_sources && agent.knowledge_sources.length > 0 && (
              <Box sx={{
                mt: 2,
                p: 2,
                backgroundColor: 'rgba(76, 175, 80, 0.04)',
                borderRadius: 1,
                border: '1px solid rgba(76, 175, 80, 0.12)'
              }}>
                <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
                  <AttachFileIcon sx={{ mr: 1, color: 'success.main', fontSize: '1.2rem' }} />
                  <Typography
                    variant="subtitle2"
                    color="success.main"
                    sx={{ fontWeight: 600, fontSize: '0.875rem' }}
                  >
                    Knowledge Files Attached ({agent.knowledge_sources.length})
                  </Typography>
                </Box>
                <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 1 }}>
                  The DatabricksKnowledgeSearchTool will automatically search these files during task execution.
                </Typography>
                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1 }}>
                  {agent.knowledge_sources.map((source, index) => (
                    <Chip
                      key={index}
                      icon={<AttachFileIcon />}
                      label={source.fileInfo?.filename || source.source}
                      size="small"
                      color="success"
                      variant="outlined"
                      sx={{ fontWeight: 500 }}
                    />
                  ))}
                </Box>
              </Box>
            )}

            {/* Genie Space Display - Show only when GenieTool is selected */}
            {formData.tools.some(toolId => {
              const tool = tools.find(t =>
                String(t.id) === String(toolId) ||
                t.id === Number(toolId) ||
                t.title === toolId
              );
              return tool?.title === 'GenieTool';
            }) && (
              <Box sx={{ mt: 2 }}>
                <Typography variant="subtitle2" sx={{ mb: 1 }}>Genie Space</Typography>
                {selectedGenieSpace ? (
                  <Tooltip title={`Space ID: ${selectedGenieSpace.id} - Click to open in Databricks`} arrow>
                    <Chip
                      label={selectedGenieSpace.name}
                      size="medium"
                      color="primary"
                      variant="outlined"
                      onClick={handleGenieSpaceClick}
                      onDelete={() => {
                        setSelectedGenieSpace(null);
                        // Remove GenieTool config when space is removed
                        setToolConfigs(prev => {
                          const newConfigs = { ...prev };
                          delete newConfigs.GenieTool;
                          return newConfigs;
                        });
                      }}
                      deleteIcon={<DeleteIcon fontSize="small" />}
                      sx={{
                        cursor: 'pointer',
                        '&:hover': {
                          backgroundColor: 'rgba(25, 118, 210, 0.08)',
                        }
                      }}
                    />
                  </Tooltip>
                ) : (
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                    <GenieSpaceSelector
                      value=""
                      onChange={(value, spaceName) => {
                        if (value) {
                          setSelectedGenieSpace({
                            id: value as string,
                            name: spaceName || (value as string)  // Use the name if provided, otherwise fallback to ID
                          });
                          // Update tool configs when space is selected
                          setToolConfigs(prev => ({
                            ...prev,
                            GenieTool: {
                              spaceId: value as string,
                              spaceName: spaceName || (value as string)
                            }
                          }));
                        }
                      }}
                      label=""
                      placeholder="Select a Genie space..."
                      required
                      error={isGenieSpaceMissing}
                      helperText={isGenieSpaceMissing ? 'Genie Space is required when GenieTool is selected' : ''}
                      fullWidth
                    />
                  </Box>
                )}
              </Box>
            )}

            {/* Perplexity Configuration - Show only when PerplexityTool is selected */}
            {formData.tools.some(toolId => {
              const tool = tools.find(t =>
                String(t.id) === String(toolId) ||
                t.id === Number(toolId) ||
                t.title === toolId
              );
              return tool?.title === 'PerplexityTool';
            }) && (
              <Box sx={{ mt: 2 }}>
                <PerplexityConfigSelector
                  value={perplexityConfig}
                  onChange={(config) => {
                    setPerplexityConfig(config);
                    // Update tool configs when configuration changes
                    setToolConfigs(prev => ({
                      ...prev,
                      PerplexityTool: config
                    }));
                  }}
                  label="Perplexity Configuration"
                  helperText="Configure Perplexity AI search parameters for this task"
                  fullWidth
                />
              </Box>
            )}

            {/* Serper Configuration - Show only when SerperDevTool is selected */}
            {formData.tools.some(toolId => {
              const tool = tools.find(t =>
                String(t.id) === String(toolId) ||
                t.id === Number(toolId) ||
                t.title === toolId
              );
              return tool?.title === 'SerperDevTool';
            }) && (
              <Box sx={{ mt: 2 }}>
                <SerperConfigSelector
                  value={serperConfig}
                  onChange={(config) => {
                    setSerperConfig(config);
                    // Update tool configs when configuration changes
                    setToolConfigs(prev => ({
                      ...prev,
                      SerperDevTool: config
                    }));
                  }}
                  label="Serper Configuration"
                  helperText="Configure Serper.dev search parameters for this task"
                  fullWidth
                />
              </Box>
            )}

            {/* MCP Server Configuration - Always show as it's independent of regular tools */}
            <Box sx={{ mt: 2 }}>
              {/* Show selected MCP servers visually */}
              {selectedMcpServers.length > 0 && (
                <Box sx={{
                  mb: 2,
                  p: 2,
                  backgroundColor: 'rgba(25, 118, 210, 0.04)',
                  borderRadius: 1,
                  border: '1px solid rgba(25, 118, 210, 0.12)'
                }}>
                  <Typography
                    variant="subtitle2"
                    color="primary"
                    sx={{
                      mb: 1,
                      fontWeight: 600,
                      fontSize: '0.875rem'
                    }}
                  >
                    Selected MCP Servers ({selectedMcpServers.length})
                  </Typography>
                  <Box sx={{
                    display: 'flex',
                    flexWrap: 'wrap',
                    gap: 1
                  }}>
                    {selectedMcpServers.map((server) => (
                      <Chip
                        key={server}
                        label={server}
                        size="medium"
                        color="primary"
                        variant="filled"
                        onDelete={() => {
                          const newServers = selectedMcpServers.filter(s => s !== server);
                          setSelectedMcpServers(newServers);
                          setToolConfigs(prev => ({
                            ...prev,
                            MCP_SERVERS: {
                              servers: newServers
                            }
                          }));
                        }}
                        sx={{
                          '& .MuiChip-deleteIcon': {
                            fontSize: '18px',
                            '&:hover': {
                              color: 'error.main'
                            }
                          },
                          fontWeight: 500
                        }}
                      />
                    ))}
                  </Box>
                </Box>
              )}

              <MCPServerSelector
                value={selectedMcpServers}
                onChange={(servers) => {
                  const serverArray = Array.isArray(servers) ? servers : (servers ? [servers] : []);
                  setSelectedMcpServers(serverArray);
                  // Update tool configs when MCP servers change - use consistent format
                  setToolConfigs(prev => ({
                    ...prev,
                    MCP_SERVERS: {
                      servers: serverArray
                    }
                  }));
                }}
                label="MCP Servers"
                placeholder="Select MCP servers for this task..."
                helperText="Choose which MCP (Model Context Protocol) servers this task should have access to"
                multiple={true}
                fullWidth
              />
            </Box>

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
                    callback_config: formData.config?.callback_config || null,
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
                    llm_guardrail: formData.config?.llm_guardrail || null,
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
          <Button onClick={() => void handleSave()} variant="contained" color="primary" disabled={isGenieSpaceMissing}>
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

      {/* Best Practices Dialog */}
      <TaskBestPractices
        open={showBestPractices}
        onClose={() => setShowBestPractices(false)}
      />

    </>
  );
};

export default TaskForm;