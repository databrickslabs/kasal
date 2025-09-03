import React, { useState, useEffect, useRef } from 'react';
import { 
  Dialog as MuiDialog, 
  DialogTitle, 
  DialogContent, 
  DialogActions, 
  Button, 
  Box, 
  Grid,
  Card,
  CardContent,
  Typography,
  IconButton,
  Tooltip,
  CircularProgress,
  Alert,
  useTheme,
  alpha,
  Menu,
  MenuItem
} from '@mui/material';
import { CrewService } from '../../api/CrewService';
import { CrewResponse, CrewSelectionDialogProps } from '../../types/crews';
import PersonIcon from '@mui/icons-material/Person';
import TaskIcon from '@mui/icons-material/Task';
import DownloadIcon from '@mui/icons-material/Download';
import UploadIcon from '@mui/icons-material/Upload';
import CalendarTodayIcon from '@mui/icons-material/CalendarToday';
import DeleteIcon from '@mui/icons-material/Delete';
import { AgentService } from '../../api/AgentService';
import { TaskService, Task } from '../../api/TaskService';
import SearchIcon from '@mui/icons-material/Search';
import InputAdornment from '@mui/material/InputAdornment';
import TextField from '@mui/material/TextField';
import { Node, Edge } from 'reactflow';
import { useTabManagerStore } from '../../store/tabManager';
import { useFlowConfigStore } from '../../store/flowConfig';

// Update type definitions for crew ID
interface CrewDialogProps {
  open: boolean;
  onClose: () => void;
  crewId: string; // Change from number to string
  onCrewSelect: (nodes: Node[], edges: Edge[]) => void;
}

const CrewDialog: React.FC<CrewDialogProps> = ({ open, onClose, onCrewSelect }): JSX.Element => {
  const [crews, setCrews] = useState<CrewResponse[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [importAnchorEl, setImportAnchorEl] = useState<null | HTMLElement>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const multipleFileInputRef = useRef<HTMLInputElement>(null);
  const searchInputRef = useRef<HTMLInputElement>(null);
  const theme = useTheme();
  const [searchQuery, setSearchQuery] = useState('');
  const { crewAIFlowEnabled } = useFlowConfigStore();

  useEffect(() => {
    if (open) {
      loadCrews();
    }
  }, [open]);

  // Focus management when dialog opens
  const handleDialogEntered = () => {
    setTimeout(() => {
      if (searchInputRef.current) {
        searchInputRef.current.focus();
      }
    }, 150); // Increased delay for reliable focus
  };

  const loadCrews = async () => {
    setLoading(true);
    try {
      const fetchedCrews = await CrewService.getCrews();
      setCrews(fetchedCrews);
      setError(null);
    } catch (error) {
      console.error('Error loading crews:', error);
      setError('Failed to load crews');
    } finally {
      setLoading(false);
    }
  };

  const handleCrewSelect = async (crewId: string) => {
    try {
      setLoading(true);
      
      // Dispatch event to signal crew loading is starting
      window.dispatchEvent(new CustomEvent('crewLoadStarted'));
      
      const selectedCrew = await CrewService.getCrew(crewId);
      
      if (!selectedCrew) {
        throw new Error('Crew not found');
      }

      console.log('Selected crew:', selectedCrew);
      console.log('Selected crew nodes:', selectedCrew.nodes);
      
      // Debug: Check if any nodes have tool_configs
      if (selectedCrew.nodes) {
        selectedCrew.nodes.forEach((node: any) => {
          if (node.data?.tool_configs && Object.keys(node.data.tool_configs).length > 0) {
            console.log(`Crew node ${node.id} (${node.type}) has tool_configs:`, node.data.tool_configs);
          }
        });
      }

      // Create a mapping of old IDs to new IDs and store created entities
      const idMapping: { [key: string]: string } = {};
      const createdAgents: { [nodeId: string]: any } = {};
      const createdTasks: { [nodeId: string]: any } = {};

      // Create agents first
      for (const node of selectedCrew.nodes || []) {
        if (node.type === 'agentNode') {
          try {
            const agentData = node.data;
            console.log('CrewDialog: Loading agent node with data:', agentData);
            if (agentData.tool_configs) {
              console.log('CrewDialog: Agent has tool_configs:', agentData.tool_configs);
            }
            // Format agent data following AgentForm pattern
            const formattedAgentData = {
              name: agentData.name || agentData.label || '',
              role: agentData.role || '',
              goal: agentData.goal || '',
              backstory: agentData.backstory || '',
              llm: agentData.llm || 'databricks-llama-4-maverick',
              tools: agentData.tools || [],
              tool_configs: agentData.tool_configs || {},  // Include tool_configs
              function_calling_llm: agentData.function_calling_llm,
              max_iter: agentData.max_iter || 25,
              max_rpm: agentData.max_rpm,
              max_execution_time: agentData.max_execution_time || 300,
              memory: agentData.memory ?? true,
              verbose: agentData.verbose || false,
              allow_delegation: agentData.allow_delegation || false,
              cache: agentData.cache ?? true,
              system_template: agentData.system_template,
              prompt_template: agentData.prompt_template,
              response_template: agentData.response_template,
              allow_code_execution: agentData.allow_code_execution || false,
              code_execution_mode: agentData.code_execution_mode || 'safe',
              max_retry_limit: agentData.max_retry_limit || 3,
              use_system_prompt: agentData.use_system_prompt ?? true,
              respect_context_window: agentData.respect_context_window ?? true,
              embedder_config: agentData.embedder_config,
              knowledge_sources: agentData.knowledge_sources || []
            };

            const newAgent = await AgentService.createAgent(formattedAgentData);
            console.log('CrewDialog: Created agent:', newAgent);
            if (newAgent?.tool_configs) {
              console.log('CrewDialog: Created agent has tool_configs:', newAgent.tool_configs);
            }
            if (newAgent && newAgent.id) {
              idMapping[node.id] = newAgent.id.toString();
              // Fetch the created agent to get full data including tool_configs
              const fetchedAgent = await AgentService.getAgent(newAgent.id);
              if (fetchedAgent) {
                console.log('CrewDialog: Fetched created agent with full data:', fetchedAgent);
                createdAgents[node.id] = fetchedAgent;  // Store the fetched agent with full data
              } else {
                createdAgents[node.id] = newAgent;  // Fallback to created agent
              }
            } else {
              throw new Error(`Failed to create agent: ${formattedAgentData.name}`);
            }
          } catch (err) {
            const error = err as Error;
            console.error('Error creating agent:', error);
            throw new Error(`Failed to create agent: ${error.message}`);
          }
        }
      }

      // Create tasks
      for (const node of selectedCrew.nodes || []) {
        if (node.type === 'taskNode') {
          try {
            const taskData = node.data;
            console.log('CrewDialog: Loading task node with data:', taskData);
            if (taskData.tool_configs) {
              console.log('CrewDialog: Task has tool_configs:', taskData.tool_configs);
            }
            // Update agent_id in task data if it exists in our mapping
            const updatedAgentId = taskData.agent_id && idMapping[taskData.agent_id] 
              ? parseInt(idMapping[taskData.agent_id]) 
              : 0;

            // Format task data following TaskForm pattern
            const formattedTaskData: Partial<Task> = {
              name: String(taskData.label || ''),
              description: String(taskData.description || ''),
              expected_output: String(taskData.expected_output || ''),
              tools: (taskData.tools || []).map((tool: unknown) => String(tool)),
              tool_configs: taskData.tool_configs || {},  // Include tool_configs
              agent_id: updatedAgentId ? String(updatedAgentId) : null,
              async_execution: Boolean(taskData.async_execution),
              context: (taskData.context || []).map((item: unknown) => String(item)),
              config: {
                cache_response: Boolean(taskData.config?.cache_response),
                cache_ttl: Number(taskData.config?.cache_ttl || 3600),
                retry_on_fail: Boolean(taskData.config?.retry_on_fail),
                max_retries: Number(taskData.config?.max_retries || 3),
                timeout: taskData.config?.timeout ? Number(taskData.config.timeout) : null,
                priority: Number(taskData.config?.priority || 1),
                error_handling: (taskData.config?.error_handling || 'default') as 'default' | 'retry' | 'ignore' | 'fail',
                output_file: taskData.config?.output_file || null,
                output_json: taskData.config?.output_json || null,
                output_pydantic: taskData.config?.output_pydantic || null,
                callback: taskData.config?.callback || null,
                human_input: Boolean(taskData.config?.human_input),
                condition: taskData.config?.condition === 'is_data_missing' ? 'is_data_missing' : undefined,
                guardrail: taskData.config?.guardrail || null,
                markdown: taskData.config?.markdown === true || taskData.config?.markdown === 'true' || taskData.markdown === true || taskData.markdown === 'true'
              }
            };

            const newTask = await TaskService.createTask(formattedTaskData);
            console.log('CrewDialog: Created task:', newTask);
            if (newTask?.tool_configs) {
              console.log('CrewDialog: Created task has tool_configs:', newTask.tool_configs);
            }
            if (newTask && newTask.id) {
              idMapping[node.id] = newTask.id.toString();
              // Fetch the created task to get full data including tool_configs
              const fetchedTask = await TaskService.getTask(newTask.id);
              if (fetchedTask) {
                console.log('CrewDialog: Fetched created task with full data:', fetchedTask);
                createdTasks[node.id] = fetchedTask;  // Store the fetched task with full data
              } else {
                createdTasks[node.id] = newTask;  // Fallback to created task
              }
            } else {
              throw new Error(`Failed to create task: ${formattedTaskData.name}`);
            }
          } catch (err) {
            const error = err as Error;
            console.error('Error creating task:', error);
            throw new Error(`Failed to create task: ${error.message}`);
          }
        }
      }

      // Update node IDs and references with position adjustments
      const updatedNodes = (selectedCrew.nodes || []).map(node => {
        const newId = node.type === 'agentNode' 
          ? `agent-${idMapping[node.id] || node.id}`
          : `task-${idMapping[node.id] || node.id}`;
        
        const updatedNode = {
          ...node,
          id: newId,
          type: node.type, // Ensure type is preserved
          // Adjust position: shift left and make smaller
          position: {
            x: (node.position?.x || 0) - 150, // Shift 150px to the left
            y: node.position?.y || 0
          },
          // Make nodes smaller
          style: {
            ...node.style,
            width: node.style?.width ? Math.max(180, (Number(node.style.width) * 0.8)) : 180, // 20% smaller, minimum 180px
            height: node.style?.height ? Math.max(120, (Number(node.style.height) * 0.8)) : 120 // 20% smaller, minimum 120px
          },
          data: {
            ...node.data,
            // Merge data from the fetched entity if available
            ...(node.type === 'agentNode' && createdAgents[node.id] ? {
              ...createdAgents[node.id],
              label: createdAgents[node.id].name || node.data.label,
              agentId: idMapping[node.id] || node.data.agentId,
              tool_configs: createdAgents[node.id].tool_configs || node.data.tool_configs || {}
            } : {}),
            ...(node.type === 'taskNode' && createdTasks[node.id] ? {
              ...createdTasks[node.id],
              label: createdTasks[node.id].name || node.data.label,
              taskId: idMapping[node.id] || node.data.taskId,
              tool_configs: createdTasks[node.id].tool_configs || node.data.tool_configs || {}
            } : {}),
            id: idMapping[node.id] || node.data.id,
            agent_id: node.data.agent_id ? idMapping[node.data.agent_id] || node.data.agent_id : node.data.agent_id,
            type: node.type === 'agentNode' ? 'agent' : 'task' // Set the internal type field
          }
        };
        console.log('Updated node:', updatedNode);
        if (updatedNode.data.tool_configs && Object.keys(updatedNode.data.tool_configs).length > 0) {
          console.log(`CrewDialog: Node ${updatedNode.id} has tool_configs:`, updatedNode.data.tool_configs);
        }
        return updatedNode;
      });

      // Update edge source and target IDs to match the new node IDs
      const updatedEdges = (selectedCrew.edges || []).map(edge => {
        const sourceNode = selectedCrew?.nodes?.find(n => n.id === edge.source);
        const targetNode = selectedCrew?.nodes?.find(n => n.id === edge.target);
        
        return {
          ...edge,
          source: sourceNode?.type === 'agentNode' 
            ? `agent-${idMapping[edge.source] || edge.source}`
            : `task-${idMapping[edge.source] || edge.source}`,
          target: targetNode?.type === 'agentNode'
            ? `agent-${idMapping[edge.target] || edge.target}`
            : `task-${idMapping[edge.target] || edge.target}`
        };
      });

      // Update the active tab with crew info
      const { activeTabId, updateTabCrewInfo } = useTabManagerStore.getState();
      if (activeTabId) {
        updateTabCrewInfo(activeTabId, selectedCrew.id, selectedCrew.name);
      }

      onCrewSelect(updatedNodes, updatedEdges);
      onClose();
    } catch (err) {
      const error = err as Error;
      console.error('Error loading crew:', error);
      setError(error.message || 'Failed to load crew');
    } finally {
      setLoading(false);
    }
  };

  const handleExportAllCrews = async (event: React.MouseEvent) => {
    event.stopPropagation();
    try {
      const allCrews = await CrewService.getCrews();
      const exportData = JSON.stringify(allCrews, null, 2);
      const blob = new Blob([exportData], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = 'all_crews.json';
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      URL.revokeObjectURL(url);
    } catch (error) {
      console.error('Error exporting crews:', error);
    }
  };

  const handleImportMenuClick = (event: React.MouseEvent<HTMLButtonElement>) => {
    event.stopPropagation();
    setImportAnchorEl(event.currentTarget);
  };

  const handleImportMenuClose = () => {
    setImportAnchorEl(null);
  };

  const handleSingleCrewImport = () => {
    fileInputRef.current?.click();
    handleImportMenuClose();
  };

  const handleMultipleCrewImport = () => {
    multipleFileInputRef.current?.click();
    handleImportMenuClose();
  };

  const handleImportCrew = async (event: React.ChangeEvent<HTMLInputElement>) => {
    try {
      const files = event.target.files;
      if (!files || files.length === 0) return;
      
      setLoading(true);
      
      // Determine if we're importing multiple files or a single file
      const _isMultipleImport = event.target.id === 'multiple-file-input';
      const crewsToImport = [];
      
      for (let i = 0; i < files.length; i++) {
        const file = files[i];
        const fileContent = await file.text();
        let crewData;
        
        try {
          crewData = JSON.parse(fileContent);
        } catch (err) {
          console.error('Failed to parse JSON file:', file.name, err);
          continue;
        }
        
        // If it's a direct array, assume it's multiple crews
        if (Array.isArray(crewData)) {
          crewsToImport.push(...crewData);
        } else {
          // Otherwise assume it's a single crew
          crewsToImport.push(crewData);
        }
      }
      
      for (const crew of crewsToImport) {
        await CrewService.saveCrew({
          name: crew.name,
          nodes: crew.nodes || [],
          edges: crew.edges || [],
          agent_ids: crew.agent_ids || [],
          task_ids: crew.task_ids || []
        });
      }
      
      loadCrews(); // Refresh the crews list
      setError(null);
    } catch (error) {
      console.error('Error importing crews:', error);
      setError('Failed to import one or more crews');
    } finally {
      setLoading(false);
      // Reset the file input
      if (fileInputRef.current) fileInputRef.current.value = '';
      if (multipleFileInputRef.current) multipleFileInputRef.current.value = '';
    }
  };

  const handleExportCrew = async (event: React.MouseEvent, crew: CrewResponse) => {
    event.stopPropagation(); // Prevent crew selection when clicking export
    try {
      const exportData = JSON.stringify(crew, null, 2);
      const blob = new Blob([exportData], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = `crew_${crew.name || crew.id}.json`;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      URL.revokeObjectURL(url);
    } catch (error) {
      console.error('Error exporting crew:', error);
      setError('Failed to export crew');
    }
  };

  const handleDeleteCrew = async (event: React.MouseEvent, crewId: string) => {
    event.stopPropagation(); // Prevent crew selection when clicking delete
    try {
      await CrewService.deleteCrew(crewId);
      loadCrews(); // Refresh the list after deletion
      setError(null);
    } catch (error) {
      console.error('Error deleting crew:', error);
      setError('Failed to delete crew');
    }
  };

  const handleClose = () => {
    onClose();
  };

  const handleExited = () => {
    setSearchQuery(''); // Clear search query after dialog is fully closed
  };

  // Helper function to detect if a crew contains flow nodes
  const isFlow = (crew: CrewResponse): boolean => {
    return crew.nodes?.some(node => node.type === 'flowNode') || false;
  };

  const filteredCrews = crews
    .filter(crew => {
      // Filter out flows if CrewAI flow is disabled
      if (!crewAIFlowEnabled && isFlow(crew)) {
        return false;
      }
      
      // Apply search filter
      if (searchQuery) {
        return crew.name.toLowerCase().includes(searchQuery.toLowerCase());
      }
      
      return true;
    });

  return (
    <>
      <MuiDialog 
        open={open} 
        onClose={handleClose}
        TransitionProps={{
          onExited: handleExited,
          onEntered: handleDialogEntered
        }}
        maxWidth="lg" 
        fullWidth
        PaperProps={{
          sx: {
            borderRadius: 2,
            maxHeight: '90vh'
          }
        }}
      >
        <DialogTitle>
          <Box display="flex" justifyContent="space-between" alignItems="center" sx={{ mb: 1 }}>
            <Typography variant="h5" sx={{ fontWeight: 600, color: theme.palette.text.primary }}>
              Select Crew
            </Typography>
            <Box display="flex" alignItems="center" gap={2}>
              <TextField
                inputRef={searchInputRef}
                size="small"
                placeholder="Search crews..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                InputProps={{
                  startAdornment: (
                    <InputAdornment position="start">
                      <SearchIcon sx={{ color: theme.palette.text.secondary }} />
                    </InputAdornment>
                  ),
                  sx: {
                    borderRadius: 1.5,
                    bgcolor: theme.palette.background.paper,
                    '& .MuiOutlinedInput-notchedOutline': {
                      borderColor: theme.palette.divider,
                    },
                    '&:hover .MuiOutlinedInput-notchedOutline': {
                      borderColor: theme.palette.primary.main,
                    },
                    '&.Mui-focused .MuiOutlinedInput-notchedOutline': {
                      borderColor: theme.palette.primary.main,
                    }
                  }
                }}
                sx={{ 
                  width: 250,
                  mr: 1
                }}
              />
              <Box display="flex" gap={1}>
                <Tooltip title="Export All Crews">
                  <IconButton 
                    onClick={handleExportAllCrews}
                    sx={{
                      '&:hover': {
                        bgcolor: alpha(theme.palette.primary.main, 0.1)
                      }
                    }}
                  >
                    <DownloadIcon />
                  </IconButton>
                </Tooltip>
                <Tooltip title="Import Plan">
                  <IconButton 
                    onClick={handleImportMenuClick}
                    sx={{
                      '&:hover': {
                        bgcolor: alpha(theme.palette.primary.main, 0.1)
                      }
                    }}
                  >
                    <UploadIcon />
                  </IconButton>
                </Tooltip>
                <Menu
                  id="import-menu"
                  anchorEl={importAnchorEl}
                  open={Boolean(importAnchorEl)}
                  onClose={handleImportMenuClose}
                >
                  <MenuItem onClick={handleSingleCrewImport}>Import Single Crew</MenuItem>
                  <MenuItem onClick={handleMultipleCrewImport}>Import Multiple Crews</MenuItem>
                </Menu>

                <input
                  type="file"
                  id="single-file-input"
                  ref={fileInputRef}
                  style={{ display: 'none' }}
                  onChange={handleImportCrew}
                  accept=".json"
                />
                <input
                  type="file"
                  id="multiple-file-input"
                  multiple
                  ref={multipleFileInputRef}
                  style={{ display: 'none' }}
                  onChange={handleImportCrew}
                  accept=".json"
                />
              </Box>
            </Box>
          </Box>
        </DialogTitle>
        <DialogContent sx={{ pb: 3 }}>
          {loading && (
            <Box display="flex" justifyContent="center" alignItems="center" sx={{ p: 5 }}>
              <CircularProgress />
            </Box>
          )}
          {error && (
            <Alert severity="error" sx={{ mb: 3 }}>
              {error}
            </Alert>
          )}
          {!loading && filteredCrews.length === 0 && (
            <Box textAlign="center" p={3}>
              <Typography variant="body1">
                {searchQuery ? 'No crews match your search' : 'No crews available'}
              </Typography>
            </Box>
          )}
          <Grid container spacing={3}>
            {filteredCrews.map((crew) => (
              <Grid item xs={12} md={4} key={crew.id}>
                <Card 
                  onClick={() => handleCrewSelect(crew.id)}
                  sx={{ 
                    cursor: 'pointer',
                    height: '100%',
                    display: 'flex',
                    flexDirection: 'column',
                    transition: 'transform 0.2s ease-in-out, box-shadow 0.2s ease-in-out',
                    '&:hover': {
                      transform: 'translateY(-4px)',
                      boxShadow: 6
                    }
                  }}
                >
                  <CardContent sx={{ flexGrow: 1, p: 2.5 }}>
                    <Typography 
                      variant="h6" 
                      sx={{ 
                        fontWeight: 600, 
                        mb: 2,
                        overflow: 'hidden',
                        textOverflow: 'ellipsis',
                        whiteSpace: 'nowrap'
                      }}
                    >
                      {crew.name}
                    </Typography>
                    
                    <Box display="flex" flexDirection="column" gap={1.5} mb={2}>
                      <Box display="flex" alignItems="center" gap={1}>
                        <PersonIcon sx={{ color: theme.palette.primary.main, fontSize: '1.2rem' }} />
                        <Typography variant="body2" sx={{ fontWeight: 500 }}>
                          {crew.agent_ids.length} Agents
                        </Typography>
                      </Box>
                      
                      <Box display="flex" alignItems="center" gap={1}>
                        <TaskIcon sx={{ color: theme.palette.secondary.main, fontSize: '1.2rem' }} />
                        <Typography variant="body2" sx={{ fontWeight: 500 }}>
                          {crew.task_ids.length} Tasks
                        </Typography>
                      </Box>
                      
                      <Box display="flex" alignItems="center" gap={1}>
                        <CalendarTodayIcon sx={{ color: theme.palette.info.main, fontSize: '1.2rem' }} />
                        <Typography variant="body2" sx={{ fontWeight: 500 }}>
                          {new Date(crew.created_at).toLocaleDateString()}
                        </Typography>
                      </Box>
                    </Box>
                    
                    <Box sx={{ display: 'flex', justifyContent: 'flex-end', gap: 1, mt: 'auto' }}>
                      <Tooltip title="Export Crew">
                        <IconButton 
                          onClick={(e) => handleExportCrew(e, crew)}
                          size="small"
                          sx={{
                            color: theme.palette.primary.main
                          }}
                        >
                          <DownloadIcon fontSize="small" />
                        </IconButton>
                      </Tooltip>
                      <Tooltip title="Delete Crew">
                        <IconButton 
                          onClick={(e) => handleDeleteCrew(e, crew.id)}
                          size="small"
                          sx={{
                            color: theme.palette.error.main
                          }}
                        >
                          <DeleteIcon fontSize="small" />
                        </IconButton>
                      </Tooltip>
                    </Box>
                  </CardContent>
                </Card>
              </Grid>
            ))}
          </Grid>
        </DialogContent>
        <DialogActions>
          <Button onClick={handleClose} color="inherit">Close</Button>
        </DialogActions>
      </MuiDialog>

      <input
        type="file"
        id="single-file-input-external"
        ref={fileInputRef}
        style={{ display: 'none' }}
        onChange={handleImportCrew}
        accept=".json"
      />
    </>
  );
};

export default CrewDialog; 