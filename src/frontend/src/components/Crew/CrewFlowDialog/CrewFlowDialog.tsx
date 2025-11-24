import React, { useState, useEffect, useRef, ChangeEvent, KeyboardEvent } from 'react';
import { 
  Dialog, 
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
  TextField,
  InputAdornment,
  Tabs,
  Tab,
  Divider
} from '@mui/material';
import { CrewService } from '../../../api/CrewService';
import { FlowService } from '../../../api/FlowService';
import { CrewResponse } from '../../../types/crews';
import { FlowResponse } from '../../../types/flow';
import { CrewFlowSelectionDialogProps } from '../../../types/crewflowDialog';
import { Node as _Node, Edge as _Edge } from 'reactflow';
import DeleteIcon from '@mui/icons-material/Delete';
import DownloadIcon from '@mui/icons-material/Download';
import SearchIcon from '@mui/icons-material/Search';
import CloseIcon from '@mui/icons-material/Close';
import AccountTreeIcon from '@mui/icons-material/AccountTree';
import PersonIcon from '@mui/icons-material/Person';
import EditIcon from '@mui/icons-material/Edit';
import UploadIcon from '@mui/icons-material/Upload';
import FileUploadIcon from '@mui/icons-material/FileUpload';
import EditFlowForm from '../../Flow/EditFlowForm';
import { AgentService } from '../../../api/AgentService';
import { TaskService } from '../../../api/TaskService';
import { Agent } from '../../../types/agent';
import { Task } from '../../../types/task';
import GroupIcon from '@mui/icons-material/Group';
import AssignmentIcon from '@mui/icons-material/Assignment';
import List from '@mui/material/List';
import ListItemButton from '@mui/material/ListItemButton';
import ListItemIcon from '@mui/material/ListItemIcon';
import ListItemText from '@mui/material/ListItemText';
import { useFlowConfigStore } from '../../../store/flowConfig';

interface TabPanelProps {
  children?: React.ReactNode;
  index: number;
  value: number;
}

function TabPanel(props: TabPanelProps) {
  const { children, value, index, ...other } = props;

  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`dialog-tabpanel-${index}`}
      aria-labelledby={`dialog-tab-${index}`}
      {...other}
    >
      {value === index && (
        <Box sx={{ pt: 2 }}>
          {children}
        </Box>
      )}
    </div>
  );
}

const CrewFlowSelectionDialog: React.FC<CrewFlowSelectionDialogProps> = ({
  open,
  onClose,
  onCrewSelect,
  onFlowSelect,
  onAgentSelect,
  onTaskSelect,
  initialTab = 0,
  showOnlyTab,
  hideFlowsTab = false,
}): JSX.Element => {
  const [tabValue, setTabValue] = useState(initialTab);
  
  // When showing only one tab, always use that tab's value
  useEffect(() => {
    if (showOnlyTab !== undefined) {
      setTabValue(showOnlyTab);
    } else {
      setTabValue(initialTab);
    }
  }, [showOnlyTab, initialTab]);
  const [crews, setCrews] = useState<CrewResponse[]>([]);
  const [flows, setFlows] = useState<FlowResponse[]>([]);
  const [agents, setAgents] = useState<Agent[]>([]);
  const [tasks, setTasks] = useState<Task[]>([]);
  const [selectedAgents, setSelectedAgents] = useState<Agent[]>([]);
  const [selectedTasks, setSelectedTasks] = useState<Task[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [importSuccess, setImportSuccess] = useState<string | null>(null);
  const searchInputRef = useRef<HTMLInputElement>(null);
  const [searchQuery, setSearchQuery] = useState('');
  
  // Clear search query when dialog opens or closes
  useEffect(() => {
    if (!open) {
      setSearchQuery('');
    }
  }, [open]);
  const [editFlowDialogOpen, setEditFlowDialogOpen] = useState(false);
  const [selectedFlowId, setSelectedFlowId] = useState<number | string | null>(null);
  const [_focusedCardIndex, _setFocusedCardIndex] = useState<number>(0);
  const firstCrewCardRef = useRef<HTMLDivElement>(null);
  const firstFlowCardRef = useRef<HTMLDivElement>(null);
  const firstAgentCardRef = useRef<HTMLDivElement>(null);
  const firstTaskCardRef = useRef<HTMLDivElement>(null);
  
  // Get flow configuration to check if CrewAI flows are enabled
  const { crewAIFlowEnabled } = useFlowConfigStore();
  
  // Helper function to detect if a crew contains flow nodes
  const isCrewActuallyFlow = (crew: CrewResponse): boolean => {
    return crew.nodes?.some(node => node.type === 'flowNode') || false;
  };
  
  // Refs for file inputs
  const flowFileInputRef = useRef<HTMLInputElement>(null);
  const bulkFileInputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (open) {
      // Load data based on current tab
      if (tabValue === 0) {
        loadCrews();
      } else if (tabValue === 1) {
        loadAgents();
      } else if (tabValue === 2) {
        loadTasks();
      } else if (tabValue === 3 && crewAIFlowEnabled) {
        loadFlows();
      }
    }
  }, [open, tabValue, crewAIFlowEnabled]);

  // Clear selections when closing dialog
  useEffect(() => {
    if (!open) {
      setSelectedAgents([]);
      setSelectedTasks([]);
    }
  }, [open]);

  // Switch to Plans tab if CrewAI flow is disabled and user is on Flows tab
  useEffect(() => {
    if (!crewAIFlowEnabled && tabValue === 3) {
      setTabValue(0);
    }
  }, [crewAIFlowEnabled, tabValue]);

  // Focus management when dialog opens
  const handleDialogEntered = () => {
    // Focus only the search box when dialog opens
    setTimeout(() => {
      if (searchInputRef.current) {
        searchInputRef.current.focus();
      }
    }, 150);
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

  const loadFlows = async () => {
    setLoading(true);
    try {
      const fetchedFlows = await FlowService.getFlows();
      setFlows(fetchedFlows);
      setError(null);
    } catch (error) {
      console.error('Error loading flows:', error);
      setError('Failed to load flows');
    } finally {
      setLoading(false);
    }
  };

  const loadAgents = async () => {
    setLoading(true);
    try {
      const fetchedAgents = await AgentService.listAgents();
      setAgents(fetchedAgents);
      setError(null);
    } catch (error) {
      console.error('Error loading agents:', error);
      setError('Failed to load agents');
    } finally {
      setLoading(false);
    }
  };

  const loadTasks = async () => {
    setLoading(true);
    try {
      const fetchedTasks = await TaskService.listTasks();
      setTasks(fetchedTasks);
      setError(null);
    } catch (error) {
      console.error('Error loading tasks:', error);
      setError('Failed to load tasks');
    } finally {
      setLoading(false);
    }
  };

  const handleCrewSelect = async (crewId: string) => {
    setLoading(true);
    
    // Dispatch event to signal crew loading is starting
    window.dispatchEvent(new CustomEvent('crewLoadStarted'));
    
    try {
      const selectedCrew = await CrewService.getCrew(crewId);
      
      if (!selectedCrew) {
        throw new Error('Crew not found');
      }

      console.log('Selected crew:', selectedCrew);
      
      // Create a mapping of old IDs to new IDs
      const idMapping: { [key: string]: string } = {};

      // Create agents first
      for (const node of selectedCrew.nodes || []) {
        if (node.type === 'agentNode') {
          try {
            const agentData = node.data;
            // Format agent data following AgentForm pattern
            const formattedAgentData = {
              name: agentData.name || agentData.label || '',
              role: agentData.role || '',
              goal: agentData.goal || '',
              backstory: agentData.backstory || '',
              llm: agentData.llm || 'databricks-llama-4-maverick',
              tools: agentData.tools || [],
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
            if (newAgent && newAgent.id) {
              idMapping[node.id] = newAgent.id.toString();
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
            // Update agent_id in task data if it exists in our mapping
            const updatedAgentId = taskData.agent_id && idMapping[taskData.agent_id] 
              ? parseInt(idMapping[taskData.agent_id]) 
              : 0;

            // Format task data following TaskForm pattern
            const formattedTaskData = {
              name: String(taskData.label || ''),
              description: String(taskData.description || ''),
              expected_output: String(taskData.expected_output || ''),
              tools: (taskData.tools || []).map((tool: unknown) => String(tool)),
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
                llm_guardrail: taskData.config?.llm_guardrail || null,
                markdown: taskData.config?.markdown === true || taskData.config?.markdown === 'true' || taskData.markdown === true || taskData.markdown === 'true'
              }
            };

            const newTask = await TaskService.createTask(formattedTaskData);
            if (newTask && newTask.id) {
              idMapping[node.id] = newTask.id.toString();
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

      // Update node IDs and references
      const updatedNodes = (selectedCrew.nodes || []).map(node => {
        const newId = node.type === 'agentNode' 
          ? `agent-${idMapping[node.id] || node.id}`
          : `task-${idMapping[node.id] || node.id}`;
        
        const updatedNode = {
          ...node,
          id: newId,
          type: node.type, // Ensure type is preserved
          data: {
            ...node.data,
            id: idMapping[node.id] || node.data.id,
            agent_id: node.data.agent_id ? idMapping[node.data.agent_id] || node.data.agent_id : node.data.agent_id,
            agentId: node.type === 'agentNode' ? idMapping[node.id] || node.data.agentId : node.data.agentId,
            taskId: node.type === 'taskNode' ? idMapping[node.id] || node.data.taskId : node.data.taskId,
            type: node.type === 'agentNode' ? 'agent' : 'task' // Set the internal type field
          }
        };
        console.log('Updated node:', updatedNode);
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
      
      // Pass the crew data to the callback - the tab creation will be handled there
      onCrewSelect(updatedNodes, updatedEdges, selectedCrew.name, selectedCrew.id.toString());
      onClose();
      
      // Dispatch event to fit view after nodes are rendered
      setTimeout(() => {
        if (typeof window !== 'undefined') {
          const event = new CustomEvent('fitViewToNodes', { bubbles: true });
          window.dispatchEvent(event);
        }
      }, 100);
    } catch (err) {
      const error = err as Error;
      console.error('Error loading crew:', error);
      setError(error.message || 'Failed to load crew');
    } finally {
      setLoading(false);
    }
  };

  const handleFlowSelect = async (flowId: string) => {
    try {
      setLoading(true);
      setError(null);
      
      console.log(`Selecting flow with ID: ${flowId}`);
      const selectedFlow = await FlowService.getFlow(flowId);
      
      // Better error handling for null or missing data
      if (!selectedFlow) {
        throw new Error('Failed to load flow data from server');
      }
      
      if (!Array.isArray(selectedFlow.nodes) || !Array.isArray(selectedFlow.edges)) {
        console.error('Invalid flow structure:', selectedFlow);
        throw new Error('Invalid flow data structure');
      }

      // Extract any flow configuration from the response
      // The FlowService should map flow_config to flowConfig
      let flowConfig = selectedFlow.flowConfig;
      
      // If still no explicit flowConfig but we have nodes with listener data, rebuild the config
      if (!flowConfig && selectedFlow.nodes.some(node => node.data?.listener)) {
        const listeners = selectedFlow.nodes
          .filter(node => node.data?.listener)
          .map(node => ({
            id: `listener-${node.id}`,
            name: node.data.label || `Listener ${node.id}`,
            crewId: String(node.data.crewRef || ''),
            crewName: node.data.crewName || node.data.label || 'Unknown',
            tasks: node.data.listener.tasks || [],
            listenToTaskIds: node.data.listener.listenToTaskIds || [],
            listenToTaskNames: node.data.listener.listenToTaskNames || [],
            conditionType: node.data.listener.conditionType || 'NONE',
            state: node.data.listener.state || {
              stateType: 'unstructured',
              stateDefinition: '',
              stateData: {}
            }
          }));
        
        flowConfig = {
          id: `flow-${Date.now()}`,
          name: selectedFlow.name,
          listeners,
          actions: [],
          startingPoints: []
        };
      }

      // Ensure flowConfig is properly structured
      if (flowConfig) {
        flowConfig = {
          id: flowConfig.id || `flow-${Date.now()}`,
          name: flowConfig.name || selectedFlow.name,
          listeners: flowConfig.listeners || [],
          actions: flowConfig.actions || [],
          startingPoints: flowConfig.startingPoints || []
        };
      }

      onFlowSelect(selectedFlow.nodes, selectedFlow.edges, flowConfig);
      onClose();
      
      // Dispatch event to fit view after nodes are rendered
      setTimeout(() => {
        if (typeof window !== 'undefined') {
          const event = new CustomEvent('fitViewToNodes', { bubbles: true });
          window.dispatchEvent(event);
        }
      }, 100);
    } catch (err) {
      const error = err as Error;
      console.error('Error loading flow:', error);
      setError(error.message || 'Failed to load flow');
    } finally {
      setLoading(false);
    }
  };

  const handleDeleteFlow = async (event: React.MouseEvent, flowId: string) => {
    event.stopPropagation();
    try {
      setLoading(true);
      
      await FlowService.deleteFlow(flowId);
      loadFlows();
    } catch (error) {
      console.error('Error deleting flow:', error);
      setError('Failed to delete flow');
    } finally {
      setLoading(false);
    }
  };

  const handleDeleteCrew = async (event: React.MouseEvent, crewId: string) => {
    event.stopPropagation();
    try {
      await CrewService.deleteCrew(crewId);
      loadCrews();
    } catch (error) {
      console.error('Error deleting crew:', error);
      setError('Failed to delete crew');
    }
  };

  const handleExportFlow = async (event: React.MouseEvent, flow: FlowResponse) => {
    event.stopPropagation();
    try {
      const exportData = JSON.stringify(flow, null, 2);
      const blob = new Blob([exportData], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = `flow_${flow.name.replace(/\s+/g, '_').toLowerCase()}.json`;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      URL.revokeObjectURL(url);
    } catch (error) {
      console.error('Error exporting flow:', error);
      setError('Failed to export flow');
    }
  };

  const handleExportCrew = async (event: React.MouseEvent, crew: CrewResponse) => {
    event.stopPropagation();
    try {
      const exportData = JSON.stringify(crew, null, 2);
      const blob = new Blob([exportData], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = `crew_${crew.name.replace(/\s+/g, '_').toLowerCase()}.json`;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      URL.revokeObjectURL(url);
    } catch (error) {
      console.error('Error exporting crew:', error);
      setError('Failed to export crew');
    }
  };

  const handleEditFlow = async (event: React.MouseEvent, flowId: string) => {
    event.stopPropagation();
    try {
      setSelectedFlowId(flowId);
      setEditFlowDialogOpen(true);
    } catch (error) {
      console.error('Error editing flow:', error);
      setError('Failed to edit flow');
    }
  };
  
  const handleEditFlowDialogClose = () => {
    setEditFlowDialogOpen(false);
    setSelectedFlowId(null);
  };
  
  const handleFlowUpdated = () => {
    loadFlows();
  };

  const handleDeleteAgent = async (event: React.MouseEvent, agentId: string) => {
    event.stopPropagation();
    try {
      setLoading(true);
      const success = await AgentService.deleteAgent(agentId);
      if (success) {
        await loadAgents();
        // Remove from selected if it was selected
        setSelectedAgents(prev => prev.filter(a => a.id !== agentId));
      }
    } catch (error) {
      console.error('Error deleting agent:', error);
      setError('Failed to delete agent');
    } finally {
      setLoading(false);
    }
  };

  const handleDeleteTask = async (event: React.MouseEvent, taskId: string) => {
    event.stopPropagation();
    try {
      setLoading(true);
      await TaskService.deleteTask(taskId);
      await loadTasks();
      // Remove from selected if it was selected
      setSelectedTasks(prev => prev.filter(t => t.id !== taskId));
    } catch (error) {
      console.error('Error deleting task:', error);
      setError('Failed to delete task');
    } finally {
      setLoading(false);
    }
  };

  const handleDeleteAllAgents = async () => {
    if (!window.confirm('Are you sure you want to delete ALL agents? This action cannot be undone.')) {
      return;
    }
    
    try {
      setLoading(true);
      setError(null);
      
      // Delete all agents
      for (const agent of agents) {
        if (agent.id) {
          await AgentService.deleteAgent(agent.id);
        }
      }
      
      // Clear the lists
      setAgents([]);
      setSelectedAgents([]);
      
      // Reload to ensure consistency
      await loadAgents();
    } catch (err) {
      setError('Failed to delete all agents');
      console.error('Error deleting all agents:', err);
      // Refresh to show current state
      await loadAgents();
    } finally {
      setLoading(false);
    }
  };

  const handleDeleteAllTasks = async () => {
    if (!window.confirm('Are you sure you want to delete ALL tasks? This action cannot be undone.')) {
      return;
    }
    
    try {
      setLoading(true);
      setError(null);
      
      // Delete all tasks
      for (const task of tasks) {
        if (task.id) {
          await TaskService.deleteTask(task.id);
        }
      }
      
      // Clear the lists
      setTasks([]);
      setSelectedTasks([]);
      
      // Reload to ensure consistency
      await loadTasks();
    } catch (err) {
      setError('Failed to delete all tasks');
      console.error('Error deleting all tasks:', err);
      // Refresh to show current state
      await loadTasks();
    } finally {
      setLoading(false);
    }
  };

  const handleAgentToggle = (agent: Agent) => {
    setSelectedAgents(prev => {
      const isSelected = prev.some(a => a.id === agent.id);
      if (isSelected) {
        return prev.filter(a => a.id !== agent.id);
      }
      return [...prev, agent];
    });
  };

  const handleTaskToggle = (task: Task) => {
    setSelectedTasks(prev => {
      const isSelected = prev.some(t => t.id === task.id);
      if (isSelected) {
        return prev.filter(t => t.id !== task.id);
      }
      return [...prev, task];
    });
  };

  const handlePlaceAgents = () => {
    if (onAgentSelect) {
      onAgentSelect(selectedAgents);
      setSelectedAgents([]);
      onClose();
    }
  };

  const handlePlaceTasks = () => {
    if (onTaskSelect) {
      onTaskSelect(selectedTasks);
      setSelectedTasks([]);
      onClose();
    }
  };

  const handleSelectAllAgents = () => {
    if (selectedAgents.length === agents.length) {
      setSelectedAgents([]);
    } else {
      setSelectedAgents([...agents]);
    }
  };

  const handleSelectAllTasks = () => {
    if (selectedTasks.length === tasks.length) {
      setSelectedTasks([]);
    } else {
      setSelectedTasks([...tasks]);
    }
  };

  const handleTabChange = (_event: React.SyntheticEvent, newValue: number) => {
    // Prevent switching to Flows tab if CrewAI flow is disabled
    if (newValue === 3 && !crewAIFlowEnabled) {
      return;
    }
    // Only allow tab changes when showing all tabs
    if (showOnlyTab === undefined) {
      setTabValue(newValue);
      // Reset search when changing tabs
      setSearchQuery('');
    }
  };



  const handleImportFlowClick = () => {
    if (flowFileInputRef.current) {
      flowFileInputRef.current.click();
    }
  };

  const handleBulkImportClick = () => {
    if (bulkFileInputRef.current) {
      bulkFileInputRef.current.click();
    }
  };



  const handleImportFlow = async (event: ChangeEvent<HTMLInputElement>) => {
    if (!event.target.files || event.target.files.length === 0) return;
    
    try {
      setLoading(true);
      setError(null);
      setImportSuccess(null);
      
      const file = event.target.files[0];
      const fileContents = await file.text();
      const flowData = JSON.parse(fileContents);
      
      // Validate flow data
      if (!flowData.name) {
        throw new Error('Invalid flow data: missing name');
      }
      
      // Save flow
      await FlowService.saveFlow({
        name: flowData.name,
        crew_id: flowData.crew_id || 0,
        nodes: flowData.nodes || [],
        edges: flowData.edges || [],
        flowConfig: flowData.flowConfig || flowData.flow_config
      });
      
      await loadFlows();
      setImportSuccess('Flow imported successfully');
      
      // Reset file input
      event.target.value = '';
    } catch (err) {
      const error = err as Error;
      console.error('Error importing flow:', error);
      setError(`Failed to import flow: ${error.message}`);
    } finally {
      setLoading(false);
    }
  };









  const handleBulkImport = async (event: ChangeEvent<HTMLInputElement>) => {
    if (!event.target.files || event.target.files.length === 0) return;

    try {
      setLoading(true);
      setError(null);
      setImportSuccess(null);

      const file = event.target.files[0];
      const fileContents = await file.text();
      let bulkData = JSON.parse(fileContents);

      // Auto-detect single crew object and wrap it
      // A single crew has: name, nodes, edges, and optionally agent_ids/task_ids
      const isSingleCrew = bulkData.name &&
                          bulkData.nodes &&
                          Array.isArray(bulkData.nodes) &&
                          bulkData.edges &&
                          Array.isArray(bulkData.edges) &&
                          !bulkData.crews &&
                          !bulkData.flows;

      if (isSingleCrew) {
        console.log('Detected single crew object, wrapping in crews array');
        bulkData = { crews: [bulkData] };
      }

      // Validate bulk data
      if (!bulkData.crews && !bulkData.flows && !bulkData.agents && !bulkData.tasks) {
        throw new Error('Invalid import data: no valid data found');
      }

      let importedCrews = 0;
      let importedFlows = 0;
      let importedAgents = 0;
      let importedTasks = 0;
      
      // Import agents first (they may be referenced by tasks)
      if (Array.isArray(bulkData.agents)) {
        for (const agent of bulkData.agents) {
          if (agent.name && agent.role) {
            await AgentService.createAgent({
              name: agent.name,
              role: agent.role,
              backstory: agent.backstory || '',
              goal: agent.goal || '',
              verbose: agent.verbose || false,
              allow_delegation: agent.allow_delegation || false,
              tools: agent.tools || [],
              max_iter: agent.max_iter || 15,
              max_rpm: agent.max_rpm || null,
              llm: agent.llm || '',
              cache: agent.cache !== undefined ? agent.cache : true,
              allow_code_execution: agent.allow_code_execution || false,
              code_execution_mode: agent.code_execution_mode || 'safe'
            });
            importedAgents++;
          }
        }
      }
      
      // Import tasks
      if (Array.isArray(bulkData.tasks)) {
        for (const task of bulkData.tasks) {
          if (task.description) {
            await TaskService.createTask({
              name: task.name || task.description.slice(0, 50),
              description: task.description,
              expected_output: task.expected_output || '',
              tools: task.tools || [],
              agent_id: task.agent_id || null,
              async_execution: task.async_execution || false,
              context: task.context || null,
              config: task.config || {}
            });
            importedTasks++;
          }
        }
      }

      // Import crews
      if (Array.isArray(bulkData.crews)) {
        for (const c of bulkData.crews) {
          const normalized = c?.crew ?? c;
          const name = normalized.name
            || normalized.title
            || normalized.plan_name
            || normalized.workflow_name
            || `Imported Crew ${new Date().toISOString().slice(0,19).replace('T',' ')}`;
          await CrewService.saveCrew({
            name,
            nodes: normalized.nodes || [],
            edges: normalized.edges || [],
            agent_ids: normalized.agent_ids || [],
            task_ids: normalized.task_ids || []
          });
          importedCrews++;
        }
      } else if (bulkData.crew) {
        const normalized = bulkData.crew;
        const name = normalized.name
          || normalized.title
          || normalized.plan_name
          || normalized.workflow_name
          || `Imported Crew ${new Date().toISOString().slice(0,19).replace('T',' ')}`;
        await CrewService.saveCrew({
          name,
          nodes: normalized.nodes || [],
          edges: normalized.edges || [],
          agent_ids: normalized.agent_ids || [],
          task_ids: normalized.task_ids || []
        });
        importedCrews++;
      }
      
      // Import flows
      if (Array.isArray(bulkData.flows)) {
        for (const flow of bulkData.flows) {
          if (flow.name) {
            await FlowService.saveFlow({
              name: flow.name,
              crew_id: flow.crew_id || 0,
              nodes: flow.nodes || [],
              edges: flow.edges || [],
              flowConfig: flow.flowConfig || flow.flow_config
            });
            importedFlows++;
          }
        }
      }
      
      // Reload data based on what was imported
      if (importedAgents > 0) {
        await loadAgents();
      }
      if (importedTasks > 0) {
        await loadTasks();
      }
      if (importedCrews > 0) {
        await loadCrews();
      }
      if (importedFlows > 0) {
        await loadFlows();
      }
      
      const parts = [];
      if (importedAgents > 0) parts.push(`${importedAgents} agents`);
      if (importedTasks > 0) parts.push(`${importedTasks} tasks`);
      if (importedCrews > 0) parts.push(`${importedCrews} crews`);
      if (importedFlows > 0) parts.push(`${importedFlows} flows`);
      
      setImportSuccess(`Import successful: ${parts.join(', ')} imported.`);
      
      // Reset file input
      event.target.value = '';
    } catch (err) {
      const error = err as Error;
      console.error('Error bulk importing:', error);
      setError(`Failed to import: ${error.message}`);
    } finally {
      setLoading(false);
    }
  };

  // Export all


  const handleExportAllFlows = async () => {
    try {
      setLoading(true);
      setError(null);
      
      // Get all flows
      const allFlows = await FlowService.getFlows();
      
      // Package in a format for export
      const exportData = {
        flows: allFlows,
        exportDate: new Date().toISOString()
      };
      
      // Export to file
      const blob = new Blob([JSON.stringify(exportData, null, 2)], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = `all_flows_export_${new Date().toISOString().slice(0, 10)}.json`;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      URL.revokeObjectURL(url);
    } catch (error) {
      console.error('Error exporting all flows:', error);
      setError('Failed to export all flows');
    } finally {
      setLoading(false);
    }
  };





  const handleExportEverything = async () => {
    try {
      setLoading(true);
      setError(null);
      
      // Get all data
      const allCrews = await CrewService.getCrews();
      const allFlows = await FlowService.getFlows();
      const allAgents = await AgentService.listAgents();
      const allTasks = await TaskService.listTasks();
      
      // Package in a format for export
      const exportData = {
        crews: allCrews,
        flows: allFlows,
        agents: allAgents,
        tasks: allTasks,
        exportDate: new Date().toISOString()
      };
      
      // Export to file
      const blob = new Blob([JSON.stringify(exportData, null, 2)], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = `complete_export_${new Date().toISOString().slice(0, 10)}.json`;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      URL.revokeObjectURL(url);
    } catch (error) {
      console.error('Error exporting all data:', error);
      setError('Failed to export all data');
    } finally {
      setLoading(false);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent, id: string, type: 'crew' | 'flow' | 'agent' | 'task', index: number) => {
    // Get the right array based on the type
    let itemsArray: CrewResponse[] | FlowResponse[] | Agent[] | Task[];
    switch(type) {
      case 'crew':
        itemsArray = crews;
        break;
      case 'flow':
        itemsArray = flows;
        break;
      case 'agent':
        itemsArray = agents;
        break;
      case 'task':
        itemsArray = tasks;
        break;
      default:
        itemsArray = [];
    }
    
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault();
      if (type === 'crew') {
        handleCrewSelect(id);
      } else {
        handleFlowSelect(id.toString());
      }
    } else if (e.key === 'ArrowRight') {
      e.preventDefault();
      const nextIndex = (index + 1) % itemsArray.length;
      const nextElement = document.querySelector(`[data-card-index="${type}-${nextIndex}"]`) as HTMLElement;
      if (nextElement) nextElement.focus();
    } else if (e.key === 'ArrowLeft') {
      e.preventDefault();
      const prevIndex = (index - 1 + itemsArray.length) % itemsArray.length;
      const prevElement = document.querySelector(`[data-card-index="${type}-${prevIndex}"]`) as HTMLElement;
      if (prevElement) prevElement.focus();
    } else if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
      e.preventDefault();
      // Calculate number of cards per row based on current viewport
      // Assuming 3 cards per row on desktop, 2 on tablet, 1 on mobile
      // This is a rough estimate that matches the Grid item sizing
      const width = window.innerWidth;
      let cardsPerRow = 1;
      if (width >= 960) cardsPerRow = 3; // md breakpoint
      else if (width >= 600) cardsPerRow = 2; // sm breakpoint
      
      // Calculate the vertical navigation
      const currentRow = Math.floor(index / cardsPerRow);
      const currentCol = index % cardsPerRow;
      let targetIndex;
      
      if (e.key === 'ArrowDown') {
        const nextRow = (currentRow + 1);
        targetIndex = nextRow * cardsPerRow + currentCol;
        // If we would go beyond the last row, wrap to the first
        if (targetIndex >= itemsArray.length) {
          // Go to same column in first row
          targetIndex = currentCol;
        }
      } else { // ArrowUp
        const prevRow = (currentRow - 1);
        if (prevRow < 0) {
          // Go to the same column in last row
          const lastRowIndex = Math.floor((itemsArray.length - 1) / cardsPerRow);
          targetIndex = lastRowIndex * cardsPerRow + currentCol;
          // If the last row doesn't have this column, go to the last item
          if (targetIndex >= itemsArray.length) {
            targetIndex = itemsArray.length - 1;
          }
        } else {
          targetIndex = prevRow * cardsPerRow + currentCol;
        }
      }
      
      // Focus the target element if it exists
      if (targetIndex >= 0 && targetIndex < itemsArray.length) {
        const targetElement = document.querySelector(`[data-card-index="${type}-${targetIndex}"]`) as HTMLElement;
        if (targetElement) targetElement.focus();
      }
    }
  };

  // Handle keyboard shortcuts at the dialog level
  const handleDialogKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    // Skip shortcuts if an input field is active
    if (
      document.activeElement instanceof HTMLInputElement ||
      document.activeElement instanceof HTMLTextAreaElement
    ) {
      return;
    }
    
    // When user presses '/' (forward slash) focus the search input
    if (event.key === '/' && searchInputRef.current) {
      event.preventDefault();
      searchInputRef.current.focus();
    }
    
    // When user presses 'f', focus the first card
    if (event.key === 'f') {
      event.preventDefault();
      if (tabValue === 0 && firstCrewCardRef.current) {
        firstCrewCardRef.current.focus();
      } else if (tabValue === 1 && firstAgentCardRef.current) {
        firstAgentCardRef.current.focus();
      } else if (tabValue === 2 && firstTaskCardRef.current) {
        firstTaskCardRef.current.focus();
      } else if (tabValue === 3 && firstFlowCardRef.current) {
        firstFlowCardRef.current.focus();
      }
    }
  };

  return (
    <>
      <Dialog
        open={open}
        onClose={onClose}
        maxWidth="md"
        fullWidth
        TransitionProps={{
          onEntered: handleDialogEntered,
        }}
        PaperProps={{
          component: "div", // This allows the dialog to receive focus
          role: "dialog",
          tabIndex: -1, // This allows the dialog to be part of the tab sequence
        }}
      >
        <DialogTitle sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Box>
            {showOnlyTab === 0 ? 'Load Existing Crews' :
             showOnlyTab === 1 ? 'Load Existing Agents' :
             showOnlyTab === 2 ? 'Load Existing Tasks' :
             showOnlyTab === 3 ? 'Load Existing Flows' :
             'Open Catalog'}
          </Box>
          <IconButton onClick={onClose}>
            <CloseIcon />
          </IconButton>
        </DialogTitle>
        <DialogContent onKeyDown={handleDialogKeyDown} data-tour="catalog-dialog">
          {error && (
            <Alert severity="error" sx={{ mb: 2 }}>
              {error}
            </Alert>
          )}
          {importSuccess && (
            <Alert severity="success" sx={{ mb: 2 }}>
              {importSuccess}
            </Alert>
          )}
          
          <Box sx={{ width: '100%' }}>
            {showOnlyTab === undefined ? (
              // Show all tabs when opened from catalog
              <Box sx={{ borderBottom: 1, borderColor: 'divider', display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 2 }}>
                <Tabs value={tabValue} onChange={handleTabChange} aria-label="catalog tabs">
                  <Tab icon={<PersonIcon />} iconPosition="start" label="Crews" id="crew-tab-0" aria-controls="tabpanel-0" sx={{ textTransform: 'none' }} />
                  <Tab icon={<GroupIcon />} iconPosition="start" label="Agents" id="agent-tab-1" aria-controls="tabpanel-1" sx={{ textTransform: 'none' }} />
                  <Tab icon={<AssignmentIcon />} iconPosition="start" label="Tasks" id="task-tab-2" aria-controls="tabpanel-2" sx={{ textTransform: 'none' }} />
                  {crewAIFlowEnabled && !hideFlowsTab && (
                    <Tab icon={<AccountTreeIcon />} iconPosition="start" label="Flows" id="flow-tab-3" aria-controls="tabpanel-3" sx={{ textTransform: 'none' }} />
                  )}
                </Tabs>
                <Box sx={{ display: 'flex', gap: 1 }}>
                  <Button startIcon={<FileUploadIcon />} variant="outlined" size="small" onClick={handleBulkImportClick}>
                    Import
                  </Button>
                  <Button startIcon={<DownloadIcon />} variant="outlined" size="small" onClick={handleExportEverything} disabled={crews.length === 0 && flows.length === 0 && agents.length === 0 && tasks.length === 0}>
                    Export
                  </Button>
                </Box>
              </Box>
            ) : showOnlyTab === 3 ? (
              // Show import/export buttons when showing only Flows tab
              <Box sx={{ borderBottom: 1, borderColor: 'divider', display: 'flex', alignItems: 'center', justifyContent: 'flex-end', gap: 1, py: 1 }}>
                <Button startIcon={<FileUploadIcon />} variant="outlined" size="small" onClick={handleImportFlowClick}>
                  Import Flow
                </Button>
                <Button startIcon={<DownloadIcon />} variant="outlined" size="small" onClick={handleExportAllFlows} disabled={flows.length === 0}>
                  Export All Flows
                </Button>
              </Box>
            ) : null}

            {/* Search and action buttons */}
            <Box sx={{ py: 1, display: 'flex', justifyContent: 'space-between' }}>
              <Box sx={{ flex: 1 }}>
                <TextField
                  placeholder="Search..."
                  size="small"
                  fullWidth
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  inputRef={searchInputRef}
                  autoComplete="off"
                  onKeyDown={(e) => {
                    // Only handle Tab and Escape, let all other typing happen normally
                    if (e.key === 'Tab' && !e.shiftKey) {
                      // When Tab is pressed in the search box, move focus to the first card
                      e.preventDefault();
                      // Check which tab is active and focus the appropriate first card
                      if (tabValue === 0 && firstCrewCardRef.current) {
                        firstCrewCardRef.current.focus();
                      } else if (tabValue === 1 && firstAgentCardRef.current) {
                        firstAgentCardRef.current.focus();
                      } else if (tabValue === 2 && firstTaskCardRef.current) {
                        firstTaskCardRef.current.focus();
                      } else if (tabValue === 3 && firstFlowCardRef.current) {
                        firstFlowCardRef.current.focus();
                      }
                    } else if (e.key === 'Escape') {
                      setSearchQuery('');
                    }
                    // All other keys should work normally for typing
                  }}
                  InputProps={{
                    startAdornment: (
                      <InputAdornment position="start">
                        <SearchIcon fontSize="small" />
                      </InputAdornment>
                    ),
                  }}
                />
              </Box>
              {showOnlyTab === undefined && (
                <Box sx={{ ml: 2, display: 'flex', gap: 1 }}>
                  {tabValue === 3 ? (
                    <>
                      <Button
                        startIcon={<UploadIcon />}
                        variant="outlined"
                        size="small"
                        onClick={handleImportFlowClick}
                      >
                        Import
                      </Button>
                      <Button
                        startIcon={<DownloadIcon />}
                        variant="outlined"
                        size="small"
                        onClick={handleExportAllFlows}
                        disabled={flows.length === 0}
                      >
                        Export Flows
                      </Button>
                    </>
                  ) : null}
                </Box>
              )}
            </Box>
            
            {(showOnlyTab === undefined || showOnlyTab === 0) && (
              <TabPanel value={tabValue} index={0}>
                {showOnlyTab === undefined && (
                  <Alert severity="info" sx={{ mb: 2 }}>
                    Loading a crew will open it in a new tab
                  </Alert>
                )}
              {loading ? (
                <Box sx={{ display: 'flex', justifyContent: 'center', p: 4 }}>
                  <CircularProgress />
                </Box>
              ) : crews.length === 0 ? (
                <Alert severity="info">
                  No crews found. Create a crew by adding agents and tasks, then click Save Crew.
                </Alert>
              ) : (
                <Grid container spacing={2}>
                  {crews
                    .filter(crew => {
                      // Filter out flows if CrewAI flow is disabled
                      if (!crewAIFlowEnabled && isCrewActuallyFlow(crew)) {
                        return false;
                      }
                      
                      // Apply search filter
                      if (searchQuery) {
                        return crew.name.toLowerCase().includes(searchQuery.toLowerCase());
                      }
                      
                      return true;
                    })
                    .map((crew, index) => (
                      <Grid item xs={12} sm={6} md={4} key={crew.id}>
                        <Card 
                          sx={{ 
                            height: '100%',
                            cursor: 'pointer',
                            '&:hover': {
                              boxShadow: 3,
                              bgcolor: 'action.hover'
                            },
                            '&:focus': {
                              outline: '2px solid',
                              outlineColor: 'primary.main',
                              boxShadow: 6,
                              bgcolor: 'action.hover'
                            },
                            opacity: 1,
                            filter: 'none',
                            transition: 'all 0.2s'
                          }}
                          onClick={() => handleCrewSelect(crew.id)}
                          onKeyDown={(e) => handleKeyDown(e, crew.id, 'crew', index)}
                          tabIndex={0}
                          ref={index === 0 ? firstCrewCardRef : undefined}
                          data-card-index={`crew-${index}`}
                        >
                          <CardContent>
                            <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                              <Typography variant="h6" component="h2" gutterBottom noWrap>
                                {crew.name}
                              </Typography>
                              <Box>
                                <Tooltip title="Export Crew">
                                  <IconButton
                                    size="small"
                                    onClick={(e) => handleExportCrew(e, crew)}
                                  >
                                    <DownloadIcon fontSize="small" />
                                  </IconButton>
                                </Tooltip>
                                <Tooltip title="Delete Crew">
                                  <IconButton
                                    size="small"
                                    onClick={(e) => handleDeleteCrew(e, crew.id)}
                                  >
                                    <DeleteIcon fontSize="small" />
                                  </IconButton>
                                </Tooltip>
                              </Box>
                            </Box>
                            <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
                              No description
                            </Typography>
                            <Typography variant="caption" color="text.secondary" display="block">
                              Created: {new Date(crew.created_at).toLocaleString()}
                            </Typography>
                            <Typography variant="caption" color="text.secondary" display="block">
                              Agents: {(() => {
                                // Count agents from nodes
                                const nodesCount = crew.nodes?.filter(n => 
                                  n.type === 'agent' || 
                                  n.type === 'agentNode' || 
                                  (n.data && (
                                    n.data.type === 'agent' || 
                                    n.data.agentId || 
                                    n.data.agentRef
                                  )) ||
                                  (typeof n.id === 'string' && (
                                    n.id.startsWith('agent-') || 
                                    n.id.startsWith('agent:')
                                  ))
                                ).length || 0;
                                
                                // Add agents directly from crew.agents if it exists
                                const agentsCount = (crew.agents && Array.isArray(crew.agents)) ? crew.agents.length : 0;
                                
                                // Add from agent_ids if it exists
                                const agentIdsCount = (crew.agent_ids && Array.isArray(crew.agent_ids)) ? crew.agent_ids.length : 0;
                                
                                // Return the largest count
                                return Math.max(nodesCount, agentsCount, agentIdsCount);
                              })()} / 
                              Tasks: {(() => {
                                // Count tasks from nodes
                                const nodesCount = crew.nodes?.filter(n => 
                                  n.type === 'task' || 
                                  n.type === 'taskNode' || 
                                  (n.data && (
                                    n.data.type === 'task' || 
                                    n.data.taskId || 
                                    n.data.taskRef
                                  )) ||
                                  (typeof n.id === 'string' && (
                                    n.id.includes('task') || 
                                    n.id.includes('Task')
                                  ))
                                ).length || 0;
                                
                                // Add tasks directly from crew.tasks if it exists
                                const tasksCount = (crew.tasks && Array.isArray(crew.tasks)) ? crew.tasks.length : 0;
                                
                                // Add from task_ids if it exists
                                const taskIdsCount = (crew.task_ids && Array.isArray(crew.task_ids)) ? crew.task_ids.length : 0;
                                
                                // Return the largest count
                                return Math.max(nodesCount, tasksCount, taskIdsCount);
                              })()}
                            </Typography>
                          </CardContent>
                        </Card>
                      </Grid>
                    ))}
                </Grid>
              )}
              </TabPanel>
            )}
            
            {/* Agents Tab Panel */}
            {(showOnlyTab === undefined || showOnlyTab === 1) && (
              <TabPanel value={tabValue} index={1}>
                {showOnlyTab === undefined && (
                  <Alert severity="info" sx={{ mb: 2 }}>
                    Loading agents will add them to the current tab
                  </Alert>
                )}
              <Box sx={{ mb: 2, display: 'flex', gap: 1, flexWrap: 'wrap' }}>
                <Button
                  variant="outlined"
                  onClick={handleSelectAllAgents}
                >
                  {selectedAgents.length === agents.length ? 'Deselect All' : 'Select All'}
                </Button>
                <Button
                  variant="contained"
                  onClick={handlePlaceAgents}
                  disabled={selectedAgents.length === 0}
                >
                  Place Agents ({selectedAgents.length})
                </Button>
                <Button
                  variant="outlined"
                  color="error"
                  onClick={handleDeleteAllAgents}
                  disabled={agents.length === 0 || loading}
                  startIcon={<DeleteIcon />}
                >
                  Delete All
                </Button>

              </Box>
              <Divider sx={{ my: 2 }} />
              {loading ? (
                <Box sx={{ display: 'flex', justifyContent: 'center', p: 4 }}>
                  <CircularProgress />
                </Box>
              ) : agents.length === 0 ? (
                <Alert severity="info">
                  No agents found. Create agents to use in your crews.
                </Alert>
              ) : (
                <List sx={{ maxHeight: '50vh', overflow: 'auto' }}>
                  {agents
                    .filter(agent => {
                      if (searchQuery) {
                        return agent.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
                               agent.role.toLowerCase().includes(searchQuery.toLowerCase());
                      }
                      return true;
                    })
                    .map((agent) => (
                      <ListItemButton
                        key={agent.id}
                        onClick={() => handleAgentToggle(agent)}
                        selected={selectedAgents.some(a => a.id === agent.id)}
                      >
                        <ListItemIcon>
                          <GroupIcon />
                        </ListItemIcon>
                        <ListItemText
                          primary={agent.name}
                          secondary={`${agent.role} - LLM: ${agent.llm}`}
                        />
                        <Tooltip title="Delete Agent">
                          <IconButton
                            onClick={(e) => handleDeleteAgent(e, agent.id || '')}
                            disabled={loading}
                          >
                            <DeleteIcon />
                          </IconButton>
                        </Tooltip>
                      </ListItemButton>
                    ))}
                </List>
              )}
              </TabPanel>
            )}
            
            {/* Tasks Tab Panel */}
            {(showOnlyTab === undefined || showOnlyTab === 2) && (
              <TabPanel value={tabValue} index={2}>
                {showOnlyTab === undefined && (
                  <Alert severity="info" sx={{ mb: 2 }}>
                    Loading tasks will add them to the current tab
                  </Alert>
                )}
              <Box sx={{ mb: 2, display: 'flex', gap: 1, flexWrap: 'wrap' }}>
                <Button
                  variant="outlined"
                  onClick={handleSelectAllTasks}
                >
                  {selectedTasks.length === tasks.length ? 'Deselect All' : 'Select All'}
                </Button>
                <Button
                  variant="contained"
                  onClick={handlePlaceTasks}
                  disabled={selectedTasks.length === 0}
                >
                  Place Tasks ({selectedTasks.length})
                </Button>
                <Button
                  variant="outlined"
                  color="error"
                  onClick={handleDeleteAllTasks}
                  disabled={tasks.length === 0 || loading}
                  startIcon={<DeleteIcon />}
                >
                  Delete All
                </Button>

              </Box>
              <Divider sx={{ my: 2 }} />
              {loading ? (
                <Box sx={{ display: 'flex', justifyContent: 'center', p: 4 }}>
                  <CircularProgress />
                </Box>
              ) : tasks.length === 0 ? (
                <Alert severity="info">
                  No tasks found. Create tasks to use in your crews.
                </Alert>
              ) : (
                <List sx={{ maxHeight: '50vh', overflow: 'auto' }}>
                  {tasks
                    .filter(task => {
                      if (searchQuery) {
                        return task.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
                               task.description.toLowerCase().includes(searchQuery.toLowerCase());
                      }
                      return true;
                    })
                    .map((task) => (
                      <ListItemButton
                        key={task.id}
                        onClick={() => handleTaskToggle(task)}
                        selected={selectedTasks.some(t => t.id === task.id)}
                      >
                        <ListItemIcon>
                          <AssignmentIcon />
                        </ListItemIcon>
                        <ListItemText
                          primary={task.name}
                          secondary={task.description}
                        />
                        <Tooltip title="Delete Task">
                          <IconButton
                            onClick={(e) => handleDeleteTask(e, task.id)}
                            disabled={loading}
                          >
                            <DeleteIcon />
                          </IconButton>
                        </Tooltip>
                      </ListItemButton>
                    ))}
                </List>
              )}
              </TabPanel>
            )}
            
            {/* Flows Tab Panel */}
            {crewAIFlowEnabled && (showOnlyTab === undefined || showOnlyTab === 3) && (
              <TabPanel value={tabValue} index={3}>
              {loading ? (
                <Box sx={{ display: 'flex', justifyContent: 'center', p: 4 }}>
                  <CircularProgress />
                </Box>
              ) : flows.length === 0 ? (
                <Alert severity="info">
                  No flows found. Create a flow by adding flow components, then save it.
                </Alert>
              ) : (
                <Grid container spacing={2}>
                  {flows
                    .filter(flow => 
                      searchQuery === '' ||
                      flow.name.toLowerCase().includes(searchQuery.toLowerCase())
                    )
                    .map((flow, index) => (
                      <Grid item xs={12} sm={6} md={4} key={flow.id}>
                        <Card 
                          sx={{ 
                            height: '100%',
                            cursor: 'pointer',
                            '&:hover': {
                              boxShadow: 3,
                              bgcolor: 'action.hover'
                            },
                            '&:focus': {
                              outline: '2px solid',
                              outlineColor: 'primary.main',
                              boxShadow: 6,
                              bgcolor: 'action.hover'
                            },
                            opacity: 1,
                            filter: 'none',
                            transition: 'all 0.2s'
                          }}
                          onClick={() => handleFlowSelect(flow.id.toString())}
                          onKeyDown={(e) => handleKeyDown(e, flow.id, 'flow', index)}
                          tabIndex={0}
                          ref={index === 0 ? firstFlowCardRef : undefined}
                          data-card-index={`flow-${index}`}
                        >
                          <CardContent>
                            <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                              <Typography variant="h6" component="h2" gutterBottom noWrap>
                                {flow.name}
                              </Typography>
                              <Box>
                                <Tooltip title="Edit Flow">
                                  <IconButton 
                                    size="small" 
                                    onClick={(e) => handleEditFlow(e, flow.id.toString())}
                                  >
                                    <EditIcon fontSize="small" />
                                  </IconButton>
                                </Tooltip>
                                <Tooltip title="Export Flow">
                                  <IconButton 
                                    size="small" 
                                    onClick={(e) => handleExportFlow(e, flow)}
                                  >
                                    <DownloadIcon fontSize="small" />
                                  </IconButton>
                                </Tooltip>
                                <Tooltip title="Delete Flow">
                                  <IconButton 
                                    size="small" 
                                    onClick={(e) => handleDeleteFlow(e, flow.id.toString())}
                                  >
                                    <DeleteIcon fontSize="small" />
                                  </IconButton>
                                </Tooltip>
                              </Box>
                            </Box>
                            <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
                              No description
                            </Typography>
                            
                            {/* Crew list section */}
                            <Typography variant="subtitle2" color="text.primary" sx={{ mt: 1 }}>
                              Crews:
                            </Typography>
                            <Box
                              sx={{
                                mt: 1,
                                maxHeight: '80px',
                                overflowY: 'auto',
                                border: '1px solid',
                                borderColor: 'divider',
                                borderRadius: 1,
                                p: 1,
                                bgcolor: 'background.default',
                                '&::-webkit-scrollbar': {
                                  width: '8px',
                                },
                                '&::-webkit-scrollbar-track': {
                                  backgroundColor: 'background.paper',
                                },
                                '&::-webkit-scrollbar-thumb': {
                                  backgroundColor: 'primary.light',
                                  borderRadius: '4px',
                                }
                              }}
                            >
                              {flow.nodes && Array.isArray(flow.nodes) && flow.nodes
                                .filter(node => node.type === 'crewNode' || node.data?.crewName)
                                .map((node, index) => {
                                  const crewName = node.data?.crewName || node.data?.label || `Crew ${index + 1}`;
                                  return (
                                    <Typography 
                                      key={node.id} 
                                      variant="body2" 
                                      sx={{ 
                                        py: 0.5,
                                        borderBottom: index < flow.nodes.filter(n => n.type === 'crewNode' || n.data?.crewName).length - 1 ? 
                                          '1px solid' : 'none',
                                        borderColor: 'divider'
                                      }}
                                    >
                                      • {crewName}
                                    </Typography>
                                  );
                                })}
                              {(!flow.nodes || !Array.isArray(flow.nodes) || 
                                !flow.nodes.some(node => node.type === 'crewNode' || node.data?.crewName)) && (
                                <Typography variant="body2" color="text.secondary">
                                  No crews found
                                </Typography>
                              )}
                            </Box>
                            
                            <Typography variant="caption" color="text.secondary" display="block" sx={{ mt: 1 }}>
                              Created: {new Date(flow.created_at).toLocaleString()}
                            </Typography>
                            <Typography variant="caption" color="text.secondary" display="block">
                              Components: {flow.nodes?.length || 0} / 
                              Connections: {flow.edges?.length || 0}
                            </Typography>
                          </CardContent>
                        </Card>
                      </Grid>
                    ))}
                </Grid>
              )}
            </TabPanel>
            )}
          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={onClose} color="primary">
            Cancel
          </Button>
        </DialogActions>
      </Dialog>

      {/* Edit Flow Dialog */}
      {selectedFlowId && (
        <EditFlowForm
          open={editFlowDialogOpen}
          onClose={handleEditFlowDialogClose}
          flowId={selectedFlowId}
          onSave={handleFlowUpdated}
        />
      )}
      
      {/* Hidden file inputs */}
      <input 
        type="file" 
        ref={flowFileInputRef} 
        style={{ display: 'none' }} 
        accept=".json"
        onChange={handleImportFlow}
      />
      <input 
        type="file" 
        ref={bulkFileInputRef} 
        style={{ display: 'none' }} 
        accept=".json"
        onChange={handleBulkImport}
      />


    </>
  );
};

export default CrewFlowSelectionDialog; 