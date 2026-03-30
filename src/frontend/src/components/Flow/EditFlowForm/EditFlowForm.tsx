import React, { useState, useEffect, useCallback } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  TextField,
  CircularProgress,
  Alert,
  Typography,
  Box,
  Divider,
  IconButton,
  Paper,
  List,
  ListItem,
  ListItemText,
  Chip,
  Snackbar,
  Tabs,
  Tab,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Checkbox,
  SelectChangeEvent,
  InputAdornment,
} from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import AddIcon from '@mui/icons-material/Add';
import DeleteIcon from '@mui/icons-material/Delete';
import SearchIcon from '@mui/icons-material/Search';
import { FlowService } from '../../../api/FlowService';
import { CrewService } from '../../../api/CrewService';
import { FlowConfiguration, FlowResponse, Listener, Action, StartingPoint, Flow, FlowSaveData } from '../../../types/flow';
import { CrewTask } from '../../../types/crewPlan';
import { validateNodePositions } from '../../../utils/flowWizardUtils';
import { v4 as uuidv4 } from 'uuid';

// Extended listener with additional fields for editing
interface ExtendedListener extends Omit<Listener, 'listenToTaskNames'> {
  listenToTaskNames?: string[];
  listenTo?: string[] | string; // Keep the listenTo property to support legacy data format
  routerConfig?: {
    defaultRoute: string;
    routes: Array<{
      name: string;
      condition: string;
      taskIds: string[];
    }>;
  };
}

export interface EditFlowFormProps {
  open: boolean;
  onClose: () => void;
  flowId: number | string | null;
  onSave: () => void;
}

// Define Interfaces for task data from different sources
interface _TaskData {
  id: string;
  name?: string;
  agent_id?: string;
  description?: string;
  expected_output?: string;
  context?: string[];
}

// Add this interface to handle the task from CrewService
interface CrewTaskNode {
  id: number;
  name?: string;
  description?: string;
  expected_output?: string;
  [key: string]: unknown; 
}

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
      id={`flow-tabpanel-${index}`}
      aria-labelledby={`flow-tab-${index}`}
      {...other}
      style={{ padding: '16px 0', height: 'calc(100% - 48px)', overflow: 'auto' }}
    >
      {value === index && <Box sx={{ height: '100%' }}>{children}</Box>}
    </div>
  );
}

const EditFlowForm: React.FC<EditFlowFormProps> = ({
  open,
  onClose,
  flowId,
  onSave,
}) => {
  const [tabValue, setTabValue] = useState(0);
  const [flow, setFlow] = useState<FlowResponse | null>(null);
  const [flowName, setFlowName] = useState('');
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [saveSuccess, setSaveSuccess] = useState(false);
  
  // Editing state
  const [listeners, setListeners] = useState<ExtendedListener[]>([]);
  const [startingPoints, setStartingPoints] = useState<StartingPoint[]>([]);
  const [actions, setActions] = useState<Action[]>([]);
  const [currentListenerIndex, setCurrentListenerIndex] = useState(-1);
  
  // Task data for selection
  const [tasks, setTasks] = useState<{ id: string; name: string; agent_id?: string }[]>([]);
  const [crews, setCrews] = useState<{ id: number; name: string }[]>([]);
  const [_allCrews, _setAllCrews] = useState<{ id: number; name: string }[]>([]);
  const [crewSearchQuery, setCrewSearchQuery] = useState('');

  useEffect(() => {
    if (open && flowId) {
      loadFlow(flowId);
    } else {
      resetForm();
    }
  }, [open, flowId]);

  // Add a new useEffect to ensure we have all tasks from each crew
  useEffect(() => {
    // This will run on every crews change
    const fetchAllTasksForCrews = async () => {
      if (!crews.length) return;
      
      console.log('Fetching all tasks for selected crews:', crews.map(c => c.name).join(', '));
      const allTasksFromCrewService: { id: string; name: string; agent_id?: string }[] = [];
      
      for (const crew of crews) {
        try {
          const crewDetails = await CrewService.getCrew(String(crew.id));
          
          if (crewDetails && crewDetails.tasks && Array.isArray(crewDetails.tasks)) {
            console.log(`Found ${crewDetails.tasks.length} tasks for crew ${crew.id} (${crew.name})`);
            
            crewDetails.tasks.forEach((task: unknown) => {
              const taskNode = task as CrewTaskNode;
              
              // Only add if not already in the list
              if (!allTasksFromCrewService.some(t => t.id === String(taskNode.id))) {
                allTasksFromCrewService.push({
                  id: String(taskNode.id),
                  name: taskNode.name || `Task ${taskNode.id}`,
                  agent_id: String(crew.id)
                });
              }
            });
          }
        } catch (error) {
          console.error(`Error fetching tasks for crew ${crew.id}:`, error);
        }
      }
      
      // Update tasks state, but preserve existing tasks
      setTasks(prevTasks => {
        // Create a map of existing tasks by ID for quick lookup
        const existingTasksMap = new Map(prevTasks.map(task => [task.id, task]));
        
        // Merge with crew service tasks, preferring existing task data if available
        allTasksFromCrewService.forEach(newTask => {
          if (!existingTasksMap.has(newTask.id)) {
            existingTasksMap.set(newTask.id, newTask);
          }
        });
        
        return Array.from(existingTasksMap.values());
      });
      
      // Update starting points to include all tasks from all crews
      setStartingPoints(prevStartingPoints => {
        // Create a map of existing starting points by task ID for quick lookup
        const existingSpMap = new Map(prevStartingPoints.map(sp => [sp.taskId, sp]));
        const newStartingPoints: StartingPoint[] = [];
        
        // Add any new tasks from crew service that aren't already in starting points
        allTasksFromCrewService.forEach(task => {
          if (!existingSpMap.has(task.id)) {
            const crew = crews.find(c => String(c.id) === task.agent_id);
            if (crew) {
              newStartingPoints.push({
                crewId: String(crew.id),
                taskId: task.id,
                taskName: task.name,
                crewName: crew.name,
                isStartPoint: false
              });
            }
          }
        });
        
        return [...prevStartingPoints, ...newStartingPoints];
      });
    };
    
    // Only run this if crews have been loaded and we're on the edit form
    if (crews.length && open && flowId) {
      fetchAllTasksForCrews();
    }
  }, [crews, open, flowId]);

  // Add a new useEffect to load all tasks for all selected crews when the crews list changes
  useEffect(() => {
    const fetchAllTasksForAllSelectedCrews = async () => {
      if (!crews.length) return;
      
      console.log('Loading ALL tasks for ALL crews in the flow:', crews.map(c => c.name).join(', '));
      
      try {
        // First, ensure we have the most up-to-date crew data
        const allAvailableCrews = await CrewService.getCrews();
        console.log(`Fetched ${allAvailableCrews.length} crews from server`);
        
        // Create a map of flow crews by ID for quick lookup
        const flowCrewsMap = new Map(crews.map(crew => [crew.id, crew]));
        const fullCrewTasksList: { id: string; name: string; agent_id?: string }[] = [];
        
        // Process each crew in our flow
        for (const crewId of Array.from(flowCrewsMap.keys())) {
          try {
            // Use the detailed crew data from the server
            const serverCrew = allAvailableCrews.find(c => String(c.id) === String(crewId));
            const crewName = serverCrew?.name || flowCrewsMap.get(crewId)?.name || `Crew ${crewId}`;
            
            // Get ALL tasks for this crew from CrewService.getTasks - this should return all tasks
            const allCrewTasks = await CrewService.getTasks(String(crewId));
            console.log(`Found ${allCrewTasks.length} tasks for crew ${crewId} (${crewName}) from CrewService.getTasks`);
            
            // Add each task to our master list
            allCrewTasks.forEach(task => {
              if (!fullCrewTasksList.some(t => t.id === String(task.id))) {
                fullCrewTasksList.push({
                  id: String(task.id),
                  name: task.name,
                  agent_id: String(crewId)
                });
                console.log(`Added task: ${task.name} (ID: ${task.id}, Crew: ${crewName})`);
              }
            });
          } catch (crewError) {
            console.error(`Error loading tasks for crew ${crewId}:`, crewError);
          }
        }
        
        // Log the complete list of tasks we found
        console.log(`Total tasks found for all crews: ${fullCrewTasksList.length}`);
        
        // Now update the tasks state
        setTasks(prevTasks => {
          // Create a map of existing tasks
          const existingTasksMap = new Map(prevTasks.map(task => [task.id, task]));
          
          // Add all new tasks
          fullCrewTasksList.forEach(task => {
            if (!existingTasksMap.has(task.id)) {
              existingTasksMap.set(task.id, task);
            }
          });
          
          return Array.from(existingTasksMap.values());
        });
        
        // And update the startingPoints to include ALL tasks
        setStartingPoints(prevStartingPoints => {
          // Create a map of existing starting points
          const existingStartingPointsMap = new Map(
            prevStartingPoints.map(point => [point.taskId, point])
          );
          
          // Create starting points for any tasks not already in the list
          const newStartingPoints: StartingPoint[] = [];
          
          fullCrewTasksList.forEach(task => {
            // Skip if already in startingPoints
            if (existingStartingPointsMap.has(task.id)) {
              return;
            }
            
            // Find crew for task
            const crewId = Number(task.agent_id);
            const crew = flowCrewsMap.get(crewId) || {
              id: crewId,
              name: allAvailableCrews.find(c => String(c.id) === String(crewId))?.name || `Crew ${crewId}`
            };
            
            newStartingPoints.push({
              crewId: String(crew.id),
              taskId: task.id,
              taskName: task.name,
              crewName: crew.name,
              isStartPoint: false
            });
          });
          
          // Log how many new starting points we're adding
          console.log(`Adding ${newStartingPoints.length} new starting points`);
          
          // Return combined list
          return [...prevStartingPoints, ...newStartingPoints];
        });
      } catch (error) {
        console.error('Error fetching crew and task data:', error);
      }
    };
    
    if (open && flowId) {
      fetchAllTasksForAllSelectedCrews();
    }
  }, [crews, open, flowId]);

  // Add a new function to fetch ALL tasks from ALL available crews
  const fetchAllAvailableTasks = useCallback(async () => {
    console.log('Fetching ALL tasks from ALL available crews');
    
    try {
      // First, get all available crews
      const allAvailableCrews = await CrewService.getCrews();
      console.log(`Found ${allAvailableCrews.length} crews total in the system`);
      
      // Keep track of all tasks we find
      const allFoundTasks: { id: string; name: string; agent_id?: string }[] = [];
      
      // For each crew, get all its tasks
      for (const crew of allAvailableCrews) {
        try {
          const crewTasks = await CrewService.getTasks(String(crew.id));
          console.log(`Crew ${crew.name} (ID: ${crew.id}) has ${crewTasks.length} tasks`);
          
          // Add each task to our master list
          crewTasks.forEach(task => {
            if (!allFoundTasks.some(t => t.id === String(task.id))) {
              allFoundTasks.push({
                id: String(task.id),
                name: task.name,
                agent_id: String(crew.id)
              });
            }
          });
        } catch (err) {
          console.error(`Error getting tasks for crew ${crew.name} (ID: ${crew.id}):`, err);
        }
      }
      
      console.log(`Total unique tasks found across all crews: ${allFoundTasks.length}`);
      
      // Now update our application state
      
      // First, add any missing tasks to the tasks list
      setTasks(prevTasks => {
        const existingTasksMap = new Map(prevTasks.map(task => [task.id, task]));
        
        allFoundTasks.forEach(task => {
          if (!existingTasksMap.has(task.id)) {
            existingTasksMap.set(task.id, task);
          }
        });
        
        return Array.from(existingTasksMap.values());
      });
      
      // Then, update starting points
      setStartingPoints(prevStartingPoints => {
        const existingStartingPointsMap = new Map(
          prevStartingPoints.map(point => [point.taskId, point])
        );
        
        // For each task, if it belongs to a crew in our flow, add it as a starting point
        const newStartingPoints: StartingPoint[] = [];
        const flowCrewIds = crews.map(c => String(c.id));
        
        allFoundTasks.forEach(task => {
          // Skip if already in startingPoints
          if (existingStartingPointsMap.has(task.id)) {
            return;
          }
          
          // Check if this task belongs to a crew in our flow
          const taskCrewId = String(task.agent_id);
          if (flowCrewIds.includes(taskCrewId)) {
            const crew = crews.find(c => String(c.id) === taskCrewId);
            if (crew) {
              newStartingPoints.push({
                crewId: String(crew.id),
                taskId: task.id,
                taskName: task.name,
                crewName: crew.name,
                isStartPoint: false
              });
            }
          }
        });
        
        console.log(`Adding ${newStartingPoints.length} new starting points`);
        return [...prevStartingPoints, ...newStartingPoints];
      });
      
      return true;
    } catch (error) {
      console.error('Error fetching all available tasks:', error);
      return false;
    }
  }, [crews]);

  // Call fetchAllAvailableTasks after initial flow load:
  useEffect(() => {
    if (flowId && crews.length > 0 && open) {
      // After initial load, fetch all available tasks to make sure we have everything
      fetchAllAvailableTasks().then(success => {
        if (success) {
          console.log("Successfully loaded all available tasks after initial flow load");
        } else {
          console.error("Failed to load all available tasks after initial flow load");
        }
      });
    }
  }, [flowId, crews.length, open, fetchAllAvailableTasks]);

  const resetForm = () => {
    setFlow(null);
    setFlowName('');
    setError(null);
    setListeners([]);
    setStartingPoints([]);
    setActions([]);
    setCurrentListenerIndex(-1);
    setTabValue(0);
  };

  const loadFlow = async (id: number | string) => {
    setLoading(true);
    setError(null);
    try {
      // Convert number ID to string if needed
      const stringId = id.toString();
      
      // Fetch the flow data
      const flowData = await FlowService.getFlow(stringId);
      
      if (!flowData) {
        throw new Error('Failed to load flow data');
      }
      
      console.log('Loaded flow data:', flowData);
      setFlow(flowData);
      
      // Set the flow name
      setFlowName(flowData.name || '');
      
      // Extract crews and tasks from the flow data
      const crewsFromFlow: { id: number; name: string }[] = [];
      
      // Process flow_config if available
      if (flowData.flowConfig || flowData.flow_config) {
        const flowConfig = flowData.flowConfig || flowData.flow_config;
        
        // Set config-based data
        if (flowConfig?.listeners && Array.isArray(flowConfig.listeners)) {
          console.log('Setting listeners from flow config:', flowConfig.listeners);
          
          // Convert listeners to extended format for UI state management
          const extendedListeners: ExtendedListener[] = flowConfig.listeners.map(listener => {
            // Handle both formats for listenToTaskIds
            const listenToTaskIds = listener.listenToTaskIds || 
                                   ((listener as ExtendedListener).listenTo && Array.isArray((listener as ExtendedListener).listenTo) 
                                     ? (listener as ExtendedListener).listenTo as string[]
                                     : (typeof (listener as ExtendedListener).listenTo === 'string' 
                                         ? [(listener as ExtendedListener).listenTo as string] 
                                         : []));
            
            // Handle both formats for listenToTaskNames
            const listenToTaskNames = listener.listenToTaskNames || [];
            
            // Add crew to the list of crews if not already added
            if (listener.crewId && !crewsFromFlow.some(c => String(c.id) === String(listener.crewId))) {
              crewsFromFlow.push({
                id: typeof listener.crewId === 'string' ? parseInt(listener.crewId, 10) || 0 : listener.crewId,
                name: listener.crewName || `Crew ${listener.crewId}`
              });
            }
            
            return {
              ...listener,
              listenToTaskIds,
              listenToTaskNames
            };
          });
          
          setListeners(extendedListeners);
        } else {
          setListeners([]);
        }
        
        // Set starting points
        if (flowConfig?.startingPoints && Array.isArray(flowConfig.startingPoints)) {
          console.log('Setting starting points from flow config:', flowConfig.startingPoints);
          
          // Add crews from starting points to the list of crews
          flowConfig.startingPoints.forEach(sp => {
            if (sp.crewId && !crewsFromFlow.some(c => String(c.id) === String(sp.crewId))) {
              crewsFromFlow.push({
                id: typeof sp.crewId === 'string' ? parseInt(sp.crewId, 10) || 0 : sp.crewId,
                name: sp.crewName || `Crew ${sp.crewId}`
              });
            }
          });
          
          setStartingPoints(flowConfig.startingPoints);
        } else {
          setStartingPoints([]);
        }
        
        // Set actions
        if (flowConfig?.actions && Array.isArray(flowConfig.actions)) {
          console.log('Setting actions from flow config:', flowConfig.actions);
          
          // Add crews from actions to the list of crews
          flowConfig.actions.forEach(action => {
            if (action.crewId && !crewsFromFlow.some(c => String(c.id) === String(action.crewId))) {
              crewsFromFlow.push({
                id: typeof action.crewId === 'string' ? parseInt(action.crewId, 10) || 0 : action.crewId,
                name: action.crewName || `Crew ${action.crewId}`
              });
            }
          });
          
          setActions(flowConfig.actions);
        } else {
          setActions([]);
        }
      } else {
        // If no flow_config, initialize empty arrays
        setListeners([]);
        setStartingPoints([]);
        setActions([]);
      }
      
      // Set crews from flow data
      console.log('Setting crews from flow data:', crewsFromFlow);
      setCrews(crewsFromFlow);
      
      // Load tasks for all selected crews
      if (crewsFromFlow.length > 0) {
        // This will trigger the useEffect to load tasks
        console.log('Triggering task loading for selected crews');
      }
      
    } catch (error) {
      console.error('Error loading flow:', error);
      setError(error instanceof Error ? error.message : 'An error occurred while loading the flow');
    } finally {
      setLoading(false);
    }
  };

  const handleSave = async () => {
    try {
      setSaving(true);
      setError(null);
      
      // Create the flow config
      const flow_config: FlowConfiguration = {
        id: flow?.flowConfig?.id || `flow-${Date.now()}`,
        name: flowName,
        type: 'default',
        listeners: listeners.map(listener => ({
          id: listener.id,
          name: listener.name || `Listener ${listener.id}`,
          crewId: listener.crewId,
          crewName: listener.crewName,
          tasks: listener.tasks || [],
          listenToTaskIds: listener.listenToTaskIds || [],
          listenToTaskNames: listener.listenToTaskNames || [],
          conditionType: listener.conditionType || 'NONE',
          state: listener.state,
          ...(listener.conditionType === 'ROUTER' && listener.routerConfig ? {
            routerConfig: listener.routerConfig
          } : {})
        })),
        actions: actions,
        startingPoints: startingPoints.filter(sp => sp.isStartPoint)
      };
      
      // Validate node positions to ensure they are all valid
      const validatedNodes = flow?.nodes ? validateNodePositions(flow.nodes) : [];
      
      const flowData: FlowSaveData = {
        name: flowName,
        crew_id: flow?.crew_id || '', // Default to empty string if not available
        nodes: validatedNodes,
        edges: flow?.edges || [],
        flowConfig: flow_config
      };
      
      // If we have a flowId, update existing flow, otherwise create a new one
      let updatedFlow: Flow;
      if (flowId) {
        // Convert number ID to string if needed
        const stringId = flowId.toString();
        updatedFlow = await FlowService.updateFlow(stringId, flowData);
      } else {
        updatedFlow = await FlowService.saveFlow(flowData);
      }
      
      console.log('Flow saved successfully:', updatedFlow);
      setSaveSuccess(true);
      
      // Notify parent
      onSave();
      
      // Auto-close after successful save
      setTimeout(() => {
        onClose();
      }, 1000);
    } catch (error) {
      console.error('Error saving flow:', error);
      setError(error instanceof Error ? error.message : 'An unknown error occurred while saving the flow.');
    } finally {
      setSaving(false);
    }
  };

  const handleTabChange = (_event: React.SyntheticEvent, newValue: number) => {
    setTabValue(newValue);
  };

  // Listener management
  const addListener = () => {
    const newListener: ExtendedListener = {
      id: `listener-${uuidv4()}`,
      name: `Listener ${listeners.length + 1}`,
      crewId: crews.length > 0 ? String(crews[0].id) : '',
      crewName: crews.length > 0 ? crews[0].name : 'Unknown Crew',
      conditionType: 'NONE',
      tasks: [],
      state: {
        stateType: 'unstructured',
        stateDefinition: '',
        stateData: {}
      },
      listenToTaskIds: []
    };
    
    setListeners(prev => [...prev, newListener]);
    setCurrentListenerIndex(listeners.length);
  };

  const deleteListener = (id: string) => {
    setListeners(prev => prev.filter(l => l.id !== id));
    
    // Also remove any actions associated with this listener
    setActions(prev => prev.filter(action => !action.id.includes(id)));
    
    if (currentListenerIndex >= listeners.length - 1) {
      setCurrentListenerIndex(Math.max(0, listeners.length - 2));
    }
  };

  // Rename updateListenerName to _updateListenerName to indicate it's not used
  const _updateListenerName = (index: number, crewId: string) => {
    const crew = crews.find(c => String(c.id) === String(crewId));
    if (!crew) return;
    
    setListeners(prev => {
      const updated = [...prev];
      updated[index] = {
        ...updated[index],
        crewId,
        crewName: crew.name
      };
      return updated;
    });
  };

  const handleListenToTaskChange = (event: SelectChangeEvent<string[]>) => {
    if (currentListenerIndex < 0) return;
    
    const taskIds = event.target.value as string[];
    setListeners(prev => {
      const updated = [...prev];
      updated[currentListenerIndex] = {
        ...updated[currentListenerIndex],
        listenToTaskIds: taskIds,
        // Don't update tasks here, as they are separate from listenToTaskIds
        // tasks should only be the tasks that are executed when the listener is triggered
      };
      return updated;
    });
  };

  const handleConditionTypeChange = (event: SelectChangeEvent<string>) => {
    if (currentListenerIndex < 0) return;
    
    const conditionType = event.target.value as 'NONE' | 'AND' | 'OR' | 'ROUTER';
    setListeners(prev => {
      const updated = [...prev];
      updated[currentListenerIndex] = {
        ...updated[currentListenerIndex],
        conditionType
      };
      return updated;
    });
  };

  const handleStateTypeChange = (event: SelectChangeEvent<string>) => {
    if (currentListenerIndex < 0) return;
    
    const stateType = event.target.value as 'structured' | 'unstructured';
    setListeners(prev => {
      const updated = [...prev];
      updated[currentListenerIndex] = {
        ...updated[currentListenerIndex],
        state: {
          ...updated[currentListenerIndex].state,
          stateType
        }
      };
      return updated;
    });
  };

  const handleStateDefinitionChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    if (currentListenerIndex < 0) return;
    
    const stateDefinition = event.target.value;
    setListeners(prev => {
      const updated = [...prev];
      updated[currentListenerIndex] = {
        ...updated[currentListenerIndex],
        state: {
          ...updated[currentListenerIndex].state,
          stateDefinition
        }
      };
      return updated;
    });
  };

  // Starting point management
  const handleToggleStartingPoint = (taskId: string) => {
    setStartingPoints(prev => 
      prev.map(sp => 
        sp.taskId === taskId 
          ? { ...sp, isStartPoint: !sp.isStartPoint }
          : sp
      )
    );
  };

  // Add a useEffect for debug logging
  useEffect(() => {
    if (tabValue === 3) {
      console.log('Rendering Starting Points tab with:', { 
        totalTasks: tasks.length,
        totalStartingPoints: startingPoints.length,
        crewCount: crews.length,
        uniqueCrewNames: Array.from(new Set(startingPoints.map(point => point.crewName))).length,
        selectedCount: startingPoints.filter(sp => sp.isStartPoint).length
      });
    }
  }, [tabValue, tasks.length, startingPoints, crews.length]);

  return (
    <>
      <Dialog 
        open={open} 
        onClose={onClose}
        maxWidth="md"
        fullWidth
        PaperProps={{
          sx: { height: '80vh' }
        }}
      >
        <DialogTitle sx={{ borderBottom: '1px solid', borderColor: 'divider' }}>
          Edit Flow
          <IconButton
            aria-label="close"
            onClick={onClose}
            sx={{
              position: 'absolute',
              right: 8,
              top: 8
            }}
          >
            <CloseIcon />
          </IconButton>
        </DialogTitle>
        
        <Box sx={{ borderBottom: 1, borderColor: 'divider', px: 2 }}>
          <Tabs value={tabValue} onChange={handleTabChange} aria-label="flow edit tabs">
            <Tab label="Basic Info" id="flow-tab-0" aria-controls="flow-tabpanel-0" />
            <Tab label="Crews" id="flow-tab-1" aria-controls="flow-tabpanel-1" />
            <Tab label="Listeners" id="flow-tab-2" aria-controls="flow-tabpanel-2" />
            <Tab label="Starting Points" id="flow-tab-3" aria-controls="flow-tabpanel-3" />
          </Tabs>
        </Box>
        
        <DialogContent sx={{ p: 2, display: 'flex', flexDirection: 'column', height: 'calc(100% - 120px)' }}>
          {loading ? (
            <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100%' }}>
              <CircularProgress />
            </Box>
          ) : error ? (
            <Alert severity="error" sx={{ mt: 2 }}>
              {error}
            </Alert>
          ) : flow ? (
            <>
              <TabPanel value={tabValue} index={0}>
                <Paper
                  variant="outlined"
                  sx={{
                    p: 3,
                    border: '1px solid',
                    borderColor: 'primary.light',
                    borderRadius: 1,
                  }}
                >
                  <Typography variant="h6" gutterBottom color="primary">
                    Basic Information
                  </Typography>
                  <TextField
                    label="Flow Name"
                    fullWidth
                    value={flowName}
                    onChange={(e) => setFlowName(e.target.value)}
                    margin="normal"
                    variant="outlined"
                    required
                    error={!flowName.trim()}
                    helperText={!flowName.trim() && "Flow name is required"}
                  />
                  
                  <Typography variant="body2" color="text.secondary" sx={{ mt: 2 }}>
                    ID: {flow.id}
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                    Created: {new Date(flow.created_at).toLocaleString()}
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                    Last Updated: {new Date(flow.updated_at).toLocaleString()}
                  </Typography>
                </Paper>
              </TabPanel>

              <TabPanel value={tabValue} index={1}>
                <Typography variant="body2" color="text.secondary" paragraph>
                  Manage the crews included in this flow. You can add or remove crews to modify which components are available in the flow.
                </Typography>

                <Box sx={{ display: 'flex', height: 'calc(100% - 50px)' }}>
                  {/* Current Crews Section */}
                  <Paper variant="outlined" sx={{ width: '50%', p: 2, mr: 2, height: '100%', overflow: 'auto' }}>
                    <Typography variant="subtitle2" gutterBottom fontWeight="medium">
                      Current Crews in Flow
                    </Typography>
                    
                    {crews.length === 0 ? (
                      <Typography variant="body2" sx={{ my: 2, textAlign: 'center', color: 'text.secondary' }}>
                        No crews in this flow. Add crews from the list on the right.
                      </Typography>
                    ) : (
                      <List sx={{ p: 0 }}>
                        {crews.map(crew => (
                          <Paper
                            key={crew.id}
                            elevation={0}
                            sx={{
                              p: 2,
                              mb: 1,
                              border: '1px solid',
                              borderColor: 'divider',
                              borderRadius: 1,
                              display: 'flex',
                              justifyContent: 'space-between',
                              alignItems: 'center'
                            }}
                          >
                            <Typography variant="body1">
                              {crew.name}
                            </Typography>
                            <IconButton 
                              size="small" 
                              color="error"
                              onClick={() => {
                                // Remove crew
                                setCrews(prevCrews => prevCrews.filter(c => c.id !== crew.id));
                              }}
                            >
                              <DeleteIcon fontSize="small" />
                            </IconButton>
                          </Paper>
                        ))}
                      </List>
                    )}
                  </Paper>

                  {/* Available Crews Section */}
                  <Paper variant="outlined" sx={{ width: '50%', p: 2, height: '100%', overflow: 'auto' }}>
                    <Typography variant="subtitle2" gutterBottom fontWeight="medium">
                      Available Crews
                    </Typography>
                    
                    <TextField
                      placeholder="Search crews..."
                      size="small"
                      fullWidth
                      sx={{ mb: 2 }}
                      value={crewSearchQuery}
                      onChange={(e) => setCrewSearchQuery(e.target.value)}
                      InputProps={{
                        startAdornment: (
                          <InputAdornment position="start">
                            <SearchIcon fontSize="small" />
                          </InputAdornment>
                        ),
                      }}
                    />
                    
                    <List sx={{ p: 0 }}>
                      {_allCrews
                        .filter(crew => 
                          crew.name.toLowerCase().includes(crewSearchQuery.toLowerCase()) &&
                          !crews.some(c => c.id === crew.id)
                        )
                        .map(crew => (
                          <Paper
                            key={crew.id}
                            elevation={0}
                            sx={{
                              p: 2,
                              mb: 1,
                              border: '1px solid',
                              borderColor: 'divider',
                              borderRadius: 1,
                              display: 'flex',
                              justifyContent: 'space-between',
                              alignItems: 'center'
                            }}
                          >
                            <Typography variant="body1">
                              {crew.name}
                            </Typography>
                            <IconButton 
                              size="small" 
                              color="primary"
                              onClick={() => {
                                setCrews(prevCrews => [...prevCrews, crew]);
                              }}
                            >
                              <AddIcon fontSize="small" />
                            </IconButton>
                          </Paper>
                        ))}
                      {_allCrews.filter(crew => 
                        crew.name.toLowerCase().includes(crewSearchQuery.toLowerCase()) &&
                        !crews.some(c => c.id === crew.id)
                      ).length === 0 && (
                        <Typography variant="body2" sx={{ textAlign: 'center', color: 'text.secondary', p: 2 }}>
                          No matching crews found
                        </Typography>
                      )}
                    </List>
                  </Paper>
                </Box>
              </TabPanel>
              
              <TabPanel value={tabValue} index={2}>
                <Typography variant="body2" color="text.secondary" paragraph>
                  Configure listeners for your flow. Listeners respond to task outputs and can trigger actions in the flow.
                  Each listener needs to know which tasks to listen to (Listen To Tasks) and which tasks to execute when triggered (Tasks To Execute).
                </Typography>
                
                <Box sx={{ display: 'flex', mb: 2, gap: 1 }}>
                  <Button
                    startIcon={<AddIcon />}
                    variant="outlined"
                    onClick={addListener}
                  >
                    Add Listener
                  </Button>
                  
                  <Button
                    variant="outlined"
                    size="small"
                    onClick={() => {
                      console.log("Current flow structure:");
                      console.log("Listeners:", listeners);
                      console.log("Actions:", actions);
                      console.log("Starting Points:", startingPoints.filter(sp => sp.isStartPoint));
                      
                      // Show a brief message to the user
                      alert(`Debug info logged to console.\nListeners: ${listeners.length}\nActions: ${actions.length}\nStarting Points: ${startingPoints.filter(sp => sp.isStartPoint).length}`);
                    }}
                  >
                    Debug Flow
                  </Button>
                </Box>
                
                {listeners.length === 0 ? (
                  <Typography variant="body2" sx={{ my: 2, textAlign: 'center' }}>
                    No listeners configured. Click the button above to add a listener.
                  </Typography>
                ) : (
                  <Box sx={{ display: 'flex', height: 'calc(100% - 80px)' }}>
                    <Paper variant="outlined" sx={{ width: '30%', p: 2, mr: 2, height: '100%', overflow: 'auto' }}>
                      <Typography variant="subtitle2" gutterBottom fontWeight="medium">
                        Listeners
                      </Typography>
                      {listeners.map((listener, index) => (
                        <Box 
                          key={listener.id}
                          onClick={() => setCurrentListenerIndex(index)}
                          sx={{
                            p: 1.5,
                            mb: 1,
                            borderRadius: 1,
                            cursor: 'pointer',
                            bgcolor: currentListenerIndex === index ? 'primary.light' : 'grey.100',
                            color: currentListenerIndex === index ? 'primary.contrastText' : 'text.primary',
                            '&:hover': {
                              bgcolor: currentListenerIndex === index ? 'primary.light' : 'grey.200'
                            },
                            position: 'relative'
                          }}
                        >
                          <Typography variant="body2" fontWeight={currentListenerIndex === index ? 'bold' : 'normal'}>
                            {listener.crewName}
                          </Typography>
                          <Typography variant="caption" color={currentListenerIndex === index ? 'primary.contrastText' : 'text.secondary'} display="block">
                            Listen To: {listener.listenToTaskIds && listener.listenToTaskIds.length > 0 
                              ? listener.listenToTaskIds.map(id => {
                                  const task = tasks.find(t => String(t.id) === id);
                                  const crew = crews.find(c => String(c.id) === String(task?.agent_id));
                                  const taskName = task?.name || `Task ${id}`;
                                  const crewName = crew?.name || (task?.agent_id ? `Crew ${task?.agent_id}` : 'System');
                                  return `${crewName}:${taskName}`;
                                }).join(', ')
                              : 'Not set'}
                          </Typography>
                          <Typography variant="caption" color={currentListenerIndex === index ? 'primary.contrastText' : 'text.secondary'} display="block">
                            Tasks: {listener.tasks.length > 0 
                              ? listener.tasks.map(task => {
                                  const displayTask = tasks.find(t => t.id === task.id);
                                  const crew = crews.find(c => String(c.id) === String(task.agent_id || displayTask?.agent_id));
                                  const taskName = task.name || displayTask?.name || `Task ${task.id}`;
                                  const crewName = crew?.name || (task.agent_id ? `Crew ${task.agent_id}` : '');
                                  return crewName ? `${crewName}:${taskName}` : taskName;
                                }).join(', ')
                              : 'None'}
                          </Typography>
                          <Typography variant="caption" color={currentListenerIndex === index ? 'primary.contrastText' : 'text.secondary'} display="block">
                            {listener.conditionType || 'No Condition'}
                          </Typography>
                          
                          <IconButton 
                            size="small" 
                            sx={{ 
                              position: 'absolute', 
                              top: 4, 
                              right: 4,
                              color: currentListenerIndex === index ? 'primary.contrastText' : 'error.main',
                            }}
                            onClick={(e) => {
                              e.stopPropagation();
                              deleteListener(listener.id);
                            }}
                          >
                            <DeleteIcon fontSize="small" />
                          </IconButton>
                        </Box>
                      ))}
                    </Paper>
                    
                    {currentListenerIndex >= 0 && currentListenerIndex < listeners.length ? (
                      <Paper variant="outlined" sx={{ width: '70%', p: 2, height: '100%', overflow: 'auto' }}>
                        <Typography variant="subtitle2" gutterBottom fontWeight="medium">
                          Configure Listener
                        </Typography>
                        
                        <FormControl fullWidth margin="normal">
                          <InputLabel id="listen-to-task-label">Listen To Tasks</InputLabel>
                          <Select
                            labelId="listen-to-task-label"
                            multiple
                            value={listeners[currentListenerIndex]?.listenToTaskIds || []}
                            onChange={handleListenToTaskChange}
                            label="Listen To Tasks"
                            renderValue={(selected) => (
                              <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                                {(selected as string[]).map((taskId) => {
                                  const task = tasks.find(t => t.id === taskId);
                                  const crew = crews.find(c => c.id.toString() === task?.agent_id);
                                  return (
                                    <Chip key={taskId} label={`${crew?.name || (task?.agent_id ? `Crew ${task?.agent_id}` : 'System')}:${task?.name || `Task ${taskId}`}`} />
                                  );
                                })}
                              </Box>
                            )}
                          >
                            {tasks.filter(task => !String(task.id).startsWith('agent-')).map((task) => {
                              const crew = crews.find(c => String(c.id) === task.agent_id);
                              return (
                                <MenuItem key={task.id} value={task.id}>
                                  <Checkbox checked={(listeners[currentListenerIndex]?.listenToTaskIds || []).includes(task.id)} />
                                  <ListItemText 
                                    primary={`${crew?.name || (task?.agent_id ? `Crew ${task?.agent_id}` : 'System')}:${task.name}`}
                                    secondary={`ID: ${task.id} (Task)`}
                                  />
                                </MenuItem>
                              );
                            })}
                          </Select>
                        </FormControl>
                        
                        <FormControl fullWidth margin="normal">
                          <InputLabel id="condition-type-label">Condition Type</InputLabel>
                          <Select
                            labelId="condition-type-label"
                            value={listeners[currentListenerIndex]?.conditionType || 'NONE'}
                            onChange={handleConditionTypeChange}
                            label="Condition Type"
                          >
                            <MenuItem value="NONE">
                              <ListItemText 
                                primary="NONE" 
                                secondary="No condition, always trigger when task completes" 
                              />
                            </MenuItem>
                            <MenuItem value="AND">
                              <ListItemText 
                                primary="AND" 
                                secondary="Trigger when ALL listened tasks complete" 
                              />
                            </MenuItem>
                            <MenuItem value="OR">
                              <ListItemText 
                                primary="OR" 
                                secondary="Trigger when ANY listened task completes" 
                              />
                            </MenuItem>
                            <MenuItem value="ROUTER">
                              <ListItemText 
                                primary="ROUTER" 
                                secondary="Route based on task output conditions" 
                              />
                            </MenuItem>
                          </Select>
                        </FormControl>
                        
                        <FormControl fullWidth margin="normal">
                          <InputLabel id="listener-tasks-label">Tasks To Execute</InputLabel>
                          <Select
                            labelId="listener-tasks-label"
                            multiple
                            value={listeners[currentListenerIndex]?.tasks.map(task => task.id) || []}
                            onChange={(event) => {
                              const selectedTaskIds = event.target.value as string[];
                              setListeners(prev => {
                                const updated = [...prev];
                                if (currentListenerIndex >= 0 && currentListenerIndex < updated.length) {
                                  // Create properly typed tasks
                                  const listenerTasks: CrewTask[] = selectedTaskIds.map(id => {
                                    const task = tasks.find(t => t.id === id) as CrewTask | undefined;
                                    return {
                                      id,
                                      name: task?.name || `Task ${id}`,
                                      agent_id: task?.agent_id || '', // Ensure agent_id is always a string
                                      description: '',
                                      expected_output: '',
                                      context: task?.context || []
                                    } as CrewTask;
                                  });
                                  
                                  // Update tasks based on selected IDs
                                  updated[currentListenerIndex] = {
                                    ...updated[currentListenerIndex],
                                    tasks: listenerTasks
                                  };
                                }
                                return updated;
                              });
                              
                              // Also update the actions array based on all selected tasks
                              const updatedActions: Action[] = [];
                              
                              // First create a map of all listeners to get their crewIds
                              const listenerMap = new Map<string, ExtendedListener>();
                              listeners.forEach(listener => {
                                listenerMap.set(listener.id, listener);
                              });
                              
                              // Go through each listener and create actions for all their tasks
                              listeners.forEach((listener, index) => {
                                // Use the updated tasks list for the current listener
                                const tasksToUse = index === currentListenerIndex 
                                  ? selectedTaskIds.map(id => {
                                      const task = tasks.find(t => t.id === id) as CrewTask | undefined;
                                      return {
                                        id,
                                        name: task?.name || `Task ${id}`,
                                        agent_id: task?.agent_id || '', // Ensure agent_id is always a string
                                        description: '',
                                        expected_output: '',
                                        context: task?.context || []
                                      } as CrewTask;
                                    })
                                  : listener.tasks;
                                  
                                tasksToUse.forEach(task => {
                                  // Find the crew for this task by agent_id, or use the listener's crew
                                  let crewId = listener.crewId;
                                  let crewName = listener.crewName;
                                  
                                  if (task.agent_id) {
                                    const crew = crews.find(c => String(c.id) === task.agent_id);
                                    if (crew) {
                                      crewId = String(crew.id);
                                      crewName = crew.name;
                                    }
                                  }
                                  
                                  // Create a unique action ID
                                  const actionId = `action-${listener.id}-${task.id}`;
                                  
                                  updatedActions.push({
                                    id: actionId,
                                    crewId,
                                    crewName,
                                    taskId: task.id,
                                    taskName: task.name || `Task ${task.id}`
                                  });
                                });
                              });
                              
                              // Update the actions state
                              setActions(updatedActions);
                            }}
                            label="Tasks To Execute"
                            renderValue={(selected) => (
                              <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                                {(selected as string[]).map((taskId) => {
                                  const task = tasks.find(t => t.id === taskId);
                                  const crew = crews.find(c => c.id.toString() === task?.agent_id);
                                  return (
                                    <Chip key={taskId} label={`${crew?.name || (task?.agent_id ? `Crew ${task?.agent_id}` : 'System')}:${task?.name || `Task ${taskId}`}`} />
                                  );
                                })}
                              </Box>
                            )}
                          >
                            {tasks.filter(task => !String(task.id).startsWith('agent-')).map((task) => {
                              const crew = crews.find(c => String(c.id) === task.agent_id);
                              return (
                                <MenuItem key={task.id} value={task.id}>
                                  <Checkbox checked={(listeners[currentListenerIndex]?.tasks.map(t => t.id) || []).includes(task.id)} />
                                  <ListItemText 
                                    primary={`${crew?.name || (task?.agent_id ? `Crew ${task?.agent_id}` : 'System')}:${task.name}`}
                                    secondary={`ID: ${task.id} (Task)`}
                                  />
                                </MenuItem>
                              );
                            })}
                          </Select>
                        </FormControl>
                        
                        <Divider sx={{ my: 3 }} />
                        
                        <Typography variant="subtitle2" gutterBottom>
                          State Configuration
                        </Typography>
                        
                        <FormControl fullWidth margin="normal">
                          <InputLabel id="state-type-label">State Type</InputLabel>
                          <Select
                            labelId="state-type-label"
                            value={listeners[currentListenerIndex]?.state?.stateType || 'unstructured'}
                            onChange={handleStateTypeChange}
                            label="State Type"
                          >
                            <MenuItem value="unstructured">
                              <ListItemText 
                                primary="Unstructured" 
                                secondary="Free-form text state" 
                              />
                            </MenuItem>
                            <MenuItem value="structured">
                              <ListItemText 
                                primary="Structured" 
                                secondary="JSON-formatted state" 
                              />
                            </MenuItem>
                          </Select>
                        </FormControl>
                        
                        <TextField
                          label="State Definition"
                          fullWidth
                          multiline
                          rows={4}
                          margin="normal"
                          value={listeners[currentListenerIndex]?.state?.stateDefinition || ''}
                          onChange={handleStateDefinitionChange}
                          placeholder={
                            listeners[currentListenerIndex]?.state?.stateType === 'structured'
                              ? '{"key": "value"}'
                              : 'Enter state description here...'
                          }
                          InputProps={{
                            sx: { fontFamily: 'monospace' }
                          }}
                        />
                      </Paper>
                    ) : (
                      <Paper variant="outlined" sx={{ width: '70%', p: 2, height: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                        <Typography color="text.secondary">
                          Select a listener from the list or add a new one
                        </Typography>
                      </Paper>
                    )}
                  </Box>
                )}
              </TabPanel>
              
              <TabPanel value={tabValue} index={3}>
                <Typography variant="body2" color="text.secondary" paragraph>
                  Configure starting points for your flow. Starting points are specific tasks that will trigger the workflow execution.
                </Typography>
                
                {/* Debug button to log current state */}
                <Button 
                  variant="outlined" 
                  size="small" 
                  sx={{ mb: 2 }}
                  onClick={async () => {
                    const success = await fetchAllAvailableTasks();
                    if (success) {
                      console.log("Successfully refreshed all tasks");
                    } else {
                      console.error("Failed to refresh all tasks");
                    }
                  }}
                >
                  Refresh Tasks
                </Button>
                
                {startingPoints.length === 0 ? (
                  <Typography variant="body1" align="center" sx={{ my: 4 }}>
                    No tasks available to set as starting points
                  </Typography>
                ) : (
                  <Paper variant="outlined" sx={{ p: 2 }}>
                    <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
                      <Typography variant="subtitle2" color="primary">
                        Select tasks to serve as starting points
                      </Typography>
                      <Box>
                        <Typography variant="caption" sx={{ ml: 1 }}>
                          {startingPoints.length} total tasks across {Array.from(new Set(startingPoints.map(p => p.crewName))).length} crews
                        </Typography>
                      </Box>
                    </Box>
                    
                    <TextField
                      placeholder="Search tasks..."
                      size="small"
                      fullWidth
                      sx={{ mb: 2 }}
                      InputProps={{
                        startAdornment: (
                          <InputAdornment position="start">
                            <SearchIcon fontSize="small" />
                          </InputAdornment>
                        ),
                      }}
                      onChange={(e) => {
                        // Implement a temporary search filter
                        const searchValue = e.target.value.toLowerCase();
                        
                        // Find all list items and hide/show based on search
                        const taskElements = document.querySelectorAll('[data-task-item]');
                        taskElements.forEach((element) => {
                          const taskElement = element as HTMLElement;
                          const taskName = taskElement.getAttribute('data-task-name')?.toLowerCase() || '';
                          const crewName = taskElement.getAttribute('data-crew-name')?.toLowerCase() || '';
                          
                          if (taskName.includes(searchValue) || crewName.includes(searchValue)) {
                            taskElement.style.display = '';
                          } else {
                            taskElement.style.display = 'none';
                          }
                        });
                      }}
                    />
                    
                    <Box sx={{ display: 'flex', mb: 2 }}>
                      <Typography variant="caption" color="text.secondary" sx={{ mr: 2 }}>
                        Total: {startingPoints.length} tasks
                      </Typography>
                      <Typography variant="caption" color="success.main" sx={{ mr: 2 }}>
                        Selected: {startingPoints.filter(sp => sp.isStartPoint).length} tasks
                      </Typography>
                    </Box>
                    
                    <List>
                      {/* Group starting points by crew for better organization */}
                      {Array.from(new Set(startingPoints.map(point => point.crewName)))
                        .sort((a, b) => a.localeCompare(b))
                        .map((crewName: string) => {
                          const crewTasks = startingPoints.filter(point => point.crewName === crewName)
                            .sort((a, b) => a.taskName.localeCompare(b.taskName));
                          return (
                            <Box key={crewName} sx={{ mb: 2 }}>
                              <Typography variant="subtitle2" sx={{ 
                                bgcolor: 'primary.light', 
                                color: 'primary.contrastText',
                                px: 2,
                                py: 1,
                                borderRadius: '4px 4px 0 0',
                              }}>
                                {crewName} ({crewTasks.length} tasks)
                              </Typography>
                              <Paper variant="outlined" sx={{ borderRadius: '0 0 4px 4px', mt: '-1px' }}>
                                {crewTasks.map((point) => (
                                  <ListItem 
                                    key={point.taskId}
                                    sx={{
                                      borderBottom: '1px solid',
                                      borderColor: 'divider',
                                      py: 1
                                    }}
                                    data-task-item="true"
                                    data-task-name={point.taskName}
                                    data-crew-name={point.crewName}
                                    secondaryAction={
                                      <Checkbox
                                        edge="end"
                                        checked={!!point.isStartPoint}
                                        onChange={() => handleToggleStartingPoint(point.taskId)}
                                        inputProps={{ 'aria-labelledby': `task-${point.taskId}` }}
                                      />
                                    }
                                  >
                                    <ListItemText
                                      id={`task-${point.taskId}`}
                                      primary={
                                        <Box sx={{ display: 'flex', alignItems: 'center' }}>
                                          <PlayArrowIcon 
                                            fontSize="small" 
                                            sx={{ 
                                              mr: 1, 
                                              color: point.isStartPoint ? 'success.main' : 'text.disabled'
                                            }} 
                                          />
                                          <Typography 
                                            variant="body1"
                                            sx={{
                                              fontWeight: point.isStartPoint ? 500 : 400
                                            }}
                                          >
                                            {point.taskName}
                                          </Typography>
                                        </Box>
                                      }
                                      secondary={`ID: ${point.taskId}`}
                                    />
                                  </ListItem>
                                ))}
                              </Paper>
                            </Box>
                          );
                        })}
                    </List>
                  </Paper>
                )}
              </TabPanel>
            </>
          ) : (
            <Typography sx={{ textAlign: 'center', py: 4 }}>No flow selected</Typography>
          )}
        </DialogContent>
        
        <DialogActions sx={{ p: 2, borderTop: '1px solid', borderColor: 'divider' }}>
          <Box sx={{ display: 'flex', justifyContent: 'space-between', width: '100%', alignItems: 'center' }}>
            <Box>
              {error && (
                <Typography color="error" variant="body2">
                  {error}
                </Typography>
              )}
            </Box>
            
            <Box sx={{ display: 'flex', gap: 1 }}>
              <Button onClick={onClose} color="inherit" disabled={saving}>
                Cancel
              </Button>
              <Button 
                onClick={handleSave} 
                variant="contained" 
                color="primary"
                disabled={saving || !flow || !flowName.trim()}
              >
                {saving ? <CircularProgress size={24} /> : 'Save'}
              </Button>
            </Box>
          </Box>
        </DialogActions>
      </Dialog>
      
      {/* Success snackbar */}
      <Snackbar 
        open={saveSuccess} 
        autoHideDuration={6000} 
        onClose={() => setSaveSuccess(false)}
      >
        <Alert 
          onClose={() => setSaveSuccess(false)} 
          severity="success" 
          sx={{ width: '100%' }}
        >
          Flow updated successfully!
        </Alert>
      </Snackbar>
    </>
  );
};

export default EditFlowForm; 