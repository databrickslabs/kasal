import React, { useState, useEffect } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Box,
  CircularProgress,
  Alert,
  Typography,
  Tabs,
  Tab,
  Paper
} from '@mui/material';
import { useTranslation } from 'react-i18next';
import * as yaml from 'yaml';
import { Node, Edge } from 'reactflow';
import { toast } from 'react-hot-toast';
import { AgentService } from '../../api/AgentService';
import { TaskService } from '../../api/TaskService';
import { AgentYaml, TaskYaml } from '../../types/crew';
import { createEdge, edgeExists } from '../../utils/edgeUtils';

// Style for JSON formatting
const jsonStyles = {
  string: { color: '#008000' },
  number: { color: '#0000ff' },
  boolean: { color: '#b22222' },
  null: { color: '#808080' },
  key: { color: '#a52a2a' },
  bracket: { color: '#000000' }
};

interface LoadCrewProps {
  open: boolean;
  onClose: () => void;
  onCrewLoad: (nodes: Node[], edges: Edge[]) => void;
  inputs: {
    agents_yaml?: string;
    tasks_yaml?: string;
  };
  runName: string;
}

/**
 * Organizes node positions in a clean layout with agents on the left and tasks on the right
 */
const organizeNodesPositions = (nodes: Node[], edges: Edge[]): Node[] => {
  // Canvas dimensions for positioning
  const _canvasWidth = 1200;
  const canvasHeight = 1000; // Increased height for more vertical space
  
  // Separate agents and tasks
  const agentNodes = nodes.filter(node => node.type === 'agentNode');
  const taskNodes = nodes.filter(node => node.type === 'taskNode');
  
  // Fixed positioning for better alignment
  const leftSideWidth = 250; // Fixed position for agents (left side)
  const columnPositions = [550, 850, 1150]; // Fixed positions for task columns
  
  // Spacing between nodes - ensure adequate vertical spacing
  const agentYSpacing = Math.min(180, (canvasHeight - 200) / Math.max(1, agentNodes.length));
  
  // Position agent nodes on the left side with equal spacing
  agentNodes.forEach((node, index) => {
    const yPosition = 150 + (index * agentYSpacing);
    node.position = {
      x: leftSideWidth,
      y: yPosition
    };
  });
  
  // Create a map of tasks connected to each agent
  const agentTaskMap: Record<string, string[]> = {};
  
  // Initialize the map
  agentNodes.forEach(node => {
    agentTaskMap[node.id] = [];
  });
  
  // Find agent-to-task connections
  edges.forEach(edge => {
    if (edge.source.startsWith('agent-') && edge.target.startsWith('task-')) {
      if (!agentTaskMap[edge.source]) {
        agentTaskMap[edge.source] = [];
      }
      agentTaskMap[edge.source].push(edge.target);
    }
  });
  
  // Find task-to-task dependencies
  const taskIncomingDeps: Record<string, string[]> = {};
  const taskOutgoingDeps: Record<string, string[]> = {};
  
  // Initialize task dependency maps
  taskNodes.forEach(node => {
    taskIncomingDeps[node.id] = [];
    taskOutgoingDeps[node.id] = [];
  });
  
  // Populate dependency maps
  edges.forEach(edge => {
    if (edge.source.startsWith('task-') && edge.target.startsWith('task-')) {
      if (!taskOutgoingDeps[edge.source]) {
        taskOutgoingDeps[edge.source] = [];
      }
      taskOutgoingDeps[edge.source].push(edge.target);
      
      if (!taskIncomingDeps[edge.target]) {
        taskIncomingDeps[edge.target] = [];
      }
      taskIncomingDeps[edge.target].push(edge.source);
    }
  });
  
  // Find root tasks (tasks with no incoming dependencies)
  const rootTasks = taskNodes.filter(node => taskIncomingDeps[node.id].length === 0);
  
  // Function to organize tasks in a topological sort
  const organizeTasksByDependencies = () => {
    // Create layers for tasks based on dependency depth
    const taskLayers: Record<string, number> = {};
    
    // Initialize with root tasks at layer 0
    rootTasks.forEach(node => {
      taskLayers[node.id] = 0;
    });
    
    // Assign layers to other tasks
    let changed = true;
    while (changed) {
      changed = false;
      
      taskNodes.forEach(node => {
        // Skip if already assigned
        if (taskLayers[node.id] !== undefined) return;
        
        // Check if all incoming dependencies have a layer assigned
        const allDepsAssigned = taskIncomingDeps[node.id].every(depId => 
          taskLayers[depId] !== undefined
        );
        
        if (allDepsAssigned && taskIncomingDeps[node.id].length > 0) {
          // Find the max layer of dependencies
          const maxDepLayer = Math.max(
            ...taskIncomingDeps[node.id].map(depId => taskLayers[depId])
          );
          
          // Assign this task to the next layer
          taskLayers[node.id] = maxDepLayer + 1;
          changed = true;
        }
      });
    }
    
    // For any tasks not assigned (might be in a cycle), assign layer 0
    taskNodes.forEach(node => {
      if (taskLayers[node.id] === undefined) {
        taskLayers[node.id] = 0;
      }
    });
    
    return taskLayers;
  };
  
  const taskLayers = organizeTasksByDependencies();
  
  // Get the maximum layer
  const maxLayer = Math.max(...Object.values(taskLayers), 0);
  
  // Ensure we have enough column positions
  while (columnPositions.length <= maxLayer) {
    // Add more column positions if needed
    const lastPosition = columnPositions[columnPositions.length - 1];
    columnPositions.push(lastPosition + 300);
  }
  
  // Count tasks per layer for better spacing calculations
  const tasksPerLayer: Record<number, number> = {};
  for (let i = 0; i <= maxLayer; i++) {
    tasksPerLayer[i] = 0;
  }
  
  // Count tasks in each layer
  Object.values(taskLayers).forEach(layer => {
    tasksPerLayer[layer] = (tasksPerLayer[layer] || 0) + 1;
  });
  
  // Calculate optimal vertical spacing for each layer
  const layerSpacings: Record<number, number> = {};
  for (let i = 0; i <= maxLayer; i++) {
    // Use more space for layers with more tasks
    const count = tasksPerLayer[i];
    if (count <= 1) {
      layerSpacings[i] = 100; // Single task in layer
    } else {
      // Distribute tasks evenly in the available height
      // Ensure minimum spacing of 120px between tasks
      layerSpacings[i] = Math.max(120, (canvasHeight - 300) / (count - 1));
    }
  }
  
  // Position tasks by layer with improved spacing
  const taskPositions: Record<string, { x: number, y: number }> = {};
  
  // First, assign initial positions by layer
  for (let layer = 0; layer <= maxLayer; layer++) {
    const tasksInLayer = Object.entries(taskLayers)
      .filter(([, l]) => l === layer)
      .map(([id]) => id);
    
    const layerX = columnPositions[layer]; // Use fixed position from column positions array
    const spacing = layerSpacings[layer];
    
    // Position tasks in this layer with proper spacing
    tasksInLayer.forEach((taskId, index) => {
      const centerOffset = ((tasksInLayer.length - 1) * spacing) / 2;
      const yPos = (canvasHeight / 2) - centerOffset + (index * spacing);
      
      taskPositions[taskId] = {
        x: layerX,
        y: yPos
      };
    });
  }
  
  // Special handling for tasks connected to agents - always in first column
  Object.entries(agentTaskMap).forEach(([agentId, taskIds]) => {
    if (taskIds.length === 0) return;
    
    const agentNode = agentNodes.find(node => node.id === agentId);
    if (!agentNode) return;
    
    // Get the agent's y position
    const agentY = agentNode.position.y;
    
    // Find root tasks connected to this agent
    const rootTasksForAgent = taskIds.filter(taskId => taskLayers[taskId] === 0);
    if (rootTasksForAgent.length === 0) return;
    
    // Calculate total height needed
    const totalHeight = (rootTasksForAgent.length - 1) * 120;
    
    // Position tasks vertically centered around the agent's y position
    rootTasksForAgent.forEach((taskId, index) => {
      const startY = agentY - (totalHeight / 2);
      
      // Update the position in our taskPositions map
      taskPositions[taskId] = {
        x: columnPositions[0], // Always use first column position
        y: startY + (index * 120)
      };
    });
  });
  
  // Apply positions to task nodes
  taskNodes.forEach(node => {
    if (taskPositions[node.id]) {
      node.position = taskPositions[node.id];
    }
  });
  
  // Final adjustment for overlapping nodes
  const resolveOverlaps = () => {
    const nodeSize = { width: 180, height: 100 }; // Approximate node dimensions
    const minDistanceX = nodeSize.width + 20;
    const minDistanceY = nodeSize.height + 20;
    let iterations = 0;
    let hasOverlap = true;
    
    // Maximum 10 iterations to prevent infinite loops
    while (hasOverlap && iterations < 10) {
      hasOverlap = false;
      iterations++;
      
      // Check each pair of task nodes for overlap
      for (let i = 0; i < taskNodes.length; i++) {
        for (let j = i + 1; j < taskNodes.length; j++) {
          const nodeA = taskNodes[i];
          const nodeB = taskNodes[j];
          
          // Skip if nodes aren't in the same column (not likely to overlap)
          if (Math.abs(nodeA.position.x - nodeB.position.x) > minDistanceX) continue;
          
          // Check vertical distance
          const verticalDistance = Math.abs(nodeA.position.y - nodeB.position.y);
          
          if (verticalDistance < minDistanceY) {
            hasOverlap = true;
            
            // Calculate adjustment needed (half for each node plus extra spacing)
            const adjustment = ((minDistanceY - verticalDistance) / 2) + 10;
            
            // Move nodes apart vertically
            if (nodeA.position.y < nodeB.position.y) {
              nodeA.position.y -= adjustment;
              nodeB.position.y += adjustment;
            } else {
              nodeA.position.y += adjustment;
              nodeB.position.y -= adjustment;
            }
          }
        }
      }
    }
    
    // Final sanity check - ensure no node is positioned off canvas
    taskNodes.forEach(node => {
      // Ensure node is within canvas boundaries
      node.position.y = Math.max(50, Math.min(canvasHeight - 50, node.position.y));
    });
  };
  
  // Run the overlap resolution
  resolveOverlaps();
  
  // Return all nodes with updated positions
  return [...agentNodes, ...taskNodes];
};

/**
 * Formats a JSON object into a pretty-printed string with syntax highlighting
 */
const formatJson = (obj: unknown): JSX.Element => {
  if (!obj) return <span style={jsonStyles.null}>null</span>;
  
  try {
    // Ensure proper JSON formatting with indentation
    const json = JSON.stringify(obj, null, 2);
    
    // Create syntax highlighting by replacing parts of the string
    const highlighted = json.replace(
      /("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+-]?\d+)?)/g,
      (match) => {
        let style: React.CSSProperties = {};
        
        if (/^"/.test(match)) {
          if (/:$/.test(match)) {
            // Key
            style = jsonStyles.key;
            // Remove quotes and colon from key
            match = match.substring(1, match.length - 2) + ':';
          } else {
            // String
            style = jsonStyles.string;
          }
        } else if (/true|false/.test(match)) {
          // Boolean
          style = jsonStyles.boolean;
        } else if (/null/.test(match)) {
          // Null
          style = jsonStyles.null;
        } else {
          // Number
          style = jsonStyles.number;
        }
        
        return `<span style="color:${style.color}">${match}</span>`;
      }
    );
    
    // Add bracket coloring
    const bracketColored = highlighted.replace(
      /[{}[\]]/g,
      (match) => `<span style="color:${jsonStyles.bracket.color}">${match}</span>`
    );
    
    // Preserve whitespace and line breaks
    return (
      <div 
        dangerouslySetInnerHTML={{ __html: bracketColored }} 
        style={{ 
          whiteSpace: 'pre-wrap',
          wordBreak: 'break-word',
          lineHeight: 1.5
        }} 
      />
    );
  } catch (error) {
    console.error('Error formatting JSON:', error);
    return <div>Error formatting JSON</div>;
  }
};

const LoadCrew: React.FC<LoadCrewProps> = ({ open, onClose, onCrewLoad, inputs, runName }) => {
  const { t } = useTranslation();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [tabValue, setTabValue] = useState(0);
  const [agentsYaml, setAgentsYaml] = useState<Record<string, AgentYaml> | null>(null);
  const [tasksYaml, setTasksYaml] = useState<Record<string, TaskYaml> | null>(null);

  // Parse YAML inputs when the component mounts or inputs change
  useEffect(() => {
    console.log('LoadCrew received inputs:', inputs);
    
    try {
      // First check if we have valid input data
      if (!inputs.agents_yaml || !inputs.tasks_yaml) {
        setError('Missing required configuration data. This run may not contain YAML information.');
        return;
      }
      
      // Parse agents_yaml
      let agentsData: Record<string, AgentYaml>;
      if (typeof inputs.agents_yaml === 'object') {
        // Already an object
        agentsData = inputs.agents_yaml as Record<string, AgentYaml>;
      } else if (typeof inputs.agents_yaml === 'string') {
        // Parse string to object
        try {
          // Try JSON parse
          agentsData = JSON.parse(inputs.agents_yaml);
        } catch (e) {
          // Try YAML parse
          agentsData = yaml.parse(inputs.agents_yaml);
        }
      } else {
        throw new Error('Invalid agents_yaml format');
      }
      
      // Parse tasks_yaml
      let tasksData: Record<string, TaskYaml>;
      if (typeof inputs.tasks_yaml === 'object') {
        // Already an object
        tasksData = inputs.tasks_yaml as Record<string, TaskYaml>;
      } else if (typeof inputs.tasks_yaml === 'string') {
        // Parse string to object
        try {
          // Try JSON parse
          tasksData = JSON.parse(inputs.tasks_yaml);
        } catch (e) {
          // Try YAML parse
          tasksData = yaml.parse(inputs.tasks_yaml);
        }
      } else {
        throw new Error('Invalid tasks_yaml format');
      }
      
      // Validate the data
      if (!agentsData || Object.keys(agentsData).length === 0) {
        throw new Error('No agents found in configuration');
      }
      
      if (!tasksData || Object.keys(tasksData).length === 0) {
        throw new Error('No tasks found in configuration');
      }
      
      console.log('Successfully parsed configuration:');
      console.log('- Agents:', Object.keys(agentsData).length);
      console.log('- Tasks:', Object.keys(tasksData).length);
      
      // Set state
      setAgentsYaml(agentsData);
      setTasksYaml(tasksData);
      setError(null);
    } catch (err) {
      console.error('Error parsing configuration:', err);
      setError('Failed to parse configuration: ' + (err instanceof Error ? err.message : String(err)));
    }
  }, [inputs]);

  const handleTabChange = (event: React.SyntheticEvent, newValue: number) => {
    setTabValue(newValue);
  };

  const handleLoadCrew = async () => {
    if (!agentsYaml || !tasksYaml) {
      setError('Invalid configuration');
      return;
    }

    setLoading(true);
    setError(null);

    try {
      // Create a mapping of agent names to IDs
      const idMapping: Record<string, string> = {};
      const nodes: Node[] = [];
      const edges: Edge[] = [];

      // Create agents
      for (const [agentName, agentConfig] of Object.entries(agentsYaml)) {
        try {
          const agentData = {
            name: agentName,
            role: agentConfig.role || '',
            goal: agentConfig.goal || '',
            backstory: agentConfig.backstory || '',
            llm: agentConfig.llm || 'gpt-4',
            tools: Array.isArray(agentConfig.tools) ? agentConfig.tools.map(t => String(t)) : [],
            function_calling_llm: agentConfig.function_calling_llm,
            max_iter: agentConfig.max_iter || 25,
            max_rpm: agentConfig.max_rpm,
            max_execution_time: agentConfig.max_execution_time || 300,
            memory: agentConfig.memory ?? true,
            verbose: agentConfig.verbose || false,
            allow_delegation: agentConfig.allow_delegation || false,
            cache: agentConfig.cache ?? true,
            system_template: agentConfig.system_template,
            prompt_template: agentConfig.prompt_template,
            response_template: agentConfig.response_template,
            allow_code_execution: agentConfig.allow_code_execution || false,
            code_execution_mode: (agentConfig.code_execution_mode === 'dangerous' || 
                                 agentConfig.code_execution_mode === 'none' ? 
                                 'safe' : agentConfig.code_execution_mode || 'safe') as 'safe' | 'unsafe',
            max_retry_limit: agentConfig.max_retry_limit || 3,
            use_system_prompt: agentConfig.use_system_prompt ?? true,
            respect_context_window: agentConfig.respect_context_window ?? true,
            embedder_config: agentConfig.embedder_config,
            knowledge_sources: agentConfig.knowledge_sources || []
          };

          const newAgent = await AgentService.createAgent(agentData);
          if (newAgent && newAgent.id) {
            const agentId = newAgent.id.toString();
            idMapping[agentName] = agentId;
            
            // Create agent node with temporary position
            const nodeId = `agent-${agentId}`;
            nodes.push({
              id: nodeId,
              type: 'agentNode',
              position: { x: 0, y: 0 }, // Temporary position, will be set by organizeNodesPositions
              data: {
                id: agentId,
                label: agentName,
                name: agentName,
                role: agentData.role,
                goal: agentData.goal,
                backstory: agentData.backstory,
                tools: agentData.tools
              }
            });
          }
        } catch (err) {
          console.error(`Error creating agent ${agentName}:`, err);
          throw new Error(`Failed to create agent: ${agentName}`);
        }
      }

      // Create a mapping of task names to IDs for dependency handling
      const taskNameToIdMapping: Record<string, string> = {};

      // Create tasks
      for (const [taskName, taskConfig] of Object.entries(tasksYaml)) {
        try {
          // Find agent ID if agent is specified
          let agentId: string | null = null;
          if (taskConfig.agent && idMapping[taskConfig.agent]) {
            agentId = idMapping[taskConfig.agent];
          }

          const taskData = {
            name: taskName,
            description: taskConfig.description || '',
            expected_output: taskConfig.expected_output || '',
            agent_id: agentId,
            tools: Array.isArray(taskConfig.tools) ? taskConfig.tools.map(t => String(t)) : [],
            context: Array.isArray(taskConfig.context) ? taskConfig.context.map(c => String(c)) : [],
            async_execution: Boolean(taskConfig.async_execution),
            config: {
              output_file: taskConfig.output_file || null,
              output_json: taskConfig.output_json || null,
              output_pydantic: taskConfig.output_pydantic || null,
              human_input: Boolean(taskConfig.human_input),
              retry_on_fail: Boolean(taskConfig.retry_on_fail),
              max_retries: Number(taskConfig.max_retries || 3),
              timeout: taskConfig.timeout ? Number(taskConfig.timeout) : null,
              priority: Number(taskConfig.priority || 1),
              error_handling: taskConfig.error_handling || 'default',
              cache_response: Boolean(taskConfig.cache_response),
              cache_ttl: Number(taskConfig.cache_ttl || 3600),
              callback: taskConfig.callback || null,
              condition: taskConfig.condition
            }
          };

          const newTask = await TaskService.createTask(taskData);
          if (newTask && newTask.id) {
            const taskId = newTask.id.toString();
            
            // Store mapping from task name to ID for dependency resolution
            taskNameToIdMapping[taskName] = taskId;
            
            // Create task node with temporary position
            const nodeId = `task-${taskId}`;
            nodes.push({
              id: nodeId,
              type: 'taskNode',
              position: { x: 0, y: 0 }, // Temporary position, will be set by organizeNodesPositions
              data: {
                id: taskId,
                label: taskName,
                name: taskName,
                description: taskData.description,
                expected_output: taskData.expected_output,
                agent_id: taskData.agent_id,
                tools: taskData.tools,
                context: taskData.context,
                async_execution: taskData.async_execution,
                config: taskData.config
              }
            });

            // Create edge from agent to task if agent is specified
            if (agentId && taskConfig.agent && idMapping[taskConfig.agent]) {
              const agentNodeId = `agent-${idMapping[taskConfig.agent]}`;
              const connection = {
                source: agentNodeId,
                target: nodeId,
                sourceHandle: null,
                targetHandle: null
              };

              if (!edgeExists(edges, connection)) {
                edges.push(createEdge(connection, 'animated', true, { stroke: '#9c27b0' }));
              }
            }
          }
        } catch (err) {
          console.error(`Error creating task ${taskName}:`, err);
          throw new Error(`Failed to create task: ${taskName}`);
        }
      }

      // Create task-to-task dependency edges after all tasks are created
      for (const [taskName, taskConfig] of Object.entries(tasksYaml)) {
        if (Array.isArray(taskConfig.context) && taskConfig.context.length > 0 && taskNameToIdMapping[taskName]) {
          const targetTaskId = taskNameToIdMapping[taskName];
          const targetNodeId = `task-${targetTaskId}`;
          
          // Create edges for each dependency
          for (const dependencyName of taskConfig.context) {
            // If the dependency is a task name and exists in our mapping
            if (typeof dependencyName === 'string' && taskNameToIdMapping[dependencyName]) {
              const sourceTaskId = taskNameToIdMapping[dependencyName];
              const sourceNodeId = `task-${sourceTaskId}`;
              
              // Create a dependency edge
              const connection = {
                source: sourceNodeId,
                target: targetNodeId,
                sourceHandle: null,
                targetHandle: null
              };

              if (!edgeExists(edges, connection)) {
                edges.push(createEdge(connection, 'animated', true, { stroke: '#9c27b0' }));
              }
              
              console.log(`Created dependency edge from ${dependencyName} to ${taskName}`);
            }
          }
        }
      }

      // Organize node positions before returning
      const organizedNodes = organizeNodesPositions(nodes, edges);
      
      // Call onCrewLoad with created nodes and edges
      onCrewLoad(organizedNodes, edges);
      toast.success('Crew loaded successfully');
    } catch (err) {
      console.error('Error loading crew:', err);
      setError(`Failed to load crew: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setLoading(false);
    }
  };

  return (
    <Dialog
      open={open}
      onClose={onClose}
      maxWidth="md"
      fullWidth
      aria-labelledby="load-crew-dialog-title"
    >
      <DialogTitle id="load-crew-dialog-title">
        {t('Load Crew from Run')}: {runName}
      </DialogTitle>
      <DialogContent>
        {error && (
          <Alert severity="error" sx={{ mb: 2 }}>
            {error}
          </Alert>
        )}

        <Box sx={{ mb: 2 }}>
          <Typography variant="subtitle1" gutterBottom>
            {t('Preview Configuration')}
          </Typography>
          
          <Tabs value={tabValue} onChange={handleTabChange} aria-label="configuration tabs">
            <Tab label="Agents" />
            <Tab label="Tasks" />
          </Tabs>
          
          <Paper 
            variant="outlined" 
            sx={{ 
              mt: 2, 
              p: 2, 
              maxHeight: 500, 
              overflow: 'auto', 
              backgroundColor: '#f8f8f8',
              fontFamily: 'monospace'
            }}
          >
            {tabValue === 0 ? (
              <Box sx={{ fontSize: '14px' }}>
                {agentsYaml ? formatJson(agentsYaml) : (
                  <Typography color="text.secondary">No agents configuration available</Typography>
                )}
              </Box>
            ) : (
              <Box sx={{ fontSize: '14px' }}>
                {tasksYaml ? formatJson(tasksYaml) : (
                  <Typography color="text.secondary">No tasks configuration available</Typography>
                )}
              </Box>
            )}
          </Paper>
        </Box>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose} disabled={loading}>
          {t('common.cancel')}
        </Button>
        <Button 
          onClick={handleLoadCrew} 
          variant="contained" 
          color="primary"
          disabled={loading || !agentsYaml || !tasksYaml}
        >
          {loading ? (
            <>
              <CircularProgress size={20} sx={{ mr: 1 }} />
              {t('Loading...')}
            </>
          ) : (
            t('Load Crew')
          )}
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default LoadCrew; 