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
 * Preserve saved node positions - do not reorganize
 * Nodes are loaded with their saved positions from the database
 */
const organizeNodesPositions = (nodes: Node[], edges: Edge[]): Node[] => {
  // Return nodes as-is to preserve their saved positions
  console.log('📐 LoadCrew: Preserving saved node positions', {
    nodeCount: nodes.length,
    edgeCount: edges.length
  });

  return nodes;
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
            llm: agentConfig.llm || 'databricks-llama-4-maverick',
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
              condition: taskConfig.condition,
              guardrail: taskConfig.guardrail || null,
              markdown: Boolean(taskConfig.markdown)
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
                // Agent-to-task edge: solid blue line, not animated
                edges.push(createEdge(connection, 'default', false, {}));
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
                // Task-to-task edge: dashed blue line, animated
                edges.push(createEdge(connection, 'default', true, {}));
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