import axios from 'axios';
import { apiClient } from '../config/api/ApiConfig';
import { Node, Edge } from 'reactflow';
import { AgentYaml, TaskYaml } from '../types/crew';
import { Task } from '../types/task';
import { JobResult } from '../types/common';
import { ModelService } from './ModelService';
import { Models } from '../types/models';

interface NodeData {
  label?: string;
  description?: string;
  [key: string]: unknown;
}

interface CrewConfig {
  agents_yaml: Record<string, AgentYaml>;
  tasks_yaml: Record<string, TaskYaml>;
  inputs: Record<string, unknown>;
  planning?: boolean;
  model?: string;
  execution_type?: string;
  schema_detection_enabled?: boolean;
  nodes?: { id: string; type: string; position: { x: number; y: number }; data: NodeData }[];
  edges?: { id: string; source: string; target: string; sourceHandle?: string; targetHandle?: string }[];
  flow_id?: string;
  flow_config?: Record<string, unknown>;
}

export interface JobResponse {
  job_id: string;
  execution_id?: string;
  status: string;
  created_at: string;
  result: JobResult;
  error: string | null;
}

export class JobExecutionService {
  private readonly modelService = ModelService.getInstance();

  async executeJob(
    nodes: Node[], 
    edges: Edge[], 
    planning = false, 
    model?: string, 
    executionType: 'crew' | 'flow' = 'crew',
    additionalInputs: Record<string, unknown> = {},
    schemaDetectionEnabled = true
  ): Promise<JobResponse> {
    try {
      // Generate a temporary ID to use for file path generation
      // This will be replaced by the actual job_id from the response later
      const tempJobId = `job_${Date.now()}`;
      
      // Log the execution type for debugging
      console.log(`[JobExecutionService] Executing job of type: ${executionType}`);
      
      // Create a base config with common properties
      const config: CrewConfig = {
        agents_yaml: {},
        tasks_yaml: {},
        inputs: additionalInputs,
        planning,
        model,
        execution_type: executionType,
        schema_detection_enabled: schemaDetectionEnabled
      };
      
      // If executing a flow, prepare flow-specific configuration
      if (executionType === 'flow') {
        console.log(`[JobExecutionService] Preparing flow execution configuration`);
        
        // Find flow nodes
        const flowNodes = nodes.filter(node => 
          node.type === 'flowNode' || 
          node.type === 'crewNode' || 
          (node.type && node.type.toLowerCase().includes('flow'))
        );
        
        if (flowNodes.length > 0) {
          console.log(`[JobExecutionService] Found ${flowNodes.length} flow nodes for execution`);
          
          // Extract flow configuration from the first flow node if available
          const firstFlowNode = flowNodes[0];
          const flowConfig = firstFlowNode.data?.flowConfig;
          
          // Always include nodes and edges in the config for flow execution
          // Map the nodes to match the expected format
          config.nodes = nodes.map(node => ({
            id: node.id,
            type: node.type || 'unknown',
            position: node.position || { x: 0, y: 0 },
            data: node.data || {}
          }));
          
          // Map the edges to match the expected format
          config.edges = edges.map(edge => ({
            id: edge.id,
            source: edge.source,
            target: edge.target,
            sourceHandle: edge.sourceHandle || undefined,
            targetHandle: edge.targetHandle || undefined
          }));
          
          if (flowConfig && flowConfig.id) {
            console.log(`[JobExecutionService] Found flow configuration with ID: ${flowConfig.id}`);
            
            // Include flow ID in the configuration
            config.flow_id = flowConfig.id;
            
            // Add flow configuration to the config if available
            if (flowConfig) {
              config.flow_config = flowConfig;
            }
          } else {
            console.log('[JobExecutionService] No flow ID found in configuration, creating dynamic flow');
          }
          
          // Include execution type and model
          config.execution_type = 'flow';
          config.model = model;
          config.schema_detection_enabled = schemaDetectionEnabled;
          
          // Add any additional inputs that might be needed
          if (Object.keys(additionalInputs).length > 0) {
            config.inputs = additionalInputs;
          }
        } else {
          throw new Error('Cannot execute flow: No flow nodes found on canvas');
        }
      } else {
        // Standard crew execution - Continue with the normal agent and task processing
        // Get models from ModelService for model-specific configurations
        let models: Models = {};
        try {
          models = await this.modelService.getActiveModels();
        } catch (error) {
          console.error('Error fetching models from ModelService:', error);
          // Fallback to synchronous method if async fails
          models = this.modelService.getActiveModelsSync();
        }
        
        // First pass: Create basic configurations
        nodes.forEach(node => {
          if (node.type === 'agentNode') {
            const agentData = node.data;
            console.log('Agent node data:', JSON.stringify(agentData, null, 2));
            const agentName = `agent_${node.id}`;
            
            // Create the agent configuration by copying all relevant fields from the node data
            const agentConfig: AgentYaml = {
              role: agentData.role || '',
              goal: agentData.goal || '',
              backstory: agentData.backstory || '',
              tools: Array.isArray(agentData.tools) ? agentData.tools : [],
              llm: agentData.llm,
              function_calling_llm: agentData.function_calling_llm,
              max_iter: agentData.max_iter,
              max_rpm: agentData.max_rpm,
              max_execution_time: agentData.max_execution_time,
              memory: agentData.memory,
              verbose: agentData.verbose,
              allow_delegation: agentData.allow_delegation,
              cache: agentData.cache,
              system_template: agentData.system_template,
              prompt_template: agentData.prompt_template,
              response_template: agentData.response_template,
              allow_code_execution: agentData.allow_code_execution,
              code_execution_mode: agentData.code_execution_mode,
              max_retry_limit: agentData.max_retry_limit,
              use_system_prompt: agentData.use_system_prompt,
              respect_context_window: agentData.respect_context_window,
              embedder_config: agentData.embedder_config,
              knowledge_sources: agentData.knowledge_sources,
            };
            
            // Apply model-specific configurations from ModelService
            if (agentData.llm && models[agentData.llm]) {
              const modelConfig = models[agentData.llm];
              // Set context window size from model configuration
              if (modelConfig.context_window) {
                agentConfig.max_context_window_size = modelConfig.context_window;
                console.log(`Set max_context_window_size=${modelConfig.context_window} for ${agentData.llm}`);
              }
              // Set max tokens from model configuration
              if (modelConfig.max_output_tokens) {
                agentConfig.max_tokens = modelConfig.max_output_tokens;
                console.log(`Set max_tokens=${modelConfig.max_output_tokens} for ${agentData.llm}`);
              }
            }

            // Remove undefined and null values to keep the YAML clean
            Object.keys(agentConfig).forEach(key => {
              const k = key as keyof AgentYaml;
              if (agentConfig[k] === undefined || agentConfig[k] === null) {
                delete agentConfig[k];
              }
            });

            config.agents_yaml[agentName] = agentConfig;
          } else if (node.type === 'taskNode') {
            const taskData = node.data as Task;
            const taskName = `task_${node.id}`;
            
            // Ensure we always have a default output file path, even if null in config
            const defaultOutputFile = `output/${tempJobId}_${node.id}.md`;
            
            const taskConfig: TaskYaml = {
              description: taskData.description,
              expected_output: taskData.expected_output,
              tools: Array.isArray(taskData.tools) ? taskData.tools : [],
              context: [],
              agent: null,
              async_execution: Boolean(taskData.async_execution),
              // Use the output_file from config if available, replace placeholders, or use default
              output_file: taskData.config?.output_file 
                ? String(taskData.config.output_file)
                    .replace('runid', tempJobId)
                    .replace('taskid', node.id)
                : defaultOutputFile,
              output_json: taskData.config?.output_json ? String(taskData.config.output_json) : null,
              output_pydantic: taskData.config?.output_pydantic ? String(taskData.config.output_pydantic) : null,
              human_input: Boolean(taskData.config?.human_input),
              retry_on_fail: Boolean(taskData.config?.retry_on_fail),
              max_retries: Number(taskData.config?.max_retries || 3),
              timeout: taskData.config?.timeout ? Number(taskData.config.timeout) : null,
              priority: Number(taskData.config?.priority || 1),
              error_handling: taskData.config?.error_handling as 'default' | 'ignore' | 'retry' | 'fail' || 'default',
              cache_response: Boolean(taskData.config?.cache_response),
              cache_ttl: Number(taskData.config?.cache_ttl || 3600),
              callback: taskData.config?.callback ? String(taskData.config.callback) : null,
              condition: taskData.config?.condition || undefined
            };

            // Add optional fields only if they are defined and not null
            if (taskData.async_execution !== undefined) {
              taskConfig.async_execution = Boolean(taskData.async_execution);
            }
            
            // We already set a default output_file above, no need to check if it's null
            if (taskData.config?.output_file) {
              // Process the output file path to replace placeholders with actual values
              const processedOutputFile = String(taskData.config.output_file)
                .replace('runid', tempJobId)
                .replace('taskid', node.id);
                
              taskConfig.output_file = processedOutputFile;
            }
            if (taskData.config?.output_json) {
              taskConfig.output_json = String(taskData.config.output_json);
            }
            if (taskData.config?.output_pydantic) {
              taskConfig.output_pydantic = String(taskData.config.output_pydantic);
            }
            if (taskData.config?.human_input !== undefined) {
              taskConfig.human_input = Boolean(taskData.config.human_input);
            }
            if (taskData.config?.retry_on_fail !== undefined) {
              taskConfig.retry_on_fail = Boolean(taskData.config.retry_on_fail);
            }
            if (taskData.config?.max_retries !== undefined) {
              taskConfig.max_retries = Number(taskData.config.max_retries);
            }
            if (taskData.config?.timeout !== undefined && taskData.config.timeout !== null) {
              taskConfig.timeout = Number(taskData.config.timeout);
            }
            if (taskData.config?.priority !== undefined) {
              taskConfig.priority = Number(taskData.config.priority);
            }
            if (taskData.config?.error_handling) {
              taskConfig.error_handling = taskData.config.error_handling as 'default' | 'ignore' | 'retry' | 'fail';
            }
            if (taskData.config?.cache_response !== undefined) {
              taskConfig.cache_response = Boolean(taskData.config.cache_response);
            }
            if (taskData.config?.cache_ttl !== undefined) {
              taskConfig.cache_ttl = Number(taskData.config.cache_ttl);
            }
            if (taskData.config?.callback) {
              taskConfig.callback = String(taskData.config.callback);
            }

            config.tasks_yaml[taskName] = taskConfig;
          }
        });

        // Second pass: Process edges to connect tasks to agents and handle task dependencies
        edges.forEach(edge => {
          const sourceNode = nodes.find(n => n.id === edge.source);
          const targetNode = nodes.find(n => n.id === edge.target);
          
          if (sourceNode?.type === 'agentNode' && targetNode?.type === 'taskNode') {
            const agentName = `agent_${edge.source}`;
            const taskName = `task_${edge.target}`;
            
            if (config.tasks_yaml[taskName]) {
              config.tasks_yaml[taskName].agent = agentName;
            }
          } else if (sourceNode?.type === 'taskNode' && targetNode?.type === 'taskNode') {
            // Handle task dependencies
            const dependencyTaskName = `task_${edge.source}`;
            const dependentTaskName = `task_${edge.target}`;
            
            if (config.tasks_yaml[dependentTaskName]) {
              // Add the dependency task name to the context array
              config.tasks_yaml[dependentTaskName].context.push(dependencyTaskName);
            }
          }
        });
        
        // CRITICAL FIX: Process agent_id fields on task nodes to create missing agent connections
        // This ensures tasks with agent_id but no incoming edge from an agent node, will still be properly connected
        nodes.forEach(node => {
          if (node.type === 'taskNode') {
            const taskData = node.data as Task;
            const taskName = `task_${node.id}`;
            
            // If task has an agent_id but no incoming edge from an agent node, create the connection
            if (taskData.agent_id) {
              console.log(`Task ${node.id} has agent_id: ${taskData.agent_id}`);
              
              // Check if there's already an edge from an agent to this task
              const hasAgentEdge = edges.some(edge => {
                const sourceNode = nodes.find(n => n.id === edge.source);
                return sourceNode?.type === 'agentNode' && edge.target === node.id;
              });
              
              if (!hasAgentEdge) {
                // Find the agent node that matches this ID
                const agentNode = nodes.find(n => 
                  n.type === 'agentNode' && 
                  (n.data.id === taskData.agent_id || n.id === taskData.agent_id)
                );
                
                if (agentNode) {
                  console.log(`Creating missing agent connection: Agent ${agentNode.id} -> Task ${node.id}`);
                  const agentName = `agent_${agentNode.id}`;
                  
                  // Set the agent in the task configuration
                  if (config.tasks_yaml[taskName]) {
                    config.tasks_yaml[taskName].agent = agentName;
                    console.log(`Set task agent to ${agentName} based on agent_id`);
                  }
                } else {
                  console.warn(`Could not find agent node for task agent_id: ${taskData.agent_id}`);
                }
              }
            }

            // Process guardrail if it exists
            if (taskData.config?.guardrail) {
              try {
                // Parse the guardrail JSON string
                const guardrailConfig = JSON.parse(taskData.config.guardrail);
                
                // Set the guardrail in the task YAML
                if (config.tasks_yaml[taskName]) {
                  config.tasks_yaml[taskName].guardrail = guardrailConfig;
                  console.log(`Set guardrail for task ${taskName}:`, guardrailConfig);
                }
              } catch (error) {
                console.error(`Error parsing guardrail for task ${taskName}:`, error);
              }
            }
          }
        });

        // Validate the configuration before sending
        if (Object.keys(config.agents_yaml).length === 0 && executionType === 'crew') {
          throw new Error('No agents configured');
        }
        if (Object.keys(config.tasks_yaml).length === 0 && executionType === 'crew') {
          throw new Error('No tasks configured');
        }

        // Sort tasks based on dependencies to ensure proper order
        config.tasks_yaml = this.sortTasksByDependencies(config.tasks_yaml);
      }
      
      // Log the final configuration before sending to the server
      if (executionType === 'flow') {
        console.log('Sending flow job to server:', JSON.stringify({
          ...config,
          // Truncate nodes and edges for better logging
          nodes: config.nodes ? `[${config.nodes.length} nodes]` : 'none',
          edges: config.edges ? `[${config.edges.length} edges]` : 'none'
        }, null, 2));
      } else {
        // For crew executions, show the full config
        console.log('Sending crew job to server:', JSON.stringify(config, null, 2));
      }
      
      const response = await apiClient.post('/executions', config);
      return response.data as JobResponse;
    } catch (error) {
      console.error('Error executing job:', error);
      if (axios.isAxiosError(error) && error.response) {
        const errorMessage = error.response.data?.detail || error.message;
        throw new Error(`${error.response.status}: ${errorMessage}`);
      }
      throw error;
    }
  }

  async getJobStatus(jobId: string): Promise<JobResponse> {
    try {
      const response = await apiClient.get<JobResponse>(
        `/executions/${jobId}`
      );
      return response.data;
    } catch (error) {
      console.error('Error fetching job status:', error);
      throw error;
    }
  }

  /**
   * Sorts tasks based on their dependencies to ensure tasks are executed in the correct order.
   * Tasks with dependencies will be placed after the tasks they depend on in the returned object.
   */
  private sortTasksByDependencies(tasksYaml: Record<string, TaskYaml>): Record<string, TaskYaml> {
    const orderedTasks: Record<string, TaskYaml> = {};
    const processedTasks = new Set<string>();
    
    // Recursive function to add a task and its dependencies
    const addTaskWithDependencies = (taskKey: string) => {
      // Skip if already processed
      if (processedTasks.has(taskKey)) return;
      
      const task = tasksYaml[taskKey];
      
      // Process dependencies first (tasks in context)
      for (const contextTask of task.context || []) {
        if (tasksYaml[contextTask]) {
          addTaskWithDependencies(contextTask);
        }
      }
      
      // Add this task after its dependencies
      orderedTasks[taskKey] = task;
      processedTasks.add(taskKey);
    };
    
    // Process all tasks to ensure proper ordering
    Object.keys(tasksYaml).forEach(taskKey => {
      addTaskWithDependencies(taskKey);
    });
    
    return orderedTasks;
  }
}

export const jobExecutionService = new JobExecutionService(); 