import { Node, Edge } from 'reactflow';
import { AgentService } from '../../../api/AgentService';
import { TaskService } from '../../../api/TaskService';
import { Agent } from '../../../types/agent';
import { Task } from '../../../types/task';
import { GeneratedAgent, GeneratedTask, GeneratedCrew, ChatMessage } from '../types';
import { CanvasLayoutManager } from '../../../utils/CanvasLayoutManager';
import { useWorkflowStore } from '../../../store/workflow';
import { useUILayoutStore } from '../../../store/uiLayout';
import { ConfigureCrewResult } from '../../../api/DispatcherService';
import { EdgeCategory, getEdgeStyleConfig } from '../../../config/edgeConfig';
import type {
  PlanReadyData,
  AgentDetailData,
  TaskDetailData,
  EntityErrorData,
  DependenciesResolvedData,
} from '../../../hooks/global/useCrewGenerationSSE';

export const createAgentGenerationHandler = (
  setNodes: React.Dispatch<React.SetStateAction<Node[]>>,
  setMessages: React.Dispatch<React.SetStateAction<ChatMessage[]>>,
  selectedModel: string,
  onNodesGenerated?: (nodes: Node[], edges: Edge[]) => void,
  layoutManagerRef?: React.MutableRefObject<CanvasLayoutManager>,
  inputRef?: React.RefObject<HTMLInputElement>
) => {
  return async (agentData: GeneratedAgent) => {
    try {
      const agentToCreate = {
        name: agentData.name,
        role: agentData.role,
        goal: agentData.goal,
        backstory: agentData.backstory,
        llm: agentData.advanced_config?.llm || selectedModel,
        tools: agentData.tools || [],
        max_iter: (agentData.advanced_config?.max_iter as number) || 25,
        max_rpm: (agentData.advanced_config?.max_rpm as number) || 10,
        max_execution_time: (agentData.advanced_config?.max_execution_time as number) || undefined,
        verbose: (agentData.advanced_config?.verbose as boolean) || false,
        allow_delegation: (agentData.advanced_config?.allow_delegation as boolean) || false,
        cache: (agentData.advanced_config?.cache as boolean) ?? true,
        system_template: (agentData.advanced_config?.system_template as string) || undefined,
        prompt_template: (agentData.advanced_config?.prompt_template as string) || undefined,
        response_template: (agentData.advanced_config?.response_template as string) || undefined,
        allow_code_execution: (agentData.advanced_config?.allow_code_execution as boolean) || false,
        code_execution_mode: (agentData.advanced_config?.code_execution_mode as 'safe' | 'unsafe') || 'safe',
        max_retry_limit: (agentData.advanced_config?.max_retry_limit as number) || 2,
        use_system_prompt: (agentData.advanced_config?.use_system_prompt as boolean) ?? true,
        respect_context_window: (agentData.advanced_config?.respect_context_window as boolean) ?? true,
        memory: (agentData.advanced_config?.memory as boolean) ?? true,
        embedder_config: agentData.advanced_config?.embedder_config ? {
          provider: (agentData.advanced_config.embedder_config as { provider?: string }).provider || 'databricks',
          config: {
            model: ((agentData.advanced_config.embedder_config as { config?: { model?: string } }).config?.model) || 'databricks-gte-large-en',
            ...((agentData.advanced_config.embedder_config as { config?: Record<string, unknown> }).config || {})
          }
        } : undefined,
        knowledge_sources: (agentData.advanced_config?.knowledge_sources as Agent['knowledge_sources']) || [],
        function_calling_llm: (agentData.advanced_config?.function_calling_llm as string) || undefined,
      };

      const savedAgent = await AgentService.createAgent(agentToCreate);

      if (savedAgent) {
        // Get fresh nodes from the store to ensure we have the latest state
        const currentNodes = useWorkflowStore.getState().nodes;

        console.log('[Agent Generation] Current nodes from store:', {
          storeNodes: currentNodes.length,
          storeAgents: currentNodes.filter(n => n.type === 'agentNode').length,
          storeTasks: currentNodes.filter(n => n.type === 'taskNode').length
        });

        const position = layoutManagerRef?.current.getAgentNodePosition(currentNodes, 'crew') || { x: 100, y: 100 };

        console.log('[Agent Generation] Calculated position:', position);

        const newNode: Node = {
          id: `agent-${savedAgent.id}`,
          type: 'agentNode',
          position,
          data: {
            label: savedAgent.name,
            agentId: savedAgent.id,
            role: savedAgent.role,
            goal: savedAgent.goal,
            backstory: savedAgent.backstory,
            llm: savedAgent.llm,
            tools: savedAgent.tools || [],
            agent: savedAgent,
          },
        };

        setTimeout(() => {
          window.dispatchEvent(new Event('fitViewToNodesInternal'));
        }, 100);

        // Call onNodesGenerated directly without nesting in setNodes
        // This prevents the race condition where setNodes returns unchanged nodes
        if (onNodesGenerated) {
          onNodesGenerated([newNode], []);
        } else {
          // Fallback: add node directly if no callback provided
          setNodes((currentNodes) => [...currentNodes, newNode]);
        }

        // Focus restoration
        const focusDelays = [100, 300, 500, 800, 1200];
        focusDelays.forEach(delay => {
          setTimeout(() => {
            inputRef?.current?.focus();
          }, delay);
        });
      } else {
        throw new Error('Failed to save agent');
      }
    } catch (error) {
      console.error('Error saving agent:', error);
      
      let errorDetail = '';
      if (error instanceof Error) {
        errorDetail = `: ${error.message}`;
      } else if (typeof error === 'object' && error !== null) {
        const apiError = error as { response?: { data?: { detail?: string; message?: string } } };
        if (apiError.response?.data?.detail) {
          errorDetail = `: ${apiError.response.data.detail}`;
        } else if (apiError.response?.data?.message) {
          errorDetail = `: ${apiError.response.data.message}`;
        }
      }
      
      const errorMsg: ChatMessage = {
        id: `error-${Date.now()}`,
        type: 'assistant',
        content: `❌ Failed to save agent "${agentData.name}"${errorDetail}. The agent will be created locally but won't be persisted.`,
        timestamp: new Date(),
      };
      setMessages(prev => [...prev, errorMsg]);
      
      // Focus restoration even after error
      const focusDelays = [100, 300, 500, 800];
      focusDelays.forEach(delay => {
        setTimeout(() => {
          inputRef?.current?.focus();
        }, delay);
      });

      // Still create the node even if saving failed
      // Get fresh nodes from the store to ensure we have the latest state
      const currentNodes = useWorkflowStore.getState().nodes;

      console.log('[Agent Generation - Error Case] Current nodes from store:', {
        storeNodes: currentNodes.length
      });

      const position = layoutManagerRef?.current.getAgentNodePosition(currentNodes, 'crew') || { x: 100, y: 100 };

      console.log('[Agent Generation - Error Case] Calculated position:', position);

      const newNode: Node = {
        id: `agent-${Date.now()}`,
        type: 'agentNode',
        position,
        data: {
          label: agentData.name,
          role: agentData.role,
          goal: agentData.goal,
          backstory: agentData.backstory,
          llm: agentData.advanced_config?.llm || selectedModel,
          tools: agentData.tools || [],
          agent: agentData,
        },
      };

      setTimeout(() => {
        window.dispatchEvent(new Event('fitViewToNodesInternal'));
      }, 100);

      // Call onNodesGenerated directly without nesting in setNodes
      if (onNodesGenerated) {
        onNodesGenerated([newNode], []);
      } else {
        // Fallback: add node directly if no callback provided
        setNodes((currentNodes) => [...currentNodes, newNode]);
      }
    }
  };
};

export const createTaskGenerationHandler = (
  setNodes: React.Dispatch<React.SetStateAction<Node[]>>,
  setEdges: React.Dispatch<React.SetStateAction<Edge[]>>,
  setMessages: React.Dispatch<React.SetStateAction<ChatMessage[]>>,
  onNodesGenerated?: (nodes: Node[], edges: Edge[]) => void,
  layoutManagerRef?: React.MutableRefObject<CanvasLayoutManager>,
  inputRef?: React.RefObject<HTMLInputElement>
) => {
  return async (taskData: GeneratedTask) => {
    try {
      const { nodes, edges } = useWorkflowStore.getState();
      const agentNodes = nodes.filter(n => n.type === 'agentNode');
      
      let assignedAgentId = "";
      
      if (agentNodes.length > 0) {
        const agentsWithoutConnections = agentNodes.filter(agentNode => {
          const hasTaskConnection = edges.some(edge => 
            edge.source === agentNode.id && 
            nodes.find(n => n.id === edge.target)?.type === 'taskNode'
          );
          return !hasTaskConnection;
        });
        
        if (agentsWithoutConnections.length > 0) {
          const agentData = agentsWithoutConnections[0].data;
          assignedAgentId = agentData.agentId || "";
          console.log(`Auto-assigning task "${taskData.name}" to agent "${agentData.label}" (ID: ${assignedAgentId}) - Priority: No connections`);
        } else if (agentNodes.length > 0) {
          const agentData = agentNodes[0].data;
          assignedAgentId = agentData.agentId || "";
          console.log(`Auto-assigning task "${taskData.name}" to agent "${agentData.label}" (ID: ${assignedAgentId}) - Priority: Has connections (fallback)`);
        }
      }

      const toolsList = (taskData.tools || []).map((tool: string | { name: string }) => {
        if (typeof tool === 'string') {
          return tool;
        } else if (tool && typeof tool === 'object' && 'name' in tool) {
          return tool.name;
        }
        return '';
      }).filter(Boolean);

      const taskToCreate = {
        name: taskData.name,
        description: taskData.description,
        expected_output: taskData.expected_output,
        tools: toolsList,
        agent_id: assignedAgentId,
        async_execution: Boolean(taskData.advanced_config?.async_execution) || false,
        markdown: Boolean(taskData.advanced_config?.markdown) || false,
        context: [],
        config: {
          cache_response: false,
          cache_ttl: 3600,
          retry_on_fail: true,
          max_retries: 3,
          timeout: null,
          priority: 1,
          error_handling: 'default' as const,
          output_file: (taskData.advanced_config?.output_file as string) || null,
          output_json: taskData.advanced_config?.output_json
            ? (typeof taskData.advanced_config.output_json === 'string'
                ? taskData.advanced_config.output_json
                : JSON.stringify(taskData.advanced_config.output_json))
            : null,
          output_pydantic: (taskData.advanced_config?.output_pydantic as string) || null,
          callback: (taskData.advanced_config?.callback as string) || null,
          human_input: Boolean(taskData.advanced_config?.human_input) || false,
          markdown: Boolean(taskData.advanced_config?.markdown) || false
          // Note: llm_guardrail is NOT set in config - it's stored at node level as a suggestion
          // User must explicitly enable it via the toggle
        }
      };

      const savedTask = await TaskService.createTask(taskToCreate);

      if (savedTask) {
        // Get fresh nodes from the store to ensure we have the latest state
        const currentNodes = useWorkflowStore.getState().nodes;

        console.log('[Task Generation] Current nodes from store:', {
          storeNodes: currentNodes.length,
          storeAgents: currentNodes.filter(n => n.type === 'agentNode').length,
          storeTasks: currentNodes.filter(n => n.type === 'taskNode').length
        });

        const position = layoutManagerRef?.current.getTaskNodePosition(currentNodes, 'crew') || { x: 400, y: 100 };

        console.log('[Task Generation] Calculated position:', position);

        const newNode: Node = {
          id: `task-${savedTask.id}`,
          type: 'taskNode',
          position,
          data: {
            label: savedTask.name,
            taskId: savedTask.id,
            description: savedTask.description,
            expected_output: savedTask.expected_output,
            tools: savedTask.tools || [],
            human_input: savedTask.config?.human_input || false,
            async_execution: savedTask.async_execution || false,
            config: savedTask.config,
            llm_guardrail: (taskData as unknown as { llm_guardrail?: { description: string; llm_model?: string } }).llm_guardrail || savedTask.config?.llm_guardrail || null,
            task: savedTask,
          },
        };

        setTimeout(() => {
          window.dispatchEvent(new Event('fitViewToNodesInternal'));
        }, 100);

        // Create edge if agent is assigned
        let newEdge: Edge | null = null;
        if (assignedAgentId) {
          const agentNodeId = `agent-${assignedAgentId}`;
          const taskNodeId = `task-${savedTask.id}`;

          // Get current layout orientation
          const { layoutOrientation } = useUILayoutStore.getState();
          const sourceHandle = layoutOrientation === 'vertical' ? 'bottom' : 'right';
          const targetHandle = layoutOrientation === 'vertical' ? 'top' : 'left';

          // Create edge with centralized styling
          const edgeStyle = getEdgeStyleConfig(EdgeCategory.AGENT_TO_TASK, false);

          newEdge = {
            id: `edge-${agentNodeId}-${taskNodeId}`,
            source: agentNodeId,
            target: taskNodeId,
            type: 'default',
            animated: false,
            sourceHandle,
            targetHandle,
            style: edgeStyle
          };
        }

        // Call onNodesGenerated directly without nesting in setNodes
        // This prevents the race condition where setNodes returns unchanged nodes
        if (onNodesGenerated) {
          onNodesGenerated([newNode], newEdge ? [newEdge] : []);
        } else {
          // Fallback: add node and edge directly if no callback provided
          setNodes((currentNodes) => [...currentNodes, newNode]);
          if (newEdge !== null) {
            const edgeToAdd = newEdge;
            setEdges((edges) => [...edges, edgeToAdd]);
          }
        }

        // Focus restoration
        const focusDelays = [100, 300, 500, 800, 1200];
        focusDelays.forEach(delay => {
          setTimeout(() => {
            inputRef?.current?.focus();
          }, delay);
        });
      } else {
        throw new Error('Failed to save task');
      }
    } catch (error) {
      console.error('Error saving task:', error);
      
      let errorDetail = '';
      if (error instanceof Error) {
        errorDetail = `: ${error.message}`;
      } else if (typeof error === 'object' && error !== null) {
        const apiError = error as { response?: { data?: { detail?: string; message?: string } } };
        if (apiError.response?.data?.detail) {
          errorDetail = `: ${apiError.response.data.detail}`;
        } else if (apiError.response?.data?.message) {
          errorDetail = `: ${apiError.response.data.message}`;
        }
      }
      
      const errorMsg: ChatMessage = {
        id: `error-${Date.now()}`,
        type: 'assistant',
        content: `❌ Failed to save task "${taskData.name}"${errorDetail}. The task will be created locally but won't be persisted.`,
        timestamp: new Date(),
      };
      setMessages(prev => [...prev, errorMsg]);
      
      // Focus restoration even after error
      const focusDelays = [100, 300, 500, 800];
      focusDelays.forEach(delay => {
        setTimeout(() => {
          inputRef?.current?.focus();
        }, delay);
      });

      // Still create the node even if saving failed
      // Get fresh nodes from the store to ensure we have the latest state
      const currentNodes = useWorkflowStore.getState().nodes;

      console.log('[Task Generation - Error Case] Current nodes from store:', {
        storeNodes: currentNodes.length
      });

      const position = layoutManagerRef?.current.getTaskNodePosition(currentNodes, 'crew') || { x: 400, y: 100 };

      console.log('[Task Generation - Error Case] Calculated position:', position);

      const newNode: Node = {
        id: `task-${Date.now()}`,
        type: 'taskNode',
        position,
        data: {
          label: taskData.name,
          description: taskData.description,
          expected_output: taskData.expected_output,
          tools: taskData.tools || [],
          human_input: taskData.advanced_config?.human_input || false,
          async_execution: taskData.advanced_config?.async_execution || false,
          task: taskData,
        },
      };

      setTimeout(() => {
        window.dispatchEvent(new Event('fitViewToNodesInternal'));
      }, 100);

      // Call onNodesGenerated directly without nesting in setNodes
      if (onNodesGenerated) {
        onNodesGenerated([newNode], []);
      } else {
        // Fallback: add node directly if no callback provided
        setNodes((currentNodes) => [...currentNodes, newNode]);
      }
    }
  };
};

export const createCrewGenerationHandler = (
  setNodes: React.Dispatch<React.SetStateAction<Node[]>>,
  setEdges: React.Dispatch<React.SetStateAction<Edge[]>>,
  setLastExecutionJobId: React.Dispatch<React.SetStateAction<string | null>>,
  setExecutingJobId: React.Dispatch<React.SetStateAction<string | null>>,
  selectedModel: string,
  onNodesGenerated?: (nodes: Node[], edges: Edge[]) => void,
  layoutManagerRef?: React.MutableRefObject<CanvasLayoutManager>,
  inputRef?: React.RefObject<HTMLInputElement>
) => {
  return (crewData: GeneratedCrew) => {
    // Clear the canvas before generating new crew
    console.log('[CrewGeneration] Clearing canvas before generating new crew');
    setNodes([]);
    setEdges([]);

    const nodes: Node[] = [];
    const edges: Edge[] = [];
    const agentIdMap = new Map<string, string>();

    setLastExecutionJobId(null);
    setExecutingJobId(null);

    const agentCount = crewData.agents?.length || 0;
    const taskCount = crewData.tasks?.length || 0;
    const layoutResult = layoutManagerRef?.current.getCrewLayoutPositions(agentCount, taskCount, 'crew') ||
      { agentPositions: [], taskPositions: [], layoutBounds: null, shouldAutoFit: false };
    const { agentPositions, taskPositions } = layoutResult;

    if (crewData.agents) {
      crewData.agents.forEach((agent: Agent, index: number) => {
        const nodeId = `agent-${agent.id || Date.now() + index}`;
        agentIdMap.set(agent.id?.toString() || agent.name, nodeId);
        
        nodes.push({
          id: nodeId,
          type: 'agentNode',
          position: agentPositions[index] || { x: 100, y: 100 + index * 150 },
          data: {
            label: agent.name,
            agentId: agent.id,
            role: agent.role,
            goal: agent.goal,
            backstory: agent.backstory,
            llm: agent.llm || selectedModel,
            tools: agent.tools || [],
            agent: agent,
          },
        });
      });
    }

    if (crewData.tasks) {
      crewData.tasks.forEach((task: Task, index: number) => {
        const taskNodeId = `task-${task.id || Date.now() + index}`;
        
        // Get llm_guardrail as a suggestion (at top level), but don't include it in config
        // This ensures the toggle defaults to disabled, but the suggestion is available when enabled
        const llmGuardrailSuggestion = (task as unknown as { llm_guardrail?: unknown }).llm_guardrail || task.config?.llm_guardrail || null;

        // Create config without llm_guardrail (user must explicitly enable it)
        const configWithoutGuardrail = task.config ? { ...task.config, llm_guardrail: undefined } : undefined;

        nodes.push({
          id: taskNodeId,
          type: 'taskNode',
          position: taskPositions[index] || { x: 400, y: 100 + index * 150 },
          data: {
            label: task.name,
            taskId: task.id,
            description: task.description,
            expected_output: task.expected_output,
            tools: task.tools || [],
            human_input: task.config?.human_input || false,
            async_execution: task.async_execution || false,
            context: task.context || [],
            config: configWithoutGuardrail,
            // Store llm_guardrail at top level as suggestion (toggle disabled by default)
            llm_guardrail: llmGuardrailSuggestion,
            task: task,
          },
        });

        if (task.agent_id) {
          const agentNodeId = agentIdMap.get(task.agent_id.toString());
          if (agentNodeId) {
            // Get current layout orientation
            const { layoutOrientation } = useUILayoutStore.getState();
            const sourceHandle = layoutOrientation === 'vertical' ? 'bottom' : 'right';
            const targetHandle = layoutOrientation === 'vertical' ? 'top' : 'left';

            // Agent-to-task edge: use centralized edge configuration
            const edgeStyle = getEdgeStyleConfig(EdgeCategory.AGENT_TO_TASK, false);

            edges.push({
              id: `edge-${agentNodeId}-${taskNodeId}`,
              source: agentNodeId,
              target: taskNodeId,
              type: 'default',
              animated: false,
              sourceHandle,
              targetHandle,
              style: edgeStyle
            });
          }
        }

        if (task.context && Array.isArray(task.context)) {
          task.context.forEach((depTaskId: string) => {
            const sourceTaskId = `task-${depTaskId}`;
            if (nodes.some(n => n.id === sourceTaskId)) {
              // Task-to-task edge: use centralized edge configuration
              const edgeStyle = getEdgeStyleConfig(EdgeCategory.TASK_TO_TASK, true);

              edges.push({
                id: `edge-${sourceTaskId}-${taskNodeId}`,
                source: sourceTaskId,
                target: taskNodeId,
                sourceHandle: 'right',
                targetHandle: 'left',
                type: 'default',
                animated: true,
                style: edgeStyle
              });
            }
          });
        }
      });
    }

    // Don't add nodes here - let the onNodesGenerated callback handle it
    // This prevents duplicate nodes when the callback also adds them
    if (onNodesGenerated) {
      onNodesGenerated(nodes, edges);
    } else {
      // Fallback: only add nodes if no callback is provided
      setNodes((currentNodes) => [...currentNodes, ...nodes]);
      setEdges((currentEdges) => [...currentEdges, ...edges]);
    }

    // Always use standard fitView for consistency
    // Increased timeout to ensure nodes are fully rendered in DOM
    console.log('[CrewGeneration] Triggering fitView after node creation');
    setTimeout(() => {
      window.dispatchEvent(new Event('fitViewToNodesInternal'));
    }, 500);

    // Focus restoration
    const focusDelays = [300, 500, 800, 1200];
    focusDelays.forEach(delay => {
      setTimeout(() => {
        inputRef?.current?.focus();
      }, delay);
    });
  };
};

export const handleConfigureCrew = (configResult: ConfigureCrewResult, inputRef?: React.RefObject<HTMLInputElement>) => {
  const { config_type: _config_type, actions } = configResult;
  
  if (actions?.open_llm_dialog) {
    setTimeout(() => {
      const event = new CustomEvent('openLLMDialog');
      window.dispatchEvent(event);
    }, 100);
  }
  
  if (actions?.open_maxr_dialog) {
    setTimeout(() => {
      const event = new CustomEvent('openMaxRPMDialog');
      window.dispatchEvent(event);
    }, 200);
  }
  
  if (actions?.open_tools_dialog) {
    setTimeout(() => {
      const event = new CustomEvent('openToolDialog');
      window.dispatchEvent(event);
    }, 300);
  }

  setTimeout(() => {
    inputRef?.current?.focus();
  }, 500);
};

/* ------------------------------------------------------------------ */
/*  Progressive (SSE-based) crew generation handlers                   */
/* ------------------------------------------------------------------ */

/** Map from plan index → ReactFlow node id, built during skeleton creation */
export type IndexNodeIdMap = {
  agents: Map<number, string>;
  tasks: Map<number, string>;
};

/**
 * Called on `plan_ready`. Clears canvas and creates skeleton nodes with
 * name/role only (loading state).
 */
export function createCrewSkeletonHandler(
  setNodes: React.Dispatch<React.SetStateAction<Node[]>>,
  setEdges: React.Dispatch<React.SetStateAction<Edge[]>>,
  setLastExecutionJobId: React.Dispatch<React.SetStateAction<string | null>>,
  setExecutingJobId: React.Dispatch<React.SetStateAction<string | null>>,
  layoutManagerRef?: React.MutableRefObject<CanvasLayoutManager>,
): (plan: PlanReadyData) => IndexNodeIdMap {
  return (plan: PlanReadyData): IndexNodeIdMap => {
    console.log('[ProgressiveCrew] plan_ready', plan);

    // Reset execution state
    setLastExecutionJobId(null);
    setExecutingJobId(null);

    const agentCount = plan.agents.length;
    const taskCount = plan.tasks.length;
    const layoutResult = layoutManagerRef?.current.getCrewLayoutPositions(agentCount, taskCount, 'crew') || {
      agentPositions: [],
      taskPositions: [],
      layoutBounds: null,
      shouldAutoFit: false,
    };
    const { agentPositions, taskPositions } = layoutResult;
    const { layoutOrientation } = useUILayoutStore.getState();

    const nodes: Node[] = [];
    const edges: Edge[] = [];
    const indexMap: IndexNodeIdMap = { agents: new Map(), tasks: new Map() };

    // Build name→nodeId map for edge creation
    const agentNameToNodeId = new Map<string, string>();

    // Create skeleton agent nodes
    plan.agents.forEach((a, i) => {
      const nodeId = `agent-skeleton-${i}-${Date.now()}`;
      indexMap.agents.set(i, nodeId);
      agentNameToNodeId.set(a.name, nodeId);

      nodes.push({
        id: nodeId,
        type: 'agentNode',
        position: agentPositions[i] || { x: 100, y: 100 + i * 150 },
        data: {
          label: a.name,
          role: a.role,
          loading: true,
        },
      });
    });

    // Build task name→nodeId map for dependency edge creation
    const taskNameToNodeId = new Map<string, string>();

    // Create skeleton task nodes + agent→task edges
    plan.tasks.forEach((t, i) => {
      const nodeId = `task-skeleton-${i}-${Date.now()}`;
      indexMap.tasks.set(i, nodeId);
      taskNameToNodeId.set(t.name, nodeId);

      nodes.push({
        id: nodeId,
        type: 'taskNode',
        position: taskPositions[i] || { x: 400, y: 100 + i * 150 },
        data: {
          label: t.name,
          loading: true,
        },
      });

      // Edge from assigned agent to this task
      if (t.assigned_agent) {
        const agentNodeId = agentNameToNodeId.get(t.assigned_agent);
        if (agentNodeId) {
          const sourceHandle = layoutOrientation === 'vertical' ? 'bottom' : 'right';
          const targetHandle = layoutOrientation === 'vertical' ? 'top' : 'left';
          const edgeStyle = getEdgeStyleConfig(EdgeCategory.AGENT_TO_TASK, false);

          edges.push({
            id: `edge-${agentNodeId}-${nodeId}`,
            source: agentNodeId,
            target: nodeId,
            type: 'default',
            animated: false,
            sourceHandle,
            targetHandle,
            style: edgeStyle,
          });
        }
      }
    });

    // Create task-to-task dependency edges from plan context arrays
    plan.tasks.forEach((t) => {
      if (t.context && Array.isArray(t.context) && t.context.length > 0) {
        const targetNodeId = taskNameToNodeId.get(t.name);
        if (!targetNodeId) return;

        t.context.forEach((depName: string) => {
          const sourceNodeId = taskNameToNodeId.get(depName);
          if (sourceNodeId && sourceNodeId !== targetNodeId) {
            const edgeStyle = getEdgeStyleConfig(EdgeCategory.TASK_TO_TASK, true);

            edges.push({
              id: `dep-edge-${sourceNodeId}-${targetNodeId}`,
              source: sourceNodeId,
              target: targetNodeId,
              type: 'default',
              animated: true,
              style: { ...edgeStyle, opacity: 0.5 },
            });
          }
        });
      }
    });

    // Replace canvas contents
    setNodes(nodes);
    setEdges(edges);

    return indexMap;
  };
}

/**
 * Called on `agent_detail`. Updates a skeleton agent node with full details.
 */
export function updateAgentNodeDetail(
  setNodes: React.Dispatch<React.SetStateAction<Node[]>>,
  setEdges: React.Dispatch<React.SetStateAction<Edge[]>>,
  indexMap: IndexNodeIdMap,
  selectedModel: string,
) {
  return (data: AgentDetailData) => {
    const oldNodeId = indexMap.agents.get(data.index);
    if (!oldNodeId) return;

    const agent = data.agent;
    const newNodeId = `agent-${agent.id}`;

    // Update the index map to the real node id
    indexMap.agents.set(data.index, newNodeId);

    // Replace the skeleton node with the real one (update id, data, remove loading)
    setNodes((prev) =>
      prev.map((n) =>
        n.id === oldNodeId
          ? {
              ...n,
              id: newNodeId,
              data: {
                ...n.data,
                label: (agent.name as string) || n.data.label,
                agentId: agent.id,
                role: agent.role,
                goal: agent.goal,
                backstory: agent.backstory,
                llm: (agent.llm as string) || selectedModel,
                tools: (agent.tools as string[]) || [],
                loading: false,
              },
            }
          : n
      )
    );

    // Update edges that reference the old skeleton id
    setEdges((prev) =>
      prev.map((e) => {
        let updated = e;
        if (e.source === oldNodeId) updated = { ...updated, source: newNodeId, id: updated.id.replace(oldNodeId, newNodeId) };
        if (e.target === oldNodeId) updated = { ...updated, target: newNodeId, id: updated.id.replace(oldNodeId, newNodeId) };
        return updated;
      })
    );
  };
}

/**
 * Called on `task_detail`. Updates a skeleton task node with full details
 * and adds task-to-task dependency edges.
 */
export function updateTaskNodeDetail(
  setNodes: React.Dispatch<React.SetStateAction<Node[]>>,
  setEdges: React.Dispatch<React.SetStateAction<Edge[]>>,
  indexMap: IndexNodeIdMap,
) {
  return (data: TaskDetailData) => {
    const oldNodeId = indexMap.tasks.get(data.index);
    if (!oldNodeId) return;

    const task = data.task;
    const newNodeId = `task-${task.id}`;
    indexMap.tasks.set(data.index, newNodeId);

    setNodes((prev) =>
      prev.map((n) =>
        n.id === oldNodeId
          ? {
              ...n,
              id: newNodeId,
              data: {
                ...n.data,
                label: (task.name as string) || n.data.label,
                taskId: task.id,
                description: task.description,
                expected_output: task.expected_output,
                tools: (task.tools as string[]) || [],
                tool_configs: (task.tool_configs as Record<string, unknown>) || n.data.tool_configs || {},
                context: (task.context as string[]) || [],
                loading: false,
              },
            }
          : n
      )
    );

    // Update edges that reference the old skeleton id
    setEdges((prev) =>
      prev.map((e) => {
        let updated = e;
        if (e.source === oldNodeId) updated = { ...updated, source: newNodeId, id: updated.id.replace(oldNodeId, newNodeId) };
        if (e.target === oldNodeId) updated = { ...updated, target: newNodeId, id: updated.id.replace(oldNodeId, newNodeId) };
        return updated;
      })
    );

    // Add task-to-task dependency edges if context exists
    const context = task.context as string[] | undefined;
    if (context && Array.isArray(context) && context.length > 0) {
      const depEdges: Edge[] = [];
      context.forEach((depTaskId: string) => {
        const sourceNodeId = `task-${depTaskId}`;
        const edgeStyle = getEdgeStyleConfig(EdgeCategory.TASK_TO_TASK, true);
        depEdges.push({
          id: `edge-${sourceNodeId}-${newNodeId}`,
          source: sourceNodeId,
          target: newNodeId,
          sourceHandle: 'right',
          targetHandle: 'left',
          type: 'default',
          animated: true,
          style: edgeStyle,
        });
      });
      if (depEdges.length > 0) {
        setEdges((prev) => [...prev, ...depEdges]);
      }
    }
  };
}

/**
 * Called on `entity_error`. Marks a skeleton node with error state.
 */
export function markNodeError(
  setNodes: React.Dispatch<React.SetStateAction<Node[]>>,
  indexMap: IndexNodeIdMap,
) {
  return (data: EntityErrorData) => {
    const map = data.entity_type === 'agent' ? indexMap.agents : indexMap.tasks;
    const nodeId = map.get(data.index);
    if (!nodeId) return;

    setNodes((prev) =>
      prev.map((n) =>
        n.id === nodeId
          ? {
              ...n,
              data: {
                ...n.data,
                loading: false,
                error: true,
                errorMessage: data.error,
              },
            }
          : n
      )
    );
  };
}

/**
 * Called on `dependencies_resolved`. Adds/updates task-to-task dependency
 * edges using real DB task IDs (replacing any skeleton-based dep edges).
 */
export function addDependencyEdges(
  setNodes: React.Dispatch<React.SetStateAction<Node[]>>,
  setEdges: React.Dispatch<React.SetStateAction<Edge[]>>,
) {
  return (data: DependenciesResolvedData) => {
    const { task_id: targetTaskId, context: depTaskIds } = data;
    if (!depTaskIds || depTaskIds.length === 0) return;

    // Find the actual node IDs for the resolved task IDs.
    // After updateTaskNodeDetail, taskId is stored in node.data.taskId.
    setNodes((nodes) => {
      const targetNode = nodes.find(
        (n) => n.type === 'taskNode' && n.data?.taskId === targetTaskId
      );
      if (!targetNode) return nodes;

      const newEdges: Edge[] = [];

      depTaskIds.forEach((depId) => {
        const sourceNode = nodes.find(
          (n) => n.type === 'taskNode' && n.data?.taskId === depId
        );
        if (!sourceNode) return;

        const edgeStyle = getEdgeStyleConfig(EdgeCategory.TASK_TO_TASK, true);
        newEdges.push({
          id: `dep-edge-${sourceNode.id}-${targetNode.id}`,
          source: sourceNode.id,
          target: targetNode.id,
          type: 'default',
          animated: true,
          style: edgeStyle,
        });
      });

      if (newEdges.length > 0) {
        // Remove any old skeleton dep-edges targeting this node, then add real ones
        setEdges((prev) => {
          const filtered = prev.filter(
            (e) => !(e.id.startsWith('dep-edge-') && e.target === targetNode.id)
          );
          return [...filtered, ...newEdges];
        });
      }

      return nodes;
    });
  };
}