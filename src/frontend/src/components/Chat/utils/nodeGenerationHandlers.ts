import { Node, Edge } from 'reactflow';
import { AgentService } from '../../../api/AgentService';
import { TaskService } from '../../../api/TaskService';
import { Agent } from '../../../types/agent';
import { Task } from '../../../types/task';
import { GeneratedAgent, GeneratedTask, GeneratedCrew, ChatMessage } from '../types';
import { CanvasLayoutManager } from '../../../utils/CanvasLayoutManager';
import { useWorkflowStore } from '../../../store/workflow';
import { ConfigureCrewResult } from '../../../api/DispatcherService';

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
        max_rpm: (agentData.advanced_config?.max_rpm as number) || 3,
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
          provider: (agentData.advanced_config.embedder_config as { provider?: string }).provider || 'openai',
          config: {
            model: ((agentData.advanced_config.embedder_config as { config?: { model?: string } }).config?.model) || 'text-embedding-3-small',
            ...((agentData.advanced_config.embedder_config as { config?: Record<string, unknown> }).config || {})
          }
        } : undefined,
        knowledge_sources: (agentData.advanced_config?.knowledge_sources as Agent['knowledge_sources']) || [],
        function_calling_llm: (agentData.advanced_config?.function_calling_llm as string) || undefined,
      };

      const savedAgent = await AgentService.createAgent(agentToCreate);
      
      if (savedAgent) {
        setNodes((nodes) => {
          const position = layoutManagerRef?.current.getAgentNodePosition(nodes, 'crew') || { x: 100, y: 100 };
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

          const updated = [...nodes, newNode];
          setTimeout(() => {
            window.dispatchEvent(new Event('fitViewToNodesInternal'));
          }, 100);
          
          if (onNodesGenerated) {
            onNodesGenerated([newNode], []);
          }
          
          return updated;
        });
        
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
      setNodes((nodes) => {
        const position = layoutManagerRef?.current.getAgentNodePosition(nodes, 'crew') || { x: 100, y: 100 };
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

        const updated = [...nodes, newNode];
        setTimeout(() => {
          window.dispatchEvent(new Event('fitViewToNodesInternal'));
        }, 100);
        
        if (onNodesGenerated) {
          onNodesGenerated([newNode], []);
        }
        
        return updated;
      });
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
        }
      };

      const savedTask = await TaskService.createTask(taskToCreate);
      
      if (savedTask) {
        setNodes((nodes) => {
          const position = layoutManagerRef?.current.getTaskNodePosition(nodes, 'crew') || { x: 400, y: 100 };
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
              task: savedTask,
            },
          };

          const updated = [...nodes, newNode];
          setTimeout(() => {
            window.dispatchEvent(new Event('fitViewToNodesInternal'));
          }, 100);
          
          if (onNodesGenerated) {
            onNodesGenerated([newNode], []);
          }
          
          return updated;
        });
        
        if (assignedAgentId) {
          setEdges((edges) => {
            const agentNodeId = `agent-${assignedAgentId}`;
            const taskNodeId = `task-${savedTask.id}`;
            const newEdge: Edge = {
              id: `edge-${agentNodeId}-${taskNodeId}`,
              source: agentNodeId,
              target: taskNodeId,
              type: 'default',
              animated: true,
            };
            return [...edges, newEdge];
          });
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
      setNodes((nodes) => {
        const position = layoutManagerRef?.current.getTaskNodePosition(nodes, 'crew') || { x: 400, y: 100 };
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

        const updated = [...nodes, newNode];
        setTimeout(() => {
          window.dispatchEvent(new Event('fitViewToNodesInternal'));
        }, 100);
        
        if (onNodesGenerated) {
          onNodesGenerated([newNode], []);
        }
        
        return updated;
      });
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
    const nodes: Node[] = [];
    const edges: Edge[] = [];
    const agentIdMap = new Map<string, string>();
    
    setLastExecutionJobId(null);
    setExecutingJobId(null);

    const agentCount = crewData.agents?.length || 0;
    const taskCount = crewData.tasks?.length || 0;
    const layoutResult = layoutManagerRef?.current.getCrewLayoutPositions(agentCount, taskCount, 'crew') || 
      { agentPositions: [], taskPositions: [], layoutBounds: null, shouldAutoFit: false };
    const { agentPositions, taskPositions, layoutBounds, shouldAutoFit } = layoutResult;

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
            task: task,
          },
        });

        if (task.agent_id) {
          const agentNodeId = agentIdMap.get(task.agent_id.toString());
          if (agentNodeId) {
            edges.push({
              id: `edge-${agentNodeId}-${taskNodeId}`,
              source: agentNodeId,
              target: taskNodeId,
              type: 'default',
              animated: true,
            });
          }
        }

        if (task.context && Array.isArray(task.context)) {
          task.context.forEach((depTaskId: string) => {
            const sourceTaskId = `task-${depTaskId}`;
            if (nodes.some(n => n.id === sourceTaskId)) {
              edges.push({
                id: `edge-${sourceTaskId}-${taskNodeId}`,
                source: sourceTaskId,
                target: taskNodeId,
                type: 'default',
                animated: true,
                style: { stroke: '#ff9800' },
              });
            }
          });
        }
      });
    }

    setNodes((currentNodes) => [...currentNodes, ...nodes]);
    setEdges((currentEdges) => [...currentEdges, ...edges]);

    if (onNodesGenerated) {
      onNodesGenerated(nodes, edges);
    }

    if (shouldAutoFit) {
      console.log('[CrewGeneration] Auto-fitting view to show all nodes');
      setTimeout(() => {
        window.dispatchEvent(new CustomEvent('autoFitCrewNodes', {
          detail: { 
            layoutBounds,
            zoom: layoutManagerRef?.current.getAutoFitZoom(layoutBounds, 'crew')
          }
        }));
      }, 400);
    } else {
      setTimeout(() => {
        window.dispatchEvent(new Event('fitViewToNodesInternal'));
      }, 100);
    }

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