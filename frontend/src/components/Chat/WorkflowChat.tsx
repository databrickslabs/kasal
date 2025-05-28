import React, { useState, useRef, useEffect } from 'react';
import {
  Box,
  TextField,
  IconButton,
  Paper,
  Typography,
  CircularProgress,
  Chip,
  Fade,
  List,
  ListItem,
  ListItemText,
  ListItemAvatar,
  Avatar,
  Divider,
} from '@mui/material';
import SendIcon from '@mui/icons-material/Send';
import SmartToyIcon from '@mui/icons-material/SmartToy';
import PersonIcon from '@mui/icons-material/Person';
import GroupIcon from '@mui/icons-material/Group';
import AssignmentIcon from '@mui/icons-material/Assignment';
import AccountTreeIcon from '@mui/icons-material/AccountTree';
import { toast } from 'react-hot-toast';
import DispatcherService, { DispatchResult } from '../../api/DispatcherService';
import { useWorkflowStore } from '../../store/workflow';
import { Node, Edge } from 'reactflow';
import { Agent } from '../../types/agent';
import { Task } from '../../types/task';
import { AgentService } from '../../api/AgentService';

interface GeneratedAgent {
  name: string;
  role: string;
  goal: string;
  backstory: string;
  tools?: string[];
  advanced_config?: {
    llm?: string;
    [key: string]: unknown;
  };
}

interface GeneratedTask {
  name: string;
  description: string;
  expected_output: string;
  tools?: string[];
  advanced_config?: {
    human_input?: boolean;
    async_execution?: boolean;
    [key: string]: unknown;
  };
}

interface GeneratedCrew {
  agents?: Agent[];
  tasks?: Task[];
}

interface ChatMessage {
  id: string;
  type: 'user' | 'assistant';
  content: string;
  timestamp: Date;
  intent?: string;
  confidence?: number;
  result?: unknown;
}

interface WorkflowChatProps {
  onNodesGenerated?: (nodes: Node[], edges: Edge[]) => void;
  selectedModel?: string;
  selectedTools?: string[];
}

const WorkflowChat: React.FC<WorkflowChatProps> = ({
  onNodesGenerated,
  selectedModel = 'databricks-llama-4-maverick',
  selectedTools = [],
}) => {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [inputValue, setInputValue] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  const { setNodes, setEdges } = useWorkflowStore();

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const getIntentIcon = (intent?: string) => {
    switch (intent) {
      case 'generate_agent':
        return <SmartToyIcon />;
      case 'generate_task':
        return <AssignmentIcon />;
      case 'generate_crew':
        return <GroupIcon />;
      default:
        return <AccountTreeIcon />;
    }
  };

  const getIntentColor = (intent?: string): 'primary' | 'secondary' | 'success' | 'default' => {
    switch (intent) {
      case 'generate_agent':
        return 'primary';
      case 'generate_task':
        return 'secondary';
      case 'generate_crew':
        return 'success';
      default:
        return 'default';
    }
  };

  const handleAgentGenerated = async (agentData: GeneratedAgent) => {
    try {
      // First, persist the agent via AgentService
      const agentToCreate = {
        name: agentData.name,
        role: agentData.role,
        goal: agentData.goal,
        backstory: agentData.backstory,
        llm: agentData.advanced_config?.llm || selectedModel,
        tools: agentData.tools || [],
        // Set default values similar to AgentForm
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
          // Find existing agent nodes to determine positioning
          const agentNodes = nodes.filter(n => n.type === 'agentNode');
          
          let position = { x: 100, y: 100 }; // Default position for first agent
          
          if (agentNodes.length > 0) {
            // If there are existing agent nodes, position the new agent below the lowest one
            const lowestAgent = agentNodes.reduce((lowest, current) => 
              current.position.y > lowest.position.y ? current : lowest
            );
            position = {
              x: lowestAgent.position.x, // Same x position as the first/lowest agent
              y: lowestAgent.position.y + 150 // 150px below for clear separation
            };
          }

          // Create the node with the persisted agent data
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
          
          // Call onNodesGenerated with the created node
          if (onNodesGenerated) {
            onNodesGenerated([newNode], []);
          }
          
          return updated;
        });
        
        toast.success(`Agent "${savedAgent.name}" created and saved successfully!`);
      } else {
        throw new Error('Failed to save agent');
      }
    } catch (error) {
      console.error('Error saving agent:', error);
      toast.error(`Failed to save agent "${agentData.name}". Please try again.`);
      
      // Still create the node even if saving failed, but with a warning
      setNodes((nodes) => {
        // Find existing agent nodes to determine positioning
        const agentNodes = nodes.filter(n => n.type === 'agentNode');
        
        let position = { x: 100, y: 100 }; // Default position for first agent
        
        if (agentNodes.length > 0) {
          // If there are existing agent nodes, position the new agent below the lowest one
          const lowestAgent = agentNodes.reduce((lowest, current) => 
            current.position.y > lowest.position.y ? current : lowest
          );
          position = {
            x: lowestAgent.position.x, // Same x position as the first/lowest agent
            y: lowestAgent.position.y + 150 // 150px below for clear separation
          };
        }

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

  const handleTaskGenerated = (taskData: GeneratedTask) => {
    setNodes((nodes) => {
      // Find existing task nodes to determine positioning
      const taskNodes = nodes.filter(n => n.type === 'taskNode');
      const agentNodes = nodes.filter(n => n.type === 'agentNode');
      
      let position = { x: 300, y: 100 };
      
      if (taskNodes.length > 0) {
        // If there are existing task nodes, position the new task below the lowest one
        const lowestTask = taskNodes.reduce((lowest, current) => 
          current.position.y > lowest.position.y ? current : lowest
        );
        position = {
          x: lowestTask.position.x, // Same x position as the lowest task
          y: lowestTask.position.y + 180 // 180px below for clear separation
        };
      } else if (agentNodes.length > 0) {
        // If no task nodes but there are agent nodes, position to the right of the rightmost agent
        const rightmostAgent = agentNodes.reduce((rightmost, current) => 
          current.position.x > rightmost.position.x ? current : rightmost
        );
        position = {
          x: rightmostAgent.position.x + 260, // 260px to the right for a clear gap
          y: rightmostAgent.position.y
        };
      }
      
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
      if (onNodesGenerated) onNodesGenerated([newNode], []);
      toast.success(`Task "${taskData.name}" created successfully!`);
      // Trigger fitView after node is added
      setTimeout(() => {
        window.dispatchEvent(new Event('fitViewToNodesInternal'));
      }, 100);
      return [...nodes, newNode];
    });
  };

  const handleCrewGenerated = (crewData: GeneratedCrew) => {
    const nodes: Node[] = [];
    const edges: Edge[] = [];
    const agentIdMap = new Map<string, string>();

    // Create agent nodes
    if (crewData.agents) {
      crewData.agents.forEach((agent: Agent, index: number) => {
        const nodeId = `agent-${agent.id || Date.now() + index}`;
        agentIdMap.set(agent.id?.toString() || agent.name, nodeId);
        
        nodes.push({
          id: nodeId,
          type: 'agentNode',
          position: { x: 100, y: 100 + index * 150 },
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

    // Create task nodes and edges
    if (crewData.tasks) {
      crewData.tasks.forEach((task: Task, index: number) => {
        const taskNodeId = `task-${task.id || Date.now() + index}`;
        
        nodes.push({
          id: taskNodeId,
          type: 'taskNode',
          position: { x: 400, y: 100 + index * 150 },
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

        // Create edge from agent to task
        if (task.agent_id) {
          const agentNodeId = agentIdMap.get(task.agent_id.toString());
          if (agentNodeId) {
            edges.push({
              id: `edge-${agentNodeId}-${taskNodeId}`,
              source: agentNodeId,
              target: taskNodeId,
              animated: true,
            });
          }
        }

        // Create edges for task dependencies
        if (task.context && Array.isArray(task.context)) {
          task.context.forEach((depTaskId: string) => {
            const sourceTaskId = `task-${depTaskId}`;
            if (nodes.some(n => n.id === sourceTaskId)) {
              edges.push({
                id: `edge-${sourceTaskId}-${taskNodeId}`,
                source: sourceTaskId,
                target: taskNodeId,
                animated: true,
                style: { stroke: '#ff9800' },
              });
            }
          });
        }
      });
    }

    // Add all nodes and edges
    setNodes((currentNodes) => [...currentNodes, ...nodes]);
    setEdges((currentEdges) => [...currentEdges, ...edges]);

    if (onNodesGenerated) {
      onNodesGenerated(nodes, edges);
    }

    toast.success(`Crew with ${nodes.length} components created successfully!`);
  };

  const handleSendMessage = async () => {
    if (!inputValue.trim() || isLoading) return;

    console.log('Sending message:', inputValue); // Debug log

    const userMessage: ChatMessage = {
      id: `msg-${Date.now()}`,
      type: 'user',
      content: inputValue,
      timestamp: new Date(),
    };

    setMessages(prev => [...prev, userMessage]);
    setInputValue('');
    setIsLoading(true);

    try {
      console.log('Calling dispatcher service...'); // Debug log
      const result: DispatchResult = await DispatcherService.dispatch({
        message: userMessage.content,
        model: selectedModel,
        tools: selectedTools,
      });
      console.log('Dispatcher response:', result); // Debug log

      const assistantMessage: ChatMessage = {
        id: `msg-${Date.now() + 1}`,
        type: 'assistant',
        content: getAssistantResponse(result),
        timestamp: new Date(),
        intent: result.dispatcher.intent,
        confidence: result.dispatcher.confidence,
        result: result.generation_result,
      };

      setMessages(prev => [...prev, assistantMessage]);

      // Handle the generated result based on intent
      if (result.generation_result) {
        switch (result.dispatcher.intent) {
          case 'generate_agent':
            await handleAgentGenerated(result.generation_result as GeneratedAgent);
            break;
          case 'generate_task':
            handleTaskGenerated(result.generation_result as GeneratedTask);
            break;
          case 'generate_crew':
            handleCrewGenerated(result.generation_result as GeneratedCrew);
            break;
        }
      }
    } catch (error) {
      console.error('Error processing message:', error);
      toast.error('Failed to process your request. Please try again.');
      
      const errorMessage: ChatMessage = {
        id: `msg-${Date.now() + 1}`,
        type: 'assistant',
        content: 'I encountered an error processing your request. Please try again or rephrase your message.',
        timestamp: new Date(),
      };
      
      setMessages(prev => [...prev, errorMessage]);
    } finally {
      setIsLoading(false);
      inputRef.current?.focus();
    }
  };

  const getAssistantResponse = (result: DispatchResult): string => {
    const { dispatcher, generation_result } = result;
    
    if (dispatcher.intent === 'unknown') {
      return "I'm not sure what you want to create. Please specify if you want to create an agent, a task, or a complete crew.";
    }

    if (!generation_result) {
      return "I understood your request but couldn't generate the result. Please try again.";
    }

    switch (dispatcher.intent) {
      case 'generate_agent': {
        const agent = generation_result as GeneratedAgent;
        return `I've created an agent named "${agent.name}" with the role of ${agent.role}. The agent has been added to your workflow canvas.`;
      }
      case 'generate_task': {
        const task = generation_result as GeneratedTask;
        return `I've created a task named "${task.name}". The task has been added to your workflow canvas.`;
      }
      case 'generate_crew': {
        const crew = generation_result as GeneratedCrew;
        const agentCount = crew.agents?.length || 0;
        const taskCount = crew.tasks?.length || 0;
        return `I've created a crew with ${agentCount} agent${agentCount !== 1 ? 's' : ''} and ${taskCount} task${taskCount !== 1 ? 's' : ''}. All components have been added to your workflow canvas.`;
      }
      default:
        return "Your request has been processed successfully.";
    }
  };

  const handleKeyPress = (e: React.KeyboardEvent) => {
    // Prevent keyboard shortcuts when chat input is focused
    e.stopPropagation();
    
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSendMessage();
    }
  };

  return (
    <Box sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>


      <Box sx={{ flex: 1, overflow: 'auto', p: 2 }}>
        {messages.length === 0 ? (
          <Box sx={{ textAlign: 'center', mt: 4 }}>
            <Typography variant="body2" color="text.secondary" paragraph>
              Try saying something like:
            </Typography>
            <List dense>
              <ListItem>
                <ListItemText 
                  primary="Create an agent that can analyze financial data"
                  secondary="Creates a single agent"
                />
              </ListItem>
              <ListItem>
                <ListItemText 
                  primary="I need a task to summarize documents"
                  secondary="Creates a single task"
                />
              </ListItem>
              <ListItem>
                <ListItemText 
                  primary="Build a research team with a researcher and writer"
                  secondary="Creates a complete crew"
                />
              </ListItem>
            </List>
          </Box>
        ) : (
          <List>
            {messages.map((message, index) => (
              <Fade in key={message.id}>
                <Box>
                  <ListItem alignItems="flex-start" sx={{ px: 0 }}>
                    <ListItemAvatar>
                      <Avatar sx={{ bgcolor: message.type === 'user' ? 'primary.main' : 'secondary.main' }}>
                        {message.type === 'user' ? <PersonIcon /> : <SmartToyIcon />}
                      </Avatar>
                    </ListItemAvatar>
                    <ListItemText
                      primary={
                        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                          <Typography variant="subtitle2">
                            {message.type === 'user' ? 'You' : 'Assistant'}
                          </Typography>
                          {message.intent && (
                            <Chip
                              size="small"
                              icon={getIntentIcon(message.intent)}
                              label={message.intent.replace('generate_', '').replace('_', ' ')}
                              color={getIntentColor(message.intent)}
                              variant="outlined"
                            />
                          )}
                          {message.confidence !== undefined && (
                            <Chip
                              size="small"
                              label={`${Math.round(message.confidence * 100)}%`}
                              variant="outlined"
                            />
                          )}
                        </Box>
                      }
                      secondary={
                        <Typography variant="body2" sx={{ mt: 1, whiteSpace: 'pre-wrap' }}>
                          {message.content}
                        </Typography>
                      }
                    />
                  </ListItem>
                  {index < messages.length - 1 && <Divider variant="inset" component="li" />}
                </Box>
              </Fade>
            ))}
          </List>
        )}
        <div ref={messagesEndRef} />
      </Box>

      <Paper elevation={3} sx={{ p: 2, borderTop: 1, borderColor: 'divider' }}>
        <Box sx={{ display: 'flex', gap: 1 }}>
          <TextField
            ref={inputRef}
            fullWidth
            variant="outlined"
            placeholder="Describe what you want to create..."
            value={inputValue}
            onChange={(e) => setInputValue(e.target.value)}
            onKeyPress={handleKeyPress}
            onKeyDown={(e) => e.stopPropagation()} // Prevent keyboard shortcuts
            disabled={isLoading}
            multiline
            maxRows={4}
            size="small"
          />
          <IconButton
            color="primary"
            onClick={handleSendMessage}
            disabled={!inputValue.trim() || isLoading}
            sx={{ alignSelf: 'flex-end' }}
          >
            {isLoading ? <CircularProgress size={24} /> : <SendIcon />}
          </IconButton>
        </Box>
      </Paper>
    </Box>
  );
};

export default WorkflowChat; 