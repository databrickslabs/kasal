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
  ListItemButton,
  Tooltip,
  Stack,
  Menu,
  MenuItem,
} from '@mui/material';
import ArrowUpwardIcon from '@mui/icons-material/ArrowUpward';
import SmartToyIcon from '@mui/icons-material/SmartToy';
import PersonIcon from '@mui/icons-material/Person';
import GroupIcon from '@mui/icons-material/Group';
import AssignmentIcon from '@mui/icons-material/Assignment';
import AccountTreeIcon from '@mui/icons-material/AccountTree';
import SettingsIcon from '@mui/icons-material/Settings';
import HistoryIcon from '@mui/icons-material/History';
import AddIcon from '@mui/icons-material/Add';
import DeleteIcon from '@mui/icons-material/Delete';
import RefreshIcon from '@mui/icons-material/Refresh';
import CloseIcon from '@mui/icons-material/Close';
import KeyboardArrowDownIcon from '@mui/icons-material/KeyboardArrowDown';
import ChevronLeftIcon from '@mui/icons-material/ChevronLeft';
import { toast } from 'react-hot-toast';
import DispatcherService, { DispatchResult, ConfigureCrewResult } from '../../api/DispatcherService';
import { useWorkflowStore } from '../../store/workflow';
import { Node, Edge } from 'reactflow';
import { Agent } from '../../types/agent';
import { Task } from '../../types/task';
import { AgentService } from '../../api/AgentService';
import { TaskService } from '../../api/TaskService';
import { ChatHistoryService, SaveMessageRequest, ChatSession, ChatMessage as BackendChatMessage } from '../../api/ChatHistoryService';
import { v4 as uuidv4 } from 'uuid';
import { ModelService } from '../../api/ModelService';
import TraceService from '../../api/TraceService';

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
  type: 'user' | 'assistant' | 'execution' | 'trace';
  content: string;
  timestamp: Date;
  intent?: string;
  confidence?: number;
  result?: unknown;
  isIntermediate?: boolean;
  agentName?: string;
  taskName?: string;
  jobId?: string;
}

interface ModelConfig {
  name: string;
  temperature?: number;
  context_window?: number;
  max_output_tokens?: number;
  enabled: boolean;
  provider?: string;
}

interface WorkflowChatProps {
  onNodesGenerated?: (nodes: Node[], edges: Edge[]) => void;
  onLoadingStateChange?: (isLoading: boolean) => void;
  selectedModel?: string;
  selectedTools?: string[];
  isVisible?: boolean;
  setSelectedModel?: (model: string) => void;
  nodes?: Node[];
  edges?: Edge[];
  onExecuteCrew?: () => void;
  onToggleCollapse?: () => void;
}

const WorkflowChat: React.FC<WorkflowChatProps> = ({
  onNodesGenerated,
  onLoadingStateChange,
  selectedModel = 'databricks-llama-4-maverick',
  selectedTools = [],
  isVisible = true,
  setSelectedModel,
  nodes = [],
  edges = [],
  onExecuteCrew,
  onToggleCollapse,
}) => {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [inputValue, setInputValue] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [sessionId, setSessionId] = useState<string>(uuidv4());
  const [chatSessions, setChatSessions] = useState<ChatSession[]>([]);
  const [showSessionList, setShowSessionList] = useState(false);
  const [isLoadingSessions, setIsLoadingSessions] = useState(false);
  const [_currentSessionName, setCurrentSessionName] = useState('New Chat');
  const [models, setModels] = useState<Record<string, ModelConfig>>({});
  const [isLoadingModels, setIsLoadingModels] = useState(false);
  const [modelMenuAnchor, setModelMenuAnchor] = useState<null | HTMLElement>(null);
  const [executingJobId, setExecutingJobId] = useState<string | null>(null);
  const [lastExecutionJobId, setLastExecutionJobId] = useState<string | null>(null);
  const [processedTraceIds, setProcessedTraceIds] = useState<Set<string>>(new Set());
  const [executionStartTime, setExecutionStartTime] = useState<Date | null>(null);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const { setNodes, setEdges } = useWorkflowStore();
  
  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  // Initialize session on component mount
  useEffect(() => {
    const initializeSession = () => {
      // Generate a new session ID for this chat instance
      const newSessionId = ChatHistoryService.generateSessionId();
      setSessionId(newSessionId);
    };
    
    initializeSession();
  }, []);

  // Load chat history when session is set
  useEffect(() => {
    const loadChatHistory = async () => {
      if (!sessionId) return;
      
      try {
        // For now, we'll start with a fresh session each time
        // In the future, we could implement session persistence or loading previous sessions
        console.log(`Initialized new chat session: ${sessionId}`);
      } catch (error) {
        console.error('Error loading chat history:', error);
        // Don't show error to user, just continue with empty chat
      }
    };

    loadChatHistory();
  }, [sessionId]);

  // Load user's chat sessions
  const loadChatSessions = async () => {
    setIsLoadingSessions(true);
    try {
      const response = await ChatHistoryService.getGroupSessions();
      setChatSessions(response.sessions || []);
    } catch (error) {
      console.error('Error loading chat sessions:', error);
      toast.error('Failed to load chat history');
    } finally {
      setIsLoadingSessions(false);
    }
  };

  // Load messages from a specific session
  const loadSessionMessages = async (selectedSessionId: string) => {
    setIsLoading(true);
    try {
      const response = await ChatHistoryService.getSessionMessages(selectedSessionId);
      
      // Convert backend messages to frontend format
      const loadedMessages: ChatMessage[] = response.messages.map((msg: BackendChatMessage) => ({
        id: msg.id,
        type: msg.message_type as 'user' | 'assistant',
        content: msg.content,
        timestamp: new Date(msg.timestamp),
        intent: msg.intent,
        confidence: msg.confidence ? parseFloat(msg.confidence) : undefined,
        result: msg.generation_result,
      }));
      
      setMessages(loadedMessages);
      setSessionId(selectedSessionId);
      setCurrentSessionName(`Session from ${new Date(response.messages[0]?.timestamp || Date.now()).toLocaleDateString()}`);
      setShowSessionList(false);
      
      // Scroll to bottom after loading messages
      setTimeout(scrollToBottom, 100);
    } catch (error) {
      console.error('Error loading session messages:', error);
      toast.error('Failed to load session messages');
    } finally {
      setIsLoading(false);
    }
  };

  // Create a new chat session
  const startNewChat = () => {
    const newSessionId = ChatHistoryService.generateSessionId();
    setSessionId(newSessionId);
    setMessages([]);
    setCurrentSessionName('New Chat');
    setShowSessionList(false);
  };

  // Notify parent of loading state changes
  useEffect(() => {
    if (onLoadingStateChange) {
      onLoadingStateChange(isLoading);
    }
  }, [isLoading, onLoadingStateChange]);

  // Focus the input when component mounts and when page loads
  useEffect(() => {
    // Multiple focus attempts to ensure it works on page load
    const focusAttempts = [0, 100, 300, 500, 1000];
    const timeouts: NodeJS.Timeout[] = [];
    
    focusAttempts.forEach(delay => {
      const timeoutId = setTimeout(() => {
        inputRef.current?.focus();
      }, delay);
      timeouts.push(timeoutId);
    });
    
    return () => {
      timeouts.forEach(clearTimeout);
    };
  }, []);

  // Restore focus when loading completes
  useEffect(() => {
    if (!isLoading) {
      const timeoutId = setTimeout(() => {
        inputRef.current?.focus();
      }, 100);
      return () => clearTimeout(timeoutId);
    }
  }, [isLoading]);

  // Focus input when component becomes visible
  useEffect(() => {
    if (isVisible) {
      const timeoutId = setTimeout(() => {
        inputRef.current?.focus();
      }, 100);
      return () => clearTimeout(timeoutId);
    }
  }, [isVisible]);

  // Listen for execution events and capture trace data
  useEffect(() => {
    const handleJobCreated = (event: CustomEvent) => {
      const { jobId, jobName } = event.detail;
      setExecutingJobId(jobId);
      setLastExecutionJobId(jobId); // Track the last execution
      setProcessedTraceIds(new Set()); // Reset processed traces for new execution
      setExecutionStartTime(new Date()); // Track when execution started
      
      // Add execution start message
      const executionMessage: ChatMessage = {
        id: `exec-start-${Date.now()}`,
        type: 'execution',
        content: `🚀 Started execution: ${jobName}`,
        timestamp: new Date(),
        jobId
      };
      
      setMessages(prev => [...prev, executionMessage]);
    };

    const handleJobCompleted = (event: CustomEvent) => {
      const { jobId, result } = event.detail;
      if (jobId === executingJobId) {
        // Add execution completion message
        const completionMessage: ChatMessage = {
          id: `exec-complete-${Date.now()}`,
          type: 'execution',
          content: `✅ Execution completed successfully`,
          timestamp: new Date(),
          result,
          jobId
        };
        
        setMessages(prev => [...prev, completionMessage]);
        
        // Delay clearing execution state to allow final traces to be captured
        setTimeout(() => {
          setExecutingJobId(null);
          setExecutionStartTime(null);
          setProcessedTraceIds(new Set());
        }, 5000); // Wait 5 seconds before stopping polling
      }
    };

    const handleJobFailed = (event: CustomEvent) => {
      const { jobId, error } = event.detail;
      if (jobId === executingJobId) {
        // Add execution failure message
        const failureMessage: ChatMessage = {
          id: `exec-failed-${Date.now()}`,
          type: 'execution',
          content: `❌ Execution failed: ${error}`,
          timestamp: new Date(),
          jobId
        };
        
        setMessages(prev => [...prev, failureMessage]);
        
        // Delay clearing execution state to allow final traces to be captured
        setTimeout(() => {
          setExecutingJobId(null);
          setExecutionStartTime(null);
          setProcessedTraceIds(new Set());
        }, 5000); // Wait 5 seconds before stopping polling
      }
    };

    // Listen for trace updates (intermediate outputs)
    const handleTraceUpdate = (event: CustomEvent) => {
      const { jobId, trace } = event.detail;
      if (jobId === executingJobId && trace) {
        // Add trace message (intermediate output)
        const traceMessage: ChatMessage = {
          id: `trace-${trace.id || Date.now()}`,
          type: 'trace',
          content: typeof trace.output === 'string' ? trace.output : JSON.stringify(trace.output, null, 2),
          timestamp: new Date(trace.created_at || Date.now()),
          isIntermediate: true,
          agentName: trace.agent_name,
          taskName: trace.task_name,
          jobId
        };
        
        setMessages(prev => [...prev, traceMessage]);
      }
    };

    // Add event listeners
    window.addEventListener('jobCreated', handleJobCreated as EventListener);
    window.addEventListener('jobCompleted', handleJobCompleted as EventListener);
    window.addEventListener('jobFailed', handleJobFailed as EventListener);
    window.addEventListener('traceUpdate', handleTraceUpdate as EventListener);

    return () => {
      window.removeEventListener('jobCreated', handleJobCreated as EventListener);
      window.removeEventListener('jobCompleted', handleJobCompleted as EventListener);
      window.removeEventListener('jobFailed', handleJobFailed as EventListener);
      window.removeEventListener('traceUpdate', handleTraceUpdate as EventListener);
    };
  }, [executingJobId, processedTraceIds, executionStartTime]);

  // Monitor traces for the executing job
  const monitorTraces = React.useCallback(async (jobId: string) => {
    try {
      console.log(`[ChatPanel] Monitoring traces for job ${jobId}`);
      
      // Get traces directly for this execution
      const traces = await TraceService.getTraces(jobId);
      
      console.log(`[ChatPanel] Found ${traces?.length || 0} total traces for job ${jobId}`);
      
      if (traces && Array.isArray(traces)) {
        // Filter traces to only show ones that haven't been processed yet
        const relevantTraces = traces.filter(trace => {
          const traceId = `${trace.id}-${trace.created_at}`;
          const isNew = !processedTraceIds.has(traceId);
          return isNew;
        });
        
        console.log(`[ChatPanel] ${relevantTraces.length} new traces to display`);
        
        // Add new traces to chat
        relevantTraces.forEach((trace) => {
          // Extract content from trace output
          let content = '';
          if (typeof trace.output === 'string') {
            content = trace.output;
          } else if (trace.output?.agent_execution && typeof trace.output.agent_execution === 'string') {
            content = trace.output.agent_execution;
          } else if (trace.output?.content && typeof trace.output.content === 'string') {
            content = trace.output.content;
          } else if (trace.output) {
            content = JSON.stringify(trace.output, null, 2);
          }
          
          // Skip empty content
          if (!content.trim()) {
            return;
          }
          
          const traceId = `${trace.id}-${trace.created_at}`;
          const traceMessage: ChatMessage = {
            id: `trace-${traceId}`,
            type: 'trace',
            content,
            timestamp: new Date(trace.created_at || Date.now()),
            isIntermediate: true,
            agentName: trace.agent_name,
            taskName: trace.task_name,
            jobId
          };
          
          setMessages(prev => [...prev, traceMessage]);
          
          // Mark this trace as processed
          setProcessedTraceIds(prev => {
            const newSet = new Set(prev);
            newSet.add(traceId);
            return newSet;
          });
        });
      }
    } catch (error) {
      console.error('[ChatPanel] Error monitoring traces:', error);
      // Log more details about the error
      if (error instanceof Error) {
        console.error('[ChatPanel] Error details:', error.message);
        console.error('[ChatPanel] Error stack:', error.stack);
      }
    }
  }, [processedTraceIds]);

  // Start trace monitoring when execution begins
  useEffect(() => {
    let interval: NodeJS.Timeout | null = null;
    
    if (executingJobId) {
      // Initial check after a short delay
      const initialTimeout = setTimeout(() => monitorTraces(executingJobId), 2000);
      
      // Start polling for traces every 2 seconds (more frequent for better UX)
      interval = setInterval(() => {
        monitorTraces(executingJobId);
      }, 2000);
      
      return () => {
        clearTimeout(initialTimeout);
        if (interval) {
          clearInterval(interval);
        }
      };
    }
    
    return () => {
      if (interval) {
        clearInterval(interval);
      }
    };
  }, [executingJobId, monitorTraces]);

  // Fetch models when component mounts
  useEffect(() => {
    const fetchModels = async () => {
      setIsLoadingModels(true);
      try {
        const modelService = ModelService.getInstance();
        const response = await modelService.getEnabledModels();
        setModels(response as Record<string, ModelConfig>);
      } catch (error) {
        console.error('Error fetching models:', error);
        // Fallback to a default model if fetch fails
        setModels({
          'databricks-llama-4-maverick': {
            name: 'databricks-llama-4-maverick',
            temperature: 0.7,
            context_window: 128000,
            max_output_tokens: 4096,
            enabled: true
          }
        });
      } finally {
        setIsLoadingModels(false);
      }
    };
    fetchModels();
  }, []);

  const getIntentIcon = (intent?: string) => {
    switch (intent) {
      case 'generate_agent':
        return <SmartToyIcon />;
      case 'generate_task':
        return <AssignmentIcon />;
      case 'generate_crew':
        return <GroupIcon />;
      case 'configure_crew':
        return <SettingsIcon />;
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

  // Save message to backend (only user and assistant messages)
  const saveMessageToBackend = async (message: ChatMessage): Promise<void> => {
    if (!sessionId || (message.type !== 'user' && message.type !== 'assistant')) return;
    
    try {
      const saveRequest: SaveMessageRequest = {
        session_id: sessionId,
        message_type: message.type,
        content: message.content,
        intent: message.intent,
        confidence: message.confidence,
        generation_result: message.result
      };

      await ChatHistoryService.saveMessage(saveRequest);
      console.log(`Message saved successfully to session ${sessionId}`);
    } catch (error) {
      console.error('Error saving message to backend:', error);
      // Show a subtle warning but don't interrupt the chat
      toast.error('Failed to save message to history', {
        duration: 2000,
        position: 'bottom-right',
      });
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
        
        // Success feedback shown in chat only
        // Aggressive focus restoration after successful agent creation
        const focusDelays = [100, 300, 500, 800, 1200];
        focusDelays.forEach(delay => {
          setTimeout(() => {
            inputRef.current?.focus();
          }, delay);
        });
      } else {
        throw new Error('Failed to save agent');
      }
    } catch (error) {
      console.error('Error saving agent:', error);
      toast.error(`Failed to save agent "${agentData.name}". Please try again.`);
      // Aggressive focus restoration even after error
      const focusDelays = [100, 300, 500, 800];
      focusDelays.forEach(delay => {
        setTimeout(() => {
          inputRef.current?.focus();
        }, delay);
      });
      
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

  const handleTaskGenerated = async (taskData: GeneratedTask) => {
    try {
      // Check if there are any agents available for auto-assignment
      const { nodes, edges } = useWorkflowStore.getState();
      const agentNodes = nodes.filter(n => n.type === 'agentNode');
      
      let assignedAgentId = "";
      
      // Auto-assign to agents with priority: agents without connections first, then agents with connections
      if (agentNodes.length > 0) {
        // Find agents that don't have any outgoing connections to tasks
        const agentsWithoutConnections = agentNodes.filter(agentNode => {
          const hasTaskConnection = edges.some(edge => 
            edge.source === agentNode.id && 
            nodes.find(n => n.id === edge.target)?.type === 'taskNode'
          );
          return !hasTaskConnection;
        });
        
        // Priority 1: Use agents without connections first
        if (agentsWithoutConnections.length > 0) {
          const agentData = agentsWithoutConnections[0].data;
          assignedAgentId = agentData.agentId || "";
          console.log(`Auto-assigning task "${taskData.name}" to agent "${agentData.label}" (ID: ${assignedAgentId}) - Priority: No connections`);
        } 
        // Priority 2: If no agents without connections, fall back to any available agent
        else if (agentNodes.length > 0) {
          const agentData = agentNodes[0].data;
          assignedAgentId = agentData.agentId || "";
          console.log(`Auto-assigning task "${taskData.name}" to agent "${agentData.label}" (ID: ${assignedAgentId}) - Priority: Has connections (fallback)`);
        }
      }

      // First, persist the task via TaskService (same pattern as handleAgentGenerated)
      const taskToCreate = {
        name: taskData.name,
        description: taskData.description,
        expected_output: taskData.expected_output,
        tools: taskData.tools || [],
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

          // Create the node with the persisted task data
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
          
          // Call onNodesGenerated with the created node
          if (onNodesGenerated) {
            onNodesGenerated([newNode], []);
          }
          
          return updated;
        });
        
        // If task was auto-assigned to an agent, create the visual connection
        if (assignedAgentId) {
          setEdges((edges) => {
            const agentNodeId = `agent-${assignedAgentId}`;
            const taskNodeId = `task-${savedTask.id}`;
            
            // Create the edge between agent and task
            const newEdge: Edge = {
              id: `edge-${agentNodeId}-${taskNodeId}`,
              source: agentNodeId,
              target: taskNodeId,
              type: 'default',
              animated: true,
            };
            
            return [...edges, newEdge];
          });
          
          // Success feedback shown in chat only
        } else {
          // Success feedback shown in chat only
        }
        // Aggressive focus restoration after successful task creation
        const focusDelays = [100, 300, 500, 800, 1200];
        focusDelays.forEach(delay => {
          setTimeout(() => {
            inputRef.current?.focus();
          }, delay);
        });
      } else {
        throw new Error('Failed to save task');
      }
    } catch (error) {
      console.error('Error saving task:', error);
      toast.error(`Failed to save task "${taskData.name}". Please try again.`);
      // Aggressive focus restoration even after error
      const focusDelays = [100, 300, 500, 800];
      focusDelays.forEach(delay => {
        setTimeout(() => {
          inputRef.current?.focus();
        }, delay);
      });
      
      // Still create the node even if saving failed, but with a warning
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
              type: 'default',
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
                type: 'default',
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

    // Success feedback shown in chat only
    // Aggressive focus restoration after crew creation
    const focusDelays = [100, 300, 500, 800, 1200];
    focusDelays.forEach(delay => {
      setTimeout(() => {
        inputRef.current?.focus();
      }, delay);
    });
  };

  const handleConfigureCrew = (configResult: ConfigureCrewResult) => {
    // Extract configuration type and actions from the result
    const { config_type, actions } = configResult;
    
    // Dispatch custom events to open dialogs (following existing pattern in CrewCanvas)
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

    // Show a toast message about what configuration is being opened
    if (config_type === 'general') {
      // Configuration dialogs opened - feedback in chat only
    } else {
      // Configuration dialog opened - feedback in chat only
    }

    // Restore focus to input after dialogs are opened
    setTimeout(() => {
      inputRef.current?.focus();
    }, 500);
  };

  // Smart context detection
  const hasCrewContent = () => {
    const hasAgents = nodes.some(node => node.type === 'agentNode');
    const hasTask = nodes.some(node => node.type === 'taskNode');
    return hasAgents && hasTask;
  };

  const isExecuteCommand = (message: string) => {
    const trimmed = message.trim().toLowerCase();
    return trimmed === 'execute crew' || trimmed === 'ec' || trimmed === 'run' || trimmed === 'execute' || trimmed.startsWith('ec ') || trimmed.startsWith('execute crew ');
  };

  const extractJobIdFromCommand = (message: string): string | null => {
    const trimmed = message.trim().toLowerCase();
    if (trimmed.startsWith('ec ')) {
      return message.trim().substring(3).trim();
    }
    if (trimmed.startsWith('execute crew ')) {
      return message.trim().substring(13).trim();
    }
    return null;
  };

  const handleSendMessage = async () => {
    if (!inputValue.trim() || isLoading) return;

    // Check if user wants to see execution traces
    if (isExecuteCommand(inputValue)) {
      // Add user message for the execute command
      const userMessage: ChatMessage = {
        id: `msg-${Date.now()}`,
        type: 'user',
        content: inputValue,
        timestamp: new Date(),
      };
      
      setMessages(prev => [...prev, userMessage]);
      setInputValue('');
      
      // Save user message to backend
      saveMessageToBackend(userMessage);
      
      // Check if user provided a specific job ID
      const specificJobId = extractJobIdFromCommand(inputValue);
      
      // Determine which job ID to use
      let jobIdToFetch: string | null = null;
      if (specificJobId) {
        jobIdToFetch = specificJobId;
      } else if (executingJobId || lastExecutionJobId) {
        jobIdToFetch = executingJobId || lastExecutionJobId;
      }
      
      if (jobIdToFetch) {
        setIsLoading(true);
        
        try {
          // Fetch all traces for this job
          const traces = await TraceService.getTraces(jobIdToFetch);
          
          if (traces && traces.length > 0) {
            // Add assistant message indicating we're showing traces
            const assistantMessage: ChatMessage = {
              id: `msg-${Date.now() + 1}`,
              type: 'assistant',
              content: `Showing ${traces.length} execution traces for job ${jobIdToFetch}:`,
              timestamp: new Date(),
            };
            setMessages(prev => [...prev, assistantMessage]);
            
            // Add each trace as a separate message
            traces.forEach((trace, index) => {
              // Extract content from trace output
              let content = '';
              if (typeof trace.output === 'string') {
                content = trace.output;
              } else if (trace.output?.agent_execution && typeof trace.output.agent_execution === 'string') {
                content = trace.output.agent_execution;
              } else if (trace.output?.content && typeof trace.output.content === 'string') {
                content = trace.output.content;
              } else if (trace.output) {
                content = JSON.stringify(trace.output, null, 2);
              }
              
              // Skip empty content
              if (!content.trim()) {
                return;
              }
              
              const traceMessage: ChatMessage = {
                id: `trace-display-${trace.id}-${index}`,
                type: 'trace',
                content,
                timestamp: new Date(trace.created_at || Date.now()),
                isIntermediate: false, // Not intermediate since this is intentional display
                agentName: trace.agent_name,
                taskName: trace.task_name,
                jobId: jobIdToFetch || undefined
              };
              
              setMessages(prev => [...prev, traceMessage]);
            });
          } else {
            // No traces found
            const assistantMessage: ChatMessage = {
              id: `msg-${Date.now() + 1}`,
              type: 'assistant',
              content: `No execution traces found for job ${jobIdToFetch}.`,
              timestamp: new Date(),
            };
            setMessages(prev => [...prev, assistantMessage]);
          }
        } catch (error) {
          console.error('Error fetching execution traces:', error);
          const errorMessage: ChatMessage = {
            id: `msg-${Date.now() + 1}`,
            type: 'assistant',
            content: 'Failed to fetch execution traces. Please try again.',
            timestamp: new Date(),
          };
          setMessages(prev => [...prev, errorMessage]);
        } finally {
          setIsLoading(false);
        }
        
        return;
      }
      
      // If user has a crew but no recent execution, execute it
      if (hasCrewContent() && onExecuteCrew) {
        onExecuteCrew();
        return;
      }
      
      // No crew or recent execution
      const assistantMessage: ChatMessage = {
        id: `msg-${Date.now() + 1}`,
        type: 'assistant',
        content: 'No recent execution found. Please run a crew first or provide a job ID.',
        timestamp: new Date(),
      };
      setMessages(prev => [...prev, assistantMessage]);
      return;
    }

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

    // Save user message to backend (async, don't wait)
    saveMessageToBackend(userMessage);

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
      
      // Save assistant message to backend (async, don't wait)
      saveMessageToBackend(assistantMessage);

      // Handle the generated result based on intent
      if (result.generation_result) {
        switch (result.dispatcher.intent) {
          case 'generate_agent':
            await handleAgentGenerated(result.generation_result as GeneratedAgent);
            break;
          case 'generate_task':
            await handleTaskGenerated(result.generation_result as GeneratedTask);
            break;
          case 'generate_crew':
            handleCrewGenerated(result.generation_result as GeneratedCrew);
            break;
          case 'configure_crew':
            handleConfigureCrew(result.generation_result as ConfigureCrewResult);
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
      
      // Save error message to backend (async, don't wait)
      saveMessageToBackend(errorMessage);
    } finally {
      setIsLoading(false);
      // Aggressive focus attempts to ensure it sticks after all async operations
      const focusDelays = [0, 50, 100, 200, 300, 500, 800, 1200];
      focusDelays.forEach(delay => {
        setTimeout(() => {
          inputRef.current?.focus();
        }, delay);
      });
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
    <Box sx={{ height: '100%', display: 'flex', flexDirection: 'column', position: 'relative' }}>
      {/* Header with session controls */}
      <Box sx={{ 
        p: 1, 
        borderBottom: 1, 
        borderColor: 'divider',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        backgroundColor: theme => theme.palette.mode === 'dark' ? 'grey.900' : 'grey.50'
      }}>
        <Typography 
          variant="subtitle2" 
          sx={{ 
            display: 'flex', 
            alignItems: 'center', 
            gap: 1,
            fontWeight: 600
          }}
        >
          <SmartToyIcon fontSize="small" />
          Kasal
        </Typography>
        <Stack direction="row" spacing={1}>
          <Tooltip title="New Chat">
            <IconButton size="small" onClick={startNewChat}>
              <AddIcon fontSize="small" />
            </IconButton>
          </Tooltip>
          <Tooltip title="Chat History">
            <IconButton 
              size="small" 
              onClick={() => {
                setShowSessionList(true);
                loadChatSessions();
              }}
            >
              <HistoryIcon fontSize="small" />
            </IconButton>
          </Tooltip>
          <Tooltip title="Collapse Chat">
            <IconButton 
              size="small" 
              onClick={onToggleCollapse}
            >
              <ChevronLeftIcon fontSize="small" />
            </IconButton>
          </Tooltip>
        </Stack>
      </Box>

      {/* Session List - Slides over the chat content */}
      <Box
        sx={{
          position: 'absolute',
          top: 0,
          right: showSessionList ? 0 : '-450px',
          width: 450,
          height: '100%',
          backgroundColor: theme => theme.palette.background.paper,
          boxShadow: theme => showSessionList ? theme.shadows[8] : 'none',
          transition: 'right 0.3s ease-in-out',
          zIndex: 10,
          display: 'flex',
          flexDirection: 'column',
        }}
      >
        {/* Header matching chat panel style */}
        <Box sx={{ 
          p: 1.5, 
          borderBottom: 1, 
          borderColor: 'divider',
          backgroundColor: theme => theme.palette.mode === 'dark' ? 'grey.900' : 'grey.50',
          display: 'flex', 
          alignItems: 'center', 
          justifyContent: 'space-between'
        }}>
          <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>Chat History</Typography>
          <Stack direction="row" spacing={0.5}>
            <Tooltip title="Refresh">
              <IconButton 
                size="small" 
                onClick={loadChatSessions}
                disabled={isLoadingSessions}
              >
                {isLoadingSessions ? <CircularProgress size={20} /> : <RefreshIcon />}
              </IconButton>
            </Tooltip>
            <Tooltip title="Close">
              <IconButton 
                size="small" 
                onClick={() => setShowSessionList(false)}
              >
                <CloseIcon fontSize="small" />
              </IconButton>
            </Tooltip>
          </Stack>
        </Box>
        
        {/* Session list content */}
        <Box sx={{ flex: 1, overflow: 'auto', p: 2 }}>
          {chatSessions.length === 0 ? (
            <Typography variant="body2" color="text.secondary" sx={{ textAlign: 'center', mt: 4 }}>
              No previous chat sessions found
            </Typography>
          ) : (
            <List sx={{ p: 0 }}>
              {chatSessions.map((session) => (
              <ListItemButton
                key={session.session_id}
                onClick={() => loadSessionMessages(session.session_id)}
                selected={session.session_id === sessionId}
                sx={{ 
                  borderRadius: 1, 
                  mb: 1,
                  border: 1,
                  borderColor: 'divider',
                  '&.Mui-selected': {
                    backgroundColor: theme => theme.palette.action.selected,
                    borderColor: theme => theme.palette.primary.main,
                  },
                  '&:hover': {
                    backgroundColor: theme => theme.palette.action.hover,
                  }
                }}
              >
                <ListItemText
                  primary={`Session ${new Date(session.latest_timestamp).toLocaleDateString()}`}
                  secondary={`${new Date(session.latest_timestamp).toLocaleTimeString()} • ${session.message_count || 0} messages`}
                />
                <Tooltip title="Delete Session">
                  <IconButton
                    edge="end"
                    size="small"
                    onClick={async (e) => {
                      e.stopPropagation();
                      try {
                        await ChatHistoryService.deleteSession(session.session_id);
                        toast.success('Session deleted');
                        loadChatSessions();
                      } catch (error) {
                        toast.error('Failed to delete session');
                      }
                    }}
                  >
                    <DeleteIcon fontSize="small" />
                  </IconButton>
                </Tooltip>
              </ListItemButton>
            ))}
            </List>
          )}
        </Box>
      </Box>

      {/* Backdrop for closing when clicking outside */}
      {showSessionList && (
        <Box
          onClick={() => setShowSessionList(false)}
          sx={{
            position: 'absolute',
            top: 0,
            left: -1000,
            right: 450,
            bottom: 0,
            zIndex: 9,
          }}
        />
      )}

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
                      <Avatar sx={{ 
                        bgcolor: message.type === 'user' ? 'primary.main' : 
                                message.type === 'execution' ? 'success.main' :
                                message.type === 'trace' ? 'grey.500' : 'secondary.main' 
                      }}>
                        {message.type === 'user' ? <PersonIcon /> : 
                         message.type === 'execution' ? <AccountTreeIcon /> :
                         message.type === 'trace' ? <PersonIcon /> : <SmartToyIcon />}
                      </Avatar>
                    </ListItemAvatar>
                    <ListItemText
                      primary={
                        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                          <Typography variant="subtitle2">
                            {message.type === 'user' ? 'You' : 
                             message.type === 'execution' ? 'Workflow' :
                             message.type === 'trace' ? (message.agentName || 'Agent') : 'Assistant'}
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
                        <Typography 
                          variant="body2" 
                          sx={{ 
                            mt: 1, 
                            whiteSpace: 'pre-wrap',
                            color: message.isIntermediate ? 'text.secondary' : 'text.primary',
                            fontStyle: message.isIntermediate ? 'italic' : 'normal',
                            opacity: message.isIntermediate ? 0.7 : 1
                          }}
                        >
                          {message.taskName && message.type === 'trace' && (
                            <Typography variant="caption" display="block" sx={{ mb: 0.5, color: 'primary.main' }}>
                              Task: {message.taskName}
                            </Typography>
                          )}
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

      <Paper elevation={3} sx={{ p: 2, borderTop: 1, borderColor: 'divider', borderRadius: 0 }}>
        <Box sx={{ position: 'relative' }}>
          <TextField
            ref={inputRef}
            fullWidth
            variant="outlined"
            placeholder={hasCrewContent() ? "Type 'execute crew' or 'ec'..." : "Describe what you want to create..."}
            value={inputValue}
            onChange={(e) => setInputValue(e.target.value)}
            onKeyDown={(e) => {
              handleKeyPress(e);
              e.stopPropagation(); // Prevent keyboard shortcuts
            }}
            disabled={isLoading}
            multiline
            maxRows={6}
            size="small"
            sx={{
              '& .MuiOutlinedInput-root': {
                paddingRight: '40px', // Just enough space for the send button
                borderColor: hasCrewContent() ? 'primary.main' : undefined,
                borderRadius: 1, // Make text input field edgy/rectangular
                '&:hover': {
                  borderColor: hasCrewContent() ? 'primary.main' : undefined,
                },
              },
            }}
            InputProps={{
              endAdornment: (
                <Box
                  sx={{
                    position: 'absolute',
                    right: 40, // Move further left to avoid send button overlap
                    bottom: 8, // Position at the bottom
                    display: 'flex',
                    alignItems: 'center',
                    gap: 1,
                  }}
                >
                  {/* Subtle Model Selector */}
                  {setSelectedModel && (
                    <Box
                      onClick={(e) => setModelMenuAnchor(e.currentTarget)}
                      sx={{
                        display: 'flex',
                        alignItems: 'center',
                        gap: 0.25,
                        cursor: 'pointer',
                        padding: '2px 6px',
                        borderRadius: 0.5,
                        fontSize: '0.75rem',
                        color: 'text.secondary',
                        transition: 'all 0.2s',
                        backgroundColor: 'rgba(255, 255, 255, 0.8)', // Add background for better visibility
                        '&:hover': {
                          backgroundColor: 'action.hover',
                          color: 'text.primary',
                        },
                      }}
                    >
                      <Typography variant="caption" sx={{ fontSize: '0.75rem' }}>
                        {(() => {
                          const modelName = models[selectedModel]?.name || selectedModel;
                          // Shorten common model names
                          if (modelName.includes('databricks-llama-4-maverick')) return 'Llama 4';
                          if (modelName.includes('databricks-meta-llama-3-1-405b-instruct')) return 'Llama 3';
                          if (modelName.includes('databricks-meta-llama-3-3-70b-instruct')) return 'Llama 3';
                          if (modelName.includes('databricks-llama-70b')) return 'Llama 3 70B';
                          if (modelName.includes('databricks-llama-405b')) return 'Llama 3 405B';
                          if (modelName.includes('gpt-4')) return 'GPT-4';
                          if (modelName.includes('gpt-3.5')) return 'GPT-3.5';
                          if (modelName.includes('claude')) return 'Claude';
                          if (modelName.length > 15) return modelName.substring(0, 15) + '...';
                          return modelName;
                        })()}
                      </Typography>
                      <KeyboardArrowDownIcon sx={{ fontSize: 14 }} />
                    </Box>
                  )}
                  <Menu
                    anchorEl={modelMenuAnchor}
                    open={Boolean(modelMenuAnchor)}
                    onClose={() => setModelMenuAnchor(null)}
                    anchorOrigin={{
                      vertical: 'top',
                      horizontal: 'right',
                    }}
                    transformOrigin={{
                      vertical: 'bottom',
                      horizontal: 'right',
                    }}
                    slotProps={{
                      paper: {
                        sx: {
                          mt: -1,
                          minWidth: 250,
                          maxHeight: 400,
                        },
                      },
                    }}
                  >
                    {isLoadingModels ? (
                      <MenuItem disabled>
                        <CircularProgress size={16} sx={{ mr: 1 }} />
                        Loading models...
                      </MenuItem>
                    ) : Object.keys(models).length === 0 ? (
                      <MenuItem disabled>No models available</MenuItem>
                    ) : (
                      Object.entries(models).map(([key, model]) => (
                        <MenuItem
                          key={key}
                          onClick={() => {
                            if (setSelectedModel) {
                              setSelectedModel(key);
                            }
                            setModelMenuAnchor(null);
                          }}
                          selected={key === selectedModel}
                          sx={{
                            fontSize: '0.813rem',
                            py: 0.75,
                            '&.Mui-selected': {
                              backgroundColor: 'action.selected',
                            },
                          }}
                        >
                          <Box sx={{ width: '100%' }}>
                            <Typography variant="body2" sx={{ fontSize: '0.813rem' }}>
                              {model.name}
                            </Typography>
                            {model.provider && (
                              <Typography
                                variant="caption"
                                sx={{
                                  fontSize: '0.688rem',
                                  color: 'text.secondary',
                                  display: 'block',
                                }}
                              >
                                {model.provider}
                              </Typography>
                            )}
                          </Box>
                        </MenuItem>
                      ))
                    )}
                  </Menu>
                </Box>
              ),
            }}
          />
          {/* Send button positioned inside the text field */}
          <IconButton
            color="primary"
            onClick={handleSendMessage}
            disabled={!inputValue.trim() || isLoading}
            sx={{ 
              position: 'absolute',
              right: 8,
              bottom: 8,
              padding: '6px',
              backgroundColor: 'primary.main',
              color: 'primary.contrastText',
              borderRadius: '50%',
              width: 28,
              height: 28,
              '&:hover': {
                backgroundColor: 'primary.dark',
              },
              '&.Mui-disabled': {
                backgroundColor: 'action.disabledBackground',
                color: 'action.disabled',
              },
            }}
          >
            {isLoading ? <CircularProgress size={16} sx={{ color: 'inherit' }} /> : <ArrowUpwardIcon sx={{ fontSize: 16 }} />}
          </IconButton>
        </Box>
      </Paper>
    </Box>
  );
};

export default WorkflowChat; 