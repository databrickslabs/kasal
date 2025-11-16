import React, { useState, useRef, useEffect } from 'react';
import {
  Box,
  TextField,
  IconButton,
  Paper,
  Typography,
  CircularProgress,
  List,
  ListItem,
  ListItemButton,
  ListItemText,
  Divider,
  Tooltip,
  Stack,
  Menu,
  MenuItem,
} from '@mui/material';
import ArrowUpwardIcon from '@mui/icons-material/ArrowUpward';
import ChatIcon from '@mui/icons-material/Chat';
import AddIcon from '@mui/icons-material/Add';
import DeleteIcon from '@mui/icons-material/Delete';
import CloseIcon from '@mui/icons-material/Close';
import RefreshIcon from '@mui/icons-material/Refresh';
import KeyboardArrowDownIcon from '@mui/icons-material/KeyboardArrowDown';
import ChevronLeftIcon from '@mui/icons-material/ChevronLeft';
import SwapHorizIcon from '@mui/icons-material/SwapHoriz';

import DispatcherService, { DispatchResult, ConfigureCrewResult } from '../../api/DispatcherService';
import { useWorkflowStore } from '../../store/workflow';
import { useCrewExecutionStore } from '../../store/crewExecution';
import { useChatMessagesStore } from '../../store/chatMessagesStore';
import { useKnowledgeConfigStore } from '../../store/knowledgeConfigStore';
import { useModelConfigStore } from '../../store/modelConfig';
import { Node as FlowNode } from 'reactflow';
import { ChatHistoryService } from '../../api/ChatHistoryService';
import { ModelService } from '../../api/ModelService';
import TraceService from '../../api/TraceService';
import { CanvasLayoutManager } from '../../utils/CanvasLayoutManager';
import { useUILayoutState, useUILayoutStore } from '../../store/uiLayout';

// Import types
import {
  WorkflowChatProps,
  ChatMessage,
  ModelConfig,
  GeneratedAgent,
  GeneratedTask,
  GeneratedCrew
} from './types';

// Import utilities
import { hasCrewContent, isExecuteCommand, extractJobIdFromCommand } from './utils/chatHelpers';
import {
  createAgentGenerationHandler,
  createTaskGenerationHandler,
  createCrewGenerationHandler,
  handleConfigureCrew
} from './utils/nodeGenerationHandlers';

// Import hooks
import { useChatSession } from './hooks/useChatSession';
import { useExecutionMonitoring } from './hooks/useExecutionMonitoring';

// Import components
import { ChatMessageItem } from './components/ChatMessageItem';
import { GroupedTraceMessages } from './components/GroupedTraceMessages';
import { KnowledgeFileUpload } from './KnowledgeFileUpload';

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
  chatSessionId: providedChatSessionId,
  onOpenLogs,
}) => {
  const [inputValue, setInputValue] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [showSessionList, setShowSessionList] = useState(false);
  const [models, setModels] = useState<Record<string, ModelConfig>>({});
  const [isLoadingModels, setIsLoadingModels] = useState(false);
  const [modelMenuAnchor, setModelMenuAnchor] = useState<null | HTMLElement>(null);

  // Variable collection state
  const [isCollectingVariables, setIsCollectingVariables] = useState(false);
  const [variablesToCollect, setVariablesToCollect] = useState<string[]>([]);
  const [collectedVariables, setCollectedVariables] = useState<Record<string, string>>({});
  const [currentVariableIndex, setCurrentVariableIndex] = useState(0);
  const [pendingExecutionType, setPendingExecutionType] = useState<'crew' | 'flow'>('crew');

  // Use Zustand store for knowledge configuration
  const {
    isMemoryBackendConfigured,
    isKnowledgeSourceEnabled,
    checkConfiguration,
  } = useKnowledgeConfigStore();

  // Use Zustand store for model configuration
  const { refreshKey } = useModelConfigStore();

  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const { setNodes, setEdges } = useWorkflowStore();
  const { setInputMode, inputMode, setInputVariables, executeCrew, executeFlow } = useCrewExecutionStore();
  const uiLayoutState = useUILayoutState();
  const { chatPanelSide, setChatPanelSide } = useUILayoutStore();

  const messagesContainerRef = useRef<HTMLDivElement>(null);
  const isUserNearBottomRef = useRef(true);
  const handleMessagesScroll = () => {
    const el = messagesContainerRef.current;
    if (!el) return;
    const threshold = 80;
    const atBottom = el.scrollHeight - el.scrollTop - el.clientHeight <= threshold;
    isUserNearBottomRef.current = atBottom;
  };


  // Create enhanced layout manager instance
  const layoutManagerRef = useRef<CanvasLayoutManager>(
    new CanvasLayoutManager({
      margin: 20,
      minNodeSpacing: 50,
      defaultUIState: {
        chatPanelVisible: isVisible,
        chatPanelCollapsed: false,
        chatPanelWidth: 450,
      }
    })
  );

  // Use Zustand store for messages
  const {
    setMessages: setZustandMessages,
    getDeduplicatedMessages,
    setCurrentSession,
  } = useChatMessagesStore();

  // Use extracted hooks (excluding messages which are now handled by Zustand)
  const {
    sessionId,
    setSessionId: _setSessionId,
    chatSessions,
    setChatSessions: _setChatSessions,
    isLoadingSessions,
    currentSessionName,
    setCurrentSessionName: _setCurrentSessionName,
    saveMessageToBackend,
    loadChatSessions,
    loadSessionMessages,
    startNewChat,
  } = useChatSession(providedChatSessionId);

  // Get messages from Zustand store
  const messages = getDeduplicatedMessages(sessionId);

  // Set current session in Zustand store when sessionId changes
  useEffect(() => {
    if (sessionId) {
      setCurrentSession(sessionId);
    }
  }, [sessionId, setCurrentSession]);

  // Create a Zustand-compatible setMessages function
  const setMessages = (updater: React.SetStateAction<ChatMessage[]>) => {
    if (typeof updater === 'function') {
      const currentMessages = getDeduplicatedMessages(sessionId);
      const newMessages = updater(currentMessages);
      setZustandMessages(sessionId, newMessages);
    } else {
      setZustandMessages(sessionId, updater);
    }
  };

  const {
    executingJobId,
    setExecutingJobId,
    lastExecutionJobId: _lastExecutionJobId,
    setLastExecutionJobId,
    executionStartTime: _executionStartTime,
  } = useExecutionMonitoring(sessionId, saveMessageToBackend, setMessages);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  // Extract variables from nodes
  const extractVariablesFromNodes = (workflowNodes: FlowNode[]): string[] => {
    const variablePattern = /\{([^}]+)\}/g;
    const foundVariables = new Set<string>();

    workflowNodes.forEach(node => {
      if (node.type === 'agentNode' || node.type === 'taskNode') {
        const data = node.data as Record<string, unknown>;
        const fieldsToCheck = [
          data.role,
          data.goal,
          data.backstory,
          data.description,
          data.expected_output,
          data.label
        ];

        fieldsToCheck.forEach(field => {
          if (field && typeof field === 'string') {
            let match;
            variablePattern.lastIndex = 0;
            while ((match = variablePattern.exec(field)) !== null) {
              foundVariables.add(match[1]);
            }
          }
        });
      }
    });

    return Array.from(foundVariables);
  };

  // Update layout manager when UI state changes
  React.useEffect(() => {
    layoutManagerRef.current.updateUIState({
      ...uiLayoutState,
      chatPanelVisible: isVisible,
    });
  }, [isVisible, uiLayoutState]);

  // Update screen dimensions on window resize
  React.useEffect(() => {
    const handleResize = () => {
      if (typeof window !== 'undefined') {
        layoutManagerRef.current.updateScreenDimensions(window.innerWidth, window.innerHeight);
      }
    };

    if (typeof window !== 'undefined') {
      window.addEventListener('resize', handleResize);

      (window as unknown as Record<string, unknown>).debugCanvasLayout = () => {
        const debug = layoutManagerRef.current.getLayoutDebugInfo();
        return debug;
      };

      return () => {
        window.removeEventListener('resize', handleResize);
        delete (window as unknown as Record<string, unknown>).debugCanvasLayout;
      };
    }
  }, []);

  useEffect(() => {
    if (isUserNearBottomRef.current) {
      scrollToBottom();
    }
  }, [messages]);

  // Notify parent of loading state changes
  useEffect(() => {
    if (onLoadingStateChange) {
      onLoadingStateChange(isLoading);
    }
  }, [isLoading, onLoadingStateChange]);

  // Focus management
  useEffect(() => {
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

  useEffect(() => {
    if (!isLoading) {
      const timeoutId = setTimeout(() => {
        inputRef.current?.focus();
      }, 100);
      return () => clearTimeout(timeoutId);
    }
  }, [isLoading]);

  useEffect(() => {
    if (isVisible) {
      const timeoutId = setTimeout(() => {
        inputRef.current?.focus();
      }, 100);
      return () => clearTimeout(timeoutId);
    }
  }, [isVisible]);

  // Fetch models when component mounts or when refreshKey changes
  useEffect(() => {
    const fetchModels = async () => {
      setIsLoadingModels(true);
      try {
        const modelService = ModelService.getInstance();
        const response = await modelService.getEnabledModels();
        setModels(response as Record<string, ModelConfig>);
      } catch (error) {

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
  }, [refreshKey]);

  // Initialize knowledge configuration on mount
  useEffect(() => {
    checkConfiguration();
  }, [checkConfiguration]);

  // Create handlers using extracted utilities
  const handleAgentGenerated = createAgentGenerationHandler(
    setNodes,
    setMessages,
    selectedModel,
    onNodesGenerated,
    layoutManagerRef,
    inputRef
  );

  const handleTaskGenerated = createTaskGenerationHandler(
    setNodes,
    setEdges,
    setMessages,
    onNodesGenerated,
    layoutManagerRef,
    inputRef
  );

  const handleCrewGenerated = createCrewGenerationHandler(
    setNodes,
    setEdges,
    setLastExecutionJobId,
    setExecutingJobId,
    selectedModel,
    onNodesGenerated,
    layoutManagerRef,
    inputRef
  );

  const handleSendMessage = async () => {
    if (!inputValue.trim() || isLoading) return;

    // Check if we're collecting variables
    if (isCollectingVariables && variablesToCollect.length > 0 && currentVariableIndex < variablesToCollect.length) {
      const currentVariable = variablesToCollect[currentVariableIndex];
      const value = inputValue.trim();

      // Save user's response
      const userMessage: ChatMessage = {
        id: `msg-${Date.now()}`,
        type: 'user',
        content: inputValue,
        timestamp: new Date(),
      };
      setMessages(prev => [...prev, userMessage]);
      setInputValue('');
      saveMessageToBackend(userMessage);

      // Store the collected variable
      const updatedVariables = { ...collectedVariables, [currentVariable]: value };
      setCollectedVariables(updatedVariables);

      // Check if we have more variables to collect
      if (currentVariableIndex + 1 < variablesToCollect.length) {
        // Ask for the next variable
        setCurrentVariableIndex(currentVariableIndex + 1);
        const nextVariable = variablesToCollect[currentVariableIndex + 1];

        const promptMessage: ChatMessage = {
          id: `msg-${Date.now() + 1}`,
          type: 'assistant',
          content: `Please provide a value for **{${nextVariable}}**:`,
          timestamp: new Date(),
        };
        setMessages(prev => [...prev, promptMessage]);
        saveMessageToBackend(promptMessage);
      } else {
        // All variables collected, execute the crew
        setIsCollectingVariables(false);
        setInputVariables(updatedVariables);

        const confirmMessage: ChatMessage = {
          id: `msg-${Date.now() + 1}`,
          type: 'assistant',
          content: `✅ All variables collected! Executing ${pendingExecutionType} with:\n${Object.entries(updatedVariables).map(([k, v]) => `- **{${k}}**: ${v}`).join('\n')}`,
          timestamp: new Date(),
        };
        setMessages(prev => [...prev, confirmMessage]);
        saveMessageToBackend(confirmMessage);

        // Execute with the collected variables
        const pendingMessage: ChatMessage = {
          id: `exec-pending-${Date.now()}`,
          type: 'execution',
          content: `⏳ Preparing to execute ${pendingExecutionType}...`,
          timestamp: new Date(),
        };
        setMessages(prev => [...prev, pendingMessage]);

        if (pendingExecutionType === 'crew') {
          await executeCrew(nodes, edges);
        } else {
          await executeFlow(nodes, edges);
        }

        // Reset collection state
        setVariablesToCollect([]);
        setCollectedVariables({});
        setCurrentVariableIndex(0);
      }

      return;
    }

    // Check if user is responding to execution prompt
    const lastMessage = messages[messages.length - 1];
    const isExecutionPromptResponse = lastMessage?.type === 'assistant' &&
                                     lastMessage?.content.includes('Would you like to execute this crew now?');

    if (isExecutionPromptResponse) {
      const response = inputValue.trim().toLowerCase();

      const userMessage: ChatMessage = {
        id: `msg-${Date.now()}`,
        type: 'user',
        content: inputValue,
        timestamp: new Date(),
      };

      setMessages(prev => [...prev, userMessage]);
      setInputValue('');
      saveMessageToBackend(userMessage);

      if (response === 'yes' || response === 'y' || response === 'yeah' || response === 'sure' || response === 'ok' || response === 'okay') {
        if (hasCrewContent(nodes)) {
          // Check if we need to collect variables
          const variables = extractVariablesFromNodes(nodes);

          if (variables.length > 0 && inputMode === 'chat') {
            // Start variable collection in chat mode
            setIsCollectingVariables(true);
            setVariablesToCollect(variables);
            setCollectedVariables({});
            setCurrentVariableIndex(0);
            setPendingExecutionType('crew');

            const introMessage: ChatMessage = {
              id: `msg-${Date.now() + 1}`,
              type: 'assistant',
              content: `I need to collect values for ${variables.length} variable${variables.length > 1 ? 's' : ''} in your workflow.\n\nPlease provide a value for **{${variables[0]}}**:`,
              timestamp: new Date(),
            };
            setMessages(prev => [...prev, introMessage]);
            saveMessageToBackend(introMessage);
          } else if (onExecuteCrew) {
            // No variables or dialog mode, execute normally
            const pendingMessage: ChatMessage = {
              id: `exec-pending-${Date.now()}`,
              type: 'execution',
              content: `⏳ Preparing to execute crew...`,
              timestamp: new Date(),
            };
            setMessages(prev => [...prev, pendingMessage]);
            onExecuteCrew();
          }
        }
      } else {
        const responseMessage: ChatMessage = {
          id: `msg-${Date.now() + 1}`,
          type: 'assistant',
          content: 'No problem! The crew is ready whenever you want to execute it. Just type "execute crew" or "ec" when you\'re ready.',
          timestamp: new Date(),
        };
        setMessages(prev => [...prev, responseMessage]);
        saveMessageToBackend(responseMessage);
      }
      return;
    }

    // Check if user wants to change input mode
    const lowerInput = inputValue.trim().toLowerCase();
    if (lowerInput === 'input mode dialog' || lowerInput === 'input dialog') {
      const userMessage: ChatMessage = {
        id: `msg-${Date.now()}`,
        type: 'user',
        content: inputValue,
        timestamp: new Date(),
      };

      setMessages(prev => [...prev, userMessage]);
      setInputValue('');
      saveMessageToBackend(userMessage);

      setInputMode('dialog');

      const responseMessage: ChatMessage = {
        id: `msg-${Date.now() + 1}`,
        type: 'assistant',
        content: '✅ Input mode changed to Dialog. When executing workflows with variables, a popup dialog will appear to collect all values at once.',
        timestamp: new Date(),
      };
      setMessages(prev => [...prev, responseMessage]);
      saveMessageToBackend(responseMessage);
      return;
    }

    if (lowerInput === 'input mode chat' || lowerInput === 'input chat') {
      const userMessage: ChatMessage = {
        id: `msg-${Date.now()}`,
        type: 'user',
        content: inputValue,
        timestamp: new Date(),
      };

      setMessages(prev => [...prev, userMessage]);
      setInputValue('');
      saveMessageToBackend(userMessage);

      setInputMode('chat');

      const responseMessage: ChatMessage = {
        id: `msg-${Date.now() + 1}`,
        type: 'assistant',
        content: '✅ Input mode changed to Chat. When executing workflows with variables, I will guide you through providing values one by one in the chat.',
        timestamp: new Date(),
      };
      setMessages(prev => [...prev, responseMessage]);
      saveMessageToBackend(responseMessage);
      return;
    }

    // Check if user wants to see execution traces
    if (isExecuteCommand(inputValue)) {
      const userMessage: ChatMessage = {
        id: `msg-${Date.now()}`,
        type: 'user',
        content: inputValue,
        timestamp: new Date(),
      };

      setMessages(prev => [...prev, userMessage]);
      setInputValue('');
      saveMessageToBackend(userMessage);

      const specificJobId = extractJobIdFromCommand(inputValue);

      if (specificJobId) {
        setIsLoading(true);

        try {
          const traces = await TraceService.getTraces(specificJobId);

          if (traces && traces.length > 0) {
            const assistantMessage: ChatMessage = {
              id: `msg-${Date.now() + 1}`,
              type: 'assistant',
              content: `Showing ${traces.length} execution traces for job ${specificJobId}:`,
              timestamp: new Date(),
            };
            setMessages(prev => [...prev, assistantMessage]);

            traces.forEach((trace, index) => {
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

              if (!content.trim()) {
                return;
              }

              const traceMessage: ChatMessage = {
                id: `trace-display-${trace.id}-${index}`,
                type: 'trace',
                content,
                timestamp: new Date(trace.created_at || Date.now()),
                isIntermediate: false,
                eventSource: trace.event_source,
                eventContext: trace.event_context,
                eventType: trace.event_type,
                jobId: specificJobId || undefined
              };

              setMessages(prev => [...prev, traceMessage]);
              saveMessageToBackend(traceMessage);
            });
          } else {
            const assistantMessage: ChatMessage = {
              id: `msg-${Date.now() + 1}`,
              type: 'assistant',
              content: `No execution traces found for job ${specificJobId}.`,
              timestamp: new Date(),
            };
            setMessages(prev => [...prev, assistantMessage]);
          }
        } catch (error) {

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

      if (hasCrewContent(nodes)) {
        // Check if we need to collect variables
        const variables = extractVariablesFromNodes(nodes);

        if (variables.length > 0 && inputMode === 'chat') {
          // Start variable collection in chat mode
          setIsCollectingVariables(true);
          setVariablesToCollect(variables);
          setCollectedVariables({});
          setCurrentVariableIndex(0);
          setPendingExecutionType('crew');

          const introMessage: ChatMessage = {
            id: `msg-${Date.now() + 1}`,
            type: 'assistant',
            content: `I need to collect values for ${variables.length} variable${variables.length > 1 ? 's' : ''} in your workflow.\n\nPlease provide a value for **{${variables[0]}}**:`,
            timestamp: new Date(),
          };
          setMessages(prev => [...prev, introMessage]);
          saveMessageToBackend(introMessage);
        } else if (onExecuteCrew) {
          // No variables or dialog mode, execute normally
          const pendingMessage: ChatMessage = {
            id: `exec-pending-${Date.now()}`,
            type: 'execution',
            content: `⏳ Preparing to execute crew...`,
            timestamp: new Date(),
          };
          setMessages(prev => [...prev, pendingMessage]);

          onExecuteCrew();
        }
        return;
      }

      const assistantMessage: ChatMessage = {
        id: `msg-${Date.now() + 1}`,
        type: 'assistant',
        content: 'No crew found. Please create a crew first using natural language.',
        timestamp: new Date(),
      };
      setMessages(prev => [...prev, assistantMessage]);
      return;
    }



    const userMessage: ChatMessage = {
      id: `msg-${Date.now()}`,
      type: 'user',
      content: inputValue,
      timestamp: new Date(),
    };

    setMessages(prev => [...prev, userMessage]);
    setInputValue('');
    setIsLoading(true);

    saveMessageToBackend(userMessage);

    // Progressive canvas feedback: add temporary placeholder nodes/edges while generating
    let cleanupPlaceholders: (() => void) | null = null;
    const lower = userMessage.content.trim().toLowerCase();
    let cleanupProgress: (() => void) | null = null;
    const wantsCrewOrPlan = /\b(create|build|make|generate|draft|compose|design)\b.*\b(plan|crew|workflow)\b/.test(lower)
      || /\b(plan|crew|workflow)\b.*\b(create|build|make|generate|draft|compose|design)\b/.test(lower)
      || lower.includes('create a plan')
      || lower.includes('create plan');
    const wantsAgent = /\b(create|add|new|make|generate)\b.*\b(agent)\b/.test(lower) || lower.includes('create an agent') || lower.includes('create agent');
    const wantsTask = /\b(create|add|new|make|generate)\b.*\b(task)\b/.test(lower) || lower.includes('create a task') || lower.includes('create task');

    const addTempProgress = (text: string) => {
      const msg: ChatMessage = {
        id: `progress-${Date.now()}`,
        type: 'assistant',
        content: text,
        timestamp: new Date(),
        isIntermediate: true,
      } as ChatMessage;
      setMessages(prev => [...prev, msg]);
      return () => setMessages(prev => prev.filter(m => m.id !== msg.id));
    };

    try {
      // Add placeholders based on intent keywords so users see progress immediately
      if (wantsCrewOrPlan) {
        cleanupProgress = addTempProgress('Generating crew with agents and tasks...');

        const now = Date.now();
        const tempAgentId = `agent-temp-${now}`;
        const tempTaskId = `task-temp-${now}`;

        // Add agent placeholder
        setNodes((cur) => {
          const pos = layoutManagerRef.current.getAgentNodePosition(cur as FlowNode[], 'crew') || { x: 100, y: 100 };
          const n: FlowNode = {
            id: tempAgentId,
            type: 'agentNode',
            position: pos,
            data: { label: 'Creating agent…', loading: true },
          };
          return [...(cur as FlowNode[]), n];
        });
        // Add task placeholder slightly after (subtle motion/progression)
        setTimeout(() => {
          setNodes((cur) => {
            const pos = layoutManagerRef.current.getTaskNodePosition(cur as FlowNode[], 'crew') || { x: 380, y: 100 };
            const n: FlowNode = {
              id: tempTaskId,
              type: 'taskNode',
              position: pos,
              data: { label: 'Creating task…', taskId: tempTaskId, loading: true },
            };
            return [...(cur as FlowNode[]), n];
          });
          // Connect placeholders with animated edge
          setEdges((cur) => [
            ...cur,
            {
              id: `edge-${tempAgentId}-${tempTaskId}`,
              source: tempAgentId,
              target: tempTaskId,
              type: 'default',
              animated: true,
              sourceHandle: 'right',
              targetHandle: 'left',
            },
          ]);
        }, 5000);

        cleanupPlaceholders = () => {
          setEdges((cur) => cur.filter((e) => e.id !== `edge-${tempAgentId}-${tempTaskId}`));
          setNodes((cur) => (cur as FlowNode[]).filter((n) => n.id !== tempAgentId && n.id !== tempTaskId));
        };
      } else if (wantsAgent) {
        cleanupProgress = addTempProgress('Creating agent...');
        const now = Date.now();
        const tempAgentId = `agent-temp-${now}`;
        setNodes((cur) => {
          const pos = layoutManagerRef.current.getAgentNodePosition(cur as FlowNode[], 'crew') || { x: 100, y: 100 };
          const n: FlowNode = {
            id: tempAgentId,
            type: 'agentNode',
            position: pos,
            data: { label: 'Creating agent…', loading: true },
          };
          return [...(cur as FlowNode[]), n];
        });
        cleanupPlaceholders = () => {
          setNodes((cur) => (cur as FlowNode[]).filter((n) => n.id !== tempAgentId));
        };
      } else if (wantsTask) {
        cleanupProgress = addTempProgress('Creating task...');
        const now = Date.now();
        const tempTaskId = `task-temp-${now}`;
        setNodes((cur) => {
          const pos = layoutManagerRef.current.getTaskNodePosition(cur as FlowNode[], 'crew') || { x: 380, y: 100 };
          const n: FlowNode = {
            id: tempTaskId,
            type: 'taskNode',
            position: pos,
            data: { label: 'Creating task…', taskId: tempTaskId, loading: true },
          };
          return [...(cur as FlowNode[]), n];
        });
        cleanupPlaceholders = () => {
          setNodes((cur) => (cur as FlowNode[]).filter((n) => n.id !== tempTaskId));
        };
      }

      const result: DispatchResult = await DispatcherService.dispatch({
        message: userMessage.content,
        model: selectedModel,
        tools: selectedTools,
      });

      // Remove any temporary progress message
      if (cleanupProgress) {
        cleanupProgress();
        cleanupProgress = null;
      }


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
      saveMessageToBackend(assistantMessage);

      // Remove any temporary placeholders before rendering final nodes
      if (cleanupPlaceholders) {
        cleanupPlaceholders();
        cleanupPlaceholders = null;
      }

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
          case 'generate_plan':
            // Plans work exactly like crews - use the same handler
            handleCrewGenerated(result.generation_result as GeneratedCrew);
            break;
          case 'configure_crew':
            handleConfigureCrew(result.generation_result as ConfigureCrewResult, inputRef);
            break;
        }
      }
    } catch (error) {


      const errorMessage: ChatMessage = {
        id: `msg-${Date.now() + 1}`,
        type: 'assistant',
        content: '❌ Failed to process your request. Please try again or rephrase your message.',
        timestamp: new Date(),
      };

      setMessages(prev => [...prev, errorMessage]);
      saveMessageToBackend(errorMessage);
    } finally {
      // Ensure placeholders/progress are removed on error/cancellation
      if (cleanupPlaceholders) {
        cleanupPlaceholders();
        cleanupPlaceholders = null;
      }
      if (cleanupProgress) {
        cleanupProgress();
        cleanupProgress = null;
      }
      setIsLoading(false);
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
        return `I've created an agent: **${agent.name}** (${agent.role})\n- Goal: ${agent.goal}\n- Backstory: ${agent.backstory}`;
      }
      case 'generate_task': {
        const task = generation_result as GeneratedTask;
        return `I've created a task: **${task.name}**\n- Description: ${task.description}\n- Expected Output: ${task.expected_output}`;
      }
      case 'generate_crew': {
        const crew = generation_result as GeneratedCrew;
        let response = "I've created a crew with:\n";

        if (crew.agents && crew.agents.length > 0) {
          response += "\n**Agents & Tasks:**\n";
          crew.agents.forEach((agent, index) => {
            response += `${index + 1}. **${agent.name}** (${agent.role}) - ${agent.goal}\n`;

            const agentTasks = crew.tasks?.filter((task) =>
              task.agent_id === agent.id || task.agent_id?.toString() === agent.id?.toString()
            ) || [];

            if (agentTasks.length > 0) {
              agentTasks.forEach((task) => {
                response += `   → ${task.name}: ${task.description}\n`;
              });
            }
          });

          const unassignedTasks = crew.tasks?.filter((task) => !task.agent_id) || [];
          if (unassignedTasks.length > 0) {
            response += "\n**Unassigned Tasks:**\n";
            unassignedTasks.forEach((task, index) => {
              response += `${index + 1}. **${task.name}** - ${task.description}\n`;
            });
          }
        }

        response += "\nTo execute plan type either **execute crew** or **ec**";
        return response;
      }
      case 'generate_plan': {
        const crew = generation_result as GeneratedCrew;
        let response = "I've created a plan with:\n";

        if (crew.agents && crew.agents.length > 0) {
          response += "\n**Agents & Tasks:**\n";
          crew.agents.forEach((agent, index) => {
            response += `${index + 1}. **${agent.name}** (${agent.role}) - ${agent.goal}\n`;

            const agentTasks = crew.tasks?.filter((task) =>
              task.agent_id === agent.id || task.agent_id?.toString() === agent.id?.toString()
            ) || [];

            if (agentTasks.length > 0) {
              agentTasks.forEach((task) => {
                response += `   → ${task.name}: ${task.description}\n`;
              });
            }
          });

          const unassignedTasks = crew.tasks?.filter((task) => !task.agent_id) || [];
          if (unassignedTasks.length > 0) {
            response += "\n**Unassigned Tasks:**\n";
            unassignedTasks.forEach((task, index) => {
              response += `${index + 1}. **${task.name}** - ${task.description}\n`;
            });
          }
        }

        response += "\nTo execute plan type either **execute crew** or **ec**";
        return response;
      }
      default:
        return "Your request has been processed successfully.";
    }
  };

  const handleKeyPress = (e: React.KeyboardEvent) => {
    e.stopPropagation();

    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSendMessage();
    }
  };
  const isSendMode = inputValue.trim().length > 0;
  const isActionDisabled = isLoading || !!executingJobId || !isSendMode;



  return (
    <Box
      sx={{
        height: '100%',
        display: 'flex',
        flexDirection: 'column',
        position: 'relative',
        overflow: 'hidden',
        maxWidth: '100%',
        width: '100%',
      }}>
      {/* Header with session controls */}
      <Box sx={{
        p: 1,
        borderBottom: 1,
        borderColor: 'divider',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        backgroundColor: theme => theme.palette.mode === 'dark' ? 'grey.900' : 'grey.50',
        flexShrink: 0,
      }}>
        <Box sx={{ display: 'flex', flexDirection: 'column' }}>
          <Typography
            variant="subtitle2"
            sx={{
              display: 'flex',
              alignItems: 'center',
              gap: 1,
              fontWeight: 600
            }}
          >
            <Box component="img" src="/kasal-icon-16.png" alt="Kasal" sx={{ width: 16, height: 16, borderRadius: 0.5 }} />
            Kasal
          </Typography>
          {currentSessionName !== 'New Chat' && (
            <Typography
              variant="caption"
              sx={{
                color: 'text.secondary',
                ml: 3
              }}
            >
              {currentSessionName}
            </Typography>
          )}
        </Box>
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
              <ChatIcon fontSize="small" />
            </IconButton>
          </Tooltip>
          <Tooltip title={chatPanelSide === 'right' ? 'Move Chat to Left' : 'Move Chat to Right'}>
            <IconButton
              size="small"
              onClick={() => setChatPanelSide(chatPanelSide === 'right' ? 'left' : 'right')}
            >
              <SwapHorizIcon fontSize="small" />
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
        {/* Session list header and content (simplified for brevity) */}
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
                  }}
                >
                  <ListItemText
                    primary={(() => {
                      const sessionJobNames = JSON.parse(localStorage.getItem('chatSessionJobNames') || '{}');
                      const jobName = sessionJobNames[session.session_id];
                      return jobName || `Session ${new Date(session.latest_timestamp).toLocaleDateString()}`;
                    })()}
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
                          const sessionJobNames = JSON.parse(localStorage.getItem('chatSessionJobNames') || '{}');
                          delete sessionJobNames[session.session_id];
                          localStorage.setItem('chatSessionJobNames', JSON.stringify(sessionJobNames));
                          loadChatSessions();
                        } catch (error) {

                          const errorMessage: ChatMessage = {
                            id: `error-${Date.now()}`,
                            type: 'assistant',
                            content: '❌ Failed to delete session. Please try again.',
                            timestamp: new Date(),
                          };
                          setMessages(prev => [...prev, errorMessage]);
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

      <Box
        ref={messagesContainerRef}
        onScroll={handleMessagesScroll}
        sx={{
          flex: 1,
          overflow: 'auto',
          px: 1, // Reduced horizontal padding from 2 to 1
          py: 2, // Keep vertical padding
          width: '100%',
          maxWidth: '100%',
          position: 'relative',
          minWidth: 0, // Prevent flex item from growing
          display: 'flex',
          flexDirection: 'column',
        }}>
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
                  secondary="Complete a plan"
                />
              </ListItem>
            </List>
          </Box>
        ) : (
          <List sx={{
            width: '100%',
            maxWidth: '100%',
            pt: 0, // Remove top padding
            pb: 0, // Remove bottom padding
          }}>
            {(() => {
              // Messages are already deduplicated by Zustand store
              const deduplicatedMessages = messages;

              const filteredMessages = deduplicatedMessages.filter(message => {
                // Filter out execution start and completion messages
                if (message.type === 'execution' && (
                  message.content.includes('🚀 Started execution:') ||
                  message.content.includes('✅ Execution completed successfully')
                )) {
                  return false;
                }
                return true;
              });

              const groupedMessages: (ChatMessage | ChatMessage[])[] = [];
              let currentTraceGroup: ChatMessage[] = [];

              filteredMessages.forEach((message, index) => {
                if (message.type === 'trace') {
                  currentTraceGroup.push(message);
                } else {
                  // If we have accumulated trace messages, add them as a group
                  if (currentTraceGroup.length > 0) {
                    groupedMessages.push([...currentTraceGroup]);
                    currentTraceGroup = [];
                  }
                  // Add the non-trace message
                  groupedMessages.push(message);
                }
              });

              // Don't forget any remaining trace messages
              if (currentTraceGroup.length > 0) {
                groupedMessages.push(currentTraceGroup);
              }

              return groupedMessages.map((item, index) => {
                if (Array.isArray(item)) {
                  // It's a group of trace messages
                  // Use first and last message IDs plus index for uniqueness
                  const groupKey = `trace-group-${item[0].id}-${item[item.length - 1].id}-${index}`;
                  return (
                    <React.Fragment key={groupKey}>
                      <GroupedTraceMessages messages={item} onOpenLogs={onOpenLogs} />
                      {index < groupedMessages.length - 1 && <Divider component="li" sx={{ ml: 0 }} />}
                    </React.Fragment>
                  );
                } else {
                  // It's a regular message
                  return (
                    <React.Fragment key={item.id}>
                      <ChatMessageItem message={item} onOpenLogs={onOpenLogs} />
                      {index < groupedMessages.length - 1 && <Divider component="li" sx={{ ml: 0 }} />}
                    </React.Fragment>
                  );
                }
              });
            })()}
          </List>
        )}
        <div ref={messagesEndRef} />
      </Box>

      <Paper
        elevation={3}
        sx={{ p: 2, borderTop: 1, borderColor: 'divider', borderRadius: 0, flexShrink: 0 }}
      >
        <Box sx={{ position: 'relative' }}>
          <TextField
            ref={inputRef}
            fullWidth
            variant="outlined"
            placeholder={executingJobId ? "Execution in progress..." : "Describe what you want to create..."}
            value={inputValue}
            onChange={(e) => setInputValue(e.target.value)}
            onKeyDown={(e) => {
              handleKeyPress(e);
              e.stopPropagation();
            }}
            disabled={isLoading || !!executingJobId}
            multiline
            maxRows={6}
            size="small"
            sx={{
              '& .MuiOutlinedInput-root': {
                paddingRight: '120px',
                borderRadius: 1,
              },
            }}
            InputProps={{
              endAdornment: (
                <Box
                  sx={{
                    position: 'absolute',
                    right: 35,
                    bottom: 8,
                    display: 'flex',
                    alignItems: 'center',
                    gap: 0.25,
                  }}
                >
                  {/* Knowledge File Upload */}
                  <Box sx={{ display: 'flex', alignItems: 'center' }}>
                    <KnowledgeFileUpload
                      executionId={sessionId || 'default'}
                      groupId={localStorage.getItem('groupId') || 'default'}
                      hasAgents={nodes.some(n => n.type === 'agentNode')}
                      hasTasks={nodes.some(n => n.type === 'taskNode')}
                      disabled={
                        isLoading ||
                        !!executingJobId ||
                        !isMemoryBackendConfigured ||
                        !isKnowledgeSourceEnabled ||
                        !nodes.some(n => n.type === 'agentNode') ||
                        !nodes.some(n => n.type === 'taskNode')
                      }
                      onFilesUploaded={(files) => {
                        console.log('Knowledge files uploaded:', files);
                      }}
                      onTasksUpdated={async (uploadedFilePath) => {
                        console.log('[WorkflowChat] Updating task nodes with file path:', uploadedFilePath);

                        // Find the agent connected to tasks (for agent_id in tool_configs)
                        const agentNode = nodes.find(n => n.type === 'agentNode');
                        const agentId = agentNode?.data?.agentId || agentNode?.id;
                        console.log('[WorkflowChat] Found agent for access control:', agentId);

                        // Update all task nodes to include DatabricksKnowledgeSearchTool with file path in tool_configs
                        const updatedNodes = nodes.map(node => {
                          if (node.type === 'taskNode') {
                            const currentTools = node.data.tools || [];
                            const currentToolConfigs = node.data.tool_configs || {};

                            // Add DatabricksKnowledgeSearchTool to tools array if not present
                            const hasKnowledgeTool = currentTools.includes('DatabricksKnowledgeSearchTool') ||
                                                      currentTools.includes('36');
                            const updatedTools = hasKnowledgeTool ? currentTools : [...currentTools, 'DatabricksKnowledgeSearchTool'];

                            // Add file path AND agent_id to tool_configs for DatabricksKnowledgeSearchTool
                            const existingFilePaths = currentToolConfigs.DatabricksKnowledgeSearchTool?.file_paths || [];
                            const updatedToolConfigs = {
                              ...currentToolConfigs,
                              DatabricksKnowledgeSearchTool: {
                                ...currentToolConfigs.DatabricksKnowledgeSearchTool,
                                file_paths: existingFilePaths.includes(uploadedFilePath)
                                  ? existingFilePaths
                                  : [...existingFilePaths, uploadedFilePath],
                                agent_id: agentId  // Add agent_id for access control filtering
                              }
                            };

                            console.log(`[WorkflowChat] Updated task ${node.data.label}:`, {
                              tools: updatedTools,
                              tool_configs: updatedToolConfigs
                            });

                            // Update the task in the backend
                            if (node.data.taskId) {
                              import('../../api/TaskService').then(({ TaskService }) => {
                                TaskService.updateTask(node.data.taskId, {
                                  tools: updatedTools,
                                  tool_configs: updatedToolConfigs
                                }).catch(err => {
                                  console.error(`Failed to update task ${node.data.taskId}:`, err);
                                });
                              });
                            }

                            return {
                              ...node,
                              data: {
                                ...node.data,
                                tools: updatedTools,
                                tool_configs: updatedToolConfigs
                              }
                            };
                          }
                          return node;
                        });

                        setNodes(updatedNodes as FlowNode[]);
                        console.log('[WorkflowChat] Task nodes updated successfully');
                      }}
                      onAgentsUpdated={(updatedAgents) => {






// Check if any agent has knowledge sources
                        const hasKnowledgeSources = updatedAgents.some(agent =>
                          agent.knowledge_sources && agent.knowledge_sources.length > 0
                        );

                        // Update the canvas nodes with the updated agent data
                        const updatedNodes = nodes.map(node => {
                          if (node.type === 'agentNode') {
                            const updatedAgent = updatedAgents.find(a => {
                              // Try multiple matching strategies
                              const matches =
                                a.id === node.data.agentId ||  // Match by agentId
                                a.id === node.data.id ||        // Match by id
                                (a.id && `agent-${a.id}` === node.id) ||  // Match by node.id pattern
                                `agent-${a.name}` === node.id;  // Match by name pattern

                              return matches;
                            });

                            if (updatedAgent) {
                              return {
                                ...node,
                                data: {
                                  ...node.data,
                                  ...updatedAgent,  // Update all agent fields
                                  agentId: updatedAgent.id,  // Ensure agentId is set
                                  tools: updatedAgent.tools,  // Explicitly set tools array
                                  knowledge_sources: updatedAgent.knowledge_sources  // Explicitly set knowledge_sources
                                }
                              };
                            }
                          }

                          // Update task nodes to add DatabricksKnowledgeSearchTool if knowledge sources exist
                          if (node.type === 'taskNode' && hasKnowledgeSources) {
                            const currentTools = node.data.tools || [];
                            const hasKnowledgeTool = currentTools.includes('DatabricksKnowledgeSearchTool') ||
                                                      currentTools.includes('36');

                            // Add the tool if it doesn't exist
                            if (!hasKnowledgeTool) {
                              return {
                                ...node,
                                data: {
                                  ...node.data,
                                  tools: [...currentTools, 'DatabricksKnowledgeSearchTool']
                                }
                              };
                            }
                          }

                          return node;
                        });
                        setNodes(updatedNodes as FlowNode[]);



                      }}
                      // Pass only agents that are currently on the canvas
                      availableAgents={nodes
                        .filter(node => node.type === 'agentNode')
                        .map(node => {







                          return {
                            ...node.data,
                            id: node.data.agentId || node.data.id  // Ensure we have an ID
                          };
                        })}
                          compact={true}
                        />
                  </Box>
                  {/* Model Selector */}
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
                        backgroundColor: 'rgba(255, 255, 255, 0.8)',
                        maxWidth: '110px',
                        '&:hover': {
                          backgroundColor: 'action.hover',
                          color: 'text.primary',
                        },
                      }}
                    >
                      <Typography
                        variant="caption"
                        sx={{
                          fontSize: '0.75rem',
                          overflow: 'hidden',
                          textOverflow: 'ellipsis',
                          whiteSpace: 'nowrap',
                          maxWidth: '90px'
                        }}
                      >
                        {(() => {
                          const modelName = models[selectedModel]?.name || selectedModel;
                          // Remove databricks- prefix for cleaner display
                          const displayName = modelName.replace(/^databricks-/, '');

                          // Special cases for better readability
                          if (displayName.includes('gpt-oss-120b')) return 'GPT OSS 120B';
                          if (displayName.includes('gpt-oss-20b')) return 'GPT OSS 20B';
                          if (displayName.includes('llama-4-maverick')) return 'Llama 4';
                          if (displayName.includes('meta-llama-3-1-405b')) return 'Llama 3.1 405B';
                          if (displayName.includes('meta-llama-3-3-70b')) return 'Llama 3.3 70B';
                          if (displayName.includes('gpt-5-mini')) return 'GPT-5 Mini';
                          if (displayName.includes('gpt-5-nano')) return 'GPT-5 Nano';
                          if (displayName.includes('gpt-5')) return 'GPT-5';
                          if (displayName.includes('gemini-2-5-pro')) return 'Gemini 2.5 Pro';
                          if (displayName.includes('gemini-2-5-flash')) return 'Gemini 2.5 Flash';
                          if (displayName.includes('claude-sonnet-4-5')) return 'Claude 4.5';
                          if (displayName.includes('claude-sonnet-4')) return 'Claude 4';
                          if (displayName.includes('qwen3-next-80b')) return 'Qwen3 80B';
                          if (displayName.includes('gemma-3-12b')) return 'Gemma 3 12B';
                          if (displayName.length > 15) return displayName.substring(0, 15) + '...';
                          return displayName;
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
                              {(() => {
                                // Remove databricks- prefix for cleaner display
                                const displayName = model.name.replace(/^databricks-/, '');

                                // Special cases for better readability
                                if (displayName.includes('gpt-oss-120b')) return 'GPT OSS 120B';
                                if (displayName.includes('gpt-oss-20b')) return 'GPT OSS 20B';
                                if (displayName.includes('llama-4-maverick')) return 'Llama 4';
                                if (displayName.includes('meta-llama-3-1-405b')) return 'Llama 3.1 405B';
                                if (displayName.includes('meta-llama-3-3-70b')) return 'Llama 3.3 70B';
                                if (displayName.includes('gpt-5-mini')) return 'GPT-5 Mini';
                                if (displayName.includes('gpt-5-nano')) return 'GPT-5 Nano';
                                if (displayName.includes('gpt-5')) return 'GPT-5';
                                if (displayName.includes('gemini-2-5-pro')) return 'Gemini 2.5 Pro';
                                if (displayName.includes('gemini-2-5-flash')) return 'Gemini 2.5 Flash';
                                if (displayName.includes('claude-sonnet-4-5')) return 'Claude Sonnet 4.5';
                                if (displayName.includes('claude-3-7-sonnet')) return 'Claude 3.7 Sonnet';
                                if (displayName.includes('claude-sonnet-4')) return 'Claude Sonnet 4';
                                if (displayName.includes('qwen3-next-80b')) return 'Qwen3 Next 80B';
                                if (displayName.includes('gemma-3-12b')) return 'Gemma 3 12B';
                                return displayName;
                              })()}
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
          {/* Send button */}
              <IconButton
                color="primary"
                onClick={handleSendMessage}
                disabled={isActionDisabled}
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
                {isLoading || executingJobId ? (
                  <CircularProgress size={16} sx={{ color: 'inherit' }} />
                ) : (
                  <ArrowUpwardIcon sx={{ fontSize: 16 }} />
                )}
              </IconButton>
        </Box>
      </Paper>
    </Box>
  );
};

export default WorkflowChat;