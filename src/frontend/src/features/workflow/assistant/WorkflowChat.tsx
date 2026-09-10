import { useGroupStore } from '../../../store/groups';
import { usePlanGenerationStore, planGenerationKey, startPlanGeneration, resumePlanGeneration, consumePlanGeneration } from './store/planGenerationStore';
import { applyCrewDispatchResult } from './utils/applyCrewDispatchResult';
import { useGenerationTrace } from './hooks/useGenerationTrace';
import { extractVariablesFromNodes } from './utils/canvasVariables';
import { generateFlowTurn } from './utils/generateFlowTurn';
import { getDefaultModel } from '../../../config/defaultModel';
import React, { useState, useRef, useEffect, useLayoutEffect, useMemo, useCallback } from 'react';
import {
  Box,
  TextField,
  IconButton,
  Paper,
  Typography,
  CircularProgress,
  List,
  ListItemButton,
  ListItemText,
  Tooltip,
  Stack,
} from '@mui/material';
import { Sparkles } from 'lucide-react';
import { improveChatPrompt } from '../../chat/api/prompt';
import ArrowUpwardIcon from '@mui/icons-material/ArrowUpward';
import { apiClient } from '../../../shared/api/client';
import { toast } from 'react-hot-toast';
import { useBuilderExecutionControls } from '../../../store/builderExecutionControls';
import DeleteIcon from '@mui/icons-material/Delete';
import CloseIcon from '@mui/icons-material/Close';
import RefreshIcon from '@mui/icons-material/Refresh';


import type { DispatchResult } from '../../../api/execution/DispatcherService';
import { useThemeStore } from '../../../store/theme';
import { useWorkflowStore } from '../../../store/workflow';
import { useCrewExecutionStore } from '../../../store/crewExecution';
import { useChatMessagesStore, deduplicateMessages } from './store/chatMessagesStore';
import { useKnowledgeConfigStore } from '../../../store/knowledgeConfigStore';
import { useModelConfigStore } from '../../../store/modelConfig';
import { useBuilderCanvasStore } from '../../../app/sessions/builderCanvasStore';
import { Node as FlowNode } from 'reactflow';
import { ChatHistoryService } from '../../../api/chat/ChatHistoryService';
import { ModelService } from '../../../api/config/ModelService';
import TraceService from '../../../api/execution/TraceService';
import { CanvasLayoutManager } from '../canvas/lib/CanvasLayoutManager';
import { buildModelLabels } from '../../../utils/modelDisplay';
import { useUILayoutState, useUILayoutStore } from '../../../store/uiLayout';

// Import types
import {
  WorkflowChatProps,
  ChatMessage,
  ModelConfig
} from './types/index';

// Import utilities
import { hasCrewContent, isExecuteCommand, isExecuteFlowCommand, extractJobIdFromCommand, filterSlashCommands, SlashCommand } from './utils/chatHelpers';
import {
  createAgentGenerationHandler,
  createTaskGenerationHandler,
  createCrewGenerationHandler,
  createCrewSkeletonHandler,
  updateAgentNodeDetail,
  updateTaskNodeDetail,
  markNodeError,
  addDependencyEdges,
  IndexNodeIdMap,
} from './utils/nodeGenerationHandlers';
import { useCrewGenerationSSE, CrewGenerationSSEHandlers, ToolConfigNeededData } from '../../../hooks/global/useCrewGenerationSSE';

// Import hooks
import { useChatSession } from './hooks/useChatSession';
import { useExecutionMonitoring } from './hooks/useExecutionMonitoring';

// Import components
import BuilderRunActivity from './components/BuilderRunActivity';
import { builderTranscript } from './utils/builderTranscript';
import { ChatMessageItem } from './components/ChatMessageItem';
import { KnowledgeFileUpload, KnowledgeFileUploadHandle } from './KnowledgeFileUpload';
import ChatInputPlusMenu from './components/ChatInputPlusMenu';
import SlashCommandMenu from './components/SlashCommandMenu';
import { openConversationCanvas } from './utils/conversationCanvas';
import { CanvasAssistantLayout } from './components/CanvasAssistantLayout';
import { BuilderAssistantHeader } from './components/BuilderAssistantHeader';
import { BuilderAssistantWelcome, BuilderAssistantStarters } from './components/BuilderAssistantWelcome';
import { HtmlPreviewDialog } from './components/HtmlPreviewDialog';

const WorkflowChat: React.FC<WorkflowChatProps> = ({
  layout = 'panel',
  builderMode = 'crew',
  onFlowGenerated,
  onNodesGenerated,
  onLoadingStateChange,
  selectedModel = getDefaultModel(),
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
  const flowRequest = useRef<AbortController | null>(null);
  useEffect(() => {
    setIsLoading(false);
    return () => { flowRequest.current?.abort(); };
  }, [providedChatSessionId, builderMode]);
  const [inputValue, setInputValue] = useState('');
  const [isImproving, setIsImproving] = useState(false);
  const improveRequest = useRef(0);
  useLayoutEffect(() => {
    setIsImproving(false);
    return () => { improveRequest.current += 1; };
  }, [providedChatSessionId]);
  const drafts = useRef(new Map<string, string>());
  const draftSession = useRef(providedChatSessionId);
  const currentDraft = useRef(inputValue);
  currentDraft.current = inputValue;
  useLayoutEffect(() => {
    if (layout !== 'canvas' || providedChatSessionId === draftSession.current) return;
    if (draftSession.current) drafts.current.set(draftSession.current, currentDraft.current);
    draftSession.current = providedChatSessionId;
    setInputValue(providedChatSessionId ? drafts.current.get(providedChatSessionId) || '' : '');
  }, [providedChatSessionId, layout]);
  const [commandHistory, setCommandHistory] = useState<string[]>([]);
  const composerDark = useThemeStore(state => state.isDarkMode);
  const composerColors = {
    surface: composerDark ? '#232930' : '#FFFFFF',
    text: composerDark ? '#E8ECEF' : '#1B1F23',
    secondary: composerDark ? '#A0AAB4' : '#5A6872',
    muted: composerDark ? '#84909C' : '#8D99A4',
    hover: composerDark ? 'rgba(255,255,255,0.07)' : '#F2F4F7',
  };
  const [historyIndex, setHistoryIndex] = useState(-1);
  const [isLoading, setIsLoading] = useState(false);
  const [showSessionList, setShowSessionList] = useState(false);
  const [models, setModels] = useState<Record<string, ModelConfig>>({});
  // Built over the whole set so colliding labels fall back to raw names —
  // three "GPT-5" rows you can't tell apart is worse than three long ids.
  const modelLabels = useMemo(() => buildModelLabels(models), [models]);
  const [isLoadingModels, setIsLoadingModels] = useState(false);

  // rather than in Configuration because it is a per-RUN choice: the value is
  // sent with the execution payload and recorded on the run's own row.
  // Shared with the designer's run control and ChatMode's composer — one answer
  const [showSlashMenu, setShowSlashMenu] = useState(false);
  const [slashMenuIndex, setSlashMenuIndex] = useState(0);
  const [slashFilteredCommands, setSlashFilteredCommands] = useState<SlashCommand[]>([]);

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

  const messagesContentRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  // Lets the composer's "+" menu open the knowledge-file picker, whose input
  // and chips live inside KnowledgeFileUpload.
  const knowledgeUploadRef = useRef<KnowledgeFileUploadHandle>(null);
  const { setNodes, setEdges } = useWorkflowStore();
  const { setInputMode, inputMode, setInputVariables, executeCrew, executeFlow } = useCrewExecutionStore();
  const uiLayoutState = useUILayoutState();
  const { chatPanelSide, setChatPanelSide } = useUILayoutStore();

  const messagesContainerRef = useRef<HTMLDivElement>(null);
  const isUserNearBottomRef = useRef(true);
  const previousScrollTopRef = useRef(0);
  const handleMessagesScroll = () => {
    const el = messagesContainerRef.current;
    if (!el) return;
    if (el.scrollTop < previousScrollTopRef.current - 1) isUserNearBottomRef.current = false;
    else if (el.scrollHeight - el.scrollTop - el.clientHeight <= 4) isUserNearBottomRef.current = true;
    previousScrollTopRef.current = el.scrollTop;
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

  const groupId = useGroupStore(state => state.currentGroupId);
  const visibleSessionRef = useRef(providedChatSessionId || sessionId);
  visibleSessionRef.current = providedChatSessionId || sessionId;
  const generationKey = planGenerationKey(groupId, sessionId);
  const planRequest = usePlanGenerationStore(state => state.requests[generationKey]);
  const planActivity = usePlanGenerationStore(state => state.activityBySession[generationKey]);

  // Subscribe to raw session messages via selector — this guarantees re-renders
  // when addMessage() is called from useExecutionMonitoring or any other source.
  // Previously, destructuring only methods from useChatMessagesStore() did not create
  // a data subscription, so the component could miss re-renders on message updates.
  const rawSessionMessages = useChatMessagesStore(
    state => state.messagesBySession[sessionId]
  );
  const messages = useMemo(
    () => deduplicateMessages(rawSessionMessages || []),
    [rawSessionMessages]
  );

  // Set current session in Zustand store when sessionId changes
  useEffect(() => {
    if (sessionId) {
      setCurrentSession(sessionId);
    }
  }, [sessionId, setCurrentSession]);

  // Create a Zustand-compatible setMessages function
  // CRITICAL: Read latest state from Zustand store (getState()) instead of the
  // render-time `messages` snapshot to avoid stale closure bugs.  When multiple
  // setMessages calls happen in the same render cycle (e.g. adding user prompt
  // then assistant response), using the render snapshot causes earlier messages
  // to be silently overwritten because each call reads the same stale array.
  const setMessages = useCallback((updater: React.SetStateAction<ChatMessage[]>) => {
    if (typeof updater === 'function') {
      const currentMessages = useChatMessagesStore.getState().messagesBySession[sessionId] || [];
      const newMessages = updater(currentMessages);
      setZustandMessages(sessionId, newMessages);
    } else {
      setZustandMessages(sessionId, updater);
    }
  }, [sessionId, setZustandMessages]);

  // A saved job link outlives both the observer and transcript loading. Restore
  // missing anchors so returning to a session never loses its durable trace.
  useEffect(() => {
    if (!planActivity?.length) return;
    setMessages(previous => {
      const linked = new Set(previous.map(message => message.jobId));
      const missing: ChatMessage[] = planActivity.filter(item => !linked.has(item.jobId)).map(item => ({
        id: `generation-trace-${item.jobId}`, type: 'trace', jobId: item.jobId,
        content: 'Design activity', timestamp: new Date(item.createdAt),
      }));
      return missing.length ? [...previous, ...missing] : previous;
    });
  }, [planActivity, setMessages]);

  const {
    executingJobId,
    setExecutingJobId,
    lastExecutionJobId: _lastExecutionJobId,
    setLastExecutionJobId,
    executionStartTime: _executionStartTime,
    markPendingExecution,
  } = useExecutionMonitoring(sessionId, saveMessageToBackend, setMessages);

  const stoppingJobs = useRef(new Set<string>());
  const [stoppingJobId, setStoppingJobId] = useState<string | null>(null);
  const isStopping = !!executingJobId && stoppingJobId === executingJobId;
  const handleStopExecution = useCallback(async () => {
    const jobId = executingJobId;
    if (!jobId || stoppingJobs.current.has(jobId)) return;
    stoppingJobs.current.add(jobId);
    setStoppingJobId(jobId);
    try {
      const { data } = await apiClient.post(`/executions/${jobId}/stop`, {
        stop_type: 'graceful',
        reason: 'Stopped by user',
        preserve_partial_results: true,
      });
      const status = String(data.status || '').toLowerCase();
      const event = status === 'completed' ? 'jobCompleted'
        : status === 'failed' ? 'jobFailed'
        : ['stopped', 'cancelled'].includes(status) ? 'jobStopped' : null;
      if (event) window.dispatchEvent(new CustomEvent(event, {
        detail: { jobId, status, result: data.partial_results, partialResults: data.partial_results, error: data.message },
      }));
    } catch {
      toast.error('Could not stop execution. Please try again.');
    } finally {
      stoppingJobs.current.delete(jobId);
      setStoppingJobId(current => current === jobId ? null : current);
    }
  }, [executingJobId]);

  useEffect(() => {
    if (layout !== 'canvas') return;
    const control = executingJobId ? { jobId: executingJobId, stopping: isStopping, stop: handleStopExecution } : null;
    useBuilderExecutionControls.setState({ [builderMode]: control });
    return () => {
      if (useBuilderExecutionControls.getState()[builderMode] === control) {
        useBuilderExecutionControls.setState({ [builderMode]: null });
      }
    };
  }, [layout, builderMode, executingJobId, isStopping, handleStopExecution]);

  const scrollToBottom = () => {
    const el = messagesContainerRef.current;
    if (!el || !isUserNearBottomRef.current) return;
    // Move only this viewport. Smooth animations restarted on each token keep
    // pulling the reader down even after they try to scroll up.
    el.scrollTop = el.scrollHeight;
    previousScrollTopRef.current = el.scrollTop;
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

  useLayoutEffect(() => {
    isUserNearBottomRef.current = true;
    scrollToBottom();
  }, [sessionId]);

  useLayoutEffect(() => { scrollToBottom(); }, [messages]);

  useEffect(() => {
    if (typeof ResizeObserver === 'undefined') return;
    const observer = new ResizeObserver(() => scrollToBottom());
    if (messagesContentRef.current) observer.observe(messagesContentRef.current);
    if (messagesContainerRef.current) observer.observe(messagesContainerRef.current);
    return () => observer.disconnect();
  }, [sessionId, layout]);

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

  // Listen for chatCommandClick events from clickable slash commands in messages
  useEffect(() => {
    const handleCommandClick = (event: Event) => {
      const { command } = (event as CustomEvent).detail;
      setInputValue(command);
      setTimeout(() => inputRef.current?.focus(), 50);
    };
    window.addEventListener('chatCommandClick', handleCommandClick);
    return () => window.removeEventListener('chatCommandClick', handleCommandClick);
  }, []);

  // Listen for save errors from SaveCrew/SaveFlow and surface them in chat
  useEffect(() => {
    const handleSaveError = (event: Event) => {
      const { message: errorMsg } = (event as CustomEvent).detail;
      const errorMessage: ChatMessage = {
        id: `msg-${Date.now()}`,
        type: 'assistant',
        content: `❌ ${errorMsg}`,
        timestamp: new Date(),
      };
      setMessages(prev => [...prev, errorMessage]);
      saveMessageToBackend(errorMessage);
    };
    window.addEventListener('saveError', handleSaveError);
    return () => window.removeEventListener('saveError', handleSaveError);
  }, [setMessages, saveMessageToBackend]);

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
          [getDefaultModel()]: {
            name: getDefaultModel(),
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

  const generationModel = planRequest?.model || selectedModel;

  // Create handlers using extracted utilities
  const handleAgentGenerated = createAgentGenerationHandler(
    setNodes,
    setMessages,
    generationModel,
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
    generationModel,
    onNodesGenerated,
    layoutManagerRef,
    inputRef
  );

  /** Detach the active tab from its saved crew before generated content
   * replaces the canvas. Without this, the next Save updates the OLD crew
   * record in place (overwriting its content, keeping its old name) instead
   * of opening the save dialog to create a new crew. */
  const detachTabFromSavedCrew = useCallback(() => {
    const { activeCanvasId, getCanvas, clearCanvasCrewInfo } = useBuilderCanvasStore.getState();
    if (!activeCanvasId) return;
    const tab = getCanvas(activeCanvasId);
    if (tab?.savedCrewId) {
      console.log(
        `[WorkflowChat] Detaching tab ${activeCanvasId} from saved crew ${tab.savedCrewId} (new crew generated)`
      );
      clearCanvasCrewInfo(activeCanvasId);
    }
  }, []);

  // ── Progressive crew generation via SSE ──────────────────────────
  const [generationId, setGenerationId] = useState<string | null>(null);
  const { generationCompletedRef, generationTraceId: flowGenerationTraceId, setGenerationTraceId, beginGenerationTrace } = useGenerationTrace(setMessages, saveMessageToBackend);
  const generationTraceId = builderMode === 'crew' ? planRequest?.jobId || null : flowGenerationTraceId;
  const indexMapRef = useRef<IndexNodeIdMap | null>(null);
  const progressMsgIdRef = useRef<string | null>(null);
  const pendingGenieConfigsRef = useRef<ToolConfigNeededData[]>([]);

  /** Append a line to the single progress message instead of creating new ones. */
  const appendProgressLine = useCallback((line: string) => {
    const msgId = progressMsgIdRef.current;
    if (!msgId) return;
    setMessages((prev) =>
      prev.map((m) =>
        m.id === msgId ? { ...m, content: m.content + '\n' + line } : m
      )
    );
  }, [setMessages]);

  const sseHandlers = useMemo<CrewGenerationSSEHandlers>(() => ({
    onPlanReady: (plan) => {
      if (visibleSessionRef.current !== sessionId) return;
      // The generated crew REPLACES the canvas content. Detach the tab from
      // any previously loaded crew, otherwise the next Save silently
      // overwrites that crew (content AND keeps its old name) instead of
      // creating a new one.
      detachTabFromSavedCrew();
      const buildSkeleton = createCrewSkeletonHandler(
        setNodes, setEdges, setLastExecutionJobId, setExecutingJobId, layoutManagerRef
      );
      indexMapRef.current = buildSkeleton(plan);
      pendingGenieConfigsRef.current = [];

      const processLabel = plan.process_type === 'parallel' ? 'parallel' : 'sequential';
      const complexityLabel = plan.complexity || 'standard';
      const msgId = `msg-progress-${generationId}`;
      progressMsgIdRef.current = msgId;
      setMessages((prev) => [
        ...prev.filter(message => message.id !== msgId),
        {
          id: msgId,
          type: 'assistant' as const,
          content: `**Crew Plan** — ${complexityLabel} ${processLabel} · ${plan.agents.length} agent${plan.agents.length === 1 ? '' : 's'} · ${plan.tasks.length} task${plan.tasks.length === 1 ? '' : 's'}`,
          metadata: { catalogName: plan.tasks[0]?.name || plan.agents[0]?.name },
          timestamp: new Date(),
        },
      ]);
    },
    onAgentDetail: (data) => {
      if (visibleSessionRef.current !== sessionId) return;
      if (indexMapRef.current) {
        updateAgentNodeDetail(setNodes, setEdges, indexMapRef.current, generationModel)(data);
      }
      const name = (data.agent.name as string) || `Agent ${data.index + 1}`;
      const role = (data.agent.role as string) || '';
      const goal = (data.agent.goal as string) || '';
      const toolCount = Array.isArray(data.agent.tools) ? data.agent.tools.length : 0;
      const toolsLabel = toolCount > 0 ? ` · ${toolCount} tool${toolCount > 1 ? 's' : ''}` : '';
      appendProgressLine(`\n**${name}** — ${role}${toolsLabel}\n\n${goal}`);
    },
    onTaskDetail: (data) => {
      if (visibleSessionRef.current !== sessionId) return;
      if (indexMapRef.current) {
        updateTaskNodeDetail(setNodes, setEdges, indexMapRef.current)(data);
      }
      const name = (data.task.name as string) || `Task ${data.index + 1}`;
      const desc = (data.task.description as string) || '';
      const shortDesc = desc.length > 120 ? desc.substring(0, 120) + '...' : desc;
      appendProgressLine(`\n${data.index + 1}. **${name}**\n\n   ${shortDesc}`);
    },
    onEntityError: (data) => {
      if (visibleSessionRef.current !== sessionId) return;
      if (indexMapRef.current) {
        markNodeError(setNodes, indexMapRef.current)(data);
      }
      appendProgressLine(`  ⚠ Failed to generate ${data.entity_type} "${data.name}"`);
    },
    onDependenciesResolved: (data) => {
      if (visibleSessionRef.current !== sessionId) return;
      addDependencyEdges(setNodes, setEdges)(data);
    },
    onToolConfigNeeded: (data) => {
      if (visibleSessionRef.current !== sessionId) return;
      pendingGenieConfigsRef.current = [...pendingGenieConfigsRef.current, data];
    },
    onComplete: () => {
      if (visibleSessionRef.current !== sessionId) return;
      generationCompletedRef.current = true;
      setGenerationId(null);
      if (!generationTraceId) setIsLoading(false);
      indexMapRef.current = null;

      // Append success line BEFORE clearing the ref so it still finds the message
      appendProgressLine('\n✓ Crew generated successfully');
      const completedPlanId = progressMsgIdRef.current;
      setMessages(prev => prev.map(message => message.id === completedPlanId ? { ...message, metadata: { ...message.metadata, catalogKind: 'crew' } } : message));
      const completedPlan = useChatMessagesStore.getState().messagesBySession[sessionId]?.find(message => message.id === completedPlanId);
      if (completedPlan) void saveMessageToBackend(completedPlan);
      progressMsgIdRef.current = null;

      // Signal the Play button to pulse
      window.dispatchEvent(new CustomEvent('crew-ready'));

      // Show GenieTool config prompt if any tasks need it
      const hasPendingConfigs = pendingGenieConfigsRef.current.length > 0;
      if (hasPendingConfigs) {
        const configMsgId = `msg-genie-config-${Date.now()}`;
        const configsSnapshot = [...pendingGenieConfigsRef.current];
        pendingGenieConfigsRef.current = [];
        setMessages((prev) => [
          ...prev,
          {
            id: configMsgId,
            type: 'assistant' as const,
            content: '',
            timestamp: new Date(),
            metadata: { type: 'genie_config_needed', configs: configsSnapshot },
          },
        ]);
      }

      // Fit view to show all generated nodes.
      // Use a longer delay when genie config prompts are shown so the
      // user isn't disoriented by a viewport jump while reading the prompt.
      setTimeout(() => {
        window.dispatchEvent(new Event('fitViewToNodesInternal'));
      }, hasPendingConfigs ? 100 : 300);
    },
    onFailed: (data) => {
      if (visibleSessionRef.current !== sessionId) return;
      setGenerationId(null);
      setIsLoading(false);
      indexMapRef.current = null;
      progressMsgIdRef.current = null;
      pendingGenieConfigsRef.current = [];
      setMessages((prev) => [
        ...prev,
        {
          id: `msg-fail-${Date.now()}`,
          type: 'assistant' as const,
          content: `Crew generation failed: ${data.error}`,
          timestamp: new Date(),
        },
      ]);
    },
  }), [setNodes, setEdges, setLastExecutionJobId, setExecutingJobId, setMessages, generationModel, layoutManagerRef, appendProgressLine, detachTabFromSavedCrew, generationTraceId, generationCompletedRef, generationId, sessionId, saveMessageToBackend]);

  useCrewGenerationSSE(generationId, sseHandlers);

  // The request belongs to the session. Only its current view consumes the
  // completed result, so another canvas can never receive these nodes.
  const applyPlanResult = useRef<(result: DispatchResult) => Promise<void>>(async () => {});
  applyPlanResult.current = result => applyCrewDispatchResult(result, {
    generationCompletedRef, detachTabFromSavedCrew, handleCrewGenerated, handleAgentGenerated, handleTaskGenerated,
    setMessages, saveMessageToBackend, setGenerationId, inputRef, onExecuteCrew, nodes, jobId: planRequest?.jobId,
  });
  useLayoutEffect(() => {
    generationCompletedRef.current = false;
    indexMapRef.current = null;
    progressMsgIdRef.current = null;
    pendingGenieConfigsRef.current = [];
    setGenerationId(null);
    setGenerationTraceId(null);
  }, [sessionId, generationCompletedRef, setGenerationTraceId]);
  useEffect(() => {
    if (builderMode !== 'crew' || (providedChatSessionId && sessionId !== providedChatSessionId)) return;
    if (!planRequest) { setIsLoading(false); return; }
    resumePlanGeneration(generationKey);
    setIsLoading(planRequest.status === 'running' || planRequest.status === 'paused');
    setGenerationId(planRequest.status === 'running' ? planRequest.jobId || null : null);
    if (planRequest.status === 'complete' && planRequest.result) {
      consumePlanGeneration(generationKey, planRequest.token);
      void applyPlanResult.current(planRequest.result).catch(error => {
        const message: ChatMessage = { id: `plan-apply-${planRequest.token}`, type: 'assistant', content: `Could not apply the generated plan: ${error instanceof Error ? error.message : 'Please try again.'}`, timestamp: new Date() };
        setMessages(prev => [...prev, message]);
        void saveMessageToBackend(message);
      });
    } else if (planRequest.status === 'failed') {
      consumePlanGeneration(generationKey, planRequest.token);
      const message: ChatMessage = { id: `plan-error-${planRequest.token}`, type: 'assistant', content: planRequest.error || 'Plan generation failed. Please try again.', timestamp: new Date() };
      setMessages(prev => [...prev, message]);
      void saveMessageToBackend(message);
    }
  }, [generationKey, planRequest, builderMode, sessionId, providedChatSessionId, setMessages, saveMessageToBackend]);

  const handleSendMessage = async () => {
    if (!inputValue.trim() || isLoading || generationTraceId) return;

    if (builderMode === 'flow' && !isCollectingVariables && !isExecuteFlowCommand(inputValue) && !/^\/?run(?:\s+(?:the\s+)?flow)?[.!]?$/i.test(inputValue.trim())) {
      await generateFlowTurn({ sessionId, inputValue, selectedModel, nodes, flowRequest, setMessages, setInputValue, setIsLoading, saveMessageToBackend, onFlowGenerated, beginGenerationTrace, setGenerationTraceId });
      return;
    }

    // Push to command history
    setCommandHistory(prev => [...prev, inputValue]);
    setHistoryIndex(-1);

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

        // Mark this session as expecting a job to start
        markPendingExecution();

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

    // Check if user wants to execute a flow
    if (isExecuteFlowCommand(inputValue) || (builderMode === 'flow' && /^\/?run(?:\s+(?:the\s+)?flow)?[.!]?$/i.test(inputValue.trim()))) {
      const userMessage: ChatMessage = {
        id: `msg-${Date.now()}`,
        type: 'user',
        content: inputValue,
        timestamp: new Date(),
      };
      setMessages(prev => [...prev, userMessage]);
      setInputValue('');
      saveMessageToBackend(userMessage);

      markPendingExecution();
      useUILayoutStore.getState().setAssistantPanelVisible(true);
      window.dispatchEvent(new CustomEvent('executeFlowEvent'));

      const pendingMessage: ChatMessage = {
        id: `exec-pending-${Date.now()}`,
        type: 'execution',
        content: '⏳ Preparing to execute flow...',
        timestamp: new Date(),
      };
      setMessages(prev => [...prev, pendingMessage]);
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
    // Force scroll to bottom when user sends a message so they always see the response
    isUserNearBottomRef.current = true;

    startPlanGeneration(groupId, sessionId, { message: userMessage.content, model: selectedModel, tools: selectedTools },
      () => saveMessageToBackend(userMessage), jobId => {
        const anchor: ChatMessage = { id: `generation-trace-${jobId}`, type: 'trace', jobId, content: 'Design activity', timestamp: new Date() };
        setMessages(prev => prev.some(message => message.id === anchor.id) ? prev : [...prev, anchor]);
        void saveMessageToBackend(anchor);
      });
  };

  const handleSlashSelect = useCallback((cmd: SlashCommand) => {
    setInputValue(cmd.command + ' ');
    setShowSlashMenu(false);
    setSlashMenuIndex(0);
    inputRef.current?.focus();
  }, []);

  const handleInputChange = useCallback((value: string) => {
    setInputValue(value);
    if (value.startsWith('/')) {
      const filtered = filterSlashCommands(value);
      setSlashFilteredCommands(filtered);
      setShowSlashMenu(filtered.length > 0);
      setSlashMenuIndex(0);
    } else {
      setShowSlashMenu(false);
    }
  }, []);

  const handleKeyPress = (e: React.KeyboardEvent) => {
    e.stopPropagation();

    if (showSlashMenu && slashFilteredCommands.length > 0) {
      if (e.key === 'ArrowUp') {
        e.preventDefault();
        setSlashMenuIndex(prev => (prev <= 0 ? slashFilteredCommands.length - 1 : prev - 1));
        return;
      }
      if (e.key === 'ArrowDown') {
        e.preventDefault();
        setSlashMenuIndex(prev => (prev >= slashFilteredCommands.length - 1 ? 0 : prev + 1));
        return;
      }
      if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        handleSlashSelect(slashFilteredCommands[slashMenuIndex]);
        return;
      }
      if (e.key === 'Escape') {
        e.preventDefault();
        setShowSlashMenu(false);
        return;
      }
    }

    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSendMessage();
    } else if (e.key === 'ArrowUp' && commandHistory.length > 0) {
      e.preventDefault();
      const newIndex = historyIndex === -1 ? commandHistory.length - 1 : Math.max(0, historyIndex - 1);
      setHistoryIndex(newIndex);
      setInputValue(commandHistory[newIndex]);
    } else if (e.key === 'ArrowDown' && historyIndex !== -1) {
      e.preventDefault();
      if (historyIndex >= commandHistory.length - 1) {
        setHistoryIndex(-1);
        setInputValue('');
      } else {
        const newIndex = historyIndex + 1;
        setHistoryIndex(newIndex);
        setInputValue(commandHistory[newIndex]);
      }
    }
  };
  const canImprove = Boolean(inputValue.trim()) && !inputValue.trim().startsWith('/') && !isLoading && !executingJobId && !isImproving;
  const handleImprovePrompt = async () => {
    if (!canImprove) return;
    const draft = inputValue;
    const request = ++improveRequest.current;
    setIsImproving(true);
    try {
      const improved = await improveChatPrompt(draft.trim(), selectedModel);
      // A late rewrite must never replace a newer draft or another canvas's input.
      if (improved && request === improveRequest.current && currentDraft.current === draft && inputRef.current) {
        setInputValue(improved);
        inputRef.current.focus();
      }
    } finally {
      if (request === improveRequest.current) setIsImproving(false);
    }
  };

  const isSendMode = inputValue.trim().length > 0;
  const isActionDisabled = isLoading || !!executingJobId || !isSendMode;



  const handleNewConversation = () => {
    if (layout === 'canvas') {
      openConversationCanvas(undefined, builderMode);
      if (builderMode === 'flow') useUILayoutStore.getState().setFlowPanelTab('crews');
    }
    else startNewChat();
    setShowSessionList(false);
  };
  const handleOpenConversation = (selectedSessionId: string) => {
    if (layout === 'canvas') {
      openConversationCanvas(selectedSessionId);
      setShowSessionList(false);
    } else { void loadSessionMessages(selectedSessionId); }
  };

  const historyContent = (
showSessionList && (
      <Box
        role="region"
        aria-label="Conversation history"
        onKeyDown={e => { if (e.key === 'Escape') { setShowSessionList(false); requestAnimationFrame(() => inputRef.current?.focus()); } }}
        sx={{ flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column' }}
      >
        {/* Session list header and content (simplified for brevity) */}
        <Box sx={{
          p: 2.5,
          borderBottom: 0,
          borderColor: 'divider',
          backgroundColor: 'transparent',
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
                autoFocus
                onClick={() => { setShowSessionList(false); requestAnimationFrame(() => inputRef.current?.focus()); }}
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
                  onClick={() => handleOpenConversation(session.session_id)}
                  disabled={layout === 'canvas' && (isLoading || !!executingJobId)}
                  selected={session.session_id === sessionId}
                  sx={{
                    borderRadius: '12px',
                    mb: 0.5,
                    border: 0,
                    '&.Mui-selected': { backgroundColor: composerColors.hover },
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

      )
  );
  const responseContent = (
<Box
        ref={messagesContainerRef}
        data-testid="builder-conversation-scroll"
        onScroll={handleMessagesScroll}
        onWheel={event => { if (event.deltaY < 0) isUserNearBottomRef.current = false; }}
        onKeyDown={event => { if (['ArrowUp', 'PageUp', 'Home'].includes(event.key)) isUserNearBottomRef.current = false; }}
        sx={{
          flex: (messages.length === 0 && !executingJobId) ? '0 0 auto' : 1,
          mt: (messages.length === 0 && !executingJobId) ? 'auto' : 0,
          minHeight: 0,
          overflow: (messages.length === 0 && !executingJobId) ? 'visible' : 'auto',
          px: (messages.length === 0 && !executingJobId) ? 0 : layout === 'canvas' ? 2 : 2.5,
          py: (messages.length === 0 && !executingJobId) || layout === 'canvas' ? 0 : 2,
          width: '100%',
          maxWidth: '100%',
          position: 'relative',
          ...(layout === 'canvas' && { '& .MuiListItem-root': { py: 0 }, '& .MuiListItemText-root': { my: 0 } }),
          minWidth: 0, // Prevent flex item from growing
          display: showSessionList ? 'none' : 'flex',
          flexDirection: 'column',
        }}>
        <Box ref={messagesContentRef} sx={{ flexShrink: 0 }}>
        {(messages.length === 0 && !executingJobId) ? (
          layout === 'canvas' ? <Box sx={{ p: 2.5 }}><Typography sx={{ fontSize: 13, color: 'text.secondary' }}>Ask Kasal to create or refine your workflow. Responses will appear here.</Typography></Box> : <BuilderAssistantWelcome dark={composerDark} hasNodes={nodes.length > 0} />
        ) : (
          <List sx={{
            width: '100%',
            maxWidth: '100%',
            pt: 0, // Remove top padding
            pb: 0, // Remove bottom padding
          }}>
            {builderTranscript(messages, executingJobId || generationTraceId).map(item => item.kind === 'activity'
              ? <BuilderRunActivity key={`activity-${item.jobId}`} jobId={item.jobId} running={item.jobId === executingJobId || item.jobId === generationTraceId} onOpenLogs={onOpenLogs} />
              : <ChatMessageItem key={item.message.id} message={item.message} onOpenLogs={onOpenLogs} appearance="assistant-panel" dark={composerDark}
                sessionId={sessionId} groupId={groupId || undefined} model={selectedModel} />)}
          </List>
        )}
        </Box>
      </Box>
  );
  const composerContent = (
<Box sx={{ display: showSessionList && layout !== 'canvas' ? 'none' : 'block', px: layout === 'canvas' ? 0 : 2.5, pt: layout === 'canvas' ? 0 : 0.5, pb: layout === 'canvas' || messages.length === 0 ? 0 : 2.5, flexShrink: 0, backgroundColor: 'transparent' }}>
        <Paper
          elevation={0}
          data-testid="builder-composer"
          sx={{
            position: 'relative', p: 2, borderRadius: '24px',
            backgroundColor: composerColors.surface, backgroundImage: 'none', color: composerColors.text,
            boxShadow: composerDark
              ? '0 2px 16px rgba(0,0,0,0.28), 0 0 0 1px rgba(255,255,255,0.06)'
              : '0 4px 20px rgba(16,24,40,0.055), 0 0 0 1px rgba(16,24,40,0.065)',
            transition: 'box-shadow 150ms ease',
            '&:focus-within': {
              boxShadow: composerDark
                ? '0 0 0 2px rgba(160,170,180,0.30), 0 4px 18px rgba(0,0,0,0.28)'
                : '0 0 0 2px rgba(90,104,114,0.16), 0 4px 18px rgba(16,24,40,0.08)',
            },
          }}
        >
          {showSlashMenu && (
            <SlashCommandMenu
              commands={slashFilteredCommands}
              selectedIndex={slashMenuIndex}
              onSelect={handleSlashSelect}
            />
          )}
          <TextField
            inputRef={inputRef}
            fullWidth
            variant="standard"
            placeholder={executingJobId ? "Execution in progress..." : builderMode === 'flow' ? "Describe a flow using your available crews..." : "Describe what you want to create..."}
            value={inputValue}
            onChange={(e) => handleInputChange(e.target.value)}
            onKeyDown={(e) => {
              handleKeyPress(e);
              e.stopPropagation();
            }}
            disabled={isLoading || !!executingJobId}
            multiline
            minRows={2}
            maxRows={6}
            size="small"
            sx={{
              '& .MuiInputBase-root': {
                padding: 0,
                alignItems: 'flex-start', color: composerColors.text,
                backgroundColor: 'transparent', fontSize: '0.9375rem', lineHeight: 1.6,
                '& fieldset': { border: 0 },
                '& textarea::placeholder': { color: composerColors.muted, opacity: 1 },
                '& textarea.Mui-disabled': { WebkitTextFillColor: composerColors.secondary },
              },
              '& .MuiInputBase-inputMultiline': {
                overflowY: 'auto',
                // Hide scrollbar but keep scroll functionality
                scrollbarWidth: 'none', // Firefox
                msOverflowStyle: 'none', // IE/Edge
                '&::-webkit-scrollbar': {
                  display: 'none', // Chrome, Safari, Opera
                },
              },
            }}
            InputProps={{ disableUnderline: true }}
            inputProps={{ 'aria-label': 'Message Kasal' }}
          />
          <Box
            sx={{
              position: 'relative',
              width: '100%',
              mt: 1,
              minHeight: 32,
              display: 'flex',
              flexWrap: 'wrap',
              alignItems: 'center',
              gap: 0.5,
              backgroundColor: 'transparent',
              '& .MuiIconButton-root': { color: composerColors.secondary },
            }}
          >
            {/* The composer's "+" — the ADD-FILES action plus the run
                settings that used to sit in the left sidebar. The
                attachment CHIPS stay below, rendered by the uploader
                itself: an attached file is state that goes out with the
                next message, so hiding it behind a menu is how people
                re-upload a file they already attached. */}


            {/* Knowledge File Upload — trigger hidden (the "+" owns it),
                chips still rendered here. */}
            <Box sx={{ display: 'flex', alignItems: 'center' }}>
              <KnowledgeFileUpload
                ref={knowledgeUploadRef}
                hideTrigger
                executionId={sessionId || 'default'}
                groupId={localStorage.getItem('groupId') || 'default'}
                hasAgents={nodes.some(n => n.type === 'agentNode')}
                hasTasks={nodes.some(n => n.type === 'taskNode')}
                // No Databricks-config gate: uploads embed into the
                // LOCAL knowledge store (SQLite, or Lakebase pgvector when
                // deployed) and the search reads the same table, so a
                // memory backend and a knowledge volume are not needed —
                // Chat mode calls this endpoint with no gate at all. The
                // agent/task conditions stay because the canvas WIRES the
                // uploaded file into an agent's knowledge sources and a
                // task's tool config; with neither there is nothing to
                // attach it to.
                disabled={
                  isLoading ||
                  !!executingJobId ||
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


                      // Update the task in the backend
                      if (node.data.taskId) {
                        import('../../../api/workflow/TaskService').then(({ TaskService }) => {
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
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5, ml: 'auto' }}>
            <Tooltip title="Improve this prompt with AI"><span>
              <IconButton aria-label="Improve prompt" disabled={!canImprove} onClick={handleImprovePrompt} sx={{ width: 32, height: 32, borderRadius: '12px', bgcolor: 'background.subtle', color: 'text.secondary' }}>
                {isImproving ? <CircularProgress size={14} color="inherit" /> : <Sparkles size={16} strokeWidth={1.7} />}
              </IconButton>
            </span></Tooltip>
            <ChatInputPlusMenu
              onAddFiles={() => knowledgeUploadRef.current?.open()}
              models={models}
              selectedModel={selectedModel}
              onModelChange={setSelectedModel}
              modelLabels={modelLabels}
              loadingModels={isLoadingModels}
              disabled={isLoading || !!executingJobId}
              attachDisabled={
                !nodes.some(n => n.type === 'agentNode') ||
                !nodes.some(n => n.type === 'taskNode')
              }
              attachDisabledReason={
                !nodes.some(n => n.type === 'agentNode')
                  ? 'Add at least one agent to the canvas before attaching files'
                  : 'Add at least one task to the canvas before attaching files'
              }
            />
            {/* Send beside the settings menu */}
            <IconButton
              aria-label="Send message"
              title={executingJobId ? 'Execution running — use Stop on the canvas' : 'Send message'}
              onClick={handleSendMessage}
              disabled={!!executingJobId || isActionDisabled}
              size="small"
              sx={{
                padding: '4px',
                backgroundColor: composerColors.hover,
                '&&': { color: composerColors.secondary },
                borderRadius: '12px',
                width: 32,
                height: 32,
                minWidth: 32,
                '&:hover': {
                  backgroundColor: composerDark ? '#333C45' : '#E7EBF2',
                },
                '&&.Mui-disabled': {
                  backgroundColor: composerColors.hover,
                  color: composerColors.muted,
                },
              }}
            >
              {isLoading && !executingJobId ? (
                <CircularProgress size={14} sx={{ color: 'inherit' }} />
              ) : (
                <ArrowUpwardIcon sx={{ fontSize: 18 }} />
              )}
            </IconButton>
            </Box>
          </Box>
        </Paper>
      </Box>
  );
  if (layout === 'canvas') return <>
    <CanvasAssistantLayout composer={composerContent} response={responseContent} sessionKey={`${builderMode}:${sessionId}`}
      responseKey={messages[messages.length - 1]?.id} hasMessages={messages.length > 0} busy={isLoading || !!executingJobId} dark={composerDark}
      onHide={onToggleCollapse} />
    <HtmlPreviewDialog />
  </>;

  return (
    <Box
      sx={{
        height: '100%',
        display: 'flex',
        flexDirection: 'column',
        position: 'relative',
        overflowX: 'hidden',
        overflowY: messages.length === 0 && !showSessionList ? 'auto' : 'hidden',
        backgroundColor: composerDark ? '#1B1F23' : '#FFFFFF',
        color: composerColors.text,
        fontFamily: "'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif",
        maxWidth: '100%',
        width: '100%',
      }}>
      {!showSessionList && (
        <BuilderAssistantHeader
          sessionName={currentSessionName}
          side={chatPanelSide}
          dark={composerDark}
          onNewChat={handleNewConversation}
          onHistory={() => { setShowSessionList(true); loadChatSessions(); }}
          onMove={() => setChatPanelSide(chatPanelSide === 'right' ? 'left' : 'right')}
          onCollapse={onToggleCollapse}
        />
      )}

      {historyContent}
      {responseContent}
      {composerContent}
      {messages.length === 0 && !showSessionList && (
        <Box sx={{ flexShrink: 0, mb: 'auto' }}>
          <BuilderAssistantStarters
            dark={composerDark}
            disabled={isLoading || !!executingJobId}
            onPrompt={prompt => { handleInputChange(prompt); inputRef.current?.focus(); }}
          />
        </Box>
      )}
      <HtmlPreviewDialog />
    </Box>
  );
};

export default WorkflowChat;
export type { WorkflowChatProps } from './types';
