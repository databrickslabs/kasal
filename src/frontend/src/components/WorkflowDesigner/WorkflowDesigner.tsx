import React, { useCallback, useEffect, useRef, useState } from 'react';
import {
  ReactFlowProvider as _ReactFlowProvider,
  Node as _Node,
  Edge as _Edge,
  OnSelectionChangeParams as _OnSelectionChangeParams,
  ReactFlowInstance as _ReactFlowInstance,
  Connection as _Connection,
  NodeChange,
  EdgeChange,
  applyNodeChanges,
  applyEdgeChanges,
} from 'reactflow';
import 'reactflow/dist/style.css';
import { Box, Snackbar, Alert, Dialog, DialogContent, Menu, Button, DialogTitle, IconButton, Typography, Drawer, SpeedDial, SpeedDialAction, SpeedDialIcon } from '@mui/material';
import ChatIcon from '@mui/icons-material/Chat';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import HistoryIcon from '@mui/icons-material/History';
import SettingsIcon from '@mui/icons-material/Settings';
import { useWorkflowStore } from '../../store/workflow';
import { useThemeManager } from '../../hooks/workflow/useThemeManager';
import { useErrorManager } from '../../hooks/workflow/useErrorManager';
import { useFlowManager } from '../../hooks/workflow/useFlowManager';
import { useCrewExecutionStore } from '../../store/crewExecution';
import { useTabManagerStore } from '../../store/tabManager';
import { useTabSync } from '../../hooks/workflow/useTabSync';
import { useTabExecutionSync } from '../../hooks/workflow/useTabExecutionSync';
import { useRunStatusStore } from '../../store/runStatus';
import { useChatPanelResize } from '../../hooks/workflow/useChatPanelResize';
import { useExecutionHistoryResize } from '../../hooks/workflow/useExecutionHistoryResize';
import { useResponsiveLayout } from '../../hooks/workflow/useResponsiveLayout';

import { v4 as _uuidv4 } from 'uuid';
import { FlowService as _FlowService } from '../../api/FlowService';
import { useAPIKeysStore as _useAPIKeysStore } from '../../store/apiKeys';
import { FlowFormData as _FlowFormData, FlowConfiguration as _FlowConfiguration } from '../../types/flow';
import { createEdge as _createEdge } from '../../utils/edgeUtils';
import { handleNodesGenerated } from '../Chat/utils/chatHelpers';
import CloseIcon from '@mui/icons-material/Close';

// Component Imports
import { InputVariablesDialog } from '../Jobs/InputVariablesDialog';
import WorkflowPanels from './WorkflowPanels';
import TabBar from './TabBar';
import ChatPanel from '../Chat/ChatPanel';
import RightSidebar from './RightSidebar';
import LeftSidebar from './LeftSidebar';
import GroupSelector from '../Common/GroupSelector';
import { useUILayoutStore } from '../../store/uiLayout';
import { useUIFitView } from '../../hooks/workflow/useUIFitView';
import { useWorkflowLayoutEvents } from '../../hooks/workflow/useWorkflowLayoutEvents';
import { useTaskExecutionStore } from '../../store/taskExecutionStore';
import { useFlowExecutionStore } from '../../store/flowExecutionStore';

// Dialog Imports
import AgentDialog from '../Agents/AgentDialog';
import TaskDialog from '../Tasks/TaskDialog';
import CrewPlanningDialog from '../Planning/CrewPlanningDialog';
import ScheduleDialog from '../Schedule/ScheduleDialog';
import JobsPanel from '../Jobs/JobsPanel';
import InteractiveTutorial from '../Tutorial/InteractiveTutorial';
import APIKeys from '../Configuration/APIKeys/APIKeys';
import Logs from '../Jobs/LLMLogs';
import ShowLogs from '../Jobs/ShowLogs';
import { executionLogService } from '../../api/ExecutionLogs';
import type { LogEntry } from '../../api/ExecutionLogs';
import Configuration from '../Configuration/Configuration';
import ToolForm from '../Tools/ToolForm';
import { CrewFlowSelectionDialog } from '../Crew/CrewFlowDialog';
import SaveCrew from '../Crew/SaveCrew';
import SaveFlow from '../Flow/SaveFlow';
import CheckpointResumeDialog from '../Flow/CheckpointResumeDialog';
import TrifectaWarningDialog from '../Crew/TrifectaWarningDialog';

// Services & Utilities
import { useAgentManager } from '../../hooks/workflow/useAgentManager';
import { useTaskManager } from '../../hooks/workflow/useTaskManager';
import { setupResizeObserverErrorHandling } from './WorkflowUtils';
import {
  usePanelManager,
  useNodePositioning,
  PANEL_STATE as _PANEL_STATE
} from './WorkflowPanelManager';
import {
  useContextMenuHandlers,
  useFlowInstanceHandlers,
  useSelectionChangeHandler,
  useFlowSelectHandler,
  useCrewFlowDialogHandler,
  useFlowSelectionDialogHandler,
  useEventBindings
} from './WorkflowEventHandlers';
import { useDialogManager } from './WorkflowDialogManager';

// Set up ResizeObserver error handling
setupResizeObserverErrorHandling();

interface WorkflowDesignerProps {
  className?: string;
}

const WorkflowDesigner: React.FC<WorkflowDesignerProps> = (): JSX.Element => {
  // Use the extracted hooks to manage state and logic
  const { isDarkMode } = useThemeManager();
  const { showError, errorMessage, handleCloseError, showErrorMessage } = useErrorManager();

  // Use workflow store for UI settings
  const {
    hasSeenTutorial,
    hasSeenHandlebar: _hasSeenHandlebar,
    setHasSeenTutorial,
    setHasSeenHandlebar,
    uiState: {
      isMinimapVisible: _isMinimapVisible,
      controlsVisible: _controlsVisible
    },
    setUIState: _setUIState
  } = useWorkflowStore();

  // Use tab manager for multi-tab support
  const {
    tabs,
    getActiveTab,
    updateTabExecutionStatus,
    updateTabFlowNodes,
    updateTabFlowEdges,
    updateTabViewMode
  } = useTabManagerStore();

  // Use run status store for job monitoring (SSE-based, no polling needed)
  const { runHistory } = useRunStatusStore();

  // Use flow store for node/edge management (crew canvas)
  const {
    nodes,
    edges,
    setNodes,
    setEdges,
    onNodesChange,
    onEdgesChange,
    onConnect,
    handleEdgeContextMenu: _handleEdgeContextMenu,
    selectedEdges: _selectedEdges,
    setSelectedEdges,
    manuallyPositionedNodes
  } = useFlowManager({ showErrorMessage });

  // CRITICAL: Flow Canvas state from tab manager (persisted per tab)
  // Get the active tab's flow nodes/edges
  const activeTab = getActiveTab();
  const flowNodes = activeTab?.flowNodes || [];
  const flowEdges = activeTab?.flowEdges || [];

  // Wrappers to update flow nodes/edges in the tab manager
  const setFlowNodes = useCallback((nodesOrUpdater: _Node[] | ((prev: _Node[]) => _Node[])) => {
    const tab = getActiveTab();
    if (!tab) return;

    const newNodes = typeof nodesOrUpdater === 'function'
      ? nodesOrUpdater(tab.flowNodes || [])
      : nodesOrUpdater;
    updateTabFlowNodes(tab.id, newNodes);
  }, [getActiveTab, updateTabFlowNodes]);

  const setFlowEdges = useCallback((edgesOrUpdater: _Edge[] | ((prev: _Edge[]) => _Edge[])) => {
    const tab = getActiveTab();
    if (!tab) return;

    const newEdges = typeof edgesOrUpdater === 'function'
      ? edgesOrUpdater(tab.flowEdges || [])
      : edgesOrUpdater;
    updateTabFlowEdges(tab.id, newEdges);
  }, [getActiveTab, updateTabFlowEdges]);

  // Flow canvas change handlers
  const onFlowNodesChange = useCallback((changes: NodeChange[]) => {
    setFlowNodes(nds => applyNodeChanges(changes, nds));
  }, []);

  const onFlowEdgesChange = useCallback((changes: EdgeChange[]) => {
    setFlowEdges(eds => applyEdgeChanges(changes, eds));
  }, []);

  const onFlowConnect = useCallback((connection: _Connection) => {
    if (!connection.source || !connection.target) return;

    // Find existing edges to the same target (convergent edges)
    const existingEdgesToTarget = flowEdges.filter(e => e.target === connection.target);

    if (existingEdgesToTarget.length > 0) {
      // Multiple edges converging to same target - merge them visually
      const targetNodeId = connection.target as string;

      // Collect all sources (existing + new)
      const allSources = [...existingEdgesToTarget.map(e => e.source), connection.source];
      const uniqueSources = Array.from(new Set(allSources));

      // Get target handle (use from connection or first existing edge)
      const targetHandleId = connection.targetHandle || existingEdgesToTarget[0]?.targetHandle || 'top';

      // Generate a stable group ID for this merge (sorted for consistency)
      const sortedSources = [...uniqueSources].sort();
      const mergeGroupId = `merge-group-${sortedSources.join('-')}-${targetNodeId}`;

      // Get shared data from first existing edge (if any)
      const sharedData = existingEdgesToTarget[0]?.data || {};

      // Remove ALL existing edges to this target
      const edgesNotGoingToTarget = flowEdges.filter(e => e.target !== targetNodeId);

      // Create individual edges for each source with merge metadata
      const newMergedEdges: _Edge[] = uniqueSources.map((sourceId, index) => {
        // Find the handle for this source
        const existingEdge = existingEdgesToTarget.find(e => e.source === sourceId);
        const sourceHandle = existingEdge?.sourceHandle ||
                            (sourceId === connection.source ? connection.sourceHandle : null) ||
                            'bottom';

        // Only the last edge in the group should show indicators (arrow, warning, labels)
        const isLastInGroup = index === uniqueSources.length - 1;

        return {
          id: `${mergeGroupId}-${sourceId}`,
          source: sourceId,
          target: targetNodeId,
          sourceHandle,
          targetHandle: targetHandleId,
          type: 'crewEdge',
          animated: true,
          style: { stroke: '#ff9800', strokeWidth: 2 }, // Orange for unconfigured
          data: {
            ...sharedData,
            listenToTaskIds: sharedData.listenToTaskIds || [],
            targetTaskIds: sharedData.targetTaskIds || [],
            mergeGroupId, // Mark this edge as part of a merged group
            isMerged: true,
            mergeGroupSize: uniqueSources.length, // How many edges in this group
            isLastInGroup // Flag to indicate this edge should show indicators
          }
        };
      });

      // Replace with new merged edges
      setFlowEdges([...edgesNotGoingToTarget, ...newMergedEdges]);
    } else {
      // No existing edges to this target, create a normal edge
      const edge: _Edge = {
        id: `${connection.source}-${connection.target}`,
        source: connection.source,
        target: connection.target,
        sourceHandle: connection.sourceHandle || 'bottom',
        targetHandle: connection.targetHandle || 'top',
        type: 'crewEdge',
        animated: true,
        style: { stroke: '#ff9800', strokeWidth: 2 }, // Orange for unconfigured
        data: {
          listenToTaskIds: [],
          targetTaskIds: []
        }
      };
      setFlowEdges(eds => [...eds, edge]);
    }
  }, [flowEdges]);

  // Use tab sync to keep tabs and flow manager in sync
  const { activeTabId: _activeTabId } = useTabSync({ nodes, edges, setNodes, setEdges });

  // Restore view mode when switching tabs
  React.useEffect(() => {
    const tab = getActiveTab();
    if (tab && tab.viewMode) {
      const shouldShowFlows = tab.viewMode === 'flow';
      if (areFlowsVisible !== shouldShowFlows) {
        setUIStoreAreFlowsVisible(shouldShowFlows);
      }
    }
  }, [activeTab?.id]); // Only trigger when tab ID changes

  // Use tab execution sync to keep execution config (process type, planning, etc.) in sync per tab
  useTabExecutionSync();

  // Use agent and task managers with original flow manager
  const {
    agents,
    addAgentNode: _addAgentNode,
    isAgentDialogOpen,
    setIsAgentDialogOpen,
    handleAgentSelect,
    handleShowAgentForm,
    fetchAgents,
    openInCreateMode: agentOpenInCreateMode,
    openAgentDialog
  } = useAgentManager({
    nodes,
    setNodes
  });

  const {
    tasks,
    addTaskNode: _addTaskNode,
    isTaskDialogOpen,
    setIsTaskDialogOpen,
    handleTaskSelect,
    handleShowTaskForm,
    fetchTasks,
    openInCreateMode: taskOpenInCreateMode,
    openTaskDialog
  } = useTaskManager({
    nodes,
    setNodes
  });

  // UI Layout store
  const {
    updateScreenDimensions,
    setChatPanelWidth,
    setChatPanelCollapsed,
    setChatPanelVisible,
    setExecutionHistoryHeight,
    setExecutionHistoryVisible,
    setPanelPosition: setUIStorePanelPosition,
    setAreFlowsVisible: setUIStoreAreFlowsVisible,
    chatPanelWidth,
    chatPanelCollapsedWidth,
    chatPanelCollapsed: isChatCollapsed,
    chatPanelSide,
    leftSidebarBaseWidth,
    rightSidebarWidth,
    executionHistoryHeight,
    chatPanelVisible: showChatPanel,
    executionHistoryVisible: showRunHistory,
    panelPosition,
    areFlowsVisible,
  } = useUILayoutStore();

  // Responsive layout — computed overrides, never mutates the store
  const { isCompact, isMobile } = useResponsiveLayout();
  const effectiveChatVisible = showChatPanel; // Always respect user toggle
  const effectiveChatCollapsed = (isCompact || isMobile) ? true : isChatCollapsed; // Force-collapse on compact & mobile
  const effectiveChatWidth = effectiveChatCollapsed ? chatPanelCollapsedWidth : chatPanelWidth;
  const effectiveLeftMargin = leftSidebarBaseWidth; // Always reserve sidebar space

  // Use the panel manager
  const {
    isDraggingPanel,
    setIsDraggingPanel,
    panelState,
    setPanelState: _setPanelState,
    handlePanelDragStart: _handlePanelDragStart,
    handleSnapToLeft: _handleSnapToLeft,
    handleSnapToRight: _handleSnapToRight,
    handleResetPanel: _handleResetPanel,
  } = usePanelManager();

  // Sync panel manager with UI store
  const toggleFlowsVisibility = React.useCallback(() => {
    const newViewMode = areFlowsVisible ? 'crew' : 'flow';
    setUIStoreAreFlowsVisible(!areFlowsVisible);
    // Save view mode to the current tab
    const tab = getActiveTab();
    if (tab) {
      updateTabViewMode(tab.id, newViewMode);
    }
  }, [areFlowsVisible, setUIStoreAreFlowsVisible, getActiveTab, updateTabViewMode]);

  const toggleChatPanel = React.useCallback(() => {
    setChatPanelVisible(!showChatPanel);
    // Trigger node repositioning when toggling chat panel visibility
    setTimeout(() => {
      const event = new CustomEvent('recalculateNodePositions', {
        detail: { reason: 'chat-panel-visibility-toggle' }
      });
      window.dispatchEvent(event);
    }, 350); // Wait for animation to complete
  }, [showChatPanel, setChatPanelVisible]);

  // Sync panel position with store
  const setPanelPosition = React.useCallback((position: number | ((prev: number) => number)) => {
    const newPosition = typeof position === 'function' ? position(panelPosition) : position;
    setUIStorePanelPosition(newPosition);
  }, [panelPosition, setUIStorePanelPosition]);

  // Toggle functions for execution history





  // Toggle execution history function
  const toggleExecutionHistory = React.useCallback(() => {
    setExecutionHistoryVisible(!showRunHistory);
  }, [showRunHistory, setExecutionHistoryVisible]);

  // Auto-open execution history when crew is executed
  React.useEffect(() => {
    const handleOpenExecutionHistory = () => {
      if (!showRunHistory) {
        setExecutionHistoryVisible(true);

        // Trigger viewport recalculation after execution history opens
        setTimeout(() => {
          window.dispatchEvent(new CustomEvent('recalculateNodePositions', {
            detail: { reason: 'execution-history-resize' }
          }));
        }, 200);
      }
    };

    window.addEventListener('openExecutionHistory', handleOpenExecutionHistory);
    return () => {
      window.removeEventListener('openExecutionHistory', handleOpenExecutionHistory);
    };

  }, [showRunHistory, setExecutionHistoryVisible]);

  // Add event listener to open flow panel when a flow is loaded from catalog
  React.useEffect(() => {
    const handleOpenFlowPanel = () => {
      if (!areFlowsVisible) {
        setUIStoreAreFlowsVisible(true);
      }
    };

    window.addEventListener('openFlowPanel', handleOpenFlowPanel);
    return () => {
      window.removeEventListener('openFlowPanel', handleOpenFlowPanel);
    };

  }, [areFlowsVisible, setUIStoreAreFlowsVisible]);

  // Use the dialog manager
  const dialogManager = useDialogManager(hasSeenTutorial, setHasSeenTutorial);


  const [isChatProcessing, setIsChatProcessing] = React.useState(false);
  const [mobileChatDrawerOpen, setMobileChatDrawerOpen] = React.useState(false);
  const [hasManuallyResized, setHasManuallyResized] = React.useState(false);
  const [executionCount, setExecutionCount] = React.useState(0);

  // Execution logs dialog state
  const [showExecutionLogsDialog, setShowExecutionLogsDialog] = React.useState(false);
  const [selectedJobLogs, setSelectedJobLogs] = React.useState<LogEntry[]>([]);
  const [selectedExecutionJobId, setSelectedExecutionJobId] = React.useState<string | null>(null);
  const [isConnectingLogs, setIsConnectingLogs] = React.useState(false);
  const [connectionError, setConnectionError] = React.useState<string | null>(null);
  const [lastViewedJobId, setLastViewedJobId] = React.useState<string | null>(null);
  const [runningTabId, setRunningTabId] = React.useState<string | null>(null);

  // Chat panel resize handlers (cap max width on compact screens)
  const chatMaxWidthOverride = isCompact ? Math.min(400, window.innerWidth * 0.4) : undefined;
  const { handleResizeStart } = useChatPanelResize(setChatPanelWidth, chatMaxWidthOverride);

  // Update screen dimensions in store on window resize
  React.useEffect(() => {
    const handleResize = () => {
      updateScreenDimensions(window.innerWidth, window.innerHeight);
    };

    // Set initial dimensions
    updateScreenDimensions(window.innerWidth, window.innerHeight);

    window.addEventListener('resize', handleResize);
    return () => window.removeEventListener('resize', handleResize);
  }, [updateScreenDimensions]);

  // Execution history resize handlers (cap max height on compact screens)
  const historyMaxHeightOverride = isCompact ? Math.min(300, window.innerHeight * 0.4) : undefined;
  const { handleHistoryResizeStart } = useExecutionHistoryResize(
    setExecutionHistoryHeight,
    setHasManuallyResized,
    historyMaxHeightOverride
  );

  // Auto-adjust execution history height based on execution count
  React.useEffect(() => {
    if (!hasManuallyResized && showRunHistory) {
      // Calculate height based on execution count
      // Header ~40px, each row ~32px, pagination ~40px, padding ~20px
      const baseHeight = 40 + 40 + 20; // Header + pagination + padding
      const rowHeight = 32;
      const maxRows = 4; // Maximum 4 rows before scrolling

      if (executionCount === 0) {
        // Just header with "no executions" message
        setExecutionHistoryHeight(baseHeight + rowHeight);
      } else {
        // Show up to 4 rows
        const visibleRows = Math.min(executionCount, maxRows);
        setExecutionHistoryHeight(baseHeight + (visibleRows * rowHeight));
      }

      // Trigger viewport recalculation after height adjustment
      setTimeout(() => {
        window.dispatchEvent(new CustomEvent('recalculateNodePositions', {
          detail: { reason: 'execution-history-resize' }
        }));
      }, 100);
    }
  }, [executionCount, hasManuallyResized, showRunHistory, setExecutionHistoryHeight]);

  // Use crew execution store
  const {
    isExecuting,
    selectedModel,
    planningEnabled,
    reasoningEnabled,
    schemaDetectionEnabled,
    tools,
    selectedTools,
    setSelectedModel,
    setPlanningEnabled,
    setReasoningEnabled,
    setSchemaDetectionEnabled,
    setSelectedTools,
    handleRunClick,
    handleGenerateCrew,
    executeTab,
    executeFlow: _executeFlow,
    setNodes: setCrewExecutionNodes,
    setEdges: setCrewExecutionEdges,
    showInputVariablesDialog,
    setShowInputVariablesDialog,
    executeWithVariables,
    // Checkpoint dialog state
    showCheckpointDialog,
    setShowCheckpointDialog,
    checkpoints,
    checkpointsLoading,
    checkpointsError,
    pendingFlowExecution,
    handleCheckpointStartFresh,
    handleCheckpointResume,
    handleCheckpointDelete,
    refreshCheckpoints,
    // Trifecta warning dialog state
    showTrifectaDialog,
    trifectaAssessment,
    handleTrifectaProceed,
    handleTrifectaCancel,
  } = useCrewExecutionStore();

  // Debug logging for running tab
  React.useEffect(() => {
    // Dependency tracking for running tab state
  }, [runningTabId, isExecuting]);

  // Add debug function once on mount
  React.useEffect(() => {
    if (typeof window !== 'undefined') {
      (window as Window & { clearStuckTabs?: () => void }).clearStuckTabs = () => {
        const state = useTabManagerStore.getState();
        state.tabs.forEach(tab => {
          if (tab.executionStatus === 'running') {
            state.updateTabExecutionStatus(tab.id, 'completed');
          }
        });
      };
    }

    return () => {
      if (typeof window !== 'undefined') {
        delete (window as Window & { clearStuckTabs?: () => void }).clearStuckTabs;
      }
    };
  }, []); // Empty dependency array - only run once

  // Reset sync refs when switching canvases so the sync effects
  // detect "new" nodes when switching back and re-sync to the store
  useEffect(() => {
    if (areFlowsVisible) {
      prevCrewNodeIdsRef.current = '';
      prevCrewEdgeIdsRef.current = '';
    } else {
      prevFlowNodeIdsRef.current = '';
      prevFlowEdgeIdsRef.current = '';
    }
  }, [areFlowsVisible]);

  // Sync nodes and edges with crew execution store
  // For crew canvas (agentNode/taskNode)
  // Uses ID comparison to prevent infinite render loops
  useEffect(() => {
    if (!areFlowsVisible) {
      const currentIds = nodes.map(n => n.id).sort().join(',');
      if (currentIds !== prevCrewNodeIdsRef.current) {
        setCrewExecutionNodes(nodes);
        prevCrewNodeIdsRef.current = currentIds;
      }
    }
  }, [nodes, setCrewExecutionNodes, areFlowsVisible]);

  useEffect(() => {
    if (!areFlowsVisible) {
      const currentIds = edges.map(e => e.id).sort().join(',');
      if (currentIds !== prevCrewEdgeIdsRef.current) {
        setCrewExecutionEdges(edges);
        prevCrewEdgeIdsRef.current = currentIds;
      }
    }
  }, [edges, setCrewExecutionEdges, areFlowsVisible]);

  // CRITICAL: Sync flow canvas nodes/edges with crew execution store
  // For flow canvas (crewNode) - this enables flow execution to work
  // Uses ID comparison to prevent infinite render loops during node deletion
  useEffect(() => {
    if (areFlowsVisible) {
      const currentIds = flowNodes.map(n => n.id).sort().join(',');
      if (currentIds !== prevFlowNodeIdsRef.current) {
        console.log('[WorkflowDesigner] Syncing flowNodes to execution store:', flowNodes.length);
        setCrewExecutionNodes(flowNodes);
        prevFlowNodeIdsRef.current = currentIds;
      }
    }
  }, [flowNodes, setCrewExecutionNodes, areFlowsVisible]);

  useEffect(() => {
    if (areFlowsVisible) {
      const currentIds = flowEdges.map(e => e.id).sort().join(',');
      if (currentIds !== prevFlowEdgeIdsRef.current) {
        console.log('[WorkflowDesigner] Syncing flowEdges to execution store:', flowEdges.length);
        setCrewExecutionEdges(flowEdges);
        prevFlowEdgeIdsRef.current = currentIds;
      }
    }
  }, [flowEdges, setCrewExecutionEdges, areFlowsVisible]);

  // JobsPanel handles refresh internally based on job changes
  // The ExecutionHistory component inside JobsPanel will automatically refresh
  // when new jobs are created or updated

  // Mark handlebar as seen immediately
  useEffect(() => {
    if (!localStorage.getItem('hasSeenHandlebar')) {
      localStorage.setItem('hasSeenHandlebar', 'true');
      setHasSeenHandlebar(true);
    }
  }, [setHasSeenHandlebar]);

  // Listen for job view events from execution history
  useEffect(() => {
    const handleJobViewed = (event: CustomEvent) => {
      const { jobId } = event.detail;
      setLastViewedJobId(jobId);
    };

    window.addEventListener('jobViewed', handleJobViewed as EventListener);

    return () => {
      window.removeEventListener('jobViewed', handleJobViewed as EventListener);
    };
  }, []);

  // Listen for openScheduleDialog events from chat slash commands
  useEffect(() => {
    const handleOpenScheduleDialog = () => {
      dialogManager.setScheduleDialogOpen(true);
    };
    window.addEventListener('openScheduleDialog', handleOpenScheduleDialog);
    return () => window.removeEventListener('openScheduleDialog', handleOpenScheduleDialog);
  }, [dialogManager]);

  // Track the currently executing job ID
  const [executingJobId, setExecutingJobId] = React.useState<string | null>(null);
  const runningTabTimeoutRef = React.useRef<NodeJS.Timeout | null>(null);

  // Track previous flow nodes to prevent infinite loop during sync
  const prevFlowNodeIdsRef = React.useRef<string>('');
  const prevFlowEdgeIdsRef = React.useRef<string>('');
  const prevCrewNodeIdsRef = React.useRef<string>('');
  const prevCrewEdgeIdsRef = React.useRef<string>('');

  // Get task execution store methods
  const { loadTaskStates, clearTaskStates } = useTaskExecutionStore();

  // Listen for job created events to track the executing job
  useEffect(() => {
    const handleJobCreated = (event: CustomEvent) => {
      const { jobId } = event.detail;

      // Only clear task states if this is a different job
      if (executingJobId !== jobId) {
        clearTaskStates();
      }

      setExecutingJobId(jobId);

      // Load task states once - SSE handles real-time updates after this
      loadTaskStates(jobId);
    };

    window.addEventListener('jobCreated', handleJobCreated as EventListener);
    return () => {
      window.removeEventListener('jobCreated', handleJobCreated as EventListener);
    };
  }, [loadTaskStates, clearTaskStates, executingJobId]);

  // Listen for job completion events to clear running tab and update status
  useEffect(() => {
    const handleJobCompleted = () => {
      // DON'T clear task states on completion - keep them visible until next run starts
      // Task states will be cleared when a new job is created (in handleJobCreated)

      // Get the active tab to ensure we clear the right one
      const activeTab = getActiveTab();

      // Also log all tabs to debug
      const tabManagerState = useTabManagerStore.getState();

      if (runningTabId) {
        tabManagerState.updateTabExecutionStatus(runningTabId, 'completed');
        setRunningTabId(null);
      } else if (activeTab?.executionStatus === 'running') {
        // Fallback: if no runningTabId but active tab is running, clear it
        tabManagerState.updateTabExecutionStatus(activeTab.id, 'completed');
      } else {
        // Extra fallback: check all tabs for running status
        tabManagerState.tabs.forEach(tab => {
          if (tab.executionStatus === 'running') {
            tabManagerState.updateTabExecutionStatus(tab.id, 'completed');
          }
        });
      }

      // Clear the safety timeout
      if (runningTabTimeoutRef.current) {
        clearTimeout(runningTabTimeoutRef.current);
        runningTabTimeoutRef.current = null;
      }

      setExecutingJobId(null);
    };

    const handleJobFailed = () => {
      // DON'T clear task states on failure - keep them visible until next run starts
      // Task states will be cleared when a new job is created (in handleJobCreated)

      // Get the active tab to ensure we clear the right one
      const activeTab = getActiveTab();

      // Also log all tabs to debug
      const tabManagerState = useTabManagerStore.getState();

      if (runningTabId) {
        tabManagerState.updateTabExecutionStatus(runningTabId, 'failed');
        setRunningTabId(null);
      } else if (activeTab?.executionStatus === 'running') {
        // Fallback: if no runningTabId but active tab is running, clear it
        tabManagerState.updateTabExecutionStatus(activeTab.id, 'failed');
      } else {
        // Extra fallback: check all tabs for running status
        tabManagerState.tabs.forEach(tab => {
          if (tab.executionStatus === 'running') {
            tabManagerState.updateTabExecutionStatus(tab.id, 'failed');
          }
        });
      }

      // Clear the safety timeout
      if (runningTabTimeoutRef.current) {
        clearTimeout(runningTabTimeoutRef.current);
        runningTabTimeoutRef.current = null;
      }

      setExecutingJobId(null);
    };

    window.addEventListener('jobCompleted', handleJobCompleted as EventListener);
    window.addEventListener('jobFailed', handleJobFailed as EventListener);

    // Debug: log when listeners are attached

    return () => {
      window.removeEventListener('jobCompleted', handleJobCompleted as EventListener);
      window.removeEventListener('jobFailed', handleJobFailed as EventListener);
    };
  }, [runningTabId, getActiveTab, clearTaskStates]);

  // Fallback: Monitor job status directly from runHistory
  useEffect(() => {

    if (executingJobId && runHistory.length > 0) {
      const job = runHistory.find(run => run.job_id === executingJobId);
      if (job) {

        if (job.status.toLowerCase() === 'completed' || job.status.toLowerCase() === 'failed') {

          // Clear the running tab if it's still set
          if (runningTabId) {
            updateTabExecutionStatus(runningTabId, job.status.toLowerCase() as 'completed' | 'failed');
            setRunningTabId(null);
          }

          // Also check all tabs for stuck running status
          // Get tabs directly from store to avoid dependency issues
          const tabManagerState = useTabManagerStore.getState();
          tabManagerState.tabs.forEach(tab => {
            if (tab.executionStatus === 'running') {
              tabManagerState.updateTabExecutionStatus(tab.id, job.status.toLowerCase() as 'completed' | 'failed');
            }
          });

          // Clear the executing job ID
          setExecutingJobId(null);

          // Manually dispatch the event in case it was missed
          // Skip dispatching - let the runStatus store handle it to avoid duplicates
        }
      } else {
        // No execution status, continue normally
      }
    }
  }, [executingJobId, runHistory, runningTabId, updateTabExecutionStatus]);

  // Add event listener to force clear stuck execution state
  useEffect(() => {
    const handleForceClearExecution = () => {

      // Clear any running tabs
      // Get tabs directly from store to avoid dependency issues
      const tabManagerState = useTabManagerStore.getState();
      tabManagerState.tabs.forEach(tab => {
        if (tab.executionStatus === 'running') {
          tabManagerState.updateTabExecutionStatus(tab.id, 'completed');
        }
      });

      if (runningTabId) {
        setRunningTabId(null);
      }
      setExecutingJobId(null);

      // Also clear safety timeout
      if (runningTabTimeoutRef.current) {
        clearTimeout(runningTabTimeoutRef.current);
        runningTabTimeoutRef.current = null;
      }
    };

    window.addEventListener('forceClearExecution', handleForceClearExecution);
    return () => {
      window.removeEventListener('forceClearExecution', handleForceClearExecution);
    };
  }, [runningTabId]);

  // Use context menu handlers
  const {
    paneContextMenu,
    handlePaneContextMenu,
    handlePaneContextMenuClose
  } = useContextMenuHandlers();

  // Use flow instance handlers
  const {
    crewFlowInstanceRef,
    flowFlowInstanceRef,
    handleCrewFlowInit,
    handleFlowFlowInit
  } = useFlowInstanceHandlers();

  // Add these refs near the other ref declarations in the component
  const updateNodePositionsTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const unmountedRef = useRef<boolean>(false);

  // Update the useEffect cleanup that handles component unmount
  useEffect(() => {
    // Component mount
    unmountedRef.current = false;

    return () => {
      // Component unmount
      unmountedRef.current = true;
      // SSE cleanup is handled by SSEConnectionManager
    };
  }, []);

  // Check for jobs on component mount and restore visual indicator states
  useEffect(() => {
    const restoreExecutionStates = async () => {
      // Get fresh data from stores
      const { tabs: currentTabs } = useTabManagerStore.getState();
      const { fetchInitialRunHistory: fetchHistory } = useRunStatusStore.getState();
      const { loadTaskStates: loadStates } = useTaskExecutionStore.getState();
      const { loadCrewStates } = useFlowExecutionStore.getState();

      // First check the tabs for any running status
      const runningTab = currentTabs.find(tab => tab.executionStatus === 'running');
      if (runningTab) {
        setRunningTabId(runningTab.id);
      }

      // Fetch the latest run history to ensure we have the most recent data
      await fetchHistory();

      // Get updated run history after fetch
      const { runHistory: updatedRunHistory } = useRunStatusStore.getState();

      if (updatedRunHistory.length > 0) {
        // Check for running jobs first
        const runningJobs = updatedRunHistory.filter(run =>
          run.status.toLowerCase() === 'running' ||
          run.status.toLowerCase() === 'queued'
        );

        if (runningJobs.length > 0) {
          // Take the most recent running job
          const mostRecentJob = runningJobs[0];
          setExecutingJobId(mostRecentJob.job_id);

          // Load task states for running job (SSE handles real-time updates after this)
          loadStates(mostRecentJob.job_id);

          // Check if this is a flow execution and load crew states
          const isFlowExecution = mostRecentJob.execution_type === 'flow' ||
            mostRecentJob.run_name?.toLowerCase().includes('flow');
          if (isFlowExecution) {
            loadCrewStates(mostRecentJob.job_id);
          }
        } else {
          // No running jobs - load states from the most recent completed/failed job
          // This preserves visual indicators across page refreshes
          const mostRecentJob = updatedRunHistory[0];
          const status = mostRecentJob.status.toLowerCase();

          if (status === 'completed' || status === 'failed') {
            loadStates(mostRecentJob.job_id);

            // Check if this was a flow execution and load crew states
            const isFlowExecution = mostRecentJob.execution_type === 'flow' ||
              mostRecentJob.run_name?.toLowerCase().includes('flow');
            if (isFlowExecution) {
              loadCrewStates(mostRecentJob.job_id);
            }
          }
        }
      }
    };

    // Run the check after a short delay to ensure stores are initialized
    const timer = setTimeout(restoreExecutionStates, 100);

    return () => {
      clearTimeout(timer);
    };
  }, []); // Run only once on mount

  // Use node positioning logic
  useNodePositioning(
    nodes,
    setNodes,
    isDraggingPanel,
    areFlowsVisible,
    panelState,
    manuallyPositionedNodes,
    crewFlowInstanceRef,
    flowFlowInstanceRef,
    updateNodePositionsTimeoutRef,
    unmountedRef
  );

  // Use selection change handler
  const onSelectionChange = useSelectionChangeHandler(setSelectedEdges);

  // Use flow selection handler - CRITICAL: Use flow state, not crew state
  const handleFlowSelect = useFlowSelectHandler(setFlowNodes, setFlowEdges);

  // Use flow add handler


  // Use crew flow dialog handler
  const {
    isCrewFlowDialogOpen,
    setIsCrewFlowDialogOpen,
    openCrewOrFlowDialog: _openCrewOrFlowDialog
  } = useCrewFlowDialogHandler();

  const [crewFlowDialogInitialTab, setCrewFlowDialogInitialTab] = useState(0);
  const [crewFlowDialogShowOnlyTab, setCrewFlowDialogShowOnlyTab] = useState<number | undefined>(undefined);

  // Use flow selection dialog handler
  const {
    isFlowDialogOpen,
    setIsFlowDialogOpen,
    openFlowDialog: _openFlowDialog
  } = useFlowSelectionDialogHandler();

  // Use event bindings (includes both run execution and crew selection handlers)
  const {
    handleRunClickWrapper: _handleRunClickWrapper,
    handleCrewSelectWrapper: _handleCrewSelectWrapper
  } = useEventBindings(
    // Cast the handleRunClick to match the expected signature
    (executionType?: 'flow' | 'crew') =>
      executionType ? handleRunClick(executionType) : Promise.resolve(),
    setNodes,
    setEdges
  );

  // Listen for catalogLoadCrew events from chat /load command
  useEffect(() => {
    const handleCatalogLoad = (event: Event) => {
      const detail = (event as CustomEvent).detail;
      if (detail?.nodes && detail?.edges) {
        _handleCrewSelectWrapper(detail.nodes, detail.edges, detail.name, detail.id);
      }
    };
    window.addEventListener('catalogLoadCrew', handleCatalogLoad);
    return () => window.removeEventListener('catalogLoadCrew', handleCatalogLoad);
  }, [_handleCrewSelectWrapper]);

  // Listen for catalogLoadFlow events from chat /load flow command
  useEffect(() => {
    const handleCatalogFlowLoad = (event: Event) => {
      const detail = (event as CustomEvent).detail;
      if (detail?.nodes && detail?.edges) {
        handleFlowSelect(detail.nodes, detail.edges, detail.flowConfig);
      }
    };
    window.addEventListener('catalogLoadFlow', handleCatalogFlowLoad);
    return () => window.removeEventListener('catalogLoadFlow', handleCatalogFlowLoad);
  }, [handleFlowSelect]);

  // Add refs for the Save dialogs
  const saveCrewRef = useRef<HTMLButtonElement>(null);
  const saveFlowRef = useRef<HTMLButtonElement>(null);

  // Handle tools change
  const handleToolsChange = (toolIds: string[]) => {
    const newSelectedTools = tools.filter(tool => tool.id && toolIds.includes(tool.id));
    setSelectedTools(newSelectedTools);
  };

  // Handle running a specific tab
  const handleRunTab = useCallback(async (tabId: string) => {
    const tab = tabs.find(t => t.id === tabId);
    if (tab) {
      // Set this tab as running
      setRunningTabId(tabId);
      updateTabExecutionStatus(tabId, 'running');

      try {
        // Execute the tab directly with its nodes and edges
        await executeTab(tabId, tab.nodes, tab.edges, tab.name);
        // Don't clear running state here - let the job completion events handle it
      } catch (error) {
        // Clear running state on error

        setRunningTabId(null);
        updateTabExecutionStatus(tabId, 'failed');
      }
    }
  }, [tabs, executeTab, updateTabExecutionStatus]);

  // Handle showing execution logs
  const handleShowExecutionLogs = useCallback(async (jobId?: string) => {
    try {
      // If no jobId provided, try to get the last viewed job
      const jobToShow = jobId || lastViewedJobId;

      if (!jobToShow) {
        // Dispatch event for chat panel to show error
        const errorEvent = new CustomEvent('executionError', {
          detail: {
            message: 'No execution found. Please run a crew first or select an execution from the history.',
            type: 'logs'
          }
        });
        window.dispatchEvent(errorEvent);
        return;
      }

      setIsConnectingLogs(true);
      setConnectionError(null);
      setSelectedExecutionJobId(jobToShow);
      setShowExecutionLogsDialog(true);
      setLastViewedJobId(jobToShow); // Track this as the last viewed job

      // Fetch historical logs via REST
      const historicalLogs = await executionLogService.getHistoricalLogs(jobToShow);
      setSelectedJobLogs(historicalLogs.map(({ job_id, execution_id, ...rest }) => ({
        ...rest,
        output: rest.output || rest.content,
        id: rest.id || Date.now()
      })));

      setIsConnectingLogs(false);
    } catch (error) {
      setConnectionError('Failed to load execution logs');
      setIsConnectingLogs(false);
    }
  }, [lastViewedJobId]);

  // FitView hooks: UI-aware and internal
  const { handleUIAwareFitView, handleFitViewToNodesInternal } = useUIFitView({
    nodes,
    crewFlowInstanceRef,
    flowFlowInstanceRef,
  });

  // Register layout-related global events and initial viewport behavior
  useWorkflowLayoutEvents({
    nodes,
    edges,
    setNodes,
    setEdges,
    flowNodes,
    flowEdges,
    setFlowNodes,
    setFlowEdges,
    areFlowsVisible,
    crewFlowInstanceRef,
    flowFlowInstanceRef,
    handleUIAwareFitView,
    handleFitViewToNodesInternal,
  });









  // Render the component
  return (
    <div className="workflow-designer">
      <Box sx={{
        width: '100%',
        height: '100vh', // Set full viewport height
        position: 'relative',
        // Remove background - let body/app background show through
        display: 'flex',
        flexDirection: 'column',
        overflow: 'hidden' // Prevent scrolling
      }}
      data-tour="workflow-designer"
    >
        {/* Interactive walkthrough tutorial */}
        <InteractiveTutorial isOpen={dialogManager.isTutorialOpen} onClose={dialogManager.handleCloseTutorial} />

        {/* Tab Bar */}
        <TabBar
          onRunTab={handleRunTab}
          isRunning={!!runningTabId}
          runningTabId={runningTabId}
          onLoadCrew={() => {
            setCrewFlowDialogInitialTab(0);
            setCrewFlowDialogShowOnlyTab(0); // Only show Plans tab
            setIsCrewFlowDialogOpen(true);
          }}

          disabled={isChatProcessing || !!runningTabId}
          hideTabsAndButtons={false}
          isMobile={isMobile}
        />

        <Box sx={{
          flex: 1,
          display: 'flex',
          flexDirection: 'row',
          overflow: 'hidden',
          position: 'relative',
          marginLeft: `${effectiveLeftMargin}px` // Push entire content area to the right of LeftSidebar
        }}>
          {/* Chat Panel on Left (when positioned left) - Hidden when flow panel is visible */}
          {effectiveChatVisible && chatPanelSide === 'left' && !areFlowsVisible && (
            <Box
              onMouseEnter={() => {
                window.postMessage({ type: 'chat-hover-state', isHovering: true }, '*');
              }}
              onMouseLeave={() => {
                window.postMessage({ type: 'chat-hover-state', isHovering: false }, '*');
              }}
              sx={{
                width: effectiveChatCollapsed ? `${chatPanelCollapsedWidth}px` : `${effectiveChatWidth}px`,
                height: showRunHistory ? `calc(100% - ${executionHistoryHeight}px)` : '100%',
                display: 'flex',
                flexDirection: 'row',
                overflow: 'hidden',
                backgroundColor: 'background.paper',
                borderRight: 1,
                borderColor: 'divider',
                zIndex: 15, // Higher than LeftSidebar (10) to ensure collapsed chat is visible
                transition: effectiveChatCollapsed ? 'width 0.3s ease-in-out' : 'none',
              }}>
              {/* Chat Content */}
              <Box sx={{ flex: 1, overflow: 'hidden' }}>
                <ChatPanel
                  chatSide={chatPanelSide}
                  onNodesGenerated={(newNodes, newEdges) => {
                    handleNodesGenerated(newNodes, newEdges, setNodes, setEdges);
                  }}
                  onLoadingStateChange={setIsChatProcessing}
                  isVisible={showChatPanel}
                  nodes={nodes}
                  edges={edges}
                  onExecuteCrew={() => {
                    // Set current tab as running when executing from chat
                    const activeTab = getActiveTab();
                    if (activeTab) {
                      setRunningTabId(activeTab.id);
                      updateTabExecutionStatus(activeTab.id, 'running');

                      // Clear any existing timeout
                      if (runningTabTimeoutRef.current) {
                        clearTimeout(runningTabTimeoutRef.current);
                      }

                      // Set a safety timeout to clear running state after 5 minutes
                      const tabIdToTimeout = activeTab.id; // Capture the tab ID
                      runningTabTimeoutRef.current = setTimeout(() => {
                        setRunningTabId((currentRunningTabId) => {
                          if (currentRunningTabId === tabIdToTimeout) {
                            return null;
                          }
                          return currentRunningTabId;
                        });
                        updateTabExecutionStatus(tabIdToTimeout, 'completed');
                      }, 5 * 60 * 1000); // 5 minutes
                    }
                    // Make sure nodes are synced to the execution store
                    setCrewExecutionNodes(nodes);
                    setCrewExecutionEdges(edges);
                    // Small delay to ensure state is updated
                    setTimeout(() => {
                      handleRunClick('crew');
                    }, 100);
                  }}
                  isCollapsed={effectiveChatCollapsed}
                  onToggleCollapse={() => {
                    if (isMobile || isCompact) {
                      // On small screens, open full-screen drawer instead of expanding inline
                      setMobileChatDrawerOpen(true);
                    } else {
                      setChatPanelCollapsed(!isChatCollapsed);
                      // Trigger node repositioning when toggling collapse
                      setTimeout(() => {
                        const event = new CustomEvent('recalculateNodePositions', {
                          detail: { reason: 'chat-panel-toggle' }
                        });
                        window.dispatchEvent(event);
                      }, 350); // Wait for animation to complete
                    }
                  }}
                  chatSessionId={getActiveTab()?.chatSessionId}
                  onOpenLogs={handleShowExecutionLogs}
                />
              </Box>
              {/* Resize Handle - on the right side when chat is on left */}
              {!effectiveChatCollapsed && (
                <Box
                  onMouseDown={handleResizeStart}
                  sx={{
                    width: 4,
                    height: '100%',
                    backgroundColor: 'divider',
                    cursor: 'ew-resize',
                    '&:hover': {
                      backgroundColor: 'primary.main',
                    },
                    transition: 'background-color 0.2s ease',
                    zIndex: 7,
                  }}
                />
              )}
            </Box>
          )}

          {/* Main content area with WorkflowPanels */}
          <Box sx={{
            flex: 1,
            display: 'flex',
            flexDirection: 'column',
            overflow: 'hidden',
            position: 'relative'
          }}>
            <WorkflowPanels
              areFlowsVisible={areFlowsVisible}
              showRunHistory={showRunHistory}
              executionHistoryHeight={executionHistoryHeight}
              panelPosition={panelPosition}
              isDraggingPanel={isDraggingPanel}
              isDarkMode={isDarkMode}
              // Crew canvas state
              nodes={nodes}
              edges={edges}
              setNodes={setNodes}
              setEdges={setEdges}
              onNodesChange={onNodesChange}
              onEdgesChange={onEdgesChange}
              onConnect={onConnect}
              // Flow canvas state (independent)
              flowNodes={flowNodes}
              flowEdges={flowEdges}
              onFlowNodesChange={onFlowNodesChange}
              onFlowEdgesChange={onFlowEdgesChange}
              onFlowConnect={onFlowConnect}
              // Common handlers
              onSelectionChange={onSelectionChange}
              onPaneContextMenu={handlePaneContextMenu}
              onCrewFlowInit={handleCrewFlowInit}
              onFlowFlowInit={handleFlowFlowInit}
              handleUIAwareFitView={handleUIAwareFitView}
              planningEnabled={planningEnabled}
              setPlanningEnabled={setPlanningEnabled}
              reasoningEnabled={reasoningEnabled}
              setReasoningEnabled={setReasoningEnabled}
              schemaDetectionEnabled={schemaDetectionEnabled}
              setSchemaDetectionEnabled={setSchemaDetectionEnabled}
              selectedModel={selectedModel}
              setSelectedModel={setSelectedModel}
              onOpenLogsDialog={() => dialogManager.setIsLogsDialogOpen(true)}
              onToggleChat={toggleChatPanel}
              isChatOpen={showChatPanel}
              setIsAgentDialogOpen={() => openAgentDialog(true)}
              setIsTaskDialogOpen={() => openTaskDialog(true)}
              setIsCrewDialogOpen={() => {
                setCrewFlowDialogInitialTab(0);
                setCrewFlowDialogShowOnlyTab(undefined);
                setIsCrewFlowDialogOpen(true);
              }}
              onOpenTutorial={() => {

                dialogManager.setIsTutorialOpen(true);
              }}
              onOpenConfiguration={() => dialogManager.setIsConfigurationDialogOpen(true)}
              onPlayPlan={() => {
                console.log('[WorkflowDesigner] onPlayPlan called, calling handleRunClick("crew")');
                handleRunClick('crew');
              }}
              onPlayFlow={() => {
                console.log('[WorkflowDesigner] onPlayFlow called from WorkflowPanels');
                console.log('[WorkflowDesigner] flowNodes count:', flowNodes.length, 'flowEdges count:', flowEdges.length);
                // Nodes/edges are auto-synced via useEffect, but ensure sync is current
                setCrewExecutionNodes(flowNodes);
                setCrewExecutionEdges(flowEdges);
                // Execute immediately - auto-sync should have already updated the store
                handleRunClick('flow');
              }}
              onPanelDragStart={e => {
                e.preventDefault();

                // Get initial positions
                const container = e.currentTarget.parentElement;
                if (!container) return;
                const rect = container.getBoundingClientRect();
                const divider = e.currentTarget as HTMLElement;

                // Store initial position for optimization
                let lastPosition = panelPosition;

                const handleMouseMove = (moveEvent: MouseEvent) => {
                  // Calculate new position without state update
                  const newPosition = ((moveEvent.clientX - rect.left) / rect.width) * 100;
                  const clampedPosition = Math.max(20, Math.min(80, newPosition));

                  // Only update if position changed by at least 0.1%
                  if (Math.abs(clampedPosition - lastPosition) < 0.1) return;

                  // Update the position of the divider directly
                  divider.style.left = `${clampedPosition}%`;

                  // Update the grid template columns
                  container.style.gridTemplateColumns = `${clampedPosition}% ${100 - clampedPosition}%`;

                  lastPosition = clampedPosition;
                };

                const handleMouseUp = () => {
                  // Only update state once at the end for a single rerender
                  setIsDraggingPanel(false);
                  setPanelPosition(lastPosition);

                  document.removeEventListener('mousemove', handleMouseMove);
                  document.removeEventListener('mouseup', handleMouseUp);
                };

                // Start drag operation
                setIsDraggingPanel(true);
                document.addEventListener('mousemove', handleMouseMove);
                document.addEventListener('mouseup', handleMouseUp);
              }}
            />

            {/* Chat Panel on Right (when positioned right) - Hidden when flow panel is visible */}
            {effectiveChatVisible && chatPanelSide === 'right' && !areFlowsVisible && (
              <Box
                onMouseEnter={() => {
                  window.postMessage({ type: 'chat-hover-state', isHovering: true }, '*');
                }}
                onMouseLeave={() => {
                  window.postMessage({ type: 'chat-hover-state', isHovering: false }, '*');
                }}
                sx={{
                  position: 'absolute',
                  top: 0,
                  right: rightSidebarWidth,
                  bottom: showRunHistory ? `${executionHistoryHeight}px` : 0,
                  width: effectiveChatCollapsed ? `${chatPanelCollapsedWidth}px` : `${effectiveChatWidth}px`,
                  display: 'flex',
                  flexDirection: 'row',
                  overflow: 'hidden',
                  backgroundColor: 'background.paper',
                  zIndex: 10,
                  transition: effectiveChatCollapsed ? 'width 0.3s ease-in-out' : 'none',
                }}>
                {/* Resize Handle */}
                {!effectiveChatCollapsed && (
                  <Box
                    onMouseDown={handleResizeStart}
                    sx={{
                      width: 4,
                      height: '100%',
                      backgroundColor: 'divider',
                      cursor: 'ew-resize',
                      '&:hover': {
                        backgroundColor: 'primary.main',
                      },
                      transition: 'background-color 0.2s ease',
                      zIndex: 7,
                    }}
                  />
                )}

                {/* Chat Panel Content */}
                <Box sx={{ flex: 1, display: 'flex', flexDirection: 'column' }}>
                  <ChatPanel
                    onNodesGenerated={(newNodes, newEdges) => {
                      handleNodesGenerated(newNodes, newEdges, setNodes, setEdges);
                    }}
                    onLoadingStateChange={setIsChatProcessing}
                    isVisible={showChatPanel}
                    nodes={nodes}
                    edges={edges}
                    onExecuteCrew={() => {
                      // Set current tab as running when executing from chat
                      const activeTab = getActiveTab();
                      if (activeTab) {
                        setRunningTabId(activeTab.id);
                        updateTabExecutionStatus(activeTab.id, 'running');

                        // Clear any existing timeout
                        if (runningTabTimeoutRef.current) {
                          clearTimeout(runningTabTimeoutRef.current);
                        }

                        // Set a safety timeout to clear running state after 5 minutes
                        const tabIdToTimeout = activeTab.id; // Capture the tab ID
                        runningTabTimeoutRef.current = setTimeout(() => {
                          setRunningTabId((currentRunningTabId) => {
                            if (currentRunningTabId === tabIdToTimeout) {
                              return null;
                            }
                            return currentRunningTabId;
                          });
                          updateTabExecutionStatus(tabIdToTimeout, 'completed');
                        }, 5 * 60 * 1000); // 5 minutes
                      }
                      // Make sure nodes are synced to the execution store
                      setCrewExecutionNodes(nodes);
                      setCrewExecutionEdges(edges);
                      // Small delay to ensure state is updated
                      setTimeout(() => {
                        handleRunClick('crew');
                      }, 100);
                    }}
                    isCollapsed={effectiveChatCollapsed}
                    onToggleCollapse={() => {
                      if (isMobile || isCompact) {
                        // On small screens, open full-screen drawer instead of expanding inline
                        setMobileChatDrawerOpen(true);
                      } else {
                        setChatPanelCollapsed(!isChatCollapsed);
                        // Trigger node repositioning when toggling collapse
                        setTimeout(() => {
                          const event = new CustomEvent('recalculateNodePositions', {
                            detail: { reason: 'chat-panel-toggle' }
                          });
                          window.dispatchEvent(event);
                        }, 350); // Wait for animation to complete
                      }
                    }}
                    chatSessionId={getActiveTab()?.chatSessionId}
                    onOpenLogs={handleShowExecutionLogs}
                    chatSide={chatPanelSide}
                  />
                </Box>
              </Box>
            )}
          </Box>
        </Box>


        {/* Jobs Panel with Run History and Kasal - Overlay on canvas */}
        {showRunHistory && (
          <Box sx={{
            position: 'absolute',
            bottom: 0,
            left: 0,
            right: 0,
            height: `${executionHistoryHeight}px`, // Dynamic height
            backgroundColor: isDarkMode ? 'rgba(26, 26, 26, 0.95)' : 'rgba(255, 255, 255, 0.95)', // Semi-transparent background
            backdropFilter: 'blur(8px)', // Glass effect
            display: 'flex',
            flexDirection: 'column',
            borderTop: 1,
            borderColor: 'divider',
            zIndex: 8, // Above canvas but below chat panel
            // Don't extend under chat panel - let chat panel overlap
          }}>
            {/* Resize Handle */}
            <Box
              onMouseDown={handleHistoryResizeStart}
              sx={{
                position: 'absolute',
                top: 0,
                left: 0,
                right: 0,
                height: 4,
                backgroundColor: 'divider',
                cursor: 'ns-resize',
                '&:hover': {
                  backgroundColor: 'primary.main',
                },
                transition: 'background-color 0.2s ease',
                zIndex: 7,
              }}
            />

            {/* Jobs Panel Content */}
            <Box sx={{
              flex: 1,
              paddingTop: '4px', // Space for resize handle
              overflow: 'hidden',
              display: 'flex',
              flexDirection: 'column'
            }}>
              <JobsPanel
                executionHistoryHeight={executionHistoryHeight}
                onExecutionCountChange={setExecutionCount}
              />
            </Box>
          </Box>
        )}

        {/* Dialogs */}
        <AgentDialog
          open={isAgentDialogOpen}
          onClose={() => setIsAgentDialogOpen(false)}
          onAgentSelect={handleAgentSelect}
          agents={agents}
          onShowAgentForm={handleShowAgentForm}
          fetchAgents={fetchAgents}
          showErrorMessage={showErrorMessage}
          openInCreateMode={agentOpenInCreateMode}
        />

        <TaskDialog
          open={isTaskDialogOpen}
          onClose={() => setIsTaskDialogOpen(false)}
          onTaskSelect={handleTaskSelect}
          tasks={tasks}
          onShowTaskForm={handleShowTaskForm}
          fetchTasks={fetchTasks}
          openInCreateMode={taskOpenInCreateMode}
        />

        <CrewPlanningDialog
          open={dialogManager.isCrewPlanningOpen}
          onClose={() => dialogManager.setCrewPlanningOpen(false)}
          onGenerateCrew={handleGenerateCrew}
          selectedModel={selectedModel}
          tools={tools}
          selectedTools={selectedTools.map(tool => tool.id || '')}
          onToolsChange={handleToolsChange}
        />

        <CrewFlowSelectionDialog
          open={isCrewFlowDialogOpen}
          onClose={() => {
            setIsCrewFlowDialogOpen(false);
            setCrewFlowDialogInitialTab(0); // Reset to default tab
            setCrewFlowDialogShowOnlyTab(undefined); // Reset to show all tabs
          }}
          onCrewSelect={_handleCrewSelectWrapper}
          onFlowSelect={handleFlowSelect}
          onAgentSelect={handleAgentSelect}
          onTaskSelect={handleTaskSelect}
          initialTab={crewFlowDialogInitialTab}
          showOnlyTab={crewFlowDialogShowOnlyTab}
          hideFlowsTab={!areFlowsVisible}
        />

        {/* Flow Selection Dialog */}
        <CrewFlowSelectionDialog
          open={isFlowDialogOpen}
          onClose={() => setIsFlowDialogOpen(false)}
          onCrewSelect={_handleCrewSelectWrapper}
          onFlowSelect={handleFlowSelect}
          onAgentSelect={handleAgentSelect}
          onTaskSelect={handleTaskSelect}
          initialTab={1} // Set to Flows tab
          hideFlowsTab={!areFlowsVisible}
        />

        <ScheduleDialog
          open={dialogManager.isScheduleDialogOpen}
          onClose={() => dialogManager.setScheduleDialogOpen(false)}
          nodes={nodes}
          edges={edges}
          planningEnabled={planningEnabled}
          selectedModel={selectedModel}
        />

        <Dialog
          open={dialogManager.isAPIKeysDialogOpen}
          onClose={() => dialogManager.setIsAPIKeysDialogOpen(false)}
          maxWidth="lg"
          fullWidth
        >
          <DialogContent>
            <APIKeys />
          </DialogContent>
        </Dialog>

        <Dialog
          open={dialogManager.isToolsDialogOpen}
          onClose={() => dialogManager.setIsToolsDialogOpen(false)}
          maxWidth="lg"
          fullWidth
        >
          <DialogContent>
            <ToolForm />
          </DialogContent>
        </Dialog>

        <Dialog
          open={dialogManager.isLogsDialogOpen}
          onClose={() => dialogManager.setIsLogsDialogOpen(false)}
          maxWidth="lg"
          fullWidth
        >
          <DialogTitle sx={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            pb: 1.5,
            borderBottom: '1px solid',
            borderColor: 'divider'
          }}>
            <Typography variant="h6">LLM Logs</Typography>
            <IconButton
              onClick={() => dialogManager.setIsLogsDialogOpen(false)}
              size="small"
              sx={{
                color: 'text.secondary',
                '&:hover': {
                  color: 'text.primary',
                }
              }}
            >
              <CloseIcon />
            </IconButton>
          </DialogTitle>
          <DialogContent>
            <Logs />
          </DialogContent>
        </Dialog>

        <Dialog
          open={dialogManager.isConfigurationDialogOpen}
          onClose={() => dialogManager.setIsConfigurationDialogOpen(false)}
          fullWidth
          maxWidth="xl"
          PaperProps={{
            sx: {
              width: '80vw',
              maxWidth: 'none',
              height: '80vh'
            }
          }}
        >
          <DialogContent sx={{ p: 0 }}>
            <Configuration onClose={() => dialogManager.setIsConfigurationDialogOpen(false)} />
          </DialogContent>
        </Dialog>

        {/* Flow creation is now handled via the FlowCanvas palette */}

        {/* Input Variables Dialog */}
        <InputVariablesDialog
          open={showInputVariablesDialog}
          onClose={() => setShowInputVariablesDialog(false)}
          onConfirm={executeWithVariables}
          nodes={nodes}
        />

        {/* Error handling */}
        <Snackbar
          open={showError}
          autoHideDuration={6000}
          onClose={handleCloseError}
          anchorOrigin={{ vertical: 'top', horizontal: 'center' }}
        >
          <Alert
            onClose={handleCloseError}
            severity="error"
            variant="filled"
            sx={{ whiteSpace: 'pre-line' }}
          >
            {errorMessage}
          </Alert>
        </Snackbar>

        {/* Add context menu for the pane */}
        <Menu
          open={paneContextMenu !== null}
          onClose={handlePaneContextMenuClose}
          anchorReference="anchorPosition"
          anchorPosition={
            paneContextMenu !== null
              ? { top: paneContextMenu.mouseY, left: paneContextMenu.mouseX }
              : undefined
          }
        >
          {/* Add your menu items here if needed */}
        </Menu>

        {/* Add SaveCrew component */}
        {nodes.length > 0 && (
          <SaveCrew
            nodes={nodes}
            edges={edges}
            trigger={<Button style={{ display: 'none' }} ref={saveCrewRef}>Save</Button>}
          />
        )}

        {/* Add SaveFlow component */}
        {/* CRITICAL: Use flowNodes/flowEdges for Flow canvas, NOT crew canvas nodes */}
        {flowNodes.length > 0 && (
          <SaveFlow
            nodes={flowNodes}
            edges={flowEdges}
            trigger={<Button style={{ display: 'none' }} ref={saveFlowRef}>Save Flow</Button>}
          />
        )}

        {/* Execution Logs Dialog */}
        <ShowLogs
          open={showExecutionLogsDialog}
          onClose={() => {
            setShowExecutionLogsDialog(false);
          }}
          logs={selectedJobLogs}
          jobId={selectedExecutionJobId || ''}
          isConnecting={isConnectingLogs}
          connectionError={connectionError}
        />

        {/* Trifecta Security Warning Dialog */}
        <TrifectaWarningDialog
          open={showTrifectaDialog}
          assessment={trifectaAssessment}
          onProceed={handleTrifectaProceed}
          onCancel={handleTrifectaCancel}
        />

        {/* Checkpoint Resume Dialog */}
        <CheckpointResumeDialog
          open={showCheckpointDialog}
          onClose={() => setShowCheckpointDialog(false)}
          checkpoints={checkpoints}
          loading={checkpointsLoading}
          error={checkpointsError}
          flowName={pendingFlowExecution?.savedFlowName}
          onStartFresh={handleCheckpointStartFresh}
          onResumeFromCheckpoint={handleCheckpointResume}
          onDeleteCheckpoint={handleCheckpointDelete}
          onRefresh={refreshCheckpoints}
        />

        {/* Right Sidebar */}
        <RightSidebar
            onOpenLogsDialog={() => dialogManager.setIsLogsDialogOpen(true)}
            onToggleChat={() => setChatPanelVisible(!showChatPanel)}
            isChatOpen={showChatPanel}
            setIsAgentDialogOpen={() => openAgentDialog(true)}
            setIsTaskDialogOpen={() => openTaskDialog(true)}
            setIsCrewDialogOpen={() => {
              // Context-aware catalog: Show only Flows tab when on Flow Canvas
              if (areFlowsVisible) {
                setCrewFlowDialogInitialTab(3); // Flows tab
                setCrewFlowDialogShowOnlyTab(3); // Show only Flows tab
              } else {
                setCrewFlowDialogInitialTab(0); // Crews tab
                setCrewFlowDialogShowOnlyTab(undefined); // Show all tabs
              }
              setIsCrewFlowDialogOpen(true);
            }}
            onSaveCrewClick={() => {
              const event = new CustomEvent('openSaveCrewDialog');
              window.dispatchEvent(event);
            }}
            onSaveFlowClick={() => {
              const event = new CustomEvent('openSaveFlowDialog');
              window.dispatchEvent(event);
            }}
            showRunHistory={showRunHistory}
            executionHistoryHeight={executionHistoryHeight}
            onOpenSchedulesDialog={() => {
              // Open schedule dialog
              dialogManager.setScheduleDialogOpen(true);
            }}
            onToggleExecutionHistory={toggleExecutionHistory}
            areFlowsVisible={areFlowsVisible}
            toggleFlowsVisibility={toggleFlowsVisibility}
            hasCrewNodes={nodes.some(node => node.type === 'agentNode' || node.type === 'taskNode' || node.type === 'managerNode')}
            hasFlowNodes={flowNodes.some(node => node.type === 'crewNode')}
            edges={flowEdges}
            onPlayPlan={() => {
              console.log('[WorkflowDesigner] RightSidebar onPlayPlan called, calling handleRunClick("crew")');
              handleRunClick('crew');
            }}
            onPlayFlow={() => {
              console.log('[WorkflowDesigner] RightSidebar onPlayFlow called');
              console.log('[WorkflowDesigner] flowNodes count:', flowNodes.length, 'flowEdges count:', flowEdges.length);
              // Nodes/edges are auto-synced via useEffect, but ensure sync is current
              setCrewExecutionNodes(flowNodes);
              setCrewExecutionEdges(flowEdges);
              // Execute immediately - auto-sync should have already updated the store
              handleRunClick('flow');
            }}
          />

        {/* Left Sidebar */}
        <LeftSidebar
            onClearCanvas={() => {
              // Context-aware: clear flow canvas or crew canvas based on which is visible
              if (areFlowsVisible) {
                setFlowNodes([]);
                setFlowEdges([]);
              } else {
                setNodes([]);
                setEdges([]);
              }
            }}
            onZoomIn={() => {
              // Context-aware: zoom the visible canvas
              const reactFlowInstance = areFlowsVisible ? flowFlowInstanceRef.current : crewFlowInstanceRef.current;
              if (reactFlowInstance) {
                reactFlowInstance.zoomIn({ duration: 200 });
              }
            }}
            onZoomOut={() => {
              // Context-aware: zoom the visible canvas
              const reactFlowInstance = areFlowsVisible ? flowFlowInstanceRef.current : crewFlowInstanceRef.current;
              if (reactFlowInstance) {
                reactFlowInstance.zoomOut({ duration: 200 });
              }
            }}
            onFitView={() => {
              // Use the UI-aware fit view that respects canvas boundaries
              handleUIAwareFitView();
            }}
            onToggleInteractivity={() => {
              // Toggle interactivity if needed
              console.log('Toggle interactivity');
            }}
            planningEnabled={planningEnabled}
            setPlanningEnabled={setPlanningEnabled}
            reasoningEnabled={reasoningEnabled}
            setReasoningEnabled={setReasoningEnabled}
            schemaDetectionEnabled={schemaDetectionEnabled}
            setSchemaDetectionEnabled={setSchemaDetectionEnabled}

            setIsConfigurationDialogOpen={dialogManager.setIsConfigurationDialogOpen}
            onOpenLogsDialog={() => dialogManager.setIsLogsDialogOpen(true)}
            showRunHistory={showRunHistory}
            executionHistoryHeight={executionHistoryHeight}
            onOpenTutorial={() => {
              console.log('[WorkflowDesigner] Opening tutorial from LeftSidebar');
              dialogManager.setIsTutorialOpen(true);
            }}
            hideRuntimeFilters={areFlowsVisible}
          />

        {/* Workspace Selector - Upper Right Corner */}
        <Box
          sx={{
            position: 'fixed',
            top: '12px',
            right: isMobile ? '8px' : '20px',
            zIndex: 1002, // Above everything else
            display: 'flex',
            alignItems: 'center'
          }}
        >
          <GroupSelector />
        </Box>

        {/* Full-screen chat drawer (opens on mobile/compact when tapping collapsed chat) */}
        {(isMobile || isCompact) && (
          <Drawer
            anchor="right"
            open={mobileChatDrawerOpen}
            onClose={() => setMobileChatDrawerOpen(false)}
            sx={{ '& .MuiDrawer-paper': { width: isMobile ? '100vw' : '80vw', maxWidth: 600 } }}
          >
            <ChatPanel
              chatSide="right"
              onNodesGenerated={(newNodes, newEdges) => {
                handleNodesGenerated(newNodes, newEdges, setNodes, setEdges);
              }}
              onLoadingStateChange={setIsChatProcessing}
              isVisible={true}
              nodes={nodes}
              edges={edges}
              onExecuteCrew={() => {
                const activeTab = getActiveTab();
                if (activeTab) {
                  setRunningTabId(activeTab.id);
                  updateTabExecutionStatus(activeTab.id, 'running');
                  if (runningTabTimeoutRef.current) {
                    clearTimeout(runningTabTimeoutRef.current);
                  }
                  const tabIdToTimeout = activeTab.id;
                  runningTabTimeoutRef.current = setTimeout(() => {
                    setRunningTabId((currentRunningTabId) => {
                      if (currentRunningTabId === tabIdToTimeout) return null;
                      return currentRunningTabId;
                    });
                    updateTabExecutionStatus(tabIdToTimeout, 'completed');
                  }, 5 * 60 * 1000);
                }
                setCrewExecutionNodes(nodes);
                setCrewExecutionEdges(edges);
                setTimeout(() => {
                  handleRunClick('crew');
                }, 100);
              }}
              isCollapsed={false}
              onToggleCollapse={() => setMobileChatDrawerOpen(false)}
              chatSessionId={getActiveTab()?.chatSessionId}
              onOpenLogs={handleShowExecutionLogs}
            />
          </Drawer>
        )}

        {/* Mobile: SpeedDial for quick actions */}
        {isMobile && (
          <SpeedDial
            ariaLabel="Actions"
            sx={{ position: 'fixed', bottom: 16, right: 16, zIndex: 1100 }}
            icon={<SpeedDialIcon />}
          >
            <SpeedDialAction icon={<ChatIcon />} tooltipTitle="Chat" onClick={() => setMobileChatDrawerOpen(true)} />
            <SpeedDialAction icon={<PlayArrowIcon />} tooltipTitle="Run" onClick={() => handleRunClick('crew')} />
            <SpeedDialAction icon={<HistoryIcon />} tooltipTitle="History" onClick={toggleExecutionHistory} />
            <SpeedDialAction icon={<SettingsIcon />} tooltipTitle="Settings" onClick={() => dialogManager.setIsConfigurationDialogOpen(true)} />
          </SpeedDial>
        )}

      </Box>
    </div>
  );
};

export default WorkflowDesigner;