import { useFlowStateStore } from '../../store/flowState';
import { buildFlowConfiguration } from '../../utils/flowConfigBuilder';
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
import { Box, Snackbar, Alert, Dialog, DialogContent, Menu, Button, Drawer, SpeedDial, SpeedDialAction, SpeedDialIcon } from '@mui/material';
import WorkspaceSplitDivider from './WorkspaceSplitDivider';
import ChatIcon from '@mui/icons-material/Chat';
import HistoryIcon from '@mui/icons-material/History';
import SettingsIcon from '@mui/icons-material/Settings';
import { useWorkflowStore } from '../../store/workflow';
import { kasalStageSurface } from '../../theme/kasalSurfaces';
import { useAppStore as useChatAppStore } from '../../features/chat/store/appStore';
import { usePermissionStore } from '../../store/permissions';
import SessionSidebar from '../sessions/SessionSidebar';
import BuilderPanelControls from '../sessions/BuilderPanelControls';
import CanvasRunButton from '../sessions/CanvasRunButton';
import SessionLibrary from '../sessions/SessionLibrary';
import CanvasTools from '../sessions/CanvasTools';
import { useBuilderSessionMode } from '../sessions/useWorkspaceSessions';
import { useThemeManager } from '../../hooks/workflow/useThemeManager';
import { useErrorManager } from '../../hooks/workflow/useErrorManager';
import { useFlowManager } from '../../hooks/workflow/useFlowManager';
import { useCrewExecutionStore } from '../../store/crewExecution';
import { useBuilderCanvasStore } from '../sessions/builderCanvasStore';
import { useBuilderCanvasSync } from '../../hooks/workflow/useBuilderCanvasSync';
import { useBuilderExecutionSync } from '../../hooks/workflow/useBuilderExecutionSync';
import { useRunStatusStore } from '../../store/runStatus';
import { useResponsiveLayout } from '../../hooks/workflow/useResponsiveLayout';

import { v4 as _uuidv4 } from 'uuid';
import { FlowService as _FlowService } from '../../api/workflow/FlowService';
import { useAPIKeysStore as _useAPIKeysStore } from '../../store/apiKeys';
import { FlowFormData as _FlowFormData, FlowConfiguration as _FlowConfiguration } from '../../types/workflow/flow';
import { createEdge as _createEdge } from '../../utils/edgeUtils';
import { handleNodesGenerated } from '../../features/workflow/assistant/utils/chatHelpers';

// Component Imports
import { InputVariablesDialog } from '../../features/executions/components/InputVariablesDialog';
import WorkflowPanels from './WorkflowPanels';
import ChatPanel from '../../features/workflow/assistant/ChatPanel';
import ChatWorkspace from '../../features/chat/ChatWorkspace';
import { useUILayoutStore } from '../../store/uiLayout';
import { useUIFitView } from '../../hooks/workflow/useUIFitView';
import { useWorkflowLayoutEvents } from '../../hooks/workflow/useWorkflowLayoutEvents';
import { useTaskExecutionStore } from '../../store/taskExecutionStore';
import { useFlowExecutionStore } from '../../store/flowExecutionStore';

// Dialog Imports
import ScheduleDialog from '../../features/workflow/scheduling/components/ScheduleDialog';
import TutorialButton from '../../features/help/tutorial/TutorialButton';
import InteractiveTutorial from '../../features/help/tutorial/InteractiveTutorial';
import APIKeys from '../../features/configuration/components/APIKeys/APIKeys';
import ShowLogs from '../../features/executions/components/ShowLogs';
import { executionLogService } from '../../api/execution/ExecutionLogs';
import type { LogEntry } from '../../api/execution/ExecutionLogs';
import Configuration from '../../features/configuration/components/Configuration';
import { CrewFlowSelectionDialog } from '../../features/workflow/crews/components/CrewFlowDialog/index';
import SaveCrew from '../../features/workflow/crews/components/SaveCrew';
import SaveFlow from '../../features/workflow/flows/components/SaveFlow';
import TrifectaWarningDialog from '../../features/workflow/crews/components/TrifectaWarningDialog';

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
    hasSeenHandlebar: _hasSeenHandlebar,
    setHasSeenTutorial,
    setHasSeenHandlebar,
    uiState: {
      isMinimapVisible: _isMinimapVisible,
      controlsVisible: _controlsVisible
    },
    setUIState: _setUIState
  } = useWorkflowStore();

  // Canvas state belongs to the selected shared session.
  const {
    getActiveCanvas,
    updateCanvasExecutionStatus,
    updateCanvasFlowNodes,
    updateCanvasFlowEdges,
  } = useBuilderCanvasStore();

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
  const activeTab = getActiveCanvas();
  const flowNodes = activeTab?.flowNodes || [];
  const flowEdges = activeTab?.flowEdges || [];

  // Wrappers to update flow nodes/edges in the tab manager
  const setFlowNodes = useCallback((nodesOrUpdater: _Node[] | ((prev: _Node[]) => _Node[])) => {
    const tab = getActiveCanvas();
    if (!tab) return;

    const newNodes = typeof nodesOrUpdater === 'function'
      ? nodesOrUpdater(tab.flowNodes || [])
      : nodesOrUpdater;
    updateCanvasFlowNodes(tab.id, newNodes);
  }, [getActiveCanvas, updateCanvasFlowNodes]);

  const setFlowEdges = useCallback((edgesOrUpdater: _Edge[] | ((prev: _Edge[]) => _Edge[])) => {
    const tab = getActiveCanvas();
    if (!tab) return;

    const newEdges = typeof edgesOrUpdater === 'function'
      ? edgesOrUpdater(tab.flowEdges || [])
      : edgesOrUpdater;
    updateCanvasFlowEdges(tab.id, newEdges);
  }, [getActiveCanvas, updateCanvasFlowEdges]);

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
  const { activeCanvasId: _activeTabId } = useBuilderCanvasSync({ nodes, edges, setNodes, setEdges });

  // View-mode reconciliation lives below, right after the uiLayout store hook,
  // so that areFlowsVisible is in scope for the effect dependency arrays.

  // Use tab execution sync to keep execution config (process type, reasoning, etc.) in sync per tab
  useBuilderExecutionSync();

  const { handleAgentSelect } = useAgentManager({ setNodes });
  const { handleTaskSelect } = useTaskManager({ setNodes });

  // UI Layout store
  const {
    updateScreenDimensions,
    setChatPanelVisible,
    setExecutionHistoryVisible,
    setPanelPosition: setUIStorePanelPosition,
    setAreFlowsVisible: setUIStoreAreFlowsVisible,
    leftSidebarBaseWidth,
    rightSidebarWidth,
    executionHistoryHeight,
    chatPanelVisible: showChatPanel,
    executionHistoryVisible: showRunHistory,
    assistantPanelVisible,
    flowPanelTab,
    assistantPanelSide,
    setAssistantPanelVisible,
    assistantResponseFocused,
    assistantPanelRatio,
    setAssistantPanelRatio,
    panelPosition,
    areFlowsVisible,
    appMode,
  } = useUILayoutStore();

  useBuilderSessionMode();
  const sessionSidebarOpen = useChatAppStore(state => state.sidebarOpen);
  const sessionSidebarWidth = sessionSidebarOpen ? 256 : leftSidebarBaseWidth;

  // Responsive layout — computed overrides, never mutates the store
  const { isCompact, isMobile } = useResponsiveLayout();
  const effectiveChatVisible = showChatPanel; // Always respect user toggle
  const responseFocused = assistantResponseFocused && showRunHistory;
  const showingResponses = assistantPanelVisible && (!areFlowsVisible || flowPanelTab === 'responses');
  const showingCrews = areFlowsVisible && flowPanelTab === 'crews';
  const responseMainWidth = (window.innerWidth - sessionSidebarWidth - rightSidebarWidth) * assistantPanelRatio;
  const effectiveLeftMargin = sessionSidebarWidth + (responseFocused && !isCompact && assistantPanelSide === 'left' ? responseMainWidth : 0); // Always reserve sidebar space

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


  // Sync panel position with store
  const setPanelPosition = React.useCallback((position: number | ((prev: number) => number)) => {
    const newPosition = typeof position === 'function' ? position(panelPosition) : position;
    setUIStorePanelPosition(newPosition);
  }, [panelPosition, setUIStorePanelPosition]);

  // Toggle functions for execution history





  // Toggle execution history function
  React.useEffect(() => {
    const id = window.setTimeout(() => window.dispatchEvent(new CustomEvent('recalculateNodePositions', { detail: { reason: 'execution-history-resize' } })), 120);
    return () => window.clearTimeout(id);
  }, [showRunHistory, assistantPanelVisible, assistantPanelSide, responseFocused, assistantPanelRatio]);



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

  // Add event listener to open flow panel when a flow is loaded from catalog.
  // Flipping areFlowsVisible is enough — the viewMode mirror effect keeps the
  // active tab's viewMode in sync so a refresh restores the flow builder.
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
  const dialogManager = useDialogManager(setHasSeenTutorial);


  const [isChatProcessing, setIsChatProcessing] = React.useState(false);

  // Once the Chat workspace has been opened, keep it mounted (just hidden) when
  // the user switches to other modes — otherwise unmounting tears down the live
  // execution SSE stream and a running crew's progress/result is lost.
  const [chatEverOpened, setChatEverOpened] = React.useState(appMode === 'chat');
  React.useEffect(() => {
    if (appMode === 'chat') setChatEverOpened(true);
  }, [appMode]);

  // Execution logs dialog state
  const [showExecutionLogsDialog, setShowExecutionLogsDialog] = React.useState(false);
  const [selectedJobLogs, setSelectedJobLogs] = React.useState<LogEntry[]>([]);
  const [selectedExecutionJobId, setSelectedExecutionJobId] = React.useState<string | null>(null);
  const [isConnectingLogs, setIsConnectingLogs] = React.useState(false);
  const [connectionError, setConnectionError] = React.useState<string | null>(null);
  const [lastViewedJobId, setLastViewedJobId] = React.useState<string | null>(null);
  const [runningTabId, setRunningTabId] = React.useState<string | null>(null);


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


  // Use crew execution store
  const {
    isExecuting,
    selectedModel,
    reasoningEnabled,
    setSelectedModel,
    setReasoningEnabled,
    handleRunClick,
    executeFlow: _executeFlow,
    setNodes: setCrewExecutionNodes,
    setEdges: setCrewExecutionEdges,
    showInputVariablesDialog,
    setShowInputVariablesDialog,
    executeWithVariables,
    pendingVariableExecution,
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
        const state = useBuilderCanvasStore.getState();
        state.canvases.forEach(tab => {
          if (tab.executionStatus === 'running') {
            state.updateCanvasExecutionStatus(tab.id, 'completed');
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
      const activeTab = getActiveCanvas();

      // Also log all tabs to debug
      const canvasState = useBuilderCanvasStore.getState();

      if (runningTabId) {
        canvasState.updateCanvasExecutionStatus(runningTabId, 'completed');
        setRunningTabId(null);
      } else if (activeTab?.executionStatus === 'running') {
        // Fallback: if no runningTabId but active tab is running, clear it
        canvasState.updateCanvasExecutionStatus(activeTab.id, 'completed');
      } else {
        // Extra fallback: check all tabs for running status
        canvasState.canvases.forEach(tab => {
          if (tab.executionStatus === 'running') {
            canvasState.updateCanvasExecutionStatus(tab.id, 'completed');
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
      const activeTab = getActiveCanvas();

      // Also log all tabs to debug
      const canvasState = useBuilderCanvasStore.getState();

      if (runningTabId) {
        canvasState.updateCanvasExecutionStatus(runningTabId, 'failed');
        setRunningTabId(null);
      } else if (activeTab?.executionStatus === 'running') {
        // Fallback: if no runningTabId but active tab is running, clear it
        canvasState.updateCanvasExecutionStatus(activeTab.id, 'failed');
      } else {
        // Extra fallback: check all tabs for running status
        canvasState.canvases.forEach(tab => {
          if (tab.executionStatus === 'running') {
            canvasState.updateCanvasExecutionStatus(tab.id, 'failed');
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
  }, [runningTabId, getActiveCanvas, clearTaskStates]);

  // Fallback: Monitor job status directly from runHistory
  useEffect(() => {

    if (executingJobId && runHistory.length > 0) {
      const job = runHistory.find(run => run.job_id === executingJobId);
      if (job) {

        if (job.status.toLowerCase() === 'completed' || job.status.toLowerCase() === 'failed') {

          // Clear the running tab if it's still set
          if (runningTabId) {
            updateCanvasExecutionStatus(runningTabId, job.status.toLowerCase() as 'completed' | 'failed');
            setRunningTabId(null);
          }

          // Also check all tabs for stuck running status
          // Get tabs directly from store to avoid dependency issues
          const canvasState = useBuilderCanvasStore.getState();
          canvasState.canvases.forEach(tab => {
            if (tab.executionStatus === 'running') {
              canvasState.updateCanvasExecutionStatus(tab.id, job.status.toLowerCase() as 'completed' | 'failed');
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
  }, [executingJobId, runHistory, runningTabId, updateCanvasExecutionStatus]);

  // Add event listener to force clear stuck execution state
  useEffect(() => {
    const handleForceClearExecution = () => {

      // Clear any running tabs
      // Get tabs directly from store to avoid dependency issues
      const canvasState = useBuilderCanvasStore.getState();
      canvasState.canvases.forEach(tab => {
        if (tab.executionStatus === 'running') {
          canvasState.updateCanvasExecutionStatus(tab.id, 'completed');
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
      const { canvases: currentTabs } = useBuilderCanvasStore.getState();
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









  // Chat mode swaps the crew/flow canvas for the embedded chat workspace.
  const isChatMode = appMode === 'chat';
  // Chat-only users (operators) must never stand on a builder canvas — not on
  // boot (appMode is persisted in localStorage and may say 'crew'), and not
  // after switching into a teamspace where their role is operator while a
  // canvas is open. setAppMode's own guard allows the corrective move to chat.
  const allowAgentBuilder = usePermissionStore((s) => s.allowAgentBuilder);
  const allowFlowBuilder = usePermissionStore((s) => s.allowFlowBuilder);
  useEffect(() => {
    const blocked =
      (appMode === 'crew' && !allowAgentBuilder) ||
      (appMode === 'flow' && !allowFlowBuilder);
    if (blocked) {
      useUILayoutStore.getState().setAppMode('chat');
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [appMode, allowAgentBuilder, allowFlowBuilder]);
  // Render the component
  return (
    <div className="workflow-designer">
      <Box sx={{
        width: '100%',
        height: '100vh', // Set full viewport height
        position: 'relative',
        // Continue the canvas surface behind the rounded sidebar corners and gaps.
        ...(!isChatMode ? kasalStageSurface(isDarkMode) : {}),
        display: 'flex',
        flexDirection: 'column',
        overflow: 'hidden' // Prevent scrolling
      }}
      data-tour="workflow-designer"
    >
        {/* Interactive walkthrough tutorial */}
        <InteractiveTutorial isOpen={dialogManager.isTutorialOpen} onClose={dialogManager.handleCloseTutorial} />

        <SessionSidebar
          onOpenSettings={() => dialogManager.setIsConfigurationDialogOpen(true)}
          library={<SessionLibrary />}
          onOpenCatalog={() => {
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

        />

        {/* Chat workspace — replaces the crew/flow canvas and all of its
            sidebars/panels when the user switches to Chat mode. Kept mounted
            (hidden) once opened so a running crew's live SSE stream and state
            survive switching to other modes and back. */}
        {chatEverOpened && (
          <Box
            sx={{
              display: isChatMode ? 'flex' : 'none',
              marginLeft: `${sessionSidebarWidth}px`,
              flex: isChatMode ? 1 : '0 0 auto',
              overflow: 'hidden',
              position: 'relative',
            }}
          >
            <ChatWorkspace onOpenSettings={() => dialogManager.setIsConfigurationDialogOpen(true)} />
          </Box>
        )}

        {!isChatMode && (
        <Box sx={{
          flex: 1,
          display: 'flex',
          flexDirection: 'row',
          overflow: 'hidden',
          position: 'relative',
          marginRight: `${rightSidebarWidth + (!isCompact && showRunHistory ? (responseFocused ? (assistantPanelSide === 'right' ? responseMainWidth : 0) : 268) : 0)}px`,
          marginTop: responseFocused && isCompact ? '55vh' : 0,
          marginLeft: `${effectiveLeftMargin}px` // Push entire content area to the right of LeftSidebar
        }}>
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
              showRunHistory={false}
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
              reasoningEnabled={reasoningEnabled}
              setReasoningEnabled={setReasoningEnabled}
              selectedModel={selectedModel}
              setSelectedModel={setSelectedModel}
              onOpenTutorial={() => {

                dialogManager.setIsTutorialOpen(true);
              }}
              onOpenConfiguration={() => dialogManager.setIsConfigurationDialogOpen(true)}
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

            {!showRunHistory && <Box sx={{ position: 'absolute', top: 4, right: 8, zIndex: 25 }}><BuilderPanelControls /></Box>}
            <CanvasTools
            runControl={<CanvasRunButton
              mode={areFlowsVisible ? 'flow' : 'crew'}
              hasNodes={areFlowsVisible ? flowNodes.some(node => node.type === 'crewNode') : nodes.some(node => ['agentNode', 'taskNode', 'managerNode'].includes(node.type || ''))}
              edges={areFlowsVisible ? flowEdges : edges}
              onRun={() => {
                if (areFlowsVisible) {
                  setCrewExecutionNodes(flowNodes);
                  setCrewExecutionEdges(flowEdges);
                }
                handleRunClick(areFlowsVisible ? 'flow' : 'crew');
              }}
            />}
            onClear={() => { if (areFlowsVisible) { setFlowNodes([]); setFlowEdges([]); } else { setNodes([]); setEdges([]); } }}
            onFit={() => areFlowsVisible ? flowFlowInstanceRef.current?.fitView({ padding: 0.2, duration: 300 }) : handleUIAwareFitView()}
            onZoomIn={() => (areFlowsVisible ? flowFlowInstanceRef : crewFlowInstanceRef).current?.zoomIn({ duration: 200 })}
            onZoomOut={() => (areFlowsVisible ? flowFlowInstanceRef : crewFlowInstanceRef).current?.zoomOut({ duration: 200 })}
            />

            {/* The shared Chat preview occupies the canvas column, so it follows
                the conversation when the user swaps left and right. */}
            <Box id="builder-assistant-preview-host" sx={{ position: 'absolute', inset: 0, zIndex: 20, pointerEvents: 'none' }} />

            {/* Keep workspace navigation available in flow mode and with the composer hidden. */}
            {(!effectiveChatVisible) && (
              <Box sx={{ position: 'absolute', left: 12, right: rightSidebarWidth + 12, bottom: isMobile ? 76 : 12, zIndex: 10, display: 'flex', justifyContent: 'center', pointerEvents: 'none' }}>
                <Box sx={{ pointerEvents: 'auto', display: 'flex', alignItems: 'center', gap: 1, p: 0.75, borderRadius: 3, bgcolor: 'background.paper' }}>
                  <Button color="inherit" size="small" onClick={() => setChatPanelVisible(true)} sx={{ fontSize: 12 }}>Show input</Button>
                </Box>
              </Box>
            )}

            {effectiveChatVisible && (
              <Box sx={{ position: 'absolute', top: 0, left: 12, right: rightSidebarWidth + 12, bottom: isMobile ? 76 : 12, zIndex: 10, pointerEvents: 'none' }}>
                <ChatPanel layout="canvas"
                  builderMode={areFlowsVisible ? 'flow' : 'crew'}
                  onFlowGenerated={(draft) => {
                    const config = buildFlowConfiguration(draft.nodes, draft.edges, draft.name);
                    setFlowNodes(draft.nodes); setFlowEdges(draft.edges);
                    useWorkflowStore.getState().setFlowConfig(config);
                    const tab = getActiveCanvas();
                    if (tab) useFlowStateStore.getState().clearDeclared(tab.id);
                    window.setTimeout(() => flowFlowInstanceRef.current?.fitView({ padding: 0.2, duration: 300 }), 150);
                  }}
                  onNodesGenerated={(newNodes, newEdges) => { handleNodesGenerated(newNodes, newEdges, setNodes, setEdges); }}
                  onLoadingStateChange={setIsChatProcessing} isVisible={showChatPanel} nodes={areFlowsVisible ? flowNodes : nodes} edges={areFlowsVisible ? flowEdges : edges}
                  onExecuteCrew={() => {
                    // Set current tab as running when executing from chat
                    const activeTab = getActiveCanvas();
                    if (activeTab) {
                      setRunningTabId(activeTab.id);
                      updateCanvasExecutionStatus(activeTab.id, 'running');

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
                        updateCanvasExecutionStatus(tabIdToTimeout, 'completed');
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
                  onToggleCollapse={() => setChatPanelVisible(false)}
                  chatSessionId={getActiveCanvas()?.chatSessionId} onOpenLogs={handleShowExecutionLogs} />
              </Box>
            )}
          </Box>
        </Box>
        )}


        {/* Responses and execution history share a resizable workspace pane. */}
        {!isChatMode && showRunHistory && !isCompact && (
          <WorkspaceSplitDivider side={assistantPanelSide} ratio={assistantPanelRatio} leftInset={sessionSidebarWidth} rightInset={rightSidebarWidth} onChange={setAssistantPanelRatio} />
        )}
        {!isChatMode && showRunHistory && (
          <Drawer anchor={assistantPanelSide} variant={isCompact && !responseFocused ? 'temporary' : 'persistent'} open onClose={() => setExecutionHistoryVisible(false)}
            PaperProps={{ 'data-testid': 'workspace-conversation-pane', sx: { background: 'transparent', left: isCompact ? 56 : assistantPanelSide === 'left' ? sessionSidebarWidth + 8 : 'auto', right: isCompact ? 56 : assistantPanelSide === 'right' ? rightSidebarWidth + 8 : 'auto', top: 0, bottom: 8, height: 'auto', width: isCompact ? 'calc(100vw - 112px)' : responseMainWidth - 16, ...(isCompact ? { bottom: 'auto', height: 'calc(55vh - 16px)' } : {}), border: 0, borderRadius: '20px', overflow: 'hidden', boxShadow: 'none' } }}>
            <BuilderPanelControls />
            {areFlowsVisible && <Box id="builder-available-crews-host" sx={{ flex: 1, minHeight: 0, display: showingCrews ? 'flex' : 'none', flexDirection: 'column' }} />}
            <Box id="builder-assistant-response-host" sx={{ flex: 1, minHeight: 0, display: showingResponses ? 'flex' : 'none', flexDirection: 'column' }} />
            <Box id="builder-assistant-composer-host" sx={{ flexShrink: 0 }} />
          </Drawer>
        )}

        {/* Dialogs */}
        <CrewFlowSelectionDialog embedded={!isChatMode}
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
        <CrewFlowSelectionDialog embedded={!isChatMode}
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
          open={dialogManager.isConfigurationDialogOpen}
          onClose={() => dialogManager.setIsConfigurationDialogOpen(false)}
          fullScreen
          aria-label="Settings"
          PaperProps={{
            sx: {
              border: 0,
              borderRadius: 0,
              boxShadow: 'none'
            }
          }}
        >
          <DialogContent sx={{ p: 0, overflow: 'hidden', display: 'flex', flexDirection: 'column' }}>
            <Configuration onClose={() => dialogManager.setIsConfigurationDialogOpen(false)} />
          </DialogContent>
        </Dialog>

        {/* Flow creation is now handled via the FlowCanvas palette */}

        {/* Input Variables Dialog */}
        <InputVariablesDialog
          open={showInputVariablesDialog}
          onClose={() => setShowInputVariablesDialog(false)}
          onConfirm={executeWithVariables}
          /* The pending run's own nodes: `nodes` here is the CREW canvas, so a
             flow run would otherwise be offered the wrong canvas's variables. */
          nodes={pendingVariableExecution?.nodes ?? nodes}
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

        {<Box sx={{ position: 'absolute', right: 4, bottom: { xs: 84, sm: 12 }, zIndex: 1203 }}>
          <TutorialButton onClick={() => dialogManager.setIsTutorialOpen(true)} />
        </Box>}

        {/* Mobile: SpeedDial for quick actions */}
        {isMobile && (
          <SpeedDial
            ariaLabel="Actions"
            sx={{ position: 'fixed', bottom: 16, right: 16, zIndex: 1100 }}
            icon={<SpeedDialIcon />}
          >
            <SpeedDialAction icon={<ChatIcon />} tooltipTitle="Chat" onClick={() => setChatPanelVisible(true)} />
            <SpeedDialAction icon={<HistoryIcon />} tooltipTitle="History" onClick={() => window.dispatchEvent(new Event('openWorkspaceActivity'))} />
            <SpeedDialAction icon={<SettingsIcon />} tooltipTitle="Settings" onClick={() => dialogManager.setIsConfigurationDialogOpen(true)} />
          </SpeedDial>
        )}

      </Box>
    </div>
  );
};

export default WorkflowDesigner;
