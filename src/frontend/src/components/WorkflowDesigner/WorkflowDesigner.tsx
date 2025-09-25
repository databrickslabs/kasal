import React, { useCallback, useEffect, useRef, useState } from 'react';
import {
  ReactFlowProvider as _ReactFlowProvider,
  Node as _Node,
  Edge as _Edge,
  OnSelectionChangeParams as _OnSelectionChangeParams,
  ReactFlowInstance as _ReactFlowInstance,
  Connection as _Connection,
} from 'reactflow';
import 'reactflow/dist/style.css';
import { Box, Snackbar, Alert, Dialog, DialogContent, Menu, Button, DialogTitle, IconButton, Typography } from '@mui/material';
import { useWorkflowStore } from '../../store/workflow';
import { useThemeManager } from '../../hooks/workflow/useThemeManager';
import { useErrorManager } from '../../hooks/workflow/useErrorManager';
import { useFlowManager } from '../../hooks/workflow/useFlowManager';
import { useCrewExecutionStore } from '../../store/crewExecution';
import { useTabManagerStore } from '../../store/tabManager';
import { useFlowConfigStore } from '../../store/flowConfig';
import { useTabSync } from '../../hooks/workflow/useTabSync';
import { useRunStatusStore } from '../../store/runStatus';
import { useChatPanelResize } from '../../hooks/workflow/useChatPanelResize';
import { useExecutionHistoryResize } from '../../hooks/workflow/useExecutionHistoryResize';

import { v4 as _uuidv4 } from 'uuid';
import { FlowService as _FlowService } from '../../api/FlowService';
import { useAPIKeysStore as _useAPIKeysStore } from '../../store/apiKeys';
import { FlowFormData as _FlowFormData, FlowConfiguration as _FlowConfiguration } from '../../types/flow';
import { ConnectionAgent, ConnectionTask } from '../../types/connection';
import { createEdge as _createEdge } from '../../utils/edgeUtils';
import CloseIcon from '@mui/icons-material/Close';

// Component Imports
import { RightPanelToggle } from './WorkflowToolbarStyle';
import { InputVariablesDialog } from '../Jobs/InputVariablesDialog';
import WorkflowPanels from './WorkflowPanels';
import TabBar from './TabBar';
import ChatPanel from '../Chat/ChatPanel';
import RightSidebar from './RightSidebar';
import LeftSidebar from './LeftSidebar';
import { useUILayoutStore } from '../../store/uiLayout';
import { useUIFitView } from '../../hooks/workflow/useUIFitView';
import { useWorkflowLayoutEvents } from '../../hooks/workflow/useWorkflowLayoutEvents';
import { useTaskExecutionStore } from '../../store/taskExecutionStore';

// Dialog Imports
import AgentDialog from '../Agents/AgentDialog';
import TaskDialog from '../Tasks/TaskDialog';
import CrewPlanningDialog from '../Planning/CrewPlanningDialog';
import { CrewDialog as _CrewDialog } from '../Crew';
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
import { AddFlowDialog } from '../Flow';
import { CrewFlowSelectionDialog } from '../Crew/CrewFlowDialog';
import SaveCrew from '../Crew/SaveCrew';

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
  useFlowDialogHandler,
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
    updateTabExecutionStatus
  } = useTabManagerStore();

  // Use flow configuration store
  const { crewAIFlowEnabled } = useFlowConfigStore();

  // Use run status store for fallback job monitoring
  const { runHistory, startPolling: startRunStatusPolling, stopPolling: stopRunStatusPolling } = useRunStatusStore();

  // Use flow store for node/edge management
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

  // Use tab sync to keep tabs and flow manager in sync
  const { activeTabId: _activeTabId } = useTabSync({ nodes, edges, setNodes, setEdges });

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
    setUIStoreAreFlowsVisible(!areFlowsVisible);
  }, [areFlowsVisible, setUIStoreAreFlowsVisible]);

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
      }
    };

    window.addEventListener('openExecutionHistory', handleOpenExecutionHistory);
    return () => {
      window.removeEventListener('openExecutionHistory', handleOpenExecutionHistory);
    };

  }, [showRunHistory, setExecutionHistoryVisible]);

  // Use the dialog manager
  const dialogManager = useDialogManager(hasSeenTutorial, setHasSeenTutorial);


  // Connection generation state
  const [isGeneratingConnections, setIsGeneratingConnections] = React.useState(false);
  const [isChatProcessing, setIsChatProcessing] = React.useState(false);
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

  // Chat panel resize handlers
  const { handleResizeStart } = useChatPanelResize(setChatPanelWidth);

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

  // Execution history resize handlers
  const { handleHistoryResizeStart } = useExecutionHistoryResize(
    setExecutionHistoryHeight,
    setHasManuallyResized
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
    executeWithVariables
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

  // Sync nodes and edges with crew execution store
  useEffect(() => {
    setCrewExecutionNodes(nodes);
  }, [nodes, setCrewExecutionNodes]);

  useEffect(() => {
    setCrewExecutionEdges(edges);
  }, [edges, setCrewExecutionEdges]);

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

  // Track the currently executing job ID
  const [executingJobId, setExecutingJobId] = React.useState<string | null>(null);
  const runningTabTimeoutRef = React.useRef<NodeJS.Timeout | null>(null);
  const taskStatusPollingInterval = React.useRef<NodeJS.Timeout | null>(null);

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

      // Clear any existing polling interval
      if (taskStatusPollingInterval.current) {
        clearInterval(taskStatusPollingInterval.current);
      }

      // Start polling every 2 seconds
      taskStatusPollingInterval.current = setInterval(() => {
        loadTaskStates(jobId);
      }, 2000);

      // Also load immediately
      loadTaskStates(jobId);

      // Ensure polling is running to monitor job status
      startRunStatusPolling();
    };

    window.addEventListener('jobCreated', handleJobCreated as EventListener);
    return () => {
      window.removeEventListener('jobCreated', handleJobCreated as EventListener);
    };
  }, [startRunStatusPolling, loadTaskStates, clearTaskStates, executingJobId]);

  // Listen for job completion events to clear running tab and update status
  useEffect(() => {
    const handleJobCompleted = () => {

      // Stop task status polling
      if (taskStatusPollingInterval.current) {
        clearInterval(taskStatusPollingInterval.current);
        taskStatusPollingInterval.current = null;
      }

      // Clear task states after a longer delay to show final states
      // Increased from 3 seconds to 10 seconds to give users time to see the final status
      setTimeout(() => {
        clearTaskStates();
      }, 10000);

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

      // Stop task status polling
      if (taskStatusPollingInterval.current) {
        clearInterval(taskStatusPollingInterval.current);
        taskStatusPollingInterval.current = null;
      }

      // Clear task states after a longer delay to show final states
      // Increased from 3 seconds to 10 seconds to give users time to see the final status
      setTimeout(() => {
        clearTaskStates();
      }, 10000);

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

      // Clean up polling interval if component unmounts
      if (taskStatusPollingInterval.current) {
        clearInterval(taskStatusPollingInterval.current);
        taskStatusPollingInterval.current = null;
      }
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
      // Clean up polling when component unmounts
      stopRunStatusPolling();
    };
  }, [stopRunStatusPolling]);

  // Check for running jobs on component mount and start polling if found
  useEffect(() => {
    const checkForRunningJobs = async () => {
      // Get fresh data from stores
      const { tabs: currentTabs } = useTabManagerStore.getState();
      const { fetchRunHistory: fetchHistory } = useRunStatusStore.getState();
      const { loadTaskStates: loadStates, clearTaskStates: clearStates } = useTaskExecutionStore.getState();

      // First check the tabs for any running status
      const runningTab = currentTabs.find(tab => tab.executionStatus === 'running');
      if (runningTab) {
        setRunningTabId(runningTab.id);
      }

      // Fetch the latest run history to ensure we have the most recent data
      await fetchHistory();

      // Get updated run history after fetch
      const { runHistory: updatedRunHistory } = useRunStatusStore.getState();

      // Check the runHistory for any running jobs
      if (updatedRunHistory.length > 0) {
        const runningJobs = updatedRunHistory.filter(run =>
          run.status.toLowerCase() === 'running' ||
          run.status.toLowerCase() === 'queued'
        );

        if (runningJobs.length > 0) {
          // Take the most recent running job
          const mostRecentJob = runningJobs[0];
          setExecutingJobId(mostRecentJob.job_id);

          // Clear any existing task states
          clearStates();

          // Clear any existing polling interval
          if (taskStatusPollingInterval.current) {
            clearInterval(taskStatusPollingInterval.current);
          }

          // Start polling every 2 seconds
          taskStatusPollingInterval.current = setInterval(() => {
            loadStates(mostRecentJob.job_id);
          }, 2000);

          // Do initial load immediately
          loadStates(mostRecentJob.job_id);
        }
      }
    };

    // Run the check after a short delay to ensure stores are initialized
    const timer = setTimeout(checkForRunningJobs, 100);

    return () => {
      clearTimeout(timer);
      // Clean up polling interval if it exists
      if (taskStatusPollingInterval.current) {
        clearInterval(taskStatusPollingInterval.current);
      }
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

  // Use flow selection handler
  const handleFlowSelect = useFlowSelectHandler(setNodes, setEdges);

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

  // Use flow dialog handler
  const handleFlowDialogAction = useFlowDialogHandler(setNodes, setEdges, showErrorMessage);

  // Use event bindings
  const {
    handleRunClickWrapper: _handleRunClickWrapper,
    handleCrewSelectWrapper
  } = useEventBindings(
    // Cast the handleRunClick to match the expected signature
    (executionType?: 'flow' | 'crew') =>
      executionType ? handleRunClick(executionType) : Promise.resolve(),
    setNodes,
    setEdges
  );

  // Add a ref for the SaveCrew dialog
  const saveCrewRef = useRef<HTMLButtonElement>(null);

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

      // Fetch historical logs and connect to WebSocket
      const historicalLogs = await executionLogService.getHistoricalLogs(jobToShow);
      setSelectedJobLogs(historicalLogs.map(({ job_id, execution_id, ...rest }) => ({
        ...rest,
        output: rest.output || rest.content,
        id: rest.id || Date.now()
      })));

      executionLogService.connectToJobLogs(jobToShow);

      const unsubscribeConnect = executionLogService.onConnected(jobToShow, () => {
        setIsConnectingLogs(false);

      });

      const unsubscribeLogs = executionLogService.onJobLogs(jobToShow, (logMessage) => {
        setSelectedJobLogs(prevLogs => [...prevLogs, {
          id: logMessage.id || Date.now(),
          output: logMessage.output || logMessage.content,
          timestamp: logMessage.timestamp
        }]);
      });

      const unsubscribeError = executionLogService.onError(jobToShow, () => {

        setConnectionError('Failed to connect to log stream');
        setIsConnectingLogs(false);
      });

      const unsubscribeClose = executionLogService.onClose(jobToShow, () => {

        setIsConnectingLogs(false);
      });

      // Store the unsubscribe functions to be called on cleanup
      return () => {
        unsubscribeConnect();
        unsubscribeLogs();
        unsubscribeError();
        unsubscribeClose();
        executionLogService.disconnectFromJobLogs(jobToShow);
      };
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
          onLoadAgents={() => {
            setCrewFlowDialogInitialTab(1);
            setCrewFlowDialogShowOnlyTab(1); // Only show Agents tab
            setIsCrewFlowDialogOpen(true);
          }}
          onLoadTasks={() => {
            setCrewFlowDialogInitialTab(2);
            setCrewFlowDialogShowOnlyTab(2); // Only show Tasks tab
            setIsCrewFlowDialogOpen(true);
          }}
          disabled={isChatProcessing || isGeneratingConnections || !!runningTabId}
        />

        <Box sx={{
          flex: 1,
          display: 'flex',
          flexDirection: 'row',
          overflow: 'hidden',
          position: 'relative',
          marginLeft: `${leftSidebarBaseWidth}px` // Push entire content area to the right of LeftSidebar
        }}>
          {/* Chat Panel on Left (when positioned left) */}
          {showChatPanel && chatPanelSide === 'left' && (
            <Box
              onMouseEnter={() => {
                window.postMessage({ type: 'chat-hover-state', isHovering: true }, '*');
              }}
              onMouseLeave={() => {
                window.postMessage({ type: 'chat-hover-state', isHovering: false }, '*');
              }}
              sx={{
                width: isChatCollapsed ? `${chatPanelCollapsedWidth}px` : `${chatPanelWidth}px`,
                height: showRunHistory ? `calc(100% - ${executionHistoryHeight}px)` : '100%',
                display: 'flex',
                flexDirection: 'row',
                overflow: 'hidden',
                backgroundColor: 'background.paper',
                borderRight: 1,
                borderColor: 'divider',
                zIndex: 15, // Higher than LeftSidebar (10) to ensure collapsed chat is visible
                transition: isChatCollapsed ? 'width 0.3s ease-in-out' : 'none',
              }}>
              {/* Chat Content */}
              <Box sx={{ flex: 1, overflow: 'hidden' }}>
                <ChatPanel
                  chatSide={chatPanelSide}
                  onNodesGenerated={(newNodes, newEdges) => {
                    setNodes(currentNodes => [...currentNodes, ...newNodes]);
                    setEdges(currentEdges => [...currentEdges, ...newEdges]);
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
                  isCollapsed={isChatCollapsed}
                  onToggleCollapse={() => {
                    setChatPanelCollapsed(!isChatCollapsed);
                    // Trigger node repositioning when toggling collapse
                    setTimeout(() => {
                      const event = new CustomEvent('recalculateNodePositions', {
                        detail: { reason: 'chat-panel-toggle' }
                      });
                      window.dispatchEvent(event);
                    }, 350); // Wait for animation to complete
                  }}
                  chatSessionId={getActiveTab()?.chatSessionId}
                  onOpenLogs={handleShowExecutionLogs}
                />
              </Box>
              {/* Resize Handle - on the right side when chat is on left */}
              {!isChatCollapsed && (
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
              showRunHistory={false}
              panelPosition={panelPosition}
              isDraggingPanel={isDraggingPanel}
              isDarkMode={isDarkMode}
              nodes={nodes}
              edges={edges}
              onNodesChange={onNodesChange}
              onEdgesChange={onEdgesChange}
              onConnect={onConnect}
              onSelectionChange={onSelectionChange}
              onPaneContextMenu={handlePaneContextMenu}
              onCrewFlowInit={handleCrewFlowInit}
              onFlowFlowInit={handleFlowFlowInit}
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
              setIsFlowDialogOpen={dialogManager.setIsFlowDialogOpen}
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

            {/* Chat Panel on Right (when positioned right) */}
            {showChatPanel && chatPanelSide === 'right' && (
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
                  width: isChatCollapsed ? `${chatPanelCollapsedWidth}px` : `${chatPanelWidth}px`,
                  display: 'flex',
                  flexDirection: 'row',
                  overflow: 'hidden',
                  backgroundColor: 'background.paper',
                  zIndex: 10,
                  transition: isChatCollapsed ? 'width 0.3s ease-in-out' : 'none',
                }}>
                {/* Resize Handle */}
                {!isChatCollapsed && (
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
                      setNodes(currentNodes => [...currentNodes, ...newNodes]);
                      setEdges(currentEdges => [...currentEdges, ...newEdges]);
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
                    isCollapsed={isChatCollapsed}
                    onToggleCollapse={() => {
                      setChatPanelCollapsed(!isChatCollapsed);
                      // Trigger node repositioning when toggling collapse
                      setTimeout(() => {
                        const event = new CustomEvent('recalculateNodePositions', {
                          detail: { reason: 'chat-panel-toggle' }
                        });
                        window.dispatchEvent(event);
                      }, 350); // Wait for animation to complete
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

        {/* Toggle buttons for panels - shown in both layouts */}
        {crewAIFlowEnabled && (
          <RightPanelToggle
            isVisible={areFlowsVisible}
            togglePanel={toggleFlowsVisibility}
            position="right"
            tooltip={areFlowsVisible ? "Hide Flows Panel" : "Show Flows Panel"}
          />
        )}




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
          onCrewSelect={handleCrewSelectWrapper}
          onFlowSelect={handleFlowSelect}
          onAgentSelect={handleAgentSelect}
          onTaskSelect={handleTaskSelect}
          initialTab={crewFlowDialogInitialTab}
          showOnlyTab={crewFlowDialogShowOnlyTab}
        />

        {/* Flow Selection Dialog */}
        <CrewFlowSelectionDialog
          open={isFlowDialogOpen}
          onClose={() => setIsFlowDialogOpen(false)}
          onCrewSelect={handleCrewSelectWrapper}
          onFlowSelect={handleFlowSelect}
          onAgentSelect={handleAgentSelect}
          onTaskSelect={handleTaskSelect}
          initialTab={1} // Set to Flows tab
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

        {/* Add FlowDialog */}
        <AddFlowDialog
          open={dialogManager.isFlowDialogOpen}
          onClose={() => dialogManager.setIsFlowDialogOpen(false)}
          onAddCrews={handleFlowDialogAction}
        />

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

        {/* Execution Logs Dialog */}
        <ShowLogs
          open={showExecutionLogsDialog}
          onClose={() => {
            setShowExecutionLogsDialog(false);
            if (selectedExecutionJobId) {
              executionLogService.disconnectFromJobLogs(selectedExecutionJobId);
            }
          }}
          logs={selectedJobLogs}
          jobId={selectedExecutionJobId || ''}
          isConnecting={isConnectingLogs}
          connectionError={connectionError}
        />

        {/* Right Sidebar */}
        <RightSidebar
          onOpenLogsDialog={() => dialogManager.setIsLogsDialogOpen(true)}
          onToggleChat={() => setChatPanelVisible(!showChatPanel)}
          isChatOpen={showChatPanel}
          setIsAgentDialogOpen={() => openAgentDialog(true)}
          setIsTaskDialogOpen={() => openTaskDialog(true)}
          setIsFlowDialogOpen={dialogManager.setIsFlowDialogOpen}
          setIsCrewDialogOpen={() => {
            setCrewFlowDialogInitialTab(0);
            setCrewFlowDialogShowOnlyTab(undefined); // Show all tabs for catalog
            setIsCrewFlowDialogOpen(true);
          }}
          onSaveCrewClick={() => {
            const event = new CustomEvent('openSaveCrewDialog');
            window.dispatchEvent(event);
          }}
          showRunHistory={showRunHistory}
          executionHistoryHeight={executionHistoryHeight}
          onOpenSchedulesDialog={() => {
            // Open schedule dialog
            dialogManager.setScheduleDialogOpen(true);
          }}
          onToggleExecutionHistory={toggleExecutionHistory}
        />

        {/* Left Sidebar */}
        <LeftSidebar
          onClearCanvas={() => {
            setNodes([]);
            setEdges([]);
          }}
          isGeneratingConnections={isGeneratingConnections}
          onGenerateConnections={async () => {
            // Implementation for generating connections using ConnectionService
            setIsGeneratingConnections(true);
            try {
              // Import required services
              const { ConnectionService } = await import('../../api/ConnectionService');

              // Debug: Log all nodes to see their structure
              console.log('All nodes:', nodes);
              console.log('Node types found:', nodes.map(node => ({ id: node.id, type: node.type, data: node.data })));

              // Extract agent nodes from the current nodes
              const agentNodes = nodes.filter(node => node.type === 'agentNode');
              const taskNodes = nodes.filter(node => node.type === 'taskNode');

              console.log('Filtered agent nodes:', agentNodes);
              console.log('Filtered task nodes:', taskNodes);

              if (agentNodes.length === 0) {
                showErrorMessage('No agents found. Please add at least one agent to generate connections.');
                return;
              }

              if (taskNodes.length === 0) {
                showErrorMessage('No tasks found. Please add at least one task to generate connections.');
                return;
              }

              // Convert agent nodes to ConnectionAgent format
              const agents: ConnectionAgent[] = agentNodes.map(node => ({
                name: node.data.label || node.data.name || `Agent ${node.id}`,
                role: node.data.role || 'Assistant',
                goal: node.data.goal || 'Complete assigned tasks effectively',
                backstory: node.data.backstory || 'A dedicated AI agent designed to help with various tasks',
                tools: node.data.tools || []
              }));

              // Convert task nodes to ConnectionTask format
              const tasks: ConnectionTask[] = taskNodes.map(node => ({
                name: node.data.label || node.data.name || `Task ${node.id}`,
                description: node.data.description || node.data.label || `Task ${node.id}`,
                expected_output: node.data.expected_output || 'Complete the task successfully',
                agent_id: node.data.agent_id || '',
                tools: node.data.tools || [],
                async_execution: node.data.async_execution || false,
                human_input: node.data.config?.human_input || false,
                markdown: node.data.config?.markdown || false,
                context: {
                  type: 'general',
                  priority: node.data.config?.priority === 1 ? 'high' :
                           node.data.config?.priority === 2 ? 'medium' : 'low',
                  complexity: 'medium',
                  required_skills: [],
                  metadata: {}
                }
              }));

              // Use Databricks Llama 70b model (try simpler format)
              const model = 'llama-3.1-70b-instruct';

              console.log('Generating connections with:', { agents, tasks, model });

              // Call the ConnectionService to generate intelligent connections
              const response = await ConnectionService.generateConnections(agents, tasks, model);

              console.log('Connection generation response:', response);

              // Create edges based on the AI-generated assignments
              const newEdges: _Edge[] = [];

              response.assignments.forEach(assignment => {
                const agentNode = agentNodes.find(node =>
                  (node.data.label || node.data.name) === assignment.agent_name
                );

                if (agentNode) {
                  assignment.tasks.forEach(taskAssignment => {
                    const taskNode = taskNodes.find(node =>
                      (node.data.label || node.data.name) === taskAssignment.task_name
                    );

                    if (taskNode) {
                      const edgeId = `${agentNode.id}-${taskNode.id}`;
                      newEdges.push({
                        id: edgeId,
                        source: agentNode.id,
                        target: taskNode.id,
                        type: 'default',
                        data: {
                          reasoning: taskAssignment.reasoning
                        }
                      });
                    }
                  });
                }
              });

              // Add dependency edges based on AI-generated dependencies
              response.dependencies?.forEach(dependency => {
                const dependentTask = taskNodes.find(node =>
                  (node.data.label || node.data.name) === dependency.task_name
                );

                if (dependentTask && dependency.depends_on) {
                  dependency.depends_on.forEach(requiredTaskName => {
                    const requiredTask = taskNodes.find(node =>
                      (node.data.label || node.data.name) === requiredTaskName
                    );

                    if (requiredTask) {
                      const edgeId = `dep-${requiredTask.id}-${dependentTask.id}`;
                      newEdges.push({
                        id: edgeId,
                        source: requiredTask.id,
                        target: dependentTask.id,
                        type: 'default',
                        style: { stroke: '#ff6b6b', strokeDasharray: '5,5' },
                        data: {
                          reasoning: dependency.reasoning,
                          isDependency: true
                        }
                      });
                    }
                  });
                }
              });

              // Update the edges in the flow
              if (newEdges.length > 0) {
                setEdges(newEdges);
                console.log(`Generated ${newEdges.length} connections based on CrewAI best practices`);
              } else {
                showErrorMessage('No connections could be generated. Please check that agent and task names are properly set.');
              }

            } catch (error) {
              console.error('Error generating connections:', error);
              showErrorMessage(`Failed to generate connections: ${error instanceof Error ? error.message : 'Unknown error'}`);
            } finally {
              setIsGeneratingConnections(false);
            }
          }}
          onZoomIn={() => {
            const reactFlowInstance = crewFlowInstanceRef.current || flowFlowInstanceRef.current;
            if (reactFlowInstance) {
              reactFlowInstance.zoomIn({ duration: 200 });
            }
          }}
          onZoomOut={() => {
            const reactFlowInstance = crewFlowInstanceRef.current || flowFlowInstanceRef.current;
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
        />

      </Box>
    </div>
  );
};

export default WorkflowDesigner;