import React, { useCallback, useEffect, useRef } from 'react';
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
import { v4 as _uuidv4 } from 'uuid';
import { FlowService as _FlowService } from '../../api/FlowService';
import { useAPIKeysStore as _useAPIKeysStore } from '../../store/apiKeys';
import { FlowFormData as _FlowFormData, FlowConfiguration as _FlowConfiguration } from '../../types/flow';
import { createEdge as _createEdge } from '../../utils/edgeUtils';
import CloseIcon from '@mui/icons-material/Close';

// Component Imports
import { BottomPanelToggle, RightPanelToggle } from './WorkflowToolbarStyle';
import WorkflowPanels from './WorkflowPanels';
import TabBar from './TabBar';
import ChatPanel from '../Chat/ChatPanel';
import RightSidebar from './RightSidebar';
import LeftSidebar from './LeftSidebar';

// Dialog Imports
import AgentDialog from '../Agents/AgentDialog';
import TaskDialog from '../Tasks/TaskDialog';
import CrewPlanningDialog from '../Planning/CrewPlanningDialog';
import { CrewDialog as _CrewDialog } from '../Crew';
import ScheduleDialog from '../Schedule/ScheduleDialog';
import JobsPanel from '../Jobs/JobsPanel';
import Tutorial from '../Tutorial/Tutorial';
import APIKeys from '../Configuration/APIKeys/APIKeys';
import Logs from '../Jobs/LLMLogs';
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
  useFlowAddHandler,
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
    tabs
  } = useTabManagerStore();

  // Use flow configuration store
  const { crewAIFlowEnabled } = useFlowConfigStore();

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
  const { activeTabId } = useTabSync({ nodes, edges, setNodes, setEdges });

  // Use agent and task managers with original flow manager
  const {
    agents,
    addAgentNode: _addAgentNode,
    isAgentDialogOpen,
    setIsAgentDialogOpen,
    handleAgentSelect,
    handleShowAgentForm,
    fetchAgents
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
    fetchTasks
  } = useTaskManager({ 
    nodes, 
    setNodes
  });

  // Use the panel manager
  const {
    panelPosition,
    setPanelPosition,
    isDraggingPanel,
    setIsDraggingPanel,
    panelState,
    setPanelState: _setPanelState,
    areFlowsVisible,
    setAreFlowsVisible: _setAreFlowsVisible,
    handlePanelDragStart: _handlePanelDragStart,
    handleSnapToLeft: _handleSnapToLeft,
    handleSnapToRight: _handleSnapToRight,
    handleResetPanel: _handleResetPanel,
    toggleFlowsVisibility,
    showRunHistory,
    setShowRunHistory,
    showChatPanel,
    setShowChatPanel: _setShowChatPanel,
    toggleChatPanel
  } = usePanelManager();

  // Use the dialog manager
  const dialogManager = useDialogManager(hasSeenTutorial, setHasSeenTutorial);

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
    executeCrew,
    executeFlow,
    setNodes: setCrewExecutionNodes,
    setEdges: setCrewExecutionEdges
  } = useCrewExecutionStore();

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
    };
  }, []);

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
  const _handleAddFlowNode = useFlowAddHandler(setNodes, setEdges, nodes, edges, showErrorMessage);

  // Use crew flow dialog handler
  const {
    isCrewFlowDialogOpen,
    setIsCrewFlowDialogOpen,
    openCrewOrFlowDialog
  } = useCrewFlowDialogHandler();

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
      // Execute the tab directly with its nodes and edges
      await executeTab(tabId, tab.nodes, tab.edges, tab.name);
    }
  }, [tabs, executeTab]);

  // Internal fitView function to handle both canvas instances
  const handleFitViewToNodesInternal = useCallback(() => {
    // Attempt to fit view on both crew and flow instances
    if (crewFlowInstanceRef.current) {
      try {
        setTimeout(() => {
          crewFlowInstanceRef.current?.fitView({
            padding: 0.2,
            includeHiddenNodes: false,
            duration: 800
          });
        }, 100);
      } catch (error) {
        console.error('Error fitting view to nodes in CrewCanvas:', error);
      }
    }
    
    if (flowFlowInstanceRef.current) {
      try {
        setTimeout(() => {
          flowFlowInstanceRef.current?.fitView({
            padding: 0.2,
            includeHiddenNodes: false,
            duration: 800
          });
        }, 100);
      } catch (error) {
        console.error('Error fitting view to nodes in FlowCanvas:', error);
      }
    }
  }, [crewFlowInstanceRef, flowFlowInstanceRef]);

  // Listen for the internal fit view event
  useEffect(() => {
    window.addEventListener('fitViewToNodesInternal', handleFitViewToNodesInternal);
    
    return () => {
      window.removeEventListener('fitViewToNodesInternal', handleFitViewToNodesInternal);
    };
  }, [handleFitViewToNodesInternal]);

  // Render the component
  return (
    <div className="workflow-designer">
      <Box sx={{ 
        width: '100%', 
        height: '100vh', // Set full viewport height
        position: 'relative',
        bgcolor: isDarkMode ? '#1a1a1a' : '#ffffff',
        display: 'flex',
        flexDirection: 'column',
        overflow: 'hidden' // Prevent scrolling
      }}>
        {/* Only render the Tutorial component when needed to avoid unmounting issues */}
        {dialogManager.isTutorialOpen && (
          <Tutorial isOpen={dialogManager.isTutorialOpen} onClose={dialogManager.handleCloseTutorial} />
        )}
        
        {/* Tab Bar */}
        <TabBar 
          onRunTab={handleRunTab}
          isRunning={isExecuting}
          runningTabId={activeTabId}
          onLoadCrew={() => setIsCrewFlowDialogOpen(true)}
        />
        
        <Box sx={{ 
          flex: 1, 
          display: 'flex', 
          flexDirection: 'row',
          overflow: 'hidden',
          position: 'relative'
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
              showRunHistory={showRunHistory}
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
              setIsAgentDialogOpen={setIsAgentDialogOpen}
              setIsTaskDialogOpen={setIsTaskDialogOpen}
              setIsFlowDialogOpen={dialogManager.setIsFlowDialogOpen}
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

            {/* Chat Panel positioned to the left of the RightSidebar */}
            {showChatPanel && (
              <Box 
                onMouseEnter={() => {
                  // Notify RightSidebar that mouse is over chat
                  window.postMessage({ type: 'chat-hover-state', isHovering: true }, '*');
                }}
                onMouseLeave={() => {
                  // Notify RightSidebar that mouse left chat
                  window.postMessage({ type: 'chat-hover-state', isHovering: false }, '*');
                }}
                sx={{ 
                  position: 'absolute',
                  top: 0,
                  right: 48, // Position to the left of RightSidebar (48px width)
                  width: '350px',
                  height: '100%',
                  display: 'flex',
                  flexDirection: 'column',
                  overflow: 'hidden',
                  borderLeft: 1,
                  borderColor: 'divider',
                  backgroundColor: 'background.paper',
                  zIndex: 6 // Higher than RightSidebar (5)
                }}>
                <ChatPanel onNodesGenerated={(newNodes, newEdges) => {
                  setNodes(currentNodes => [...currentNodes, ...newNodes]);
                  setEdges(currentEdges => [...currentEdges, ...newEdges]);
                }} />
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
        
        <BottomPanelToggle
          isVisible={showRunHistory}
          togglePanel={() => setShowRunHistory(!showRunHistory)}
          position="bottom"
          tooltip={showRunHistory ? "Hide Jobs Panel" : "Show Jobs Panel (Run History)"}
        />
        


        {/* Jobs Panel with Run History and Kasal */}
        {showRunHistory && (
          <Box sx={{ 
            height: '200px', // Adjusted to fit 4 rows, header, and pagination
            backgroundColor: isDarkMode ? '#1a1a1a' : '#ffffff',
            display: 'flex',
            flexDirection: 'column',
            borderTop: 1,
            borderColor: 'divider'
          }}>
            <JobsPanel />
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
        />

        <TaskDialog
          open={isTaskDialogOpen}
          onClose={() => setIsTaskDialogOpen(false)}
          onTaskSelect={handleTaskSelect}
          tasks={tasks}
          onShowTaskForm={handleShowTaskForm}
          fetchTasks={fetchTasks}
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
          onClose={() => setIsCrewFlowDialogOpen(false)}
          onCrewSelect={handleCrewSelectWrapper}
          onFlowSelect={handleFlowSelect}
        />

        {/* Flow Selection Dialog */}
        <CrewFlowSelectionDialog
          open={isFlowDialogOpen}
          onClose={() => setIsFlowDialogOpen(false)}
          onCrewSelect={handleCrewSelectWrapper}
          onFlowSelect={handleFlowSelect}
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

        {/* Right Sidebar */}
        <RightSidebar
          onOpenLogsDialog={() => dialogManager.setIsLogsDialogOpen(true)}
          onToggleChat={() => _setShowChatPanel(!showChatPanel)}
          isChatOpen={showChatPanel}
          setIsAgentDialogOpen={setIsAgentDialogOpen}
          setIsTaskDialogOpen={setIsTaskDialogOpen}
          setIsFlowDialogOpen={dialogManager.setIsFlowDialogOpen}
          setIsCrewPlanningOpen={dialogManager.setCrewPlanningOpen}
          setIsCrewDialogOpen={openCrewOrFlowDialog}
          setIsAgentGenerationDialogOpen={() => {
            const event = new CustomEvent('openAgentGenerationDialog');
            window.dispatchEvent(event);
          }}
          setIsTaskGenerationDialogOpen={() => {
            const event = new CustomEvent('openTaskGenerationDialog');
            window.dispatchEvent(event);
          }}
          handleExecuteCrew={() => executeCrew(nodes, edges)}
          handleExecuteFlow={() => executeFlow(nodes, edges)}
          isExecuting={isExecuting}
          onSaveCrewClick={() => {
            const event = new CustomEvent('openSaveCrewDialog');
            window.dispatchEvent(event);
          }}
          showRunHistory={showRunHistory}
        />

        {/* Left Sidebar */}
        <LeftSidebar
          onClearCanvas={() => {
            setNodes([]);
            setEdges([]);
          }}
          onGenerateConnections={async () => {
            // Implementation for generating connections
            console.log('Generating connections...');
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
            const reactFlowInstance = crewFlowInstanceRef.current || flowFlowInstanceRef.current;
            if (reactFlowInstance) {
              reactFlowInstance.fitView({ padding: 0.2, duration: 200 });
            }
          }}
          onToggleInteractivity={() => {
            // Toggle interactivity if needed
            console.log('Toggle interactivity');
          }}
          isGeneratingConnections={false}
          planningEnabled={planningEnabled}
          setPlanningEnabled={setPlanningEnabled}
          reasoningEnabled={reasoningEnabled}
          setReasoningEnabled={setReasoningEnabled}
          schemaDetectionEnabled={schemaDetectionEnabled}
          setSchemaDetectionEnabled={setSchemaDetectionEnabled}
          selectedModel={selectedModel}
          setSelectedModel={setSelectedModel}
          setIsConfigurationDialogOpen={dialogManager.setIsConfigurationDialogOpen}
          showRunHistory={showRunHistory}
        />
      </Box>
    </div>
  );
};

export default WorkflowDesigner; 