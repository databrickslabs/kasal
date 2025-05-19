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
import { Box, Snackbar, Alert, Dialog, DialogContent, Menu, Tabs, Tab, Button } from '@mui/material';
import { History } from '@mui/icons-material';
import { useWorkflowStore } from '../../store/workflow';
import { useThemeManager } from '../../hooks/workflow/useThemeManager';
import { useErrorManager } from '../../hooks/workflow/useErrorManager';
import { useFlowManager } from '../../hooks/workflow/useFlowManager';
import { useCrewExecutionStore } from '../../store/crewExecution';
import { v4 as _uuidv4 } from 'uuid';
import { FlowService as _FlowService } from '../../api/FlowService';
import { useAPIKeysStore as _useAPIKeysStore } from '../../store/apiKeys';
import { FlowFormData as _FlowFormData, FlowConfiguration as _FlowConfiguration } from '../../types/flow';
import { createEdge as _createEdge } from '../../utils/edgeUtils';

// Component Imports
import WorkflowToolbar from './WorkflowToolbar';
import { BottomPanelToggle, RightPanelToggle } from './WorkflowToolbarStyle';
import WorkflowPanels from './WorkflowPanels';

// Dialog Imports
import AgentDialog from '../Agents/AgentDialog';
import TaskDialog from '../Tasks/TaskDialog';
import CrewPlanningDialog from '../Planning/CrewPlanningDialog';
import { CrewDialog as _CrewDialog } from '../Crew';
import ScheduleDialog from '../Schedule/ScheduleDialog';
import RunHistory, { RunHistoryRef } from '../Jobs/ExecutionHistory';
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
  
  // Use flow store for node/edge management
  const { 
    nodes, 
    setNodes, 
    edges, 
    setEdges,
    onNodesChange, 
    onEdgesChange, 
    onConnect, 
    handleEdgeContextMenu: _handleEdgeContextMenu,
    selectedEdges: _selectedEdges, 
    setSelectedEdges,
    manuallyPositionedNodes
  } = useFlowManager({ showErrorMessage });

  // Use agent and task managers
  const {
    agents,
    addAgentNode: _addAgentNode,
    isAgentDialogOpen,
    setIsAgentDialogOpen,
    handleAgentSelect,
    handleShowAgentForm,
    fetchAgents
  } = useAgentManager({ nodes, setNodes });

  const {
    tasks,
    addTaskNode: _addTaskNode,
    isTaskDialogOpen,
    setIsTaskDialogOpen,
    handleTaskSelect,
    handleShowTaskForm,
    fetchTasks
  } = useTaskManager({ nodes, setNodes });

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
    setShowRunHistory
  } = usePanelManager();

  // Use the dialog manager
  const dialogManager = useDialogManager(hasSeenTutorial, setHasSeenTutorial);

  // Use crew execution store
  const {
    isExecuting,
    selectedModel,
    planningEnabled,
    schemaDetectionEnabled,
    tools,
    selectedTools,
    jobId,
    setSelectedModel,
    setPlanningEnabled,
    setSchemaDetectionEnabled,
    setSelectedTools,
    handleRunClick,
    handleGenerateCrew,
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

  // Create ref for RunHistory component
  const runHistoryRef = useRef<RunHistoryRef>(null);
  
  // Add effect to handle job ID changes
  useEffect(() => {
    if (jobId && runHistoryRef.current) {
      runHistoryRef.current.refreshRuns();
    }
  }, [jobId]);

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
    handleRunClickWrapper,
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
        
        <WorkflowToolbar
          selectedModel={selectedModel}
          setSelectedModel={setSelectedModel}
          planningEnabled={planningEnabled}
          setPlanningEnabled={setPlanningEnabled}
          setIsScheduleDialogOpen={dialogManager.setScheduleDialogOpen}
          setIsAgentDialogOpen={setIsAgentDialogOpen}
          setIsTaskDialogOpen={setIsTaskDialogOpen}
          setIsFlowDialogOpen={dialogManager.setIsFlowDialogOpen}
          setIsCrewPlanningOpen={dialogManager.setCrewPlanningOpen}
          setIsLogsDialogOpen={dialogManager.setIsLogsDialogOpen}
          setIsConfigurationDialogOpen={dialogManager.setIsConfigurationDialogOpen}
          handleRunClick={handleRunClickWrapper}
          isRunning={isExecuting}
          setIsCrewDialogOpen={openCrewOrFlowDialog}
          nodes={nodes}
          edges={edges}
          saveCrewRef={saveCrewRef}
          schemaDetectionEnabled={schemaDetectionEnabled}
          setSchemaDetectionEnabled={setSchemaDetectionEnabled}
        />

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

        {/* Toggle buttons for panels - shown in both layouts */}
        <RightPanelToggle
          isVisible={areFlowsVisible}
          togglePanel={toggleFlowsVisibility}
          position="right"
          tooltip={areFlowsVisible ? "Hide Flows Panel" : "Show Flows Panel"}
        />
        
        <BottomPanelToggle
          isVisible={showRunHistory}
          togglePanel={() => setShowRunHistory(!showRunHistory)}
          position="bottom"
          tooltip={showRunHistory ? "Hide Run History Panel" : "Show Run History Panel"}
        />

        {/* Tabbed interface for Run History */}
        {showRunHistory && (
          <Box sx={{ 
            height: '160px',
            backgroundColor: isDarkMode ? '#1a1a1a' : '#ffffff',
            display: 'flex',
            flexDirection: 'column'
          }}>
            <Box sx={{ borderBottom: 1, borderColor: 'divider' }}>
              <Tabs 
                value={0} 
                variant="fullWidth"
                sx={{ 
                  minHeight: '36px',
                  '& .MuiTab-root': {
                    minHeight: '36px',
                    py: 0.5
                  }
                }}
              >
                <Tab 
                  icon={<History fontSize="small" />} 
                  iconPosition="start" 
                  label="Run History" 
                  sx={{ textTransform: 'none' }}
                />
              </Tabs>
            </Box>
            <Box sx={{ 
              flexGrow: 1, 
              overflow: 'auto',
              display: 'flex',
              flexDirection: 'column'
            }}>
              <RunHistory ref={runHistoryRef} onCrewLoad={(nodes, edges) => {
                setNodes(nodes);
                setEdges(edges);
              }} />
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
      </Box>
    </div>
  );
};

export default WorkflowDesigner; 