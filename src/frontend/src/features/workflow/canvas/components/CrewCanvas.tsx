import { normalizeEdgeColor } from '../../../../config/edgeConfig';
import React, { useCallback, useRef, useState, memo, useEffect, useLayoutEffect } from 'react';
import ReactFlow, {
  Background,
  Node,
  Edge,
  NodeChange,
  EdgeChange,
  Connection,
  OnSelectionChangeParams,
  ReactFlowInstance,
  ConnectionMode,
  BackgroundVariant,
  getConnectedEdges
} from 'reactflow';
import 'reactflow/dist/style.css';
import { Box, Snackbar, Alert, Button } from '@mui/material';
import { useThemeManager } from '../../../../hooks/workflow/useThemeManager';
import { nodeTypes as importedNodeTypes, edgeTypes as importedEdgeTypes } from './nodeTypes';

import useShortcuts from '../../../../hooks/global/useShortcuts';
import { Agent } from '../../../../types/workflow/agent';
import { ToolService } from '../../../../api/tools/ToolService';
import { Tool as ToolType } from '../../../../types/workflow/agent';
import { useJobManagementStore } from '../../../../store/jobManagement';
import { useCrewExecutionStore } from '../../../../store/crewExecution';
import { useErrorStore } from '../../../../store/error';
import { useRunStatusStore } from '../../../../store/runStatus';
import { useCrewExecution } from '../../../../hooks/workflow/useCrewExecution';
import { useConnectionGenerator } from '../../../../hooks/workflow/useConnectionGenerator';
import { useAgentHandlers } from '../../../../hooks/workflow/useAgentHandlers';
import { useTaskHandlers } from '../../../../hooks/workflow/useTaskHandlers';
import { useCrewFlowHandlers } from '../../../../hooks/workflow/useCrewFlowHandlers';
import { useToolHandlers } from '../../../../hooks/workflow/useToolHandlers';
import { useCanvasHandlers } from '../../../../hooks/workflow/useCanvasHandlers';
import { useDialogHandlers } from '../../../../hooks/workflow/useDialogHandlers';
import ManagerNodeController from './ManagerNodeController';
import FlowBackLink from '../../../../app/workspace/FlowBackLink';
import { useAgentTaskLayout } from '../../../../hooks/workflow/useAgentTaskLayout';

// Node and edge types are imported from flow-config

// Import dialog components
import AgentGenerationDialog from '../../agents/components/AgentGenerationDialog';
import TaskGenerationDialog from '../../tasks/components/TaskGenerationDialog';
import CrewFlowSelectionDialog from '../../crews/components/CrewFlowDialog/CrewFlowDialog';
import LLMSelectionDialog from '../../agents/components/LLMSelectionDialog';
import MaxRPMSelectionDialog from '../../agents/components/MaxRPMSelectionDialog';
import ToolSelectionDialog from '../../agents/components/ToolSelectionDialog';
import MCPConfigDialog from '../../../configuration/mcp/components/MCPConfigDialog';

// Import types

// Use imported node and edge types from flow-config
const nodeTypes = importedNodeTypes;
const edgeTypes = importedEdgeTypes;

interface CrewCanvasProps {
  nodes: Node[];
  edges: Edge[];
  setNodes: (nodes: Node[] | ((nodes: Node[]) => Node[])) => void;
  setEdges: (edges: Edge[] | ((edges: Edge[]) => Edge[])) => void;
  onNodesChange: (changes: NodeChange[]) => void;
  onEdgesChange: (changes: EdgeChange[]) => void;
  onConnect: (connection: Connection) => void;
  onSelectionChange?: (params: OnSelectionChangeParams) => void;
  onPaneContextMenu?: (event: React.MouseEvent) => void;
  onInit?: (instance: ReactFlowInstance) => void;
  // FitView handler
  handleUIAwareFitView: () => void;
  // Runtime features props
  reasoningEnabled: boolean;
  setReasoningEnabled: (enabled: boolean) => void;
  // Model selection props
  selectedModel: string;
  setSelectedModel: (model: string) => void;
  // Dialog props
  // Execution history visibility
  showRunHistory?: boolean;
  // Tutorial and configuration
  onOpenTutorial?: () => void;
  onOpenConfiguration?: () => void;
}


const CrewCanvas: React.FC<CrewCanvasProps> = ({
  nodes,
  edges,
  setNodes,
  setEdges,
  onNodesChange,
  onEdgesChange,
  onConnect,
  onSelectionChange,
  onPaneContextMenu,
  onInit,
  handleUIAwareFitView,
  reasoningEnabled: _reasoningEnabled,
  setReasoningEnabled: _setReasoningEnabled,
  selectedModel: _selectedModelProp,
  setSelectedModel: _setSelectedModelProp,
  showRunHistory,
  onOpenTutorial: _onOpenTutorial,
  onOpenConfiguration: _onOpenConfiguration,
}) => {

  useAgentTaskLayout(nodes, edges, setNodes);
  const [isRendering, setIsRendering] = useState(true);
  const containerRef = useRef<HTMLDivElement>(null);
  const reactFlowInstanceRef = useRef<ReactFlowInstance | null>(null);
  const { isDarkMode } = useThemeManager();

  const errorStore = useErrorStore();
  const _runStatusStore = useRunStatusStore();
  
  const fetchAgents = useCallback(async () => {
    try {
      setAgents([]);
    } catch (error) {
      console.warn('Error fetching agents:', error);
    }
  }, []);
  
  // Local state for success messages
  const [showSuccess, setShowSuccess] = useState<boolean>(false);
  const [successMessage, setSuccessMessage] = useState<string>('');

  // Dialog states without underscore prefix
  const [isAgentGenerationDialogOpen, setIsAgentGenerationDialogOpen] = useState(false);
  const [isTaskGenerationDialogOpen, setIsTaskGenerationDialogOpen] = useState(false);
  const [isCrewFlowDialogOpen, setIsCrewFlowDialogOpen] = useState(false);
  const [isLLMSelectionDialogOpen, setIsLLMSelectionDialogOpen] = useState(false);
  const [isMaxRPMSelectionDialogOpen, setIsMaxRPMSelectionDialogOpen] = useState(false);
  const [isToolDialogOpen, setIsToolDialogOpen] = useState(false);
  const [isMCPConfigDialogOpen, setIsMCPConfigDialogOpen] = useState(false);
  const [_isUpdatingAgents, _setIsUpdatingAgents] = useState(false);
  const [_agents, setAgents] = useState<Agent[]>([]);
  const [tools, setTools] = useState<ToolType[]>([]);

  const { 
    selectedTools: _selectedAgentGenerationTools, 
    setSelectedTools: _setSelectedAgentGenerationTools,
    selectedTools: _jobTrackerSelectedTools,
    setSelectedTools: _setJobTrackerSelectedTools
  } = useJobManagementStore();

  const {
    selectedModel: _selectedModel,
    processType: _processType,
    setProcessType: _setProcessType
  } = useCrewExecutionStore();
  const { handleExecuteCrew, isExecuting: _isExecuting } = useCrewExecution();

  const {
    handleEdgesChange,
    handleNodesChange,
    handleClear
  } = useCanvasHandlers({
    nodes,
    edges,
    onNodesChange,
    onEdgesChange
  });

  const {
    handleAgentGenerated: _handleAgentGenerated,
    handleUpdateAllAgentsLLM: _handleUpdateAllAgentsLLM,
    handleUpdateAllAgentsMaxRPM: _handleUpdateAllAgentsMaxRPM
  } = useAgentHandlers({
    nodes,
    onNodesChange,
    reactFlowInstanceRef,
    setSuccessMessage,
    setShowSuccess,
    fetchAgents
  });

  const {
    handleTaskGenerated: _handleTaskGenerated
  } = useTaskHandlers({
    nodes,
    onNodesChange,
    reactFlowInstanceRef,
    setSuccessMessage,
    setShowSuccess
  });

  const {
    handleCrewSelect: _handleCrewSelect,
    handleFlowSelect: _handleFlowSelect
  } = useCrewFlowHandlers({
    onNodesChange,
    onEdgesChange,
    setSuccessMessage,
    setShowSuccess
  });

  const {
    handleChangeToolsForAllAgents: _handleChangeToolsForAllAgents
  } = useToolHandlers({
    reactFlowInstanceRef,
    setSuccessMessage,
    setShowSuccess
  });

  const {
    handleChangeLLM,
    handleChangeMaxRPM,
    handleChangeTools,
    handleExecuteCrewButtonClick
  } = useDialogHandlers({
    nodes,
    edges,
    setIsLLMSelectionDialogOpen,
    setIsMaxRPMSelectionDialogOpen,
    setIsToolDialogOpen,
    handleExecuteCrew,
    setSuccessMessage,
    setShowSuccess
  });

  const {
    isGeneratingConnections: _isGeneratingConnections,
    handleGenerateConnections 
  } = useConnectionGenerator({
    reactFlowInstanceRef,
    onConnect,
    setSuccessMessage,
    setShowSuccess
  });

  const handleGenerateConnectionsWrapper = useCallback(async () => {
    await handleGenerateConnections();
    return Promise.resolve();
  }, [handleGenerateConnections]);

  useEffect(() => {
    if ((nodes.length > 0 || edges.length > 0) && 
        (nodes.length === 0 && edges.length === 0)) {
      onNodesChange(
        nodes.map((node: Node) => ({
          type: 'add' as const,
          item: node
        }))
      );
      
      onEdgesChange(
        edges.map((edge: Edge) => ({
          type: 'add' as const,
          item: edge
        }))
      );
    }
  }, [nodes, edges, onNodesChange, onEdgesChange]);

  useEffect(() => {
    const originalError = console.error;
    console.error = (msg, ...args) => {
      if (typeof msg === 'string' && 
          (msg.includes('ResizeObserver loop') || 
           msg.includes('ResizeObserver Loop') ||
           msg.includes('ResizeObserver') ||
           msg.includes('undelivered notifications'))) {
        return;
      }
      originalError(msg, ...args);
    };

    return () => {
      console.error = originalError;
    };
  }, []);

  useEffect(() => {
    const handleError = (event: ErrorEvent) => {
      if (event.message && (
        event.message.includes('react-flow') || 
        event.message.includes('ReactFlow') ||
        event.message.includes('Uncaught') && event.message.includes('rendering')
      )) {
        errorStore.setErrorMessage(event.message);
      }
    };

    window.addEventListener('error', handleError);
    return () => {
      window.removeEventListener('error', handleError);
    };
  }, [errorStore]);

  useLayoutEffect(() => {
    setIsRendering(true);
    
    const timer = setTimeout(() => {
      setIsRendering(false);
    }, 50);
    
    return () => clearTimeout(timer);
  }, []);

  useEffect(() => {
    if (errorStore.showError) {
      errorStore.clearError();
    }
  }, [errorStore]);

  const nodesWithDimensions = React.useMemo(() => {
    // Filter out any flow-related nodes first
    const crewNodes = nodes.filter(node => {
      // Exclude flow-related nodes
      if (!node || typeof node !== 'object') return false;

      const nodeType = node.type?.toLowerCase() || '';
      return nodeType === 'agentnode' || nodeType === 'tasknode' || nodeType === 'managernode';
    });

    return crewNodes.map(node => {
      if (node.type !== 'agentNode' && node.type !== 'taskNode') return node;
      // Let React Flow measure the actual card. Old fixed wrapper heights were
      // shorter than the content, so placement and fit-view underestimated it.
      const { width: _width, height: _height, ...style } = node.style || {};
      return { ...node, style };
    });
  }, [nodes]);

  const handleInit = useCallback((instance: ReactFlowInstance) => {
    reactFlowInstanceRef.current = instance;
    
    try {
      // Defer to UI-aware fit view handled by useWorkflowLayoutEvents
      // Fire a signal that the crew flow has initialized; listeners will center with chat-aware bounds
      setTimeout(() => {
        window.dispatchEvent(new Event('crewFlowInitialized'));
        window.dispatchEvent(new Event('fitViewToNodesInternal'));
      }, 150);

      if (onInit) {
        onInit(instance);
      }
    } catch (error) {
      console.warn('Error during ReactFlow initialization:', error);
    }
  }, [onInit]);

  const crewEdges = React.useMemo(() => {
    try {
      const crewNodeIds = new Set(nodes.map(node => node.id));

      // First, deduplicate edges by creating a Map with edge key
      const edgeMap = new Map<string, Edge>();

      edges.forEach(edge => {
        const isManagerEdge = edge.source === 'manager-node' || edge.target === 'manager-node';

        if (edge &&
            typeof edge === 'object' &&
            edge.source &&
            edge.target &&
            crewNodeIds.has(edge.source) &&
            crewNodeIds.has(edge.target)) {

          // Create a unique key for the edge
          const edgeKey = `${edge.source}-${edge.target}-${edge.sourceHandle || 'default'}-${edge.targetHandle || 'default'}`;

          // Only keep the first occurrence of each edge
          if (!edgeMap.has(edgeKey)) {
            // Edge properties (animated, style) are already set by workflow.ts
            // No need for duplicate enhancement here
            edgeMap.set(edgeKey, edge);
          }
        }
      });

      // Convert map back to array
      const uniqueEdges = Array.from(edgeMap.values());

      // Ensure edge type is set and preserve animated property and ID
      const edgesWithType = uniqueEdges.map(edge => ({
        ...edge,
        id: edge.id, // Explicitly preserve ID
        type: edge.type || 'default',
        markerEnd: typeof edge.markerEnd === 'object' ? { ...edge.markerEnd, color: normalizeEdgeColor(edge.markerEnd.color) } : edge.markerEnd, // Ensure edge type is set
        // Preserve the animated property from the edge (don't override it)
      }));

      return edgesWithType;
    } catch (error) {

      return [];
    }
  }, [edges, nodes]); // Removed runStatusStore.hasRunningJobs dependency

  const _handleDeleteSelected = useCallback((selectedNodes: Node[], selectedEdges: Edge[]) => {
    // First, remove the selected nodes
    onNodesChange(selectedNodes.map(node => ({ type: 'remove', id: node.id })));
    
    // Find all edges connected to the nodes being deleted (including orphaned edges)
    const connectedEdges = getConnectedEdges(selectedNodes, edges);
    
    // Combine explicitly selected edges with edges connected to deleted nodes
    const allEdgesToDelete = new Set([
      ...selectedEdges.map(edge => edge.id),
      ...connectedEdges.map(edge => edge.id)
    ]);
    
    // Remove all edges that need to be deleted
    onEdgesChange(Array.from(allEdgesToDelete).map(edgeId => ({ type: 'remove', id: edgeId })));
  }, [onNodesChange, onEdgesChange, edges]);

  useShortcuts({
    flowInstance: reactFlowInstanceRef.current,
    onDeleteSelected: _handleDeleteSelected,
    onClearCanvas: handleClear,
    onZoomIn: () => reactFlowInstanceRef.current?.zoomIn(),
    onZoomOut: () => reactFlowInstanceRef.current?.zoomOut(),
    onFitView: handleUIAwareFitView,
    // Don't override onExecuteCrew - let useShortcuts use its default handler with workflow store
    onExecuteFlow: () => {
      if (nodes.length > 0 || edges.length > 0) {
        const currentNodes = nodes.map(node => ({ ...node }));
        const currentEdges = edges.map(edge => ({ ...edge }));
        return handleExecuteCrew(currentNodes, currentEdges);
      }
      return undefined;
    },
    onGenerateConnections: handleGenerateConnectionsWrapper,
    onOpenSaveCrew: () => {
      const event = new CustomEvent('openSaveCrewDialog');
      window.dispatchEvent(event);
    },
    onOpenCrewFlowDialog: () => setIsCrewFlowDialogOpen(true),
    onChangeLLMForAllAgents: handleChangeLLM,
    onChangeMaxRPMForAllAgents: handleChangeMaxRPM,
    onChangeToolsForAllAgents: handleChangeTools,
    onOpenLLMDialog: () => setIsLLMSelectionDialogOpen(true),
    onOpenToolDialog: () => setIsToolDialogOpen(true),
    onOpenMaxRPMDialog: () => setIsMaxRPMSelectionDialogOpen(true),
    onOpenMCPConfigDialog: () => setIsMCPConfigDialogOpen(true),
    disabled: false,
    useWorkflowStore: true,
    instanceId: 'crew-canvas',  // Unique identifier for this instance
    priority: 10  // Higher priority than flow canvas
  });


  useEffect(() => {
    fetchTools();
  }, []);
  
  const fetchTools = async () => {
    _setIsUpdatingAgents(true);
    try {
      const toolsList = await ToolService.listEnabledTools();
      const formattedTools: ToolType[] = toolsList.map(tool => ({
        id: tool.id.toString(),
        title: tool.title,
        description: tool.description,
        icon: tool.icon || '',
        enabled: tool.enabled
      }));
      setTools(formattedTools);
    } catch (error) {
      console.warn('Error fetching tools:', error);
    } finally {
      _setIsUpdatingAgents(false);
    }
  };

  useEffect(() => {
    const fitViewToNodes = () => {
      // Dispatch the internal event that triggers UI-aware fit view
      window.dispatchEvent(new Event('fitViewToNodesInternal'));
    };
    
    const openAgentGenerationDialog = () => {
      setIsAgentGenerationDialogOpen(true);
    };
    
    const openTaskGenerationDialog = () => {
      setIsTaskGenerationDialogOpen(true);
    };
    
    const handleEdgeDelete = (event: CustomEvent) => {
      const { id } = event.detail;

      onEdgesChange([{ type: 'remove', id }]);
    };
    
    window.addEventListener('fitViewToNodes', fitViewToNodes);
    window.addEventListener('openAgentGenerationDialog', openAgentGenerationDialog);
    window.addEventListener('openTaskGenerationDialog', openTaskGenerationDialog);
    window.addEventListener('edge:delete', handleEdgeDelete as EventListener);
    
    return () => {
      window.removeEventListener('fitViewToNodes', fitViewToNodes);
      window.removeEventListener('openAgentGenerationDialog', openAgentGenerationDialog);
      window.removeEventListener('openTaskGenerationDialog', openTaskGenerationDialog);
      window.removeEventListener('edge:delete', handleEdgeDelete as EventListener);
    };
  }, [nodes, onNodesChange, onEdgesChange, errorStore]);


  // Handler for max RPM selection
  const _handleMaxRPMSelected = useCallback(async (maxRPM: string) => {
    const numericMaxRPM = parseInt(maxRPM, 10);
    if (!isNaN(numericMaxRPM)) {
      await _handleUpdateAllAgentsMaxRPM(numericMaxRPM);
    }
  }, [_handleUpdateAllAgentsMaxRPM]);

  return (
    <Box 
      ref={containerRef}
      sx={{ 
        width: '100%', 
        height: '100%',
        display: 'flex',
        flexDirection: 'column',
        position: 'relative',
        background: 'transparent',
      }}
    >
      {errorStore.showError ? (
        <Box 
          sx={{ 
            position: 'absolute', 
            top: '50%', 
            left: '50%', 
            transform: 'translate(-50%, -50%)',
            backgroundColor: 'rgba(255, 255, 255, 0.9)',
            padding: 2,
            borderRadius: 1,
            maxWidth: '80%',
            textAlign: 'center',
            zIndex: 1000,
            border: '1px solid #f44336',
            color: '#f44336'
          }}
        >
          <div>Error in ReactFlow component: {errorStore.errorMessage}</div>
          <Button onClick={() => errorStore.clearError()}>Dismiss</Button>
        </Box>
      ) : null}
      {isRendering ? (
        <Box 
          sx={{ 
            display: 'flex', 
            justifyContent: 'center', 
            alignItems: 'center', 
            height: '100%',
            color: isDarkMode ? '#fff' : '#333'
          }}
        >
          Loading...
        </Box>
      ) : (
        <ReactFlow
          key="crew-canvas"
          data-tour="canvas-area"
          nodes={nodesWithDimensions}
          edges={crewEdges}
          onNodesChange={handleNodesChange}
          onEdgesChange={handleEdgesChange}
          edgeUpdaterRadius={10}
          reconnectRadius={10}
          onConnect={onConnect}
          onSelectionChange={onSelectionChange}
          onPaneContextMenu={onPaneContextMenu}
          onInit={handleInit}
          nodeTypes={nodeTypes}
          edgeTypes={edgeTypes}
          connectionMode={ConnectionMode.Loose}
          // Removed automatic fitView to prevent ResizeObserver loops
          // fitView is handled manually in handleInit and via controls
          attributionPosition="bottom-left"
          minZoom={0.01}
          maxZoom={4}
          defaultViewport={{ x: 0, y: 0, zoom: 1 }}
          proOptions={{ hideAttribution: true }}
          style={{ background: 'transparent' }}
          nodesDraggable={true}
          nodesConnectable={true}
          elementsSelectable={true}
          edgesFocusable={false}
          selectNodesOnDrag={false}
          selectionOnDrag={false}
          panOnDrag={true}
          translateExtent={[[-10000, -10000], [10000, 10000]]}
          nodeExtent={[[-10000, -10000], [10000, 10000]]}
          snapToGrid={false}
          snapGrid={[15, 15]}
          multiSelectionKeyCode="Shift"
          selectionKeyCode="Shift"
          deleteKeyCode="Delete"
          elevateEdgesOnSelect={false}
          elevateNodesOnSelect={true}
        >
          <Background
            color={isDarkMode ? '#333' : '#aaa'}
            gap={16}
            size={1}
            variant={BackgroundVariant.Dots}
          />

          {/* Manager node controller - handles automatic creation/removal based on process type */}
          <ManagerNodeController
            nodes={nodes}
            edges={edges}
            setNodes={setNodes}
            setEdges={setEdges}
          />



        </ReactFlow>
      )}

      {/* Back to the flow this crew belongs to. Renders nothing when it does
          not apply, which is most of the time. */}
      <FlowBackLink />

      <Snackbar 
        open={showSuccess} 
        autoHideDuration={4000} 
        onClose={() => setShowSuccess(false)}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'left' }}
      >
        <Alert onClose={() => setShowSuccess(false)} severity="success" sx={{ width: '100%' }}>
          {successMessage}
        </Alert>
      </Snackbar>

      {/* Dialog Components */}
      <AgentGenerationDialog
        open={isAgentGenerationDialogOpen}
        onClose={() => setIsAgentGenerationDialogOpen(false)}
        onAgentGenerated={_handleAgentGenerated}
        tools={tools}
        selectedTools={_selectedAgentGenerationTools}
        onToolsChange={_setSelectedAgentGenerationTools}
      />
      <TaskGenerationDialog
        open={isTaskGenerationDialogOpen}
        onClose={() => setIsTaskGenerationDialogOpen(false)}
        onTaskGenerated={_handleTaskGenerated}
      />
      <CrewFlowSelectionDialog
        embedded
        open={isCrewFlowDialogOpen}
        onClose={() => setIsCrewFlowDialogOpen(false)}
        onCrewSelect={_handleCrewSelect}
        onFlowSelect={_handleFlowSelect}
      />
      <LLMSelectionDialog
        open={isLLMSelectionDialogOpen}
        onClose={() => setIsLLMSelectionDialogOpen(false)}
        onSelectLLM={_handleUpdateAllAgentsLLM}
      />
      <MaxRPMSelectionDialog
        open={isMaxRPMSelectionDialogOpen}
        onClose={() => setIsMaxRPMSelectionDialogOpen(false)}
        onSelectMaxRPM={_handleMaxRPMSelected}
      />
      <ToolSelectionDialog
        open={isToolDialogOpen}
        onClose={() => setIsToolDialogOpen(false)}
        onSelectTools={_handleChangeToolsForAllAgents}
      />
      <MCPConfigDialog
        open={isMCPConfigDialogOpen}
        onClose={() => setIsMCPConfigDialogOpen(false)}
      />
    </Box>
  );
};

export default memo(CrewCanvas);
