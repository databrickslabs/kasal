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
  NodeTypes,
  EdgeTypes,
  BackgroundVariant
} from 'reactflow';
import 'reactflow/dist/style.css';
import { Box, Snackbar, Alert, Button } from '@mui/material';
import { useThemeManager } from '../../hooks/workflow/useThemeManager';

import useShortcuts from '../../hooks/global/useShortcuts';
import { Agent } from '../../types/agent';
import { ToolService } from '../../api/ToolService';
import { Tool as ToolType } from '../../types/agent';
import { Tool as PlanningTool } from '../../types/tool';
import { useJobManagementStore } from '../../store/jobManagement';
import { RunHistoryRef } from '../../components/Jobs/ExecutionHistory';
import { useCrewExecutionStore } from '../../store/crewExecution';
import { useErrorStore } from '../../store/error';
import { useRunStatusStore } from '../../store/runStatus';
import { logEdgeDetails } from '../../utils/flowUtils';
import { useCrewExecution } from '../../hooks/workflow/useCrewExecution';
import { useConnectionGenerator } from '../../hooks/workflow/useConnectionGenerator';
import { useAgentHandlers } from '../../hooks/workflow/useAgentHandlers';
import { useTaskHandlers } from '../../hooks/workflow/useTaskHandlers';
import { useCrewFlowHandlers } from '../../hooks/workflow/useCrewFlowHandlers';
import { useToolHandlers } from '../../hooks/workflow/useToolHandlers';
import { useCanvasHandlers } from '../../hooks/workflow/useCanvasHandlers';
import { useDialogHandlers } from '../../hooks/workflow/useDialogHandlers';
import LeftSidebar from './LeftSidebar';
import RightSidebar from './RightSidebar';

// Node types
import AgentNode from '../Agents/AgentNode';
import TaskNode from '../Tasks/TaskNode';

// Edge type
import AnimatedEdge from '../Common/AnimatedEdge';

// Import dialog components
import AgentGenerationDialog from '../Agents/AgentGenerationDialog';
import TaskGenerationDialog from '../Tasks/TaskGenerationDialog';
import CrewPlanningDialog from '../Planning/CrewPlanningDialog';
import CrewFlowSelectionDialog from '../Crew/CrewFlowDialog/CrewFlowDialog';
import LLMSelectionDialog from '../Agents/LLMSelectionDialog';
import MaxRPMSelectionDialog from '../Agents/MaxRPMSelectionDialog';
import ToolSelectionDialog from '../Agents/ToolSelectionDialog';
import MCPConfigDialog from '../Dialogs/MCPConfigDialog';

// Import types
import { Crew, CrewAgent, CrewTask } from '../../types/crewPlan';

// Node and edge types configuration
const nodeTypes: NodeTypes = {
  agentNode: AgentNode,
  taskNode: TaskNode
};

const edgeTypes: EdgeTypes = {
  default: AnimatedEdge
};

interface CrewCanvasProps {
  nodes: Node[];
  edges: Edge[];
  onNodesChange: (changes: NodeChange[]) => void;
  onEdgesChange: (changes: EdgeChange[]) => void;
  onConnect: (connection: Connection) => void;
  onSelectionChange?: (params: OnSelectionChangeParams) => void;
  onPaneContextMenu?: (event: React.MouseEvent) => void;
  onInit?: (instance: ReactFlowInstance) => void;
  // Runtime features props
  planningEnabled: boolean;
  setPlanningEnabled: (enabled: boolean) => void;
  reasoningEnabled: boolean;
  setReasoningEnabled: (enabled: boolean) => void;
  schemaDetectionEnabled: boolean;
  setSchemaDetectionEnabled: (enabled: boolean) => void;
  // Model selection props
  selectedModel: string;
  setSelectedModel: (model: string) => void;
  // Dialog props
  onOpenLogsDialog: () => void;
  onToggleChat: () => void;
  isChatOpen: boolean;
  onOpenScheduleDialog: () => void;
  setIsAgentDialogOpen: (open: boolean) => void;
  setIsTaskDialogOpen: (open: boolean) => void;
  setIsFlowDialogOpen: (open: boolean) => void;
  // Execution history visibility
  showRunHistory?: boolean;
}

// Utility function to convert crew plan to nodes and edges
const convertCrewPlanToGraph = (crewPlan: Crew): { nodes: Node[]; edges: Edge[] } => {
  // Create a map of agent names to indices
  const agentIndices = new Map<string, number>();
  crewPlan.agents.forEach((agent, index) => {
    agentIndices.set(agent.name, index);
  });

  // Convert agents to nodes
  const agentNodes: Node[] = crewPlan.agents.map((agent: CrewAgent, index: number) => ({
    id: `agent-${index}`,
    type: 'agentNode',
    data: { ...agent },
    position: { x: 100 * index, y: 100 }
  }));

  // Convert tasks to nodes
  const taskNodes: Node[] = crewPlan.tasks.map((task: CrewTask, index: number) => ({
    id: `task-${index}`,
    type: 'taskNode',
    data: { ...task },
    position: { x: 100 * index, y: 300 }
  }));

  // Create edges based on task assignments
  const edges: Edge[] = [];
  crewPlan.tasks.forEach((task: CrewTask, taskIndex: number) => {
    if (task.agent_id) {
      // Find the agent index by name
      const agentIndex = agentIndices.get(task.agent_id);
      if (agentIndex !== undefined) {
        edges.push({
          id: `edge-${taskIndex}`,
          source: `agent-${agentIndex}`,
          target: `task-${taskIndex}`,
          type: 'default'
        });
      }
    }
  });

  return {
    nodes: [...agentNodes, ...taskNodes],
    edges
  };
};

const CrewCanvas: React.FC<CrewCanvasProps> = ({
  nodes,
  edges,
  onNodesChange,
  onEdgesChange,
  onConnect,
  onSelectionChange,
  onPaneContextMenu,
  onInit,
  planningEnabled,
  setPlanningEnabled,
  reasoningEnabled,
  setReasoningEnabled,
  schemaDetectionEnabled,
  setSchemaDetectionEnabled,
  selectedModel,
  setSelectedModel,
  onOpenLogsDialog,
  onToggleChat,
  isChatOpen,
  onOpenScheduleDialog,
  setIsAgentDialogOpen,
  setIsTaskDialogOpen,
  setIsFlowDialogOpen,
  showRunHistory
}) => {
  const [isRendering, setIsRendering] = useState(true);
  const containerRef = useRef<HTMLDivElement>(null);
  const reactFlowInstanceRef = useRef<ReactFlowInstance | null>(null);
  const { isDarkMode } = useThemeManager();

  
  // Add CSS for proper stacking order (edges behind nodes)
  useEffect(() => {
    const styleId = 'reactflow-stacking-order-fix';
    if (!document.getElementById(styleId)) {
      const style = document.createElement('style');
      style.id = styleId;
      style.textContent = `
        /* Force edges to stay behind nodes */
        .react-flow__edges {
          z-index: 0 !important;
        }
        .react-flow__edge {
          z-index: 0 !important;
        }
        /* Ensure nodes are above edges */
        .react-flow__nodes {
          z-index: 5 !important;
        }
        .react-flow__node {
          z-index: 10 !important;
        }
        /* Ensure hovered nodes are at the very top */
        .react-flow__node:hover {
          z-index: 1000 !important;
        }
      `;
      document.head.appendChild(style);
    }
  }, []);
  
  const errorStore = useErrorStore();
  const runStatusStore = useRunStatusStore();
  
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
  const [isCrewPlanningDialogOpen, setIsCrewPlanningDialogOpen] = useState(false);
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
    tools: jobTrackerTools,
    selectedTools: _jobTrackerSelectedTools,
    setSelectedTools: _setJobTrackerSelectedTools
  } = useJobManagementStore();

  const { selectedModel: _selectedModel } = useCrewExecutionStore();
  const { handleExecuteCrew, isExecuting: _isExecuting } = useCrewExecution();

  const _runHistoryRef = useRef<RunHistoryRef>(null);

  const _planningTools = jobTrackerTools.map((tool: ToolType) => ({
    ...tool,
    icon: tool.icon || '',
    enabled: tool.enabled !== false
  })) as PlanningTool[];

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
    isGeneratingConnections, 
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
      return nodeType === 'agentnode' || nodeType === 'tasknode';
    });

    return crewNodes.map(node => {
      const defaultWidth = node.type === 'agentNode' ? 180 : 280;
      const defaultHeight = node.type === 'agentNode' ? 180 : 140;
      
      if (!node.style || (!node.style.width && !node.style.height)) {
        return {
          ...node,
          width: typeof node.style?.width === 'number' ? node.style.width : defaultWidth,
          height: typeof node.style?.height === 'number' ? node.style.height : defaultHeight,
          style: {
            ...node.style,
            width: node.style?.width || defaultWidth,
            height: node.style?.height || defaultHeight
          }
        };
      }
      
      return {
        ...node,
        width: typeof node.width === 'number' ? node.width : 
               typeof node.style?.width === 'number' ? node.style.width : defaultWidth,
        height: typeof node.height === 'number' ? node.height : 
                typeof node.style?.height === 'number' ? node.style.height : defaultHeight
      };
    });
  }, [nodes]);

  const handleInit = useCallback((instance: ReactFlowInstance) => {
    reactFlowInstanceRef.current = instance;
    
    try {
      const attemptFitView = (attempt = 1, maxAttempts = 3) => {
        try {
          if (instance && attempt <= maxAttempts) {
            const delay = 100 * attempt;
            
            setTimeout(() => {
              try {
                instance.fitView({
                  padding: 0.2,
                  includeHiddenNodes: false,
                  duration: 800
                });
              } catch (error) {
                // Log error and retry if needed
                console.warn(`fitView attempt ${attempt} failed:`, error);
                if (attempt < maxAttempts) {
                  attemptFitView(attempt + 1, maxAttempts);
                }
              }
            }, delay);
          }
        } catch (error) {
          // Log any other errors
          console.warn('Error in attemptFitView:', error);
        }
      };
      
      attemptFitView();
      
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
      
      const filteredEdges = edges.filter(edge => 
        edge && 
        typeof edge === 'object' &&
        edge.source && 
        edge.target && 
        crewNodeIds.has(edge.source) && 
        crewNodeIds.has(edge.target)
      );
      
      // Apply animation to edges when jobs are running
      const edgesWithAnimation = filteredEdges.map(edge => ({
        ...edge,
        animated: runStatusStore.hasRunningJobs // Make edges animated when jobs are running
      }));
      
      // Log edge details for debugging
      logEdgeDetails(edgesWithAnimation, "CrewCanvas: Filtered crew edges:");
      
      return edgesWithAnimation;
    } catch (error) {
      console.error("CrewCanvas: Error filtering edges:", error);
      return [];
    }
  }, [edges, nodes, runStatusStore.hasRunningJobs]); // Add hasRunningJobs to dependencies

  const _handleDeleteSelected = useCallback((selectedNodes: Node[], selectedEdges: Edge[]) => {
    onNodesChange(selectedNodes.map(node => ({ type: 'remove', id: node.id })));
    onEdgesChange(selectedEdges.map(edge => ({ type: 'remove', id: edge.id })));
  }, [onNodesChange, onEdgesChange]);

  const { shortcuts } = useShortcuts({
    flowInstance: reactFlowInstanceRef.current,
    onDeleteSelected: _handleDeleteSelected,
    onClearCanvas: handleClear,
    onZoomIn: () => reactFlowInstanceRef.current?.zoomIn(),
    onZoomOut: () => reactFlowInstanceRef.current?.zoomOut(),
    onFitView: () => reactFlowInstanceRef.current?.fitView({ padding: 0.2 }),
    onExecuteCrew: handleExecuteCrewButtonClick,
    onExecuteFlow: () => {
      if (nodes.length > 0 || edges.length > 0) {
        const currentNodes = nodes.map(node => ({ ...node }));
        const currentEdges = edges.map(edge => ({ ...edge }));
        return handleExecuteCrew(currentNodes, currentEdges);
      }
      return undefined;
    },
    onOpenAgentDialog: () => setIsAgentGenerationDialogOpen(true),
    onOpenTaskDialog: () => setIsTaskGenerationDialogOpen(true),
    onOpenCrewPlanningDialog: () => setIsCrewPlanningDialogOpen(true),
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
    useWorkflowStore: true
  });

  useEffect(() => {
    // Log shortcut changes for debugging purposes
    console.log('CrewCanvas - Shortcuts updated:', shortcuts);
  }, [shortcuts]);

  useEffect(() => {
    const checkDialogState = () => {
      const _hasOpenDialog = document.querySelector('.MuiDialog-root') !== null;
    };

    const observer = new MutationObserver(checkDialogState);
    observer.observe(document.body, { 
      childList: true, 
      subtree: true 
    });

    return () => observer.disconnect();
  }, []);

  useEffect(() => {
    fetchTools();
  }, []);
  
  const fetchTools = async () => {
    _setIsUpdatingAgents(true);
    try {
      const toolsList = await ToolService.listTools();
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
      if (reactFlowInstanceRef.current) {
        try {
          reactFlowInstanceRef.current.fitView({
            padding: 0.2,
            includeHiddenNodes: false,
            duration: 800
          });
        } catch (error) {
          console.warn('Error fitting view in CrewCanvas:', error);
        }
      }
    };
    
    const openAgentGenerationDialog = () => {
      setIsAgentGenerationDialogOpen(true);
    };
    
    const openTaskGenerationDialog = () => {
      setIsTaskGenerationDialogOpen(true);
    };
    
    window.addEventListener('fitViewToNodes', fitViewToNodes);
    window.addEventListener('openAgentGenerationDialog', openAgentGenerationDialog);
    window.addEventListener('openTaskGenerationDialog', openTaskGenerationDialog);
    
    return () => {
      window.removeEventListener('fitViewToNodes', fitViewToNodes);
      window.removeEventListener('openAgentGenerationDialog', openAgentGenerationDialog);
      window.removeEventListener('openTaskGenerationDialog', openTaskGenerationDialog);
    };
  }, [nodes, onNodesChange, errorStore]);

  // Handler for crew plan generation
  const _handleCrewPlanGenerated = useCallback((crewPlan: Crew, shouldExecute: boolean) => {
    const { nodes: crewNodes, edges: crewEdges } = convertCrewPlanToGraph(crewPlan);
    _handleCrewSelect(crewNodes, crewEdges);
    if (shouldExecute) {
      handleExecuteCrewButtonClick();
    }
  }, [_handleCrewSelect, handleExecuteCrewButtonClick]);

  // Add _handleOpenCrewPlanningDialog callback
  const _handleOpenCrewPlanningDialog = useCallback(() => {
    setIsCrewPlanningDialogOpen(true);
    setSuccessMessage('Crew Planning dialog opened');
    setShowSuccess(true);
  }, [setSuccessMessage, setShowSuccess]);

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
        backgroundColor: isDarkMode ? '#1a1a1a' : '#f5f5f5',
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
          nodes={nodesWithDimensions}
          edges={crewEdges}
          onNodesChange={handleNodesChange}
          onEdgesChange={handleEdgesChange}
          onConnect={onConnect}
          onSelectionChange={onSelectionChange}
          onPaneContextMenu={onPaneContextMenu}
          onInit={handleInit}
          nodeTypes={nodeTypes}
          edgeTypes={edgeTypes}
          connectionMode={ConnectionMode.Loose}
          fitView
          attributionPosition="bottom-left"
          minZoom={0.1}
          maxZoom={4}
          defaultViewport={{ x: 0, y: 0, zoom: 1 }}
          proOptions={{ hideAttribution: true }}
          style={{ background: isDarkMode ? '#1a1a1a' : '#f8f8f8' }}
          nodesDraggable={true}
          nodesConnectable={true}
          elementsSelectable={true}
          selectNodesOnDrag={true}
          selectionOnDrag={true}
          panOnDrag={[1, 2]}
          translateExtent={[[-2000, -2000], [3000, 3000]]}
          nodeExtent={[[-2000, -2000], [3000, 3000]]}
          snapToGrid={false}
          snapGrid={[15, 15]}
          multiSelectionKeyCode="Shift"
          selectionKeyCode="Shift"
          deleteKeyCode="Delete"
          elevateEdgesOnSelect={true}
          elevateNodesOnSelect={true}
        >
          <Background
            color={isDarkMode ? '#333' : '#aaa'}
            gap={16}
            size={1}
            variant={BackgroundVariant.Dots}
          />

          <LeftSidebar
            onClearCanvas={handleClear}
            onGenerateConnections={handleGenerateConnectionsWrapper}
            onZoomIn={() => reactFlowInstanceRef.current?.zoomIn()}
            onZoomOut={() => reactFlowInstanceRef.current?.zoomOut()}
            onFitView={() => reactFlowInstanceRef.current?.fitView()}
            onToggleInteractivity={() => {
              if (reactFlowInstanceRef.current) {
                const currentNodes = reactFlowInstanceRef.current.getNodes();
                const updatedNodes = currentNodes.map(node => ({
                  ...node,
                  selectable: !node.selectable
                }));
                reactFlowInstanceRef.current.setNodes(updatedNodes);
              }
            }}
            isGeneratingConnections={isGeneratingConnections}
            planningEnabled={planningEnabled}
            setPlanningEnabled={setPlanningEnabled}
            reasoningEnabled={reasoningEnabled}
            setReasoningEnabled={setReasoningEnabled}
            schemaDetectionEnabled={schemaDetectionEnabled}
            setSchemaDetectionEnabled={setSchemaDetectionEnabled}
            selectedModel={selectedModel}
            setSelectedModel={setSelectedModel}
            showRunHistory={showRunHistory}
          />

          <RightSidebar
            onOpenLogsDialog={onOpenLogsDialog}
            onToggleChat={onToggleChat}
            isChatOpen={isChatOpen}
            onOpenScheduleDialog={onOpenScheduleDialog}
            setIsAgentDialogOpen={setIsAgentDialogOpen}
            setIsTaskDialogOpen={setIsTaskDialogOpen}
            setIsFlowDialogOpen={setIsFlowDialogOpen}
            setIsAgentGenerationDialogOpen={setIsAgentGenerationDialogOpen}
            setIsTaskGenerationDialogOpen={setIsTaskGenerationDialogOpen}
            showRunHistory={showRunHistory}
          />

        </ReactFlow>
      )}

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
      <CrewPlanningDialog
        open={isCrewPlanningDialogOpen}
        onClose={() => setIsCrewPlanningDialogOpen(false)}
        onGenerateCrew={(crewPlan: Crew, shouldExecute: boolean) => {
          console.log('CrewCanvas received crew plan:', crewPlan, 'Should execute:', shouldExecute);
          console.log('CrewCanvas task async_execution values:', crewPlan.tasks.map(t => ({ name: t.name, async: t.async_execution })));
          
          const newNodes: Node[] = [];
          const newEdges: Edge[] = [];
          
          // Step 1: Process agents and create agent nodes
          crewPlan.agents.forEach((agent: CrewAgent, index: number) => {
            const nodeId = `agent-${agent.id}`;
            console.log(`CrewCanvas: Creating agent node: ${nodeId} (${agent.name})`);
            newNodes.push({
              id: nodeId,
              type: 'agentNode',
              position: { x: 100, y: 100 + (index * 150) },
              data: {
                label: agent.name,
                agentId: agent.id,
                role: agent.role || '',
                goal: agent.goal || '',
                backstory: agent.backstory || '',
                llm: agent.llm || _selectedModel,
                tools: agent.tools || [],
                agent: agent // Pass the full agent object if needed by the node
              }
            });
          });
          
          // Step 2: Process tasks and create task nodes
          crewPlan.tasks.forEach((task: CrewTask, index: number) => {
            const nodeId = `task-${task.id}`;
            console.log(`CrewCanvas: Creating task node: ${nodeId} (${task.name}), async_execution:`, task.async_execution);
            newNodes.push({
              id: nodeId,
              type: 'taskNode',
              // Position tasks to the right of agents
              position: { x: 400, y: 100 + (index * 150) }, 
              data: {
                label: task.name,
                taskId: task.id,
                description: task.description || task.name,
                expected_output: task.expected_output || '',
                human_input: task.human_input || false,
                tools: task.tools || [],
                async_execution: task.async_execution !== undefined ? Boolean(task.async_execution) : false,
                // Include context in node data if TaskNode needs it, otherwise it's just for edges
                context: task.context || [],
                config: {
                  markdown: task.markdown || false
                },
                task: task // Pass the full task object
              }
            });
          });

          // Step 3: Create edges based on agent assignments and task context (dependencies)
          crewPlan.tasks.forEach((task: CrewTask) => {
            const targetNodeId = `task-${task.id}`;

            // Create agent-to-task assignment edges
            if (task.agent_id) {
              const sourceNodeId = `agent-${task.agent_id}`;
              newEdges.push({
                id: `edge-${task.id}`,
                source: sourceNodeId,
                target: targetNodeId,
                type: 'default'
              });
            }
          });

          // Update nodes and edges
          onNodesChange(newNodes.map(node => ({ type: 'add', item: node })));
          onEdgesChange(newEdges.map(edge => ({ type: 'add', item: edge })));

          if (shouldExecute) {
            handleExecuteCrewButtonClick();
          }
        }}
        selectedModel={selectedModel}
        tools={tools.map(tool => ({
          ...tool,
          icon: tool.icon || ''
        }))}
        selectedTools={_selectedAgentGenerationTools}
        onToolsChange={_setSelectedAgentGenerationTools}
      />
      <CrewFlowSelectionDialog
        open={isCrewFlowDialogOpen}
        onClose={() => setIsCrewFlowDialogOpen(false)}
        onCrewSelect={_handleCrewSelect}
        onFlowSelect={_handleFlowSelect}
      />
      <LLMSelectionDialog
        open={isLLMSelectionDialogOpen}
        onClose={() => setIsLLMSelectionDialogOpen(false)}
        onSelectLLM={handleChangeLLM}
      />
      <MaxRPMSelectionDialog
        open={isMaxRPMSelectionDialogOpen}
        onClose={() => setIsMaxRPMSelectionDialogOpen(false)}
        onSelectMaxRPM={_handleMaxRPMSelected}
      />
      <ToolSelectionDialog
        open={isToolDialogOpen}
        onClose={() => setIsToolDialogOpen(false)}
        onSelectTools={handleChangeTools}
      />
      <MCPConfigDialog
        open={isMCPConfigDialogOpen}
        onClose={() => setIsMCPConfigDialogOpen(false)}
      />
    </Box>
  );
};

export default memo(CrewCanvas);