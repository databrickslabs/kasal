import React from 'react';
import { Box } from '@mui/material';
import { Node, Edge, NodeChange, EdgeChange, Connection, OnSelectionChangeParams, ReactFlowInstance } from 'reactflow';
import CrewCanvas from './CrewCanvas';
import FlowCanvas from './FlowCanvas';
import { useFlowConfigStore } from '../../store/flowConfig';

interface WorkflowPanelsProps {
  areFlowsVisible: boolean;
  showRunHistory: boolean;
  executionHistoryHeight?: number;
  panelPosition: number;
  isDraggingPanel: boolean;
  isDarkMode: boolean;
  // Crew canvas state (from tabs)
  nodes: Node[];
  edges: Edge[];
  setNodes: (nodes: Node[] | ((nodes: Node[]) => Node[])) => void;
  setEdges: (edges: Edge[] | ((edges: Edge[]) => Edge[])) => void;
  onNodesChange: (changes: NodeChange[]) => void;
  onEdgesChange: (changes: EdgeChange[]) => void;
  onConnect: (connection: Connection) => void;
  // CRITICAL: Flow canvas state (independent from tabs)
  flowNodes: Node[];
  flowEdges: Edge[];
  onFlowNodesChange: (changes: NodeChange[]) => void;
  onFlowEdgesChange: (changes: EdgeChange[]) => void;
  onFlowConnect: (connection: Connection) => void;
  // Common handlers
  onSelectionChange: (params: OnSelectionChangeParams) => void;
  onPaneContextMenu: (event: React.MouseEvent) => void;
  onCrewFlowInit: (instance: ReactFlowInstance) => void;
  onFlowFlowInit: (instance: ReactFlowInstance) => void;
  onPanelDragStart: (e: React.MouseEvent) => void;
  // FitView handler
  handleUIAwareFitView: () => void;
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
  setIsAgentDialogOpen: (open: boolean) => void;
  setIsTaskDialogOpen: (open: boolean) => void;
  setIsCrewDialogOpen: (open: boolean) => void;
  onOpenTutorial?: () => void;
  onOpenConfiguration?: () => void;
  // Play button handlers
  onPlayPlan?: () => void;
  onPlayFlow?: () => void;
}

const WorkflowPanels: React.FC<WorkflowPanelsProps> = ({
  areFlowsVisible,
  showRunHistory,
  executionHistoryHeight = 0,
  panelPosition,
  isDraggingPanel,
  isDarkMode,
  // Crew canvas state
  nodes,
  edges,
  setNodes,
  setEdges,
  onNodesChange,
  onEdgesChange,
  onConnect,
  // Flow canvas state
  flowNodes,
  flowEdges,
  onFlowNodesChange,
  onFlowEdgesChange,
  onFlowConnect,
  // Common handlers
  onSelectionChange,
  onPaneContextMenu,
  onCrewFlowInit,
  onFlowFlowInit,
  onPanelDragStart,
  handleUIAwareFitView,
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
  setIsAgentDialogOpen,
  setIsTaskDialogOpen,
  setIsCrewDialogOpen,
  onOpenTutorial,
  onOpenConfiguration,
  onPlayPlan,
  onPlayFlow
}) => {
  const { crewAIFlowEnabled } = useFlowConfigStore();
  if (areFlowsVisible && crewAIFlowEnabled) {
      // Show ONLY FlowCanvas when flows are visible
      return (
    <Box sx={{
      height: '100%', // Take full height of parent
      position: 'relative',
      mt: 0, // No margin top since TabBar is above
      mb: 0, // Remove bottom margin
        borderBottom: '1px solid',
        borderColor: isDarkMode ? 'rgba(255,255,255,0.1)' : 'rgba(0,0,0,0.1)',
        display: 'block',
        overflow: 'hidden', // Prevent any content from overflowing
        width: '100%',
        maxWidth: '100%'
      }}>
        {/* Flow canvas - Full width view */}
        <Box
          sx={{
            position: 'relative',
            width: '100%',
            height: '100%',
            minHeight: 0,
            overflow: 'hidden',
          }}
        >
          <FlowCanvas
            nodes={flowNodes}
            edges={flowEdges}
            onNodesChange={onFlowNodesChange}
            onEdgesChange={onFlowEdgesChange}
            onConnect={onFlowConnect}
            onSelectionChange={onSelectionChange}
            onPaneContextMenu={onPaneContextMenu}
            onInit={onFlowFlowInit}
            showRunHistory={showRunHistory}
            executionHistoryHeight={executionHistoryHeight}
          />
        </Box>
      </Box>
    );
  }

  // Single column layout when flows are hidden
  return (
    <Box sx={{ 
      height: '100%', // Take full height of parent
      position: 'relative', 
      mt: 0, // No margin top since TabBar is above
      mb: 0, // Remove bottom margin
      borderBottom: '1px solid',
      borderColor: isDarkMode ? 'rgba(255,255,255,0.1)' : 'rgba(0,0,0,0.1)',
      display: 'block',
      overflow: 'hidden',
      width: '100%',
      maxWidth: '100%'
    }}>
      <Box 
        sx={{ 
          width: '100%',
          height: '100%',
          position: 'relative'
        }}
        data-crew-container
      >
        <CrewCanvas
          nodes={nodes}
          edges={edges}
          setNodes={setNodes}
          setEdges={setEdges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onConnect={onConnect}
          onSelectionChange={onSelectionChange}
          onPaneContextMenu={onPaneContextMenu}
          onInit={onCrewFlowInit}
          handleUIAwareFitView={handleUIAwareFitView}
          planningEnabled={planningEnabled}
          setPlanningEnabled={setPlanningEnabled}
          reasoningEnabled={reasoningEnabled}
          setReasoningEnabled={setReasoningEnabled}
          schemaDetectionEnabled={schemaDetectionEnabled}
          setSchemaDetectionEnabled={setSchemaDetectionEnabled}
          selectedModel={selectedModel}
          setSelectedModel={setSelectedModel}
          onOpenLogsDialog={onOpenLogsDialog}
          onToggleChat={onToggleChat}
          isChatOpen={isChatOpen}
          setIsAgentDialogOpen={setIsAgentDialogOpen}
          setIsTaskDialogOpen={setIsTaskDialogOpen}
          setIsCrewDialogOpen={setIsCrewDialogOpen}
          showRunHistory={showRunHistory}
          onOpenTutorial={onOpenTutorial}
          onOpenConfiguration={onOpenConfiguration}
          onPlayPlan={onPlayPlan}
          onPlayFlow={onPlayFlow}
        />
      </Box>
    </Box>
  );
};

export default WorkflowPanels; 