import React from 'react';
import { Box } from '@mui/material';
import { Node, Edge, NodeChange, EdgeChange, Connection, OnSelectionChangeParams, ReactFlowInstance } from 'reactflow';
import CrewCanvas from './CrewCanvas';
import FlowCanvas from './FlowCanvas';

interface WorkflowPanelsProps {
  areFlowsVisible: boolean;
  showRunHistory: boolean;
  panelPosition: number;
  isDraggingPanel: boolean;
  isDarkMode: boolean;
  nodes: Node[];
  edges: Edge[];
  onNodesChange: (changes: NodeChange[]) => void;
  onEdgesChange: (changes: EdgeChange[]) => void;
  onConnect: (connection: Connection) => void;
  onSelectionChange: (params: OnSelectionChangeParams) => void;
  onPaneContextMenu: (event: React.MouseEvent) => void;
  onCrewFlowInit: (instance: ReactFlowInstance) => void;
  onFlowFlowInit: (instance: ReactFlowInstance) => void;
  onPanelDragStart: (e: React.MouseEvent) => void;
}

const WorkflowPanels: React.FC<WorkflowPanelsProps> = ({
  areFlowsVisible,
  showRunHistory,
  panelPosition,
  isDraggingPanel,
  isDarkMode,
  nodes,
  edges,
  onNodesChange,
  onEdgesChange,
  onConnect,
  onSelectionChange,
  onPaneContextMenu,
  onCrewFlowInit,
  onFlowFlowInit,
  onPanelDragStart,
}) => {
  if (areFlowsVisible) {
    return (
      <Box sx={{ 
        height: showRunHistory ? 'calc(100vh - 64px - 160px)' : 'calc(100vh - 64px - 40px)', 
        position: 'relative', 
        mt: '64px', // Toolbar height
        mb: 0, // Remove bottom margin
        borderBottom: '1px solid',
        borderColor: isDarkMode ? 'rgba(255,255,255,0.1)' : 'rgba(0,0,0,0.1)',
        display: 'grid',
        gridTemplateColumns: `${panelPosition}% ${100 - panelPosition}%`,
        overflow: 'hidden', // Prevent any content from overflowing
        width: '100%',
        maxWidth: '100%',
        transition: isDraggingPanel ? 'none' : 'grid-template-columns 0.3s ease-in-out'
      }}>
        {/* Main crew canvas (always visible) */}
        <Box 
          sx={{ 
            position: 'relative',
            width: '100%',
            height: '100%',
            borderRight: `1px solid ${isDarkMode ? 'rgba(255,255,255,0.1)' : 'rgba(0,0,0,0.1)'}`
          }}
          data-crew-container
        >
          <CrewCanvas
            nodes={nodes}
            edges={edges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onConnect={onConnect}
            onSelectionChange={onSelectionChange}
            onPaneContextMenu={onPaneContextMenu}
            onInit={onCrewFlowInit}
          />
        </Box>
        
        {/* Draggable divider when both panels are visible */}
        <Box
          sx={{
            position: 'absolute',
            top: 0,
            left: `${panelPosition}%`,
            width: '8px',
            height: '100%',
            transform: 'translateX(-50%)',
            cursor: 'col-resize',
            zIndex: 100,
            background: isDarkMode ? 'rgba(255,255,255,0.05)' : 'rgba(0,0,0,0.05)',
            '&::after': {
              content: '""',
              position: 'absolute',
              top: 0,
              left: '50%',
              width: '2px',
              height: '100%',
              transform: 'translateX(-50%)',
              backgroundColor: isDarkMode ? 'rgba(255,255,255,0.2)' : 'rgba(0,0,0,0.15)',
            },
            '&:hover': {
              background: isDarkMode ? 'rgba(255,255,255,0.1)' : 'rgba(0,0,0,0.08)',
              '&::after': {
                backgroundColor: isDarkMode ? 'rgba(255,255,255,0.4)' : 'rgba(0,0,0,0.3)',
              }
            },
            '&:active': {
              background: isDarkMode ? 'rgba(255,255,255,0.15)' : 'rgba(0,0,0,0.12)',
              '&::after': {
                backgroundColor: isDarkMode ? 'rgba(255,255,255,0.5)' : 'rgba(0,0,0,0.4)',
              }
            }
          }}
          onMouseDown={onPanelDragStart}
        />
        
        {/* Flow canvas */}
        <Box 
          sx={{ 
            position: 'relative',
            width: '100%',
            height: '100%',
          }}
        >
          <FlowCanvas
            nodes={nodes}
            edges={edges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onConnect={onConnect}
            onSelectionChange={onSelectionChange}
            onPaneContextMenu={onPaneContextMenu}
            onInit={onFlowFlowInit}
          />
        </Box>
      </Box>
    );
  }

  // Single column layout when flows are hidden
  return (
    <Box sx={{ 
      height: showRunHistory ? 'calc(100vh - 64px - 160px)' : 'calc(100vh - 64px - 40px)', 
      position: 'relative', 
      mt: '64px', // Toolbar height
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
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onConnect={onConnect}
          onSelectionChange={onSelectionChange}
          onPaneContextMenu={onPaneContextMenu}
          onInit={onCrewFlowInit}
        />
      </Box>
    </Box>
  );
};

export default WorkflowPanels; 