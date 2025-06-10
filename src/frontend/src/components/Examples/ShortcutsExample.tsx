import React, { useCallback, useRef, useState } from 'react';
import ReactFlow, {
  Background,
  Controls,
  ReactFlowInstance,
  Node,
  Edge,
  NodeChange,
  EdgeChange,
  Connection,
  applyNodeChanges,
  applyEdgeChanges,
  addEdge,
} from 'reactflow';
import 'reactflow/dist/style.css';
import useShortcuts from '../../hooks/global/useShortcuts';
import { Box, Snackbar, Alert, Typography, Paper, List, ListItem, ListItemText } from '@mui/material';

// Example node data
const initialNodes: Node[] = [
  { id: '1', data: { label: 'Node 1' }, position: { x: 250, y: 100 } },
  { id: '2', data: { label: 'Node 2' }, position: { x: 250, y: 300 } }
];

const initialEdges: Edge[] = [
  { id: 'e1-2', source: '1', target: '2' }
];

/**
 * Example component showing how to integrate keyboard shortcuts with a canvas
 */
const ShortcutsExample: React.FC = () => {
  // State management
  const [nodes, setNodes] = useState<Node[]>(initialNodes);
  const [edges, setEdges] = useState<Edge[]>(initialEdges);
  const [notification, setNotification] = useState({ show: false, message: '' });
  
  // Flow instance reference
  const reactFlowInstanceRef = useRef<ReactFlowInstance | null>(null);
  
  // Handle Flow changes
  const onNodesChange = useCallback((changes: NodeChange[]) => {
    setNodes((nds) => applyNodeChanges(changes, nds));
  }, []);
  
  const onEdgesChange = useCallback((changes: EdgeChange[]) => {
    setEdges((eds) => applyEdgeChanges(changes, eds));
  }, []);
  
  const onConnect = useCallback((params: Connection) => {
    setEdges((eds) => addEdge(params, eds));
  }, []);
  
  // Handle ReactFlow initialization
  const onInit = useCallback((instance: ReactFlowInstance) => {
    reactFlowInstanceRef.current = instance;
  }, []);
  
  // Show notification
  const showNotification = useCallback((message: string) => {
    setNotification({
      show: true,
      message
    });
    
    // Auto-hide after 3 seconds
    setTimeout(() => {
      setNotification(prev => ({ ...prev, show: false }));
    }, 3000);
  }, []);
  
  // Shortcut action handlers
  const handleDeleteSelected = useCallback((selectedNodes: Node[], selectedEdges: Edge[]) => {
    if (selectedNodes.length > 0 || selectedEdges.length > 0) {
      // Handle node deletion
      const nodesToRemove = selectedNodes.map(node => ({
        id: node.id,
        type: 'remove' as const
      }));
      
      // Handle edge deletion
      const edgesToRemove = selectedEdges.map(edge => ({
        id: edge.id,
        type: 'remove' as const
      }));
      
      // Apply changes
      onNodesChange(nodesToRemove);
      onEdgesChange(edgesToRemove);
      
      // Show notification
      showNotification(`Deleted ${selectedNodes.length} nodes and ${selectedEdges.length} edges`);
    }
  }, [onNodesChange, onEdgesChange, showNotification]);
  
  const handleClearCanvas = useCallback(() => {
    setNodes([]);
    setEdges([]);
    showNotification('Canvas cleared');
  }, [showNotification]);
  
  const handleReset = useCallback(() => {
    setNodes(initialNodes);
    setEdges(initialEdges);
    showNotification('Canvas reset to initial state');
  }, [showNotification]);
  
  // Initialize shortcuts with our custom mappings
  const { shortcuts } = useShortcuts({
    flowInstance: reactFlowInstanceRef.current,
    onDeleteSelected: handleDeleteSelected,
    onClearCanvas: handleClearCanvas,
    onFitView: () => {
      if (reactFlowInstanceRef.current) {
        reactFlowInstanceRef.current.fitView({ padding: 0.2 });
        showNotification('Fit view to all nodes');
      }
    }
  });
  
  return (
    <Box
      sx={{
        width: '100%',
        height: '100%',
        display: 'flex',
        flexDirection: 'column',
      }}
    >
      <Typography variant="h6" sx={{ p: 2, borderBottom: '1px solid #eee' }}>
        Keyboard Shortcuts Example
      </Typography>
      
      <Box
        sx={{
          display: 'flex',
          flex: 1,
          position: 'relative',
        }}
      >
        {/* Main canvas area */}
        <Box
          sx={{
            flex: 1,
            height: '100%',
            position: 'relative',
          }}
        >
          <ReactFlow
            nodes={nodes}
            edges={edges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onConnect={onConnect}
            onInit={onInit}
            fitView
          >
            <Background />
            <Controls />
          </ReactFlow>
          
          {/* Shortcut info indicator */}
          <Box
            sx={{
              position: 'absolute',
              bottom: 10,
              left: 10,
              padding: '4px 8px',
              backgroundColor: 'rgba(0, 0, 0, 0.6)',
              borderRadius: '4px',
              color: '#fff',
              fontSize: '0.75rem',
              pointerEvents: 'none'
            }}
          >
            <Typography variant="caption">
              Try keyboard shortcuts: 'dd' to delete, Alt+C to clear canvas
            </Typography>
          </Box>
        </Box>
        
        {/* Sidebar with shortcuts info */}
        <Paper
          sx={{
            width: 300,
            height: '100%',
            overflow: 'auto',
            borderLeft: '1px solid #eee',
          }}
          elevation={0}
        >
          <Box sx={{ p: 2 }}>
            <Typography variant="subtitle1" gutterBottom>
              Available Shortcuts:
            </Typography>
            <List dense>
              {shortcuts.map((shortcut, index) => (
                <ListItem key={index}>
                  <ListItemText 
                    primary={shortcut.description}
                    secondary={`Keys: ${shortcut.keys.join(' + ')}`}
                  />
                </ListItem>
              ))}
            </List>
            
            <Box sx={{ mt: 2 }}>
              <Typography variant="body2" color="text.secondary">
                Note: Click on the canvas first to enable keyboard shortcuts.
                They won't work if you're focused on an input or textarea.
              </Typography>
            </Box>
            
            <Box
              sx={{
                mt: 2,
                p: 1,
                backgroundColor: '#f5f5f5',
                borderRadius: 1,
              }}
            >
              <Typography variant="body2">
                <strong>Tip:</strong> You can add your own custom shortcuts by modifying the shortcuts array.
              </Typography>
            </Box>
            
            <Box
              sx={{
                mt: 2,
                textAlign: 'center',
              }}
            >
              <Typography 
                variant="button" 
                sx={{ 
                  cursor: 'pointer',
                  color: 'primary.main',
                  '&:hover': { textDecoration: 'underline' }
                }}
                onClick={handleReset}
              >
                Reset Canvas
              </Typography>
            </Box>
          </Box>
        </Paper>
      </Box>
      
      {/* Notifications */}
      <Snackbar
        open={notification.show}
        autoHideDuration={3000}
        onClose={() => setNotification(prev => ({ ...prev, show: false }))}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
      >
        <Alert severity="info" sx={{ width: '100%' }}>
          {notification.message}
        </Alert>
      </Snackbar>
    </Box>
  );
};

export default ShortcutsExample; 