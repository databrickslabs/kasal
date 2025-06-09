# Hooks Documentation

This directory contains React hooks organized by usage pattern and functionality.

## Directory Structure

- **`components/`** - Component-specific hooks
- **`context/`** - Context-related hooks  
- **`global/`** - Application-wide hooks
- **`workflow/`** - Workflow and canvas management hooks

## useShortcuts

The `useShortcuts` hook provides keyboard shortcut functionality for canvas components and workflow operations.

### Usage

```tsx
import useShortcuts from '../hooks/useShortcuts';
import { ReactFlowInstance } from 'reactflow';

const MyCanvasComponent = () => {
  const flowInstanceRef = useRef<ReactFlowInstance | null>(null);
  
  const handleDeleteSelected = useCallback((selectedNodes, selectedEdges) => {
    // Handle deletion logic
    console.log('Deleting selected elements', { selectedNodes, selectedEdges });
  }, []);
  
  const handleClearCanvas = useCallback(() => {
    // Handle clearing logic
    console.log('Clearing canvas');
  }, []);
  
  // Initialize shortcuts
  const { shortcuts } = useShortcuts({
    flowInstance: flowInstanceRef.current,
    onDeleteSelected: handleDeleteSelected,
    onClearCanvas: handleClearCanvas,
  });
  
  // Render component with shortcut functionality
  return (
    <div>
      {/* Your canvas component */}
    </div>
  );
};
```

### Integration Example

Here's a complete example showing how to integrate the shortcuts with a React Flow canvas:

```tsx
import React, { useCallback, useRef, useState } from 'react';
import ReactFlow, {
  Background,
  Controls,
  ReactFlowInstance,
  Node,
  Edge,
  NodeChange,
  EdgeChange,
  Connection
} from 'reactflow';
import 'reactflow/dist/style.css';
import useShortcuts from '../hooks/useShortcuts';
import { Box, Snackbar, Alert, Typography } from '@mui/material';

// Example node data
const initialNodes: Node[] = [
  { id: '1', data: { label: 'Node 1' }, position: { x: 250, y: 100 } },
  { id: '2', data: { label: 'Node 2' }, position: { x: 250, y: 300 } }
];

const initialEdges: Edge[] = [
  { id: 'e1-2', source: '1', target: '2' }
];

const CanvasWithShortcuts: React.FC = () => {
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
  
  // Initialize shortcuts
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
        height: '100vh',
        position: 'relative'
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
          Press 'dd' to delete, Alt+C to clear canvas
        </Typography>
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

export default CanvasWithShortcuts;
```

### Key Features

- **Multi-key sequences** - Support for vim-style key combinations
- **Dialog awareness** - Automatically disabled when dialogs are open
- **Workflow integration** - Built-in validation for crew/flow operations
- **Extensible** - Easy to add new shortcuts and handlers

For a complete list of available shortcuts, see **[Keyboard Shortcuts Guide](../../../docs/SHORTCUTS.md)**.

### API

#### Options

The `useShortcuts` hook accepts the following options:

| Option | Type | Description |
|--------|------|-------------|
| `shortcuts` | `ShortcutConfig[]` | Custom shortcut configurations (optional) |
| `flowInstance` | `ReactFlowInstance \| null` | Reference to the React Flow instance |
| `onDeleteSelected` | `(selectedNodes: Node[], selectedEdges: Edge[]) => void` | Handler for deleting selected elements |
| `onClearCanvas` | `() => void` | Handler for clearing the canvas |
| `onUndo` | `() => void` | Handler for undo action |
| `onRedo` | `() => void` | Handler for redo action |
| `onSelectAll` | `() => void` | Handler for selecting all nodes |
| `onCopy` | `() => void` | Handler for copying selected nodes |
| `onPaste` | `() => void` | Handler for pasting nodes |
| `onZoomIn` | `() => void` | Handler for zoom in action |
| `onZoomOut` | `() => void` | Handler for zoom out action |
| `onFitView` | `() => void` | Handler for fit view action |
| `onToggleFullscreen` | `() => void` | Handler for toggling fullscreen mode |
| `disabled` | `boolean` | Disable shortcuts (default: false) |

#### Returns

The hook returns an object with the following properties:

| Property | Type | Description |
|----------|------|-------------|
| `shortcuts` | `ShortcutConfig[]` | The current shortcut configurations | 