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
} from 'reactflow';
import 'reactflow/dist/style.css';
import { Box, Snackbar, Alert } from '@mui/material';
import { useThemeManager } from '../../hooks/workflow/useThemeManager';
import FlowCanvasControls from './FlowCanvasControls';
import useShortcuts from '../../hooks/global/useShortcuts';

// Node types
import { CrewNode } from '../Flow';

// Edge types
import AnimatedEdge from '../Common/AnimatedEdge';
import CrewEdge from '../Flow/CrewEdge';

// Node and edge types configuration
const nodeTypes = {
  crewNode: CrewNode
};

const edgeTypes = {
  default: AnimatedEdge,
  crewEdge: CrewEdge
};

interface FlowCanvasProps {
  nodes: Node[];
  edges: Edge[];
  onNodesChange: (changes: NodeChange[]) => void;
  onEdgesChange: (changes: EdgeChange[]) => void;
  onConnect: (connection: Connection) => void;
  onSelectionChange?: (params: OnSelectionChangeParams) => void;
  onPaneContextMenu?: (event: React.MouseEvent) => void;
  onInit?: (instance: ReactFlowInstance) => void;
}

// Global error handler for ResizeObserver errors
// This needs to be outside the component to ensure it's only added once
if (typeof window !== 'undefined') {
  const errorHandler = (event: ErrorEvent) => {
    if (
      event.message && 
      (event.message.includes('ResizeObserver loop') || 
       event.message.includes('ResizeObserver Loop'))
    ) {
      event.stopImmediatePropagation();
      event.preventDefault();
      console.debug('ResizeObserver error suppressed:', event.message);
    }
  };
  
  window.addEventListener('error', errorHandler);
  window.addEventListener('unhandledrejection', (event) => {
    if (event.reason && event.reason.message && 
        (event.reason.message.includes('ResizeObserver loop') || 
         event.reason.message.includes('ResizeObserver Loop'))) {
      event.preventDefault();
      console.debug('ResizeObserver promise rejection suppressed:', event.reason.message);
    }
  });
}

const FlowCanvas: React.FC<FlowCanvasProps> = ({
  nodes,
  edges,
  onNodesChange,
  onEdgesChange,
  onConnect,
  onSelectionChange,
  onPaneContextMenu,
  onInit
}) => {
  const { isDarkMode } = useThemeManager();
  const [controlsVisible, _setControlsVisible] = useState(false);
  const reactFlowInstanceRef = useRef<ReactFlowInstance | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  // Track render state to prevent unnecessary re-renders
  const [isRendering, setIsRendering] = useState(true); // Start with true to show loading
  const [isStable, setIsStable] = useState(false);

  // Add error boundary for catching ReactFlow errors
  const [hasError, setHasError] = useState(false);
  
  // Add state for notifications
  const [showNotification, setShowNotification] = useState(false);
  const [notificationMessage, setNotificationMessage] = useState('');
  
  // Add function to show notification
  const showTemporaryNotification = useCallback((message: string) => {
    setNotificationMessage(message);
    setShowNotification(true);
    
    // Auto-hide after 3 seconds
    setTimeout(() => {
      setShowNotification(false);
    }, 3000);
  }, []);

  // Define handleClearCanvas before it's used
  const handleClearCanvas = useCallback(() => {
    // Get all flow node IDs
    const flowNodeIds = nodes
      .filter(node => node.type === 'crewNode')
      .map(node => node.id);
    
    // Create removal changes for all flow nodes
    if (flowNodeIds.length > 0) {
      const nodesToRemove = flowNodeIds.map(id => ({
        id,
        type: 'remove' as const,
      }));
      
      // Apply the changes to remove all flow nodes
      onNodesChange(nodesToRemove);
    }
    
    // Get all edges connected to flow nodes
    const edgesToRemove = edges
      .filter(edge => 
        flowNodeIds.includes(edge.source) || 
        flowNodeIds.includes(edge.target)
      )
      .map(edge => ({
        id: edge.id,
        type: 'remove' as const,
      }));
    
    // Apply the changes to remove all related edges
    if (edgesToRemove.length > 0) {
      onEdgesChange(edgesToRemove);
    }
    
    showTemporaryNotification(`Canvas cleared: removed ${flowNodeIds.length} nodes and ${edgesToRemove.length} edges`);
  }, [nodes, edges, onNodesChange, onEdgesChange, showTemporaryNotification]);

  // Handle shortcut actions
  const handleDeleteSelected = useCallback((selectedNodes: Node[], selectedEdges: Edge[]) => {
    if (selectedNodes.length > 0) {
      const nodesToRemove = selectedNodes.map(node => ({
        id: node.id,
        type: 'remove' as const,
      }));
      
      onNodesChange(nodesToRemove);
    }
    
    if (selectedEdges.length > 0) {
      const edgesToRemove = selectedEdges.map(edge => ({
        id: edge.id,
        type: 'remove' as const,
      }));
      
      onEdgesChange(edgesToRemove);
    }
    
    // Show success notification if something was deleted
    if (selectedNodes.length > 0 || selectedEdges.length > 0) {
      showTemporaryNotification(`Deleted ${selectedNodes.length} nodes and ${selectedEdges.length} edges`);
    }
  }, [onNodesChange, onEdgesChange, showTemporaryNotification]);

  // Initialize shortcuts - prefix with _ to indicate it's intentionally unused in the JSX
  const { shortcuts: _shortcuts } = useShortcuts({
    flowInstance: reactFlowInstanceRef.current,
    onDeleteSelected: handleDeleteSelected,
    onClearCanvas: handleClearCanvas,
    onFitView: () => {
      if (reactFlowInstanceRef.current) {
        reactFlowInstanceRef.current.fitView({ padding: 0.2 });
        showTemporaryNotification('Fit view to all nodes');
      }
    },
    onOpenFlowDialog: () => {
      // Dispatch an event to open the Flow selection dialog
      const event = new CustomEvent('openFlowDialog');
      window.dispatchEvent(event);
      showTemporaryNotification('Opening Flow selection dialog (press lf)');
    },
    disabled: isRendering || hasError
  });

  // Filter to only show flow nodes - define this before using it
  const flowNodes = React.useMemo(() => {
    try {
      // Safely filter nodes to prevent rendering issues
      return nodes.filter(node => {
        // Check for invalid nodes
        if (!node || typeof node !== 'object') return false;
        
        const nodeName = node.data?.label?.toLowerCase() || '';
        const nodeType = node.type?.toLowerCase() || '';
        
        return (
          nodeName.includes('flow') || 
          nodeType.includes('flow') || 
          nodeType === 'crewnode' || 
          (node.data && node.data.flowConfig)
        );
      });
    } catch (error) {
      console.error('Error filtering flow nodes:', error);
      return [];
    }
  }, [nodes]);
  
  // Automatically fit view when nodes change
  useEffect(() => {
    // Only run if we're not rendering and we have an instance
    if (!isRendering && reactFlowInstanceRef.current && flowNodes.length > 0) {
      // Add a small delay to allow nodes to properly render
      const fitViewTimer = setTimeout(() => {
        if (reactFlowInstanceRef.current) {
          reactFlowInstanceRef.current.fitView({ 
            padding: 0.2, 
            includeHiddenNodes: false,
            duration: 800 // Smooth animation
          });
        }
      }, 300);
      
      return () => clearTimeout(fitViewTimer);
    }
  }, [flowNodes.length, isRendering]);
  
  // Ensure nodes have stable dimensions before rendering
  const nodeWithDimensions = React.useMemo(() => {
    return flowNodes.map(node => {
      // If node doesn't have specified dimensions, add default ones
      if (!node.style || (!node.style.width && !node.style.height)) {
        return {
          ...node,
          style: {
            ...node.style,
            width: node.style?.width || 180,
            height: node.style?.height || 80
          }
        };
      }
      return node;
    });
  }, [flowNodes]);

  // Use layout effect to stabilize initial render with staggered approach
  useLayoutEffect(() => {
    setIsRendering(true);
    
    // First phase - wait for DOM to be ready
    const initialTimer = setTimeout(() => {
      if (!isStable) {
        setIsStable(true);
      }
    }, 0);
    
    // Second phase - wait longer to ensure ResizeObserver has settled
    const renderTimer = setTimeout(() => {
      setIsRendering(false);
    }, 100);
    
    return () => {
      clearTimeout(initialTimer);
      clearTimeout(renderTimer);
    };
  }, [isStable, flowNodes.length]); // Re-run when node count changes

  // Reset error state when nodes or edges change
  useEffect(() => {
    if (hasError) {
      setHasError(false);
    }
  }, [nodes, edges, hasError]);

  const handleInit = useCallback((instance: ReactFlowInstance) => {
    reactFlowInstanceRef.current = instance;
    
    // Delay fitting view to allow nodes to properly render
    setTimeout(() => {
      if (instance) {
        try {
          // Check for overlapping nodes and adjust their positions
          if (flowNodes.length > 0) {
            const nodePositions = new Map();
            const updatedNodes: Node[] = [];
            
            // First pass: collect all node positions
            flowNodes.forEach(node => {
              const key = `${Math.round(node.position.x)},${Math.round(node.position.y)}`;
              if (!nodePositions.has(key)) {
                nodePositions.set(key, []);
              }
              nodePositions.get(key).push(node);
            });
            
            // Second pass: adjust positions of overlapping nodes
            let hasOverlappingNodes = false;
            nodePositions.forEach((nodes, key) => {
              if (nodes.length > 1) {
                hasOverlappingNodes = true;
                // We have overlapping nodes
                console.log(`Found ${nodes.length} overlapping nodes at position ${key}`);
                
                nodes.forEach((node: Node, index: number) => {
                  if (index === 0) {
                    // Keep the first node at its original position
                    updatedNodes.push(node);
                  } else {
                    // Adjust position for subsequent nodes
                    // Place nodes in a grid pattern
                    const columns = Math.ceil(Math.sqrt(nodes.length));
                    const row = Math.floor(index / columns);
                    const col = index % columns;
                    
                    // Calculate offset from original position
                    const offsetX = col * 300;  // 300px horizontal spacing
                    const offsetY = row * 200;  // 200px vertical spacing
                    
                    updatedNodes.push({
                      ...node,
                      position: {
                        x: node.position.x + offsetX,
                        y: node.position.y + offsetY
                      }
                    });
                  }
                });
              } else {
                // Not overlapping, keep as is
                updatedNodes.push(nodes[0]);
              }
            });
            
            // Apply the updated positions if we found and fixed overlapping nodes
            if (updatedNodes.length > 0 && hasOverlappingNodes) {
              instance.setNodes(updatedNodes);
              
              // Check if we need to update edge types
              const currentEdges = instance.getEdges();
              const edgesNeedingUpdate = currentEdges.filter(edge => 
                !edge.type || edge.type === 'default'
              );
              
              if (edgesNeedingUpdate.length > 0) {
                console.log(`Setting proper edge types for ${edgesNeedingUpdate.length} edges`);
                const updatedEdges = currentEdges.map(edge => {
                  if (!edge.type || edge.type === 'default') {
                    return {
                      ...edge,
                      type: 'crewEdge',
                      animated: true
                    };
                  }
                  return edge;
                });
                
                instance.setEdges(updatedEdges);
              }
              
              // Check for missing edges between nodes that should be connected
              const _allNodesById = new Map(updatedNodes.map(node => [node.id, node]));
              const existingEdges = new Set(currentEdges.map(edge => `${edge.source}-${edge.target}`));
              const missingEdges: Edge[] = [];
              
              // Check for flow configuration in node data to establish connections
              updatedNodes.forEach((sourceNode: Node) => {
                if (sourceNode.data?.flowConfig?.listeners) {
                  const flowConfig = sourceNode.data.flowConfig;
                  
                  // For each listener, check if we need to create edges
                  flowConfig.listeners.forEach((listener: {
                    crewId?: number | string;
                    listenToTaskIds?: string[];
                    tasks?: Array<{ id: string; name: string; }>;
                  }) => {
                    if (listener.crewId) {
                      // This listener belongs to a specific crew - find matching node
                      const listenerSourceNodes = updatedNodes.filter((node: Node) => 
                        node.data?.crewId === listener.crewId
                      );
                      
                      // For each task this listener listens to, find target nodes
                      if (listener.listenToTaskIds && listener.listenToTaskIds.length > 0) {
                        listener.listenToTaskIds.forEach((taskId: string) => {
                          // Extract crew ID from task ID if in format crewId:taskId
                          let targetCrewId: string | null = null;
                          if (taskId.includes(':')) {
                            [targetCrewId] = taskId.split(':');
                          }
                          
                          if (targetCrewId) {
                            // Find target nodes for this crew
                            const targetNodes = updatedNodes.filter((node: Node) => 
                              node.data?.crewId === Number(targetCrewId) || 
                              node.data?.crewId === targetCrewId
                            );
                            
                            // Create edges from source to target if missing
                            listenerSourceNodes.forEach((source: Node) => {
                              targetNodes.forEach((target: Node) => {
                                const edgeKey = `${source.id}-${target.id}`;
                                if (!existingEdges.has(edgeKey) && source.id !== target.id) {
                                  // Find task names for the edge label
                                  const taskNames = listener.tasks && Array.isArray(listener.tasks) 
                                    ? listener.tasks.map((task: { id: string; name: string; }) => task.name).join(', ')
                                    : 'Task';
                                    
                                  // Determine the best handles to use based on relative positions
                                  const sourceX = source.position.x;
                                  const sourceY = source.position.y;
                                  const targetX = target.position.x;
                                  const targetY = target.position.y;
                                  
                                  let sourceHandle, targetHandle;
                                  
                                  // If target is to the right of source
                                  if (targetX > sourceX + 200) {
                                    sourceHandle = 'right';
                                    targetHandle = 'left-target';
                                  }
                                  // If target is to the left of source
                                  else if (targetX < sourceX - 200) {
                                    sourceHandle = 'left';
                                    targetHandle = 'right-target';
                                  }
                                  // If target is below source
                                  else if (targetY > sourceY + 100) {
                                    sourceHandle = 'bottom';
                                    targetHandle = 'top-target';
                                  }
                                  // If target is above source
                                  else {
                                    sourceHandle = 'top';
                                    targetHandle = 'bottom-target';
                                  }
                                  
                                  missingEdges.push({
                                    id: `edge-${source.id}-${target.id}-${Date.now()}`,
                                    source: source.id,
                                    target: target.id,
                                    sourceHandle,
                                    targetHandle,
                                    type: 'crewEdge',
                                    animated: true,
                                    data: {
                                      label: taskNames
                                    }
                                  });
                                  
                                  existingEdges.add(edgeKey);
                                }
                              });
                            });
                          }
                        });
                      }
                    }
                  });
                }
              });
              
              // Add missing edges if any were found
              if (missingEdges.length > 0) {
                console.log(`Adding ${missingEdges.length} missing edges between nodes`);
                instance.setEdges([...currentEdges, ...missingEdges]);
              }
            }
            
            // Now fit view
            instance.fitView({ padding: 0.2, includeHiddenNodes: false });
          }
        } catch (error) {
          console.warn('FlowCanvas fitView error:', error);
        }
      }
    }, 200); // Longer delay to ensure nodes are stable
    
    if (onInit) {
      onInit(instance);
    }
  }, [onInit, flowNodes]);

  // Filter edges that connect flow nodes
  const flowEdges = React.useMemo(() => {
    try {
      // Build set of node IDs for faster lookup
      const flowNodeIds = new Set(flowNodes.map(node => node.id));
      
      // Filter edges that connect to flow nodes
      return edges.filter(edge => 
        edge && 
        typeof edge === 'object' &&
        edge.source && 
        edge.target && 
        flowNodeIds.has(edge.source) && 
        flowNodeIds.has(edge.target)
      );
    } catch (error) {
      console.error('Error filtering flow edges:', error);
      return [];
    }
  }, [edges, flowNodes]);

  // Stable callback for node changes to prevent unnecessary renders
  // This is used inside ReactFlow, so we don't need to prefix with _
  const handleNodesChange = useCallback((changes: NodeChange[]) => {
    // Only apply changes to flow nodes
    const flowNodeIds = new Set(flowNodes.map(node => node.id));
    
    // Filter changes, checking each change type to access id safely
    const filteredChanges = changes.filter(change => {
      // Different change types need different handling
      switch (change.type) {
        case 'position':
        case 'dimensions':
        case 'remove':
        case 'select':
          // These types have an id field we can check directly
          return flowNodeIds.has(change.id);
        case 'add':
          // For add changes, check data in the item
          return change.item && flowNodeIds.has(change.item.id);
        default:
          // For unknown change types, include them by default
          return true;
      }
    });
    
    if (filteredChanges.length > 0) {
      onNodesChange(filteredChanges);
    }
  }, [flowNodes, onNodesChange]);

  // Add effect to listen for notification events
  useEffect(() => {
    const handleNotification = (event: CustomEvent<{ message: string }>) => {
      if (event.detail && event.detail.message) {
        showTemporaryNotification(event.detail.message);
      }
    };
    
    window.addEventListener('showNotification', handleNotification as EventListener);
    
    return () => {
      window.removeEventListener('showNotification', handleNotification as EventListener);
    };
  }, [showTemporaryNotification]);

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
      {isRendering ? (
        // Show loading or error state
        <Box sx={{ 
          display: 'flex', 
          justifyContent: 'center', 
          alignItems: 'center',
          height: '100%',
          color: 'text.secondary',
          fontSize: '0.875rem'
        }}>
          {hasError ? 'Error rendering canvas' : 'Loading flow canvas...'}
        </Box>
      ) : (
        <ReactFlow
          nodes={nodeWithDimensions}
          edges={flowEdges}
          onNodesChange={handleNodesChange}
          onEdgesChange={onEdgesChange}
          onConnect={onConnect}
          onInit={handleInit}
          nodeTypes={nodeTypes}
          edgeTypes={edgeTypes}
          onSelectionChange={onSelectionChange}
          onPaneContextMenu={onPaneContextMenu}
          proOptions={{ hideAttribution: true }}
          connectionMode={ConnectionMode.Loose}
          snapToGrid={true}
          minZoom={0.1}
          maxZoom={4}
          fitView
          fitViewOptions={{ padding: 0.2 }}
          style={{ background: isDarkMode ? '#1a1a1a' : '#f5f5f5' }}
        >
          <Background 
            color={isDarkMode ? '#333' : '#aaa'} 
            gap={16} 
            size={1}
            variant={BackgroundVariant.Dots}
          />
          {controlsVisible && <FlowCanvasControls onClearCanvas={handleClearCanvas} />}
        </ReactFlow>
      )}
      
      {/* Add info about available shortcuts */}
      <Box 
        sx={{ 
          position: 'absolute', 
          bottom: 10, 
          left: 10, 
          fontSize: '0.75rem', 
          color: 'text.secondary',
          opacity: 0.7,
          pointerEvents: 'none',
        }}
      >
        Tip: Press &quot;del&quot; to delete selected items, dd to clear canvas
      </Box>
      
      {/* Notification */}
      <Snackbar
        open={showNotification}
        autoHideDuration={3000}
        onClose={() => setShowNotification(false)}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
      >
        <Alert severity="info" sx={{ width: '100%' }}>
          {notificationMessage}
        </Alert>
      </Snackbar>
    </Box>
  );
};

// Wrap the component with memo for better performance
export default memo(FlowCanvas); 