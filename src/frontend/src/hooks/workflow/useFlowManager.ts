import { useCallback, useEffect, useMemo } from 'react';
import {
  Edge,
  Connection,
  EdgeChange,
  NodeChange,
  NodePositionChange,
  applyEdgeChanges,
  applyNodeChanges
} from 'reactflow';
import { useTranslation } from 'react-i18next';
import { useWorkflowStore } from '../../store/workflow';

interface UseFlowManagerProps {
  showErrorMessage: (message: string) => void;
}

export const useFlowManager = ({ showErrorMessage }: UseFlowManagerProps) => {
  const { t } = useTranslation();
  const {
    nodes,
    edges,
    setNodes,
    setEdges,
    selectedEdges,
    setSelectedEdges,
    contextMenu,
    setContextMenu,
    flowConfig,
    setFlowConfig,
    draggedNodeIds: storeNodeIds,
    setDraggedNodeIds,
    manuallyPositionedNodes: storePositionedNodes,
    setManuallyPositionedNodes,
    clearCanvas: storeClearCanvas,
    deleteEdge,
    addEdge: storeAddEdge
  } = useWorkflowStore();

  // Create local Set objects from store arrays with useMemo to avoid recreating on every render
  const draggedNodeIds = useMemo(() => new Set(storeNodeIds), [storeNodeIds]);
  const manuallyPositionedNodes = useMemo(() => new Set(storePositionedNodes), [storePositionedNodes]);

  // Wrapper for clearCanvas that also resets task statuses
  const clearCanvas = useCallback(() => {
    // Reset task statuses when canvas is cleared
    storeClearCanvas();
  }, [storeClearCanvas]);

  // Optimized handler for node changes
  const onNodesChange = useCallback((changes: NodeChange[]) => {
    // For dragging operations, handle them more efficiently
    const positionChanges = changes.filter(
      (change): change is NodePositionChange => change.type === 'position'
    );
    
    const otherChanges = changes.filter(
      change => change.type !== 'position' || !('position' in change)
    );
    
    // If we have position changes for dragging, handle them separately
    if (positionChanges.length > 0) {
      // Track which nodes are being dragged for optimized rendering
      const newDraggedNodeIds = new Set(draggedNodeIds);
      const newManuallyPositionedNodes = new Set(manuallyPositionedNodes);
      
      // Update position directly for dragged nodes
      setNodes(nodes.map(node => {
        // Find if we have a position change for this node
        const posChange = positionChanges.find(
          change => change.id === node.id
        );
        
        if (posChange) {
          // Update dragging state for this node
          if (posChange.dragging) {
            // Node is currently being dragged
            newDraggedNodeIds.add(node.id);
            
            // When dragging starts, mark this node as manually positioned
            // This prevents automatic repositioning in WorkflowDesigner
            newManuallyPositionedNodes.add(node.id);
          } else if (newDraggedNodeIds.has(node.id)) {
            // Drag operation ended
            newDraggedNodeIds.delete(node.id);
          }
          
          // Only update position if we have valid coordinates
          if (posChange.position && 
              isFinite(posChange.position.x) && 
              isFinite(posChange.position.y)) {
            return {
              ...node,
              position: {
                x: posChange.position.x,
                y: posChange.position.y
              }
            };
          }
        }
        
        return node;
      }));
      
      // Update the dragged node IDs in the store
      setDraggedNodeIds(Array.from(newDraggedNodeIds) as string[]);
      
      // Update manually positioned nodes in the store
      setManuallyPositionedNodes(Array.from(newManuallyPositionedNodes) as string[]);
    }
    
    // Handle other types of changes with standard react-flow method
    if (otherChanges.length > 0) {
      setNodes(applyNodeChanges(otherChanges, nodes));
    }
  }, [nodes, setNodes, draggedNodeIds, manuallyPositionedNodes, setDraggedNodeIds, setManuallyPositionedNodes]);

  // Reset manually positioned nodes when nodes are added or removed
  useEffect(() => {
    // This effect is used to clean up the manuallyPositionedNodes set
    // if nodes are removed from the canvas
    const allNodeIds = new Set(nodes.map(node => node.id));
    let hasChanges = false;
    
    // Create a new set with only valid node IDs
    const newPositionedNodes = new Set<string>();
    
    manuallyPositionedNodes.forEach((id: string) => {
      if (allNodeIds.has(id)) {
        newPositionedNodes.add(id);
      } else {
        hasChanges = true;
      }
    });
    
    // Only update the store if there are changes
    if (hasChanges) {
      setManuallyPositionedNodes(Array.from(newPositionedNodes));
    }
  }, [nodes, manuallyPositionedNodes, setManuallyPositionedNodes]);

  // Handler for edge changes
  const onEdgesChange = useCallback((changes: EdgeChange[]) => {
    setEdges(applyEdgeChanges(changes, edges));
  }, [edges, setEdges]);

  // Handler for connecting nodes
  const onConnect = useCallback((params: Connection) => {
    if (params.source && params.target) {
      const sourceNode = nodes.find(n => n.id === params.source);
      const targetNode = nodes.find(n => n.id === params.target);

      if (!sourceNode || !targetNode) {
        return;
      }

      if (sourceNode.type === 'agentNode' && targetNode.type === 'agentNode') {
        showErrorMessage(t('nemo.errors.agentConnection'));
        return;
      }

      storeAddEdge(params);
    }
  }, [nodes, storeAddEdge, t, showErrorMessage]);

  // Handler for context menu on edges
  const handleEdgeContextMenu = useCallback((event: React.MouseEvent, edge: Edge) => {
    event.preventDefault();
    setContextMenu({
      mouseX: event.clientX,
      mouseY: event.clientY,
      edgeId: edge.id,
    });
  }, [setContextMenu]);

  // Handler for closing context menu
  const handleContextMenuClose = useCallback(() => {
    setContextMenu(null);
  }, [setContextMenu]);

  // Handler for deleting an edge
  const handleDeleteEdge = useCallback((edgeId: string) => {
    deleteEdge(edgeId);
  }, [deleteEdge]);

  return {
    nodes,
    setNodes,
    edges,
    setEdges,
    onNodesChange,
    onEdgesChange,
    onConnect,
    handleClearCanvas: clearCanvas,
    handleEdgeContextMenu,
    handleContextMenuClose,
    handleDeleteEdge,
    selectedEdges,
    setSelectedEdges,
    contextMenu,
    flowConfig,
    setFlowConfig,
    manuallyPositionedNodes: Array.from(manuallyPositionedNodes),
    setManuallyPositionedNodes: (nodeIds: string[]) => setManuallyPositionedNodes(nodeIds)
  };
}; 