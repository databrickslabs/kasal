import { useCallback } from 'react';
import { Node, Edge, Connection, EdgeChange, NodeChange, NodePositionChange } from 'reactflow';
import { useTranslation } from 'react-i18next';
import { useWorkflowStore } from '../../store/workflow';

interface UseWorkflowReduxProps {
  showErrorMessage: (message: string) => void;
}

export const useWorkflowRedux = ({ showErrorMessage }: UseWorkflowReduxProps) => {
  const { t } = useTranslation();
  const {
    nodes,
    edges,
    selectedEdges,
    contextMenu,
    flowConfig,
    draggedNodeIds,
    manuallyPositionedNodes,
    setNodes,
    setEdges,
    setSelectedEdges,
    setContextMenu,
    setFlowConfig,
    setDraggedNodeIds,
    setManuallyPositionedNodes,
    clearCanvas,
    deleteEdge,
    addEdge,
    updateNodePosition
  } = useWorkflowStore();

  // Optimized handler for node changes
  const onNodesChange = useCallback((changes: NodeChange[]) => {
    const positionChanges = changes.filter(
      (change): change is NodePositionChange => change.type === 'position'
    );
    
    const otherChanges = changes.filter(
      change => change.type !== 'position' || !('position' in change)
    );
    
    if (positionChanges.length > 0) {
      const newDraggedNodeIds = new Set<string>(draggedNodeIds);
      const newManuallyPositionedNodes = new Set<string>(manuallyPositionedNodes);
      
      positionChanges.forEach(change => {
        if (change.dragging) {
          newDraggedNodeIds.add(change.id);
          newManuallyPositionedNodes.add(change.id);
        } else if (newDraggedNodeIds.has(change.id)) {
          newDraggedNodeIds.delete(change.id);
        }
        
        if (change.position && 
            isFinite(change.position.x) && 
            isFinite(change.position.y)) {
          updateNodePosition({
            nodeId: change.id,
            position: change.position
          });
        }
      });
      
      setDraggedNodeIds(Array.from(newDraggedNodeIds) as string[]);
      setManuallyPositionedNodes(Array.from(newManuallyPositionedNodes) as string[]);
    }
    
    if (otherChanges.length > 0) {
      const updatedNodes = otherChanges.reduce((acc: Node[], change) => {
        if (change.type === 'remove') {
          return acc.filter((node: Node) => node.id !== change.id);
        }
        // Handle other change types as needed
        return acc;
      }, nodes);
      
      setNodes(updatedNodes);
    }
  }, [nodes, draggedNodeIds, manuallyPositionedNodes, setDraggedNodeIds, setManuallyPositionedNodes, updateNodePosition, setNodes]);

  // Handler for edge changes
  const onEdgesChange = useCallback((changes: EdgeChange[]) => {
    const updatedEdges = changes.reduce((acc: Edge[], change) => {
      if (change.type === 'remove') {
        return acc.filter((edge: Edge) => edge.id !== change.id);
      }
      // Handle other change types as needed
      return acc;
    }, edges);
    
    setEdges(updatedEdges);
  }, [edges, setEdges]);

  // Handler for connecting nodes
  const onConnect = useCallback((params: Connection) => {
    if (params.source && params.target) {
      const sourceNode = nodes.find((node: Node) => node.id === params.source);
      const targetNode = nodes.find((node: Node) => node.id === params.target);

      if (!sourceNode || !targetNode) {
        return;
      }

      if (sourceNode.type === 'agentNode' && targetNode.type === 'agentNode') {
        showErrorMessage(t('nemo.errors.agentConnection'));
        return;
      }

      addEdge(params);
    }
  }, [nodes, addEdge, t, showErrorMessage]);

  // Handler for clearing the canvas
  const handleClearCanvas = useCallback(() => {
    clearCanvas();
  }, [clearCanvas]);

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
    handleClearCanvas,
    handleEdgeContextMenu,
    selectedEdges,
    setSelectedEdges,
    contextMenu,
    handleContextMenuClose,
    handleDeleteEdge,
    flowConfig,
    setFlowConfig,
    manuallyPositionedNodes,
    setManuallyPositionedNodes
  };
}; 