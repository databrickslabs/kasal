import { useCallback } from 'react';
import { Node, NodeChange, EdgeChange, Edge } from 'reactflow';

interface UseCanvasHandlersProps {
  nodes: Node[];
  edges?: Edge[];
  onNodesChange: (changes: NodeChange[]) => void;
  onEdgesChange: (changes: EdgeChange[]) => void;
}

export const useCanvasHandlers = ({
  nodes,
  edges = [],
  onNodesChange,
  onEdgesChange
}: UseCanvasHandlersProps) => {
  const handleEdgesChange = useCallback((changes: EdgeChange[]) => {
    console.log("Edge changes:", changes);
    
    // Filter out duplicate edge additions
    const processedChanges = changes.filter((change, index) => {
      if (change.type === 'add') {
        // Check if this edge is already being added in an earlier change
        const isDuplicate = changes.slice(0, index).some(
          prevChange => prevChange.type === 'add' && 
          prevChange.item?.id === change.item?.id
        );
        
        if (isDuplicate) {
          console.log(`Filtering out duplicate edge addition: ${change.item?.id}`);
          return false;
        }
      }
      return true;
    });
    
    onEdgesChange(processedChanges);
  }, [onEdgesChange]);

  const handleNodesChange = useCallback((changes: NodeChange[]) => {
    console.log("Node changes:", changes);
    onNodesChange(changes);
  }, [onNodesChange]);

  const handleClear = useCallback(() => {
    console.log("Clearing all nodes and edges");
    
    // Clear both nodes and edges atomically to prevent orphaned connections
    // First clear all edges, then all nodes to ensure proper cleanup
    if (edges.length > 0) {
      console.log(`Removing ${edges.length} edges`);
      onEdgesChange(edges.map(edge => ({ type: 'remove' as const, id: edge.id })));
    }
    
    if (nodes.length > 0) {
      console.log(`Removing ${nodes.length} nodes`);
      onNodesChange(nodes.map(node => ({ type: 'remove' as const, id: node.id })));
    }
    
    // Force a state update to ensure the canvas is completely cleared
    setTimeout(() => {
      // Additional cleanup - this ensures any remaining state inconsistencies are resolved
      onEdgesChange([]);
      onNodesChange([]);
    }, 0);
  }, [nodes, edges, onNodesChange, onEdgesChange]);

  return {
    handleEdgesChange,
    handleNodesChange,
    handleClear
  };
}; 