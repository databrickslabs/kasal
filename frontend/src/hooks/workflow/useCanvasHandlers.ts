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
    onEdgesChange(changes);
  }, [onEdgesChange]);

  const handleNodesChange = useCallback((changes: NodeChange[]) => {
    console.log("Node changes:", changes);
    onNodesChange(changes);
  }, [onNodesChange]);

  const handleClear = useCallback(() => {
    console.log("Clearing all nodes and edges");
    // Remove all nodes
    if (nodes.length > 0) {
      onNodesChange(nodes.map(node => ({ type: 'remove' as const, id: node.id })));
    }
    
    // Remove all edges by creating remove changes for each edge
    if (edges.length > 0) {
      onEdgesChange(edges.map(edge => ({ type: 'remove' as const, id: edge.id })));
    }
  }, [nodes, edges, onNodesChange, onEdgesChange]);

  return {
    handleEdgesChange,
    handleNodesChange,
    handleClear
  };
}; 