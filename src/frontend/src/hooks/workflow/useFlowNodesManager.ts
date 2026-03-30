import { useCallback, useEffect, useState } from 'react';
import { Node, NodeChange, applyNodeChanges, useKeyPress } from 'reactflow';
import { FlowNodeManagerProps } from '../../types/crew';

export const useFlowNodesManager = ({ nodes, setNodes }: FlowNodeManagerProps) => {
  const [selectedNodes, setSelectedNodes] = useState<Node[]>([]);
  const deleteKeyPressed = useKeyPress('Delete');

  const onNodesChange = useCallback((changes: NodeChange[]) => {
    setNodes((nds: Node[]) => applyNodeChanges(changes, nds));
  }, [setNodes]);

  const onSelectionChange = useCallback((params: { nodes: Node[] }) => {
    setSelectedNodes(params.nodes);
  }, []);

  useEffect(() => {
    if (deleteKeyPressed && selectedNodes.length > 0) {
      setNodes((nds) => nds.filter((node) => 
        !selectedNodes.some(selectedNode => selectedNode.id === node.id)
      ));
      setSelectedNodes([]);
    }
  }, [deleteKeyPressed, selectedNodes, setNodes]);

  return {
    onNodesChange,
    onSelectionChange,
    selectedNodes
  };
}; 