import { useCallback } from 'react';
import { Node, Edge, NodeChange, EdgeChange } from 'reactflow';
import { FlowConfiguration } from '../../types/flow';

interface UseCrewFlowHandlersProps {
  onNodesChange: (changes: NodeChange[]) => void;
  onEdgesChange: (changes: EdgeChange[]) => void;
  setSuccessMessage: (message: string) => void;
  setShowSuccess: (show: boolean) => void;
}

export const useCrewFlowHandlers = ({
  onNodesChange,
  onEdgesChange,
  setSuccessMessage,
  setShowSuccess
}: UseCrewFlowHandlersProps) => {
  const handleCrewSelect = useCallback((nodes: Node[], edges: Edge[]) => {
    console.log("Loading crew with", nodes.length, "nodes and", edges.length, "edges");
    
    onNodesChange(nodes.map((node: Node) => ({ type: 'add' as const, item: node })));
    onEdgesChange(edges.map((edge: Edge) => ({ type: 'add' as const, item: edge })));
    
    setSuccessMessage('Crew loaded successfully');
    setShowSuccess(true);
  }, [onNodesChange, onEdgesChange, setSuccessMessage, setShowSuccess]);

  const handleFlowSelect = useCallback((nodes: Node[], edges: Edge[], flowConfig?: FlowConfiguration) => {
    onNodesChange(nodes.map((node: Node) => ({ type: 'add' as const, item: node })));
    onEdgesChange(edges.map((edge: Edge) => ({ type: 'add' as const, item: edge })));
    
    setSuccessMessage('Flow loaded successfully');
    setShowSuccess(true);
  }, [onNodesChange, onEdgesChange, setSuccessMessage, setShowSuccess]);

  return {
    handleCrewSelect,
    handleFlowSelect
  };
}; 