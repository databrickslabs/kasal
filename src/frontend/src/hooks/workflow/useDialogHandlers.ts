import { useCallback } from 'react';
import { Node, Edge } from 'reactflow';
import { useErrorStore } from '../../store/error';

// Define the response type
interface CrewExecutionResponse {
  job_id: string;
}

interface UseDialogHandlersProps {
  nodes: Node[];
  edges: Edge[];
  setIsLLMSelectionDialogOpen: (open: boolean) => void;
  setIsMaxRPMSelectionDialogOpen: (open: boolean) => void;
  setIsToolDialogOpen: (open: boolean) => void;
  handleExecuteCrew: (nodes: Node[], edges: Edge[]) => Promise<CrewExecutionResponse | undefined>;
  setSuccessMessage: (message: string) => void;
  setShowSuccess: (show: boolean) => void;
}

export const useDialogHandlers = ({
  nodes,
  edges,
  setIsLLMSelectionDialogOpen,
  setIsMaxRPMSelectionDialogOpen,
  setIsToolDialogOpen,
  handleExecuteCrew,
  setSuccessMessage,
  setShowSuccess
}: UseDialogHandlersProps) => {
  const errorStore = useErrorStore();

  const handleChangeLLM = useCallback(() => {
    const agentNodes = nodes.filter(node => node.type === 'agentNode');
    
    if (agentNodes.length === 0) {
      const errorMessage = `This operation requires at least one agent node. Currently have 0 agents.`;
      errorStore.setErrorMessage(errorMessage);
      return;
    }
    
    setIsLLMSelectionDialogOpen(true);
  }, [nodes, errorStore, setIsLLMSelectionDialogOpen]);

  const handleChangeMaxRPM = useCallback(() => {
    const agentNodes = nodes.filter(node => node.type === 'agentNode');
    
    if (agentNodes.length === 0) {
      const errorMessage = `This operation requires at least one agent node. Currently have 0 agents.`;
      errorStore.setErrorMessage(errorMessage);
      return;
    }
    
    setIsMaxRPMSelectionDialogOpen(true);
  }, [nodes, errorStore, setIsMaxRPMSelectionDialogOpen]);

  const handleChangeTools = useCallback(() => {
    const agentNodes = nodes.filter(node => node.type === 'agentNode');
    
    if (agentNodes.length === 0) {
      const errorMessage = `This operation requires at least one agent node. Currently have 0 agents.`;
      errorStore.setErrorMessage(errorMessage);
      return;
    }
    
    setIsToolDialogOpen(true);
  }, [nodes, errorStore, setIsToolDialogOpen]);

  const handleExecuteCrewButtonClick = useCallback(async () => {
    try {
      const currentNodes = nodes.map(node => ({ ...node }));
      const currentEdges = edges.map(edge => ({ ...edge }));
      
      const response = await handleExecuteCrew(currentNodes, currentEdges);
      
      if (response?.job_id) {
        setSuccessMessage(response.job_id);
        setShowSuccess(true);
        
        setTimeout(() => {
          setShowSuccess(false);
        }, 3000);
      }
    } catch (error) {
      console.error('Error executing crew:', error);
      errorStore.showErrorMessage('Failed to execute crew workflow');
    }
  }, [handleExecuteCrew, errorStore, nodes, edges, setSuccessMessage, setShowSuccess]);

  return {
    handleChangeLLM,
    handleChangeMaxRPM,
    handleChangeTools,
    handleExecuteCrewButtonClick
  };
}; 