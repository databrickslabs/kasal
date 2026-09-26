import { useCallback } from 'react';
import { Node, Edge } from 'reactflow';
import { useShallow } from 'zustand/react/shallow';
import { useCrewExecutionStore } from '../../store/crewExecution';
import { useErrorStore } from '../../store/error';

interface CrewExecutionResponse {
  job_id: string;
}

interface UseCrewExecutionResult {
  handleExecuteCrew: (nodes: Node[], edges: Edge[]) => Promise<CrewExecutionResponse | undefined>;
  isExecuting: boolean;
}

export const useCrewExecution = (): UseCrewExecutionResult => {
  const showErrorMessage = useErrorStore(state => state.showErrorMessage);

  const {
    isExecuting,
    setJobId,
    setIsExecuting,
    executeCrew
  } = useCrewExecutionStore(useShallow(state => ({
    isExecuting: state.isExecuting,
    setJobId: state.setJobId,
    setIsExecuting: state.setIsExecuting,
    executeCrew: state.executeCrew,
  })));

  const handleExecuteCrew = useCallback(async (nodes: Node[], edges: Edge[]): Promise<CrewExecutionResponse | undefined> => {
    try {
      if (typeof setIsExecuting === 'function') {
        setIsExecuting(true);
      }
      
      const currentNodes = nodes.map(node => ({ ...node }));
      const currentEdges = edges.map(edge => ({ ...edge }));
      
      const response = await executeCrew(currentNodes, currentEdges);
      
      if (response && response.job_id) {
        if (typeof setJobId === 'function') {
          setJobId(response.job_id);
        }

        // SSE will handle real-time updates automatically
        // No polling needed

        return response;
      }
      return undefined;
    } catch (error) {
      console.error('Error executing crew:', error);
      showErrorMessage('Failed to execute crew workflow');
      return undefined;
    } finally {
      if (typeof setIsExecuting === 'function') {
        setIsExecuting(false);
      }
    }
  }, [setIsExecuting, executeCrew, setJobId, showErrorMessage]);

  return {
    handleExecuteCrew,
    isExecuting
  };
}; 