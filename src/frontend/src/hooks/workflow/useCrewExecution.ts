import { useCallback } from 'react';
import { Node, Edge } from 'reactflow';
import { useCrewExecutionStore } from '../../store/crewExecution';
import { useErrorStore } from '../../store/error';
import { useRunStatusStore } from '../../store/runStatus';

interface CrewExecutionResponse {
  job_id: string;
}

interface UseCrewExecutionResult {
  handleExecuteCrew: (nodes: Node[], edges: Edge[]) => Promise<CrewExecutionResponse | undefined>;
  isExecuting: boolean;
}

export const useCrewExecution = (): UseCrewExecutionResult => {
  const errorStore = useErrorStore();
  const runStatusStore = useRunStatusStore();
  
  const { 
    isExecuting,
    setJobId,
    setIsExecuting,
    executeCrew
  } = useCrewExecutionStore();

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
        
        runStatusStore.startPolling();
        
        return response;
      }
      return undefined;
    } catch (error) {
      console.error('Error executing crew:', error);
      errorStore.showErrorMessage('Failed to execute crew workflow');
      return undefined;
    } finally {
      if (typeof setIsExecuting === 'function') {
        setIsExecuting(false);
      }
    }
  }, [setIsExecuting, executeCrew, setJobId, runStatusStore, errorStore]);

  return {
    handleExecuteCrew,
    isExecuting
  };
}; 