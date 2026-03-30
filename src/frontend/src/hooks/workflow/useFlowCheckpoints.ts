import { useState, useCallback, useEffect } from 'react';
import { FlowService, FlowCheckpoint, FlowCheckpointListResponse } from '../../api/FlowService';

interface UseFlowCheckpointsProps {
  flowId: string | null;
  autoFetch?: boolean;
}

interface UseFlowCheckpointsReturn {
  checkpoints: FlowCheckpoint[];
  loading: boolean;
  error: string | null;
  hasCheckpoints: boolean;
  fetchCheckpoints: () => Promise<FlowCheckpoint[]>;
  deleteCheckpoint: (executionId: number) => Promise<boolean>;
  clearError: () => void;
}

/**
 * Hook for managing flow checkpoints (resume points from previous executions).
 *
 * @param flowId - The flow ID to fetch checkpoints for
 * @param autoFetch - Whether to automatically fetch checkpoints when flowId changes (default: true)
 * @returns Object with checkpoints data and management functions
 */
export const useFlowCheckpoints = ({
  flowId,
  autoFetch = true
}: UseFlowCheckpointsProps): UseFlowCheckpointsReturn => {
  const [checkpoints, setCheckpoints] = useState<FlowCheckpoint[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  /**
   * Fetch available checkpoints for the flow.
   * Only fetches 'active' checkpoints (not expired or already resumed).
   */
  const fetchCheckpoints = useCallback(async (): Promise<FlowCheckpoint[]> => {
    if (!flowId) {
      setCheckpoints([]);
      return [];
    }

    setLoading(true);
    setError(null);

    try {
      const response: FlowCheckpointListResponse = await FlowService.getFlowCheckpoints(flowId, 'active');
      const fetchedCheckpoints = response.checkpoints || [];
      setCheckpoints(fetchedCheckpoints);
      return fetchedCheckpoints;
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : 'Failed to fetch checkpoints';
      setError(errorMessage);
      setCheckpoints([]);
      return [];
    } finally {
      setLoading(false);
    }
  }, [flowId]);

  /**
   * Delete/expire a checkpoint so it won't appear in resume options.
   *
   * @param executionId - The execution ID of the checkpoint to delete
   * @returns True if deletion was successful
   */
  const deleteCheckpoint = useCallback(async (executionId: number): Promise<boolean> => {
    if (!flowId) {
      return false;
    }

    try {
      const success = await FlowService.deleteFlowCheckpoint(flowId, executionId);
      if (success) {
        // Remove the deleted checkpoint from the local state
        setCheckpoints(prev => prev.filter(cp => cp.execution_id !== executionId));
      }
      return success;
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : 'Failed to delete checkpoint';
      setError(errorMessage);
      return false;
    }
  }, [flowId]);

  /**
   * Clear any error state.
   */
  const clearError = useCallback(() => {
    setError(null);
  }, []);

  // Auto-fetch checkpoints when flowId changes
  useEffect(() => {
    if (autoFetch && flowId) {
      fetchCheckpoints();
    }
  }, [flowId, autoFetch, fetchCheckpoints]);

  return {
    checkpoints,
    loading,
    error,
    hasCheckpoints: checkpoints.length > 0,
    fetchCheckpoints,
    deleteCheckpoint,
    clearError
  };
};

export default useFlowCheckpoints;
