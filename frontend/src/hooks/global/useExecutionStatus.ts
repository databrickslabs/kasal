import { useState, useEffect, useCallback } from 'react';
import { runService, Run } from '../../api/ExecutionHistoryService';
import { useRunStatusStore } from '../../store/runStatus';

export const useRunStatus = (jobId: string) => {
  const [isTracking, setIsTracking] = useState(false);
  const { updateRunStatus, removeActiveRun } = useRunStatusStore();

  const startTracking = useCallback(async () => {
    if (isTracking) return;
    setIsTracking(true);

    try {
      const status = await runService.getJobStatus(jobId);
      updateRunStatus(jobId, status.status as Run['status'], status.error);

      if (['completed', 'failed'].includes(status.status)) {
        stopTracking();
      }
    } catch (error) {
      console.error('Error tracking job status:', error);
      stopTracking();
    }
  }, [jobId, isTracking, updateRunStatus]);

  const stopTracking = useCallback(() => {
    setIsTracking(false);
    removeActiveRun(jobId);
  }, [jobId, removeActiveRun]);

  useEffect(() => {
    if (isTracking) {
      const interval = setInterval(startTracking, 5000);
      return () => clearInterval(interval);
    }
  }, [isTracking, startTracking]);

  return {
    startTracking,
    stopTracking,
    isTracking,
  };
}; 