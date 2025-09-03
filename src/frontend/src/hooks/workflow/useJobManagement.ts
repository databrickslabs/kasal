import { useState, useCallback } from 'react';
import { useCrewExecutionStore } from '../../store/crewExecution';
import { useRunStatus } from '../global/useExecutionStatus';
import { RunService } from '../../api/ExecutionHistoryService';

interface UseJobManagementProps {
  onJobStatusChanged?: (jobId: string, status: string) => void;
}

export const useJobManagement = ({ onJobStatusChanged }: UseJobManagementProps = {}) => {
  const [jobId, setJobId] = useState<string | null>(null);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(false);

  const {
    setCurrentTaskId,
    setCompletedTaskIds,
    setRunHistory,
    setUserActive,
    cleanup: cleanupStore
  } = useCrewExecutionStore();

  const { startTracking, stopTracking } = useRunStatus(jobId || '');

  const handleJobStatusChanged = useCallback((jobId: string, status: string) => {
    if (onJobStatusChanged) {
      onJobStatusChanged(jobId, status);
    }
  }, [onJobStatusChanged]);

  const executeJob = useCallback(async (agentsYaml: string, tasksYaml: string) => {
    setIsLoading(true);
    setErrorMessage(null);
    try {
      const response = await RunService.getInstance().executeJob(agentsYaml, tasksYaml);
      if (response?.job_id) {
        setJobId(response.job_id);
        startTracking();
      }
    } catch (err) {
      setErrorMessage(err instanceof Error ? err.message : 'Failed to execute job');
    } finally {
      setIsLoading(false);
    }
  }, [startTracking]);

  const stopJob = useCallback(() => {
    if (jobId) {
      stopTracking();
      setJobId(null);
    }
  }, [jobId, stopTracking]);

  const cleanup = useCallback(() => {
    stopJob();
    cleanupStore();
  }, [stopJob, cleanupStore]);

  return {
    jobId,
    errorMessage,
    isLoading,
    executeJob,
    stopJob,
    cleanup,
    setCurrentTaskId,
    setCompletedTaskIds,
    setRunHistory,
    setUserActive
  };
}; 