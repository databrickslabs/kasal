import { useState, useCallback, useEffect, useMemo } from 'react';
import type { Run } from '../../api/ExecutionHistoryService';
import { useRunStatusStore } from '../../store/runStatus';
import { toast } from 'react-hot-toast';
import { useTranslation } from 'react-i18next';
import { runService } from '../../api/ExecutionHistoryService';
import { logger } from '../../utils/logger';

// Create a specialized logger for this module
const historyLogger = logger.createChild('ExecutionHistory');

type SortField = 'status' | 'duration' | 'created_at';

export const useRunHistory = () => {
  const { t } = useTranslation();
  const [searchQuery, setSearchQuery] = useState('');
  const [page, setPage] = useState(1);
  const [sortField, setSortField] = useState<SortField>('created_at');
  const [sortOrder, setSortOrder] = useState<'asc' | 'desc'>('desc');
  const jobsPerPage = 200;

  const {
    runHistory,
    isLoading,
    error,
    fetchRunHistory,
    setError
  } = useRunStatusStore();

  // Helper function to calculate duration for sorting
  const calculateSortDuration = (run: Run): number => {
    if (!run?.created_at) return 0;
    
    // Use completed_at, updated_at, or current time depending on status
    let endTime;
    if (run.status === 'running' || run.status === 'queued' || run.status === 'pending') {
      endTime = new Date(); // For active jobs, use current time
    } else {
      // For completed/failed jobs, use completed_at or updated_at
      endTime = run.completed_at ? new Date(run.completed_at) : 
                run.updated_at ? new Date(run.updated_at) : new Date();
    }
    
    const startTime = new Date(run.created_at);
    return endTime.getTime() - startTime.getTime();
  };

  // Memoize fetchRuns to prevent unnecessary re-renders
  const fetchRuns = useCallback(async () => {
    try {
      historyLogger.debug('fetchRuns called, updating via store...');
      // Use the store's built-in fetchRunHistory method
      await fetchRunHistory();
      
      // Get the latest state from the store after fetching
      const storeState = useRunStatusStore.getState();
      
      // Log the result
      historyLogger.debug(`fetchRunHistory completed with ${storeState.runHistory.length} items`);
      
      // Return a properly structured response using the store's data
      return {
        runs: storeState.runHistory,
        total: storeState.runHistory.length,
        limit: 50,
        offset: 0
      };
    } catch (err) {
      historyLogger.error('Error in fetchRuns:', err);
      toast.error(t('runHistory.fetchRunsError'));
      
      // Even on error, return the current state
      const currentState = useRunStatusStore.getState();
      return {
        runs: currentState.runHistory || [],
        total: currentState.runHistory?.length || 0,
        limit: 50,
        offset: 0
      };
    }
  }, [fetchRunHistory, t]);

  const handlePageChange = (_event: React.ChangeEvent<unknown>, value: number) => {
    setPage(value);
  };

  const handleSearchChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setSearchQuery(event.target.value);
    setPage(1); // Reset to first page when searching
  };

  const handleDeleteRun = async (runId: string) => {
    try {
      historyLogger.info(`Deleting run with ID: ${runId}`);
      const result = await runService.deleteRun(runId);
      historyLogger.debug('Delete result:', result);
      
      // Immediately remove the deleted run from the UI
      const currentState = useRunStatusStore.getState();
      
      // Remove this specific run from both runHistory and activeRuns
      useRunStatusStore.setState({
        ...currentState,
        runHistory: currentState.runHistory.filter(run => run.job_id !== runId),
        activeRuns: Object.fromEntries(
          Object.entries(currentState.activeRuns)
            .filter(([id]) => id !== runId)
        )
      });
      
      // Then fetch from scratch to ensure we have the latest data
      await fetchRunHistory();
      
      toast.success(t('runHistory.deleteRunSuccess'));
    } catch (err) {
      historyLogger.error('Error deleting run:', err);
      toast.error(t('runHistory.deleteRunError'));
      setError('Failed to delete run');
    }
  };

  const handleDeleteAllRuns = async () => {
    try {
      historyLogger.info('Deleting all runs');
      const result = await runService.deleteAllRuns();
      historyLogger.debug('Delete all result:', result);
      
      // Immediately clear all runs from the UI
      const currentState = useRunStatusStore.getState();
      
      // Clear both runHistory and activeRuns
      useRunStatusStore.setState({
        ...currentState,
        runHistory: [],
        activeRuns: {}
      });
      
      // Then fetch from scratch to ensure we have the latest data
      await fetchRunHistory();
      
      toast.success(t('runHistory.deleteAllSuccess'));
    } catch (err) {
      historyLogger.error('Error deleting all runs:', err);
      toast.error(t('runHistory.deleteAllError'));
      setError('Failed to delete all runs');
    }
  };

  const handleSort = (field: SortField) => {
    if (sortField === field) {
      setSortOrder(sortOrder === 'asc' ? 'desc' : 'asc');
    } else {
      setSortField(field);
      setSortOrder('asc');
    }
  };

  // Filter the runs based on the search query
  const filteredRuns = useMemo(() => {
    // Ensure runHistory is an array
    const safeRunHistory = Array.isArray(runHistory) ? runHistory : [];
    
    return searchQuery
      ? safeRunHistory.filter((run) => {
          // Convert search query and run name to lowercase for case-insensitive search
          const query = searchQuery.toLowerCase();
          const runName = run?.run_name?.toLowerCase() || '';
          return runName.includes(query);
        })
      : safeRunHistory;
  }, [runHistory, searchQuery]);

  const sortedRuns = useMemo(() => {
    // Make sure filteredRuns is an array
    if (!Array.isArray(filteredRuns)) return [];
    
    return [...filteredRuns].sort((a, b) => {
      const multiplier = sortOrder === 'asc' ? 1 : -1;
      
      if (sortField === 'status') {
        return multiplier * ((a?.status || '').localeCompare(b?.status || ''));
      } else if (sortField === 'duration') {
        // Calculate durations properly
        const aDuration = calculateSortDuration(a);
        const bDuration = calculateSortDuration(b);
        return multiplier * (aDuration - bDuration);
      } else {
        // Default sort by created_at
        const aDate = a?.created_at ? new Date(a.created_at).getTime() : 0;
        const bDate = b?.created_at ? new Date(b.created_at).getTime() : 0;
        return multiplier * (aDate - bDate);
      }
    });
  }, [filteredRuns, sortField, sortOrder]);

  const totalRuns = sortedRuns.length;
  const totalPages = Math.ceil(totalRuns / jobsPerPage);

  const getCurrentPageJobs = useCallback(() => {
    const startIndex = (page - 1) * jobsPerPage;
    return sortedRuns.slice(startIndex, startIndex + jobsPerPage);
  }, [page, sortedRuns]);

  // Auto-refresh on an interval
  useEffect(() => {
    // Check if there are any running jobs
    const hasRunningJobs = runHistory.some(
      run => run.status === 'running' || run.status === 'queued' || run.status === 'pending'
    );
    
    // Only set an interval for refresh if we have running jobs
    if (hasRunningJobs) {
      const refreshInterval = 10000; // 10 seconds for running jobs
      
      const intervalId = setInterval(() => {
        historyLogger.debug('Refreshing data, running jobs detected');
        fetchRuns().catch(err => 
          historyLogger.error('Error refreshing in interval:', err)
        );
      }, refreshInterval);
      
      // Clean up interval on unmount
      return () => clearInterval(intervalId);
    } else {
      // If there are no running jobs, do a single refresh after 30 seconds
      // This ensures we get updates but don't spam with logs
      const timeoutId = setTimeout(() => {
        fetchRuns().catch(err => 
          historyLogger.error('Error in delayed refresh:', err)
        );
      }, 30000);
      
      return () => clearTimeout(timeoutId);
    }
  }, [fetchRuns, runHistory]);

  // Initial fetch
  useEffect(() => {
    fetchRuns();
  }, [fetchRuns]);

  return {
    runs: sortedRuns,
    searchQuery,
    loading: isLoading,
    error,
    page,
    totalPages,
    totalRuns,
    jobsPerPage,
    sortField,
    sortOrder,
    fetchRuns,
    handlePageChange,
    handleSearchChange,
    handleDeleteAllRuns,
    handleDeleteRun,
    getCurrentPageJobs,
    handleSort,
    refresh: fetchRunHistory,
    setError
  };
}; 