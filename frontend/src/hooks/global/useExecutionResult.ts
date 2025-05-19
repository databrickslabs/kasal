import { useCallback } from 'react';
import { Run } from '../../api/ExecutionHistoryService';
import { useRunResultStore } from '../../store/runResult';

export const useRunResult = () => {
  const { selectedRun, isOpen, showRunResult: show, closeRunResult: close, setSelectedRun: set } = useRunResultStore();

  const handleShowRunResult = useCallback((run?: Run) => {
    if (run) {
      show(run);
    }
  }, [show]);

  const handleCloseRunResult = useCallback(() => {
    close();
  }, [close]);

  const handleSetSelectedRun = useCallback((run: Run | null) => {
    set(run);
  }, [set]);

  return {
    selectedRun,
    isOpen,
    showRunResult: handleShowRunResult,
    closeRunResult: handleCloseRunResult,
    setSelectedRun: handleSetSelectedRun,
  };
}; 