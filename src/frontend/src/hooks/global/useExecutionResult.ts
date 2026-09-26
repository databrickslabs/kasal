import { useShallow } from 'zustand/react/shallow';
import { useCallback } from 'react';
import { Run } from '../../api/execution/ExecutionHistoryService';
import { useRunResultStore } from '../../store/runResult';

export const useRunResult = () => {
  const { selectedRun, isOpen, showRunResult: show, closeRunResult: close, setSelectedRun: set } = useRunResultStore(useShallow(state => ({
    selectedRun: state.selectedRun,
    isOpen: state.isOpen,
    showRunResult: state.showRunResult,
    closeRunResult: state.closeRunResult,
    setSelectedRun: state.setSelectedRun,
  })));

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