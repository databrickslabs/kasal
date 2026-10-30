import { useShallow } from 'zustand/react/shallow';
import { useCallback } from 'react';
import { Run, runService } from '../../api/execution/ExecutionHistoryService';
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
    if (!run) return;
    show(run);
    // List rows carry no result; open at once, then swap in the full run.
    void runService.withPayload(run).then((full) => {
      if (full !== run && useRunResultStore.getState().selectedRun?.job_id === run.job_id) {
        set(full);
      }
    });
  }, [show, set]);

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