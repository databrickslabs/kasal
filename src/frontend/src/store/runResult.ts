import { create } from 'zustand';
import { Run } from '../api/ExecutionHistoryService';

interface RunResultState {
  selectedRun: Run | null;
  isOpen: boolean;
  showRunResult: (run: Run) => void;
  closeRunResult: () => void;
  setSelectedRun: (run: Run | null) => void;
}

export const useRunResultStore = create<RunResultState>((set) => ({
  selectedRun: null,
  isOpen: false,
  showRunResult: (run: Run) => set({ selectedRun: run, isOpen: true }),
  closeRunResult: () => set({ selectedRun: null, isOpen: false }),
  setSelectedRun: (run: Run | null) => set({ selectedRun: run }),
})); 