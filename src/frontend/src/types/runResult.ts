import { Run } from '../api/ExecutionHistoryService';

export interface RunResultContextType {
  selectedRun: Run | null;
  setSelectedRun: (run: Run | null) => void;
  showRunResult: (run?: Run) => void;
  closeRunResult: () => void;
  isOpen: boolean;
} 