import { createContext } from 'react';
import type { RunStep } from '../../../chat/components/Preview/traceEventStep';

/** Opens a run's memory beside the builder conversation, across its portals. */
export const BuilderPreviewContext = createContext<{
  openMemory: (jobId: string) => void;
  openStep: (jobId: string, step: RunStep) => void;
} | null>(null);
