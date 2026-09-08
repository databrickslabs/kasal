import { createContext } from 'react';
import type { RunStep } from '../../../chat/components/Preview/traceEventStep';
import type { PreviewContent } from '../../../chat/types/preview';

/** Opens a run's memory beside the builder conversation, across its portals. */
export const BuilderPreviewContext = createContext<{
  openSchedule?: (executionId: string, defaultName: string, onCreated: (name: string) => void) => void;
  openOptimize?: (crewId: string, crewName: string) => void;
  openMemory: (jobId: string) => void;
  openStep: (jobId: string, step: RunStep) => void;
  openResult?: (content: PreviewContent) => void;
  previewMessageId?: string;
  closePreview?: () => void;
} | null>(null);
