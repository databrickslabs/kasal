import { create } from 'zustand';

export interface BuilderExecutionControl {
  jobId: string;
  stopping: boolean;
  stop: () => Promise<void>;
}

// Both controls share the active conversation's exact run and stop handler.
// This is transient UI state, never persisted across workspaces or reloads.
export const useBuilderExecutionControls = create<{
  crew: BuilderExecutionControl | null;
  flow: BuilderExecutionControl | null;
}>(() => ({ crew: null, flow: null }));
