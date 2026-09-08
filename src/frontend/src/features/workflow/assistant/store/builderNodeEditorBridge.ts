import { create } from 'zustand';

export interface BuilderNodeEditorEntry {
  id: `agent:${string}` | `task:${string}` | `connection:${string}`;
  label: string;
  host: HTMLElement;
  onClose: () => void;
}

/** Node editors retain their ReactFlow context via portals. The active builder
 * supplies only a destination; none of the form or save state moves here. */
export const useBuilderNodeEditorBridge = create<{
  open: ((editor: BuilderNodeEditorEntry) => void) | null;
  release: ((id: BuilderNodeEditorEntry['id']) => void) | null;
}>(() => ({ open: null, release: null }));
