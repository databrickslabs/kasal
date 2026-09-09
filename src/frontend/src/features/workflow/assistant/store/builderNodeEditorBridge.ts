import { create } from 'zustand';

export interface BuilderNodeEditorEntry {
  id: `agent:${string}` | `task:${string}` | `connection:${string}` | `catalog:${string}`;
  label: string;
  host: HTMLElement;
  onClose: () => void;
}

export const isCanvasEditorId = (id: string) =>
  id.startsWith('agent:') || id.startsWith('task:') || id.startsWith('connection:');

/** Node editors retain their ReactFlow context via portals. The active builder
 * supplies only a destination; none of the form or save state moves here. */
export const useBuilderNodeEditorBridge = create<{
  open: ((editor: BuilderNodeEditorEntry) => void) | null;
  release: ((id: BuilderNodeEditorEntry['id']) => void) | null;
}>(() => ({ open: null, release: null }));
