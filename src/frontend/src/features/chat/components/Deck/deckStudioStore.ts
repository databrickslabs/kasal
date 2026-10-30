import { useCallback } from 'react';
import { useStore } from 'zustand';
import { createStore } from 'zustand/vanilla';
import type { TraceEntryData } from '../Chat/ChatMessage';
import type { RunStep } from '../Preview/traceEventStep';

interface SlideActivity { startedAt: number; jobId?: string; step: TraceEntryData }
interface DeckStudioState {
  deck: string;
  source: string;
  history: { label: string; prev: string }[];
  saving: boolean;
  working: ReadonlySet<number>;
  error: string | null;
  errors: Record<number, string | null>;
  activities: Record<number, SlideActivity>;
  activitySteps: Record<number, RunStep | null>;
  // Shared between mounted editors and requests that finish while the editor is closed.
  current: { current: string };
  pending: { current: Promise<void> | null };
  active: { current: Set<number> };
}

/** Owned by one deck card, not the modal. No global registry or browser storage. */
export const createDeckStudioStore = (code: string) => createStore<DeckStudioState>(() => ({
  deck: code, source: code, history: [], saving: false,
  working: new Set(), error: null, errors: {}, activities: {}, activitySteps: {},
  current: { current: code }, pending: { current: null }, active: { current: new Set() },
}));

export type DeckStudioStore = ReturnType<typeof createDeckStudioStore>;

/** Functional updates read the live store, including callbacks from a closed editor. */
export function useDeckState<K extends keyof DeckStudioState>(store: DeckStudioStore, key: K) {
  const value = useStore(store, state => state[key]);
  const setValue = useCallback((update: DeckStudioState[K] | ((previous: DeckStudioState[K]) => DeckStudioState[K])) => {
    store.setState(state => ({
      ...state,
      [key]: typeof update === 'function' ? update(state[key]) : update,
    }));
  }, [store, key]);
  return [value, setValue] as const;
}
