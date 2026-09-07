import { create } from 'zustand';
import { persist } from 'zustand/middleware';

interface Preference { pinned?: boolean; archived?: boolean }
interface Preferences {
  entries: Record<string, Preference>;
  update: (key: string, patch: Preference) => void;
  remove: (key: string) => void;
}

/** Presentation preferences don't delete the underlying canvas or conversation. */
export const useSessionPreferences = create<Preferences>()(persist((set) => ({
  entries: {},
  update: (key, patch) => set(state => ({ entries: {
    ...state.entries, [key]: { ...state.entries[key], ...patch },
  } })),
  remove: key => set(state => {
    const entries = { ...state.entries };
    delete entries[key];
    return { entries };
  }),
}), { name: 'kasal-session-preferences' }));
