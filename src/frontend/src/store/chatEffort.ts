import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import { DEFAULT_EFFORT, EffortSettings } from '../types/workflow/effort';

/** Chat preference survives refreshes; each submitted run carries a snapshot. */
export const useChatEffortStore = create<{
  settings: EffortSettings;
  setSettings: (settings: EffortSettings) => void;
}>()(persist(set => ({
  settings: DEFAULT_EFFORT,
  setSettings: settings => set({ settings }),
}), { name: 'kasal-chat-effort' }));
