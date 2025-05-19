import { create } from 'zustand';
import { DEFAULT_SHORTCUTS } from '../hooks/global/useShortcuts';
import { ShortcutConfig } from '../types/shortcuts';

interface ShortcutsState {
  // State
  shortcuts: ShortcutConfig[];
  showShortcuts: boolean;
  
  // Actions
  setShortcuts: (shortcuts: ShortcutConfig[]) => void;
  toggleShortcuts: () => void;
  setShortcutsVisible: (visible: boolean) => void;
}

export const useShortcutsStore = create<ShortcutsState>((set) => ({
  // Initial state
  shortcuts: DEFAULT_SHORTCUTS,
  showShortcuts: false,
  
  // Actions
  setShortcuts: (shortcuts) => set({ shortcuts }),
  toggleShortcuts: () => set((state) => ({ showShortcuts: !state.showShortcuts })),
  setShortcutsVisible: (visible) => set({ showShortcuts: visible })
})); 