import { useShortcutsStore as useStore } from '../../store/shortcuts';

/**
 * Hook for accessing the shortcuts state and actions
 * Provides the same API as the previous ShortcutsContext
 */
export const useShortcutsStore = () => {
  const {
    shortcuts,
    showShortcuts,
    setShortcuts,
    toggleShortcuts,
    setShortcutsVisible
  } = useStore();

  return {
    shortcuts,
    showShortcuts,
    setShortcuts,
    toggleShortcuts,
    setShortcutsVisible
  };
};