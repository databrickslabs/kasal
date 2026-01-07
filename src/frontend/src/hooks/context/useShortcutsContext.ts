import { useShortcutsStore } from '../../store/shortcuts';
import { ShortcutsContextType } from '../../types/shortcuts';

export const useShortcutsContext = (): ShortcutsContextType => {
  const context = useShortcutsStore();
  
  // Add debugging on development environment
  if (import.meta.env.DEV) {
    console.debug('ShortcutsContext is being used (via Zustand store)', { 
      showShortcuts: context.showShortcuts,
      shortcutsCount: context.shortcuts.length
    });
  }
  
  return context;
}; 