import { useCallback } from 'react';
import { useTabManagerStore } from '../../store/tabManager';

/**
 * Hook to manage tab dirty state when nodes are modified
 */
export const useTabDirtyState = () => {
  const { getActiveTab, markTabDirty } = useTabManagerStore();

  /**
   * Mark the currently active tab as dirty
   */
  const markCurrentTabDirty = useCallback(() => {
    const activeTab = getActiveTab();
    if (activeTab) {
      console.log('Marking tab as dirty:', activeTab.id, activeTab.name);
      markTabDirty(activeTab.id);
      
      // Dispatch a custom event to notify other components
      window.dispatchEvent(new CustomEvent('tabMarkedDirty', {
        detail: { tabId: activeTab.id, tabName: activeTab.name }
      }));
    }
  }, [getActiveTab, markTabDirty]);

  /**
   * Mark a specific tab as dirty
   */
  const markTabDirtyById = useCallback((tabId: string) => {
    console.log('Marking specific tab as dirty:', tabId);
    markTabDirty(tabId);
    
    // Dispatch a custom event to notify other components
    window.dispatchEvent(new CustomEvent('tabMarkedDirty', {
      detail: { tabId }
    }));
  }, [markTabDirty]);

  return {
    markCurrentTabDirty,
    markTabDirtyById
  };
};