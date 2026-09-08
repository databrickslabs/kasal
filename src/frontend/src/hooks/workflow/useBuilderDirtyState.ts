import { useCallback } from 'react';
import { useBuilderCanvasStore } from '../../app/sessions/builderCanvasStore';

/**
 * Hook to manage tab dirty state when nodes are modified
 */
export const useBuilderDirtyState = () => {
  const { getActiveCanvas, markCanvasDirty } = useBuilderCanvasStore();

  /**
   * Mark the currently active tab as dirty
   */
  const markCurrentTabDirty = useCallback(() => {
    const activeTab = getActiveCanvas();
    if (activeTab) {
      console.log('Marking tab as dirty:', activeTab.id, activeTab.name);
      markCanvasDirty(activeTab.id);
      
      // Dispatch a custom event to notify other components
      window.dispatchEvent(new CustomEvent('tabMarkedDirty', {
        detail: { tabId: activeTab.id, tabName: activeTab.name }
      }));
    }
  }, [getActiveCanvas, markCanvasDirty]);

  /**
   * Mark a specific tab as dirty
   */
  const markTabDirtyById = useCallback((tabId: string) => {
    console.log('Marking specific tab as dirty:', tabId);
    markCanvasDirty(tabId);
    
    // Dispatch a custom event to notify other components
    window.dispatchEvent(new CustomEvent('tabMarkedDirty', {
      detail: { tabId }
    }));
  }, [markCanvasDirty]);

  return {
    markCurrentTabDirty,
    markTabDirtyById
  };
};