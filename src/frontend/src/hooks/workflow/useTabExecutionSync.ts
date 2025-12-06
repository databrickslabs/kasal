import { useEffect, useRef, useCallback } from 'react';
import { useTabManagerStore, TabExecutionConfig } from '../../store/tabManager';
import { useCrewExecutionStore } from '../../store/crewExecution';

/**
 * Hook to sync execution configuration (process type, planning, reasoning, etc.)
 * between the global execution store and per-tab storage.
 *
 * This ensures each tab can have its own runtime configuration that persists
 * when switching between tabs.
 */
export const useTabExecutionSync = () => {
  const {
    activeTabId,
    updateTabExecutionConfig,
    getTabExecutionConfig
  } = useTabManagerStore();

  const {
    processType,
    planningEnabled,
    planningLLM,
    reasoningEnabled,
    reasoningLLM,
    managerLLM,
    setProcessType,
    setPlanningEnabled,
    setPlanningLLM,
    setReasoningEnabled,
    setReasoningLLM,
    setManagerLLM,
    isLoadingCrew
  } = useCrewExecutionStore();

  // Track the last active tab to detect tab switches
  const lastActiveTabIdRef = useRef<string | null>(null);
  // Track if we're currently restoring config to prevent save loops
  const isRestoringRef = useRef(false);
  // Track if this is the initial mount
  const isInitialMountRef = useRef(true);

  /**
   * Save current execution config to the active tab
   */
  const saveConfigToTab = useCallback(() => {
    if (!activeTabId || isRestoringRef.current || isLoadingCrew) {
      return;
    }

    const config: TabExecutionConfig = {
      processType,
      planningEnabled,
      planningLLM,
      reasoningEnabled,
      reasoningLLM,
      managerLLM
    };

    console.log('[useTabExecutionSync] Saving config to tab:', activeTabId, config);
    updateTabExecutionConfig(activeTabId, config);
  }, [
    activeTabId,
    processType,
    planningEnabled,
    planningLLM,
    reasoningEnabled,
    reasoningLLM,
    managerLLM,
    updateTabExecutionConfig,
    isLoadingCrew
  ]);

  /**
   * Restore execution config from a tab
   */
  const restoreConfigFromTab = useCallback((tabId: string) => {
    const config = getTabExecutionConfig(tabId);

    if (!config) {
      console.log('[useTabExecutionSync] No config found for tab:', tabId, '- using current values');
      return;
    }

    console.log('[useTabExecutionSync] Restoring config from tab:', tabId, config);

    isRestoringRef.current = true;

    // Restore all config values
    if (config.processType !== undefined) {
      setProcessType(config.processType);
    }
    if (config.planningEnabled !== undefined) {
      setPlanningEnabled(config.planningEnabled);
    }
    if (config.planningLLM !== undefined) {
      setPlanningLLM(config.planningLLM);
    }
    if (config.reasoningEnabled !== undefined) {
      setReasoningEnabled(config.reasoningEnabled);
    }
    if (config.reasoningLLM !== undefined) {
      setReasoningLLM(config.reasoningLLM);
    }
    if (config.managerLLM !== undefined) {
      setManagerLLM(config.managerLLM);
    }

    // Reset the restoring flag after a short delay to allow state updates to settle
    setTimeout(() => {
      isRestoringRef.current = false;
    }, 100);
  }, [
    getTabExecutionConfig,
    setProcessType,
    setPlanningEnabled,
    setPlanningLLM,
    setReasoningEnabled,
    setReasoningLLM,
    setManagerLLM
  ]);

  /**
   * Handle tab switches - save current config and restore new tab's config
   */
  useEffect(() => {
    // Skip during crew loading to avoid interfering with crew config restoration
    if (isLoadingCrew) {
      return;
    }

    if (activeTabId !== lastActiveTabIdRef.current) {
      console.log('[useTabExecutionSync] Tab switch detected:', {
        from: lastActiveTabIdRef.current,
        to: activeTabId
      });

      // Save config to the old tab before switching (if there was one)
      if (lastActiveTabIdRef.current && !isInitialMountRef.current) {
        const oldConfig: TabExecutionConfig = {
          processType,
          planningEnabled,
          planningLLM,
          reasoningEnabled,
          reasoningLLM,
          managerLLM
        };
        console.log('[useTabExecutionSync] Saving config to previous tab:', lastActiveTabIdRef.current, oldConfig);
        updateTabExecutionConfig(lastActiveTabIdRef.current, oldConfig);
      }

      // Restore config from the new tab
      if (activeTabId) {
        restoreConfigFromTab(activeTabId);
      }

      // Update the reference
      lastActiveTabIdRef.current = activeTabId;
      isInitialMountRef.current = false;
    }
  }, [
    activeTabId,
    processType,
    planningEnabled,
    planningLLM,
    reasoningEnabled,
    reasoningLLM,
    managerLLM,
    updateTabExecutionConfig,
    restoreConfigFromTab,
    isLoadingCrew
  ]);

  /**
   * Save config whenever execution settings change (debounced)
   */
  useEffect(() => {
    // Skip on initial mount or during restoration
    if (isInitialMountRef.current || isRestoringRef.current || isLoadingCrew) {
      return;
    }

    // Debounce the save to avoid too many updates
    const timeoutId = setTimeout(saveConfigToTab, 300);
    return () => clearTimeout(timeoutId);
  }, [
    processType,
    planningEnabled,
    planningLLM,
    reasoningEnabled,
    reasoningLLM,
    managerLLM,
    saveConfigToTab,
    isLoadingCrew
  ]);

  return {
    saveConfigToTab,
    restoreConfigFromTab
  };
};
