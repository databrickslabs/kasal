import { useEffect, useRef, useCallback } from 'react';
import { useBuilderCanvasStore, CanvasExecutionConfig } from '../../app/sessions/builderCanvasStore';
import { useCrewExecutionStore } from '../../store/crewExecution';

/**
 * Hook to sync execution configuration (process type, reasoning, etc.)
 * between the global execution store and per-tab storage.
 *
 * This ensures each tab can have its own runtime configuration that persists
 * when switching between tabs.
 */
export const useBuilderExecutionSync = () => {
  const {
    activeCanvasId,
    updateCanvasExecutionConfig,
    getCanvasExecutionConfig
  } = useBuilderCanvasStore();

  const {
    processType,
    reasoningEnabled,
    reasoningLLM,
    reasoningConfig,
    managerLLM,
    selectedModel,
    setProcessType,
    setReasoningEnabled,
    setReasoningLLM,
    setReasoningConfig,
    setManagerLLM,
    setSelectedModel,
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
    if (!activeCanvasId || isRestoringRef.current || isLoadingCrew) {
      return;
    }

    const config: CanvasExecutionConfig = {
      processType,
      reasoningEnabled,
      reasoningLLM,
      reasoningConfig,
      managerLLM,
      selectedModel
    };

    console.log('[useBuilderExecutionSync] Saving config to tab:', activeCanvasId, config);
    updateCanvasExecutionConfig(activeCanvasId, config);
  }, [
    activeCanvasId,
    processType,
    reasoningEnabled,
    reasoningLLM,
    reasoningConfig,
    managerLLM,
    selectedModel,
    updateCanvasExecutionConfig,
    isLoadingCrew
  ]);

  /**
   * Restore execution config from a tab
   */
  const restoreConfigFromTab = useCallback((tabId: string) => {
    const config = getCanvasExecutionConfig(tabId);

    if (!config) {
      console.log('[useBuilderExecutionSync] No config found for tab:', tabId, '- using current values');
      return;
    }

    console.log('[useBuilderExecutionSync] Restoring config from tab:', tabId, config);

    isRestoringRef.current = true;

    if (config.selectedModel !== undefined) setSelectedModel(config.selectedModel);

    // Restore all config values
    if (config.processType !== undefined) {
      setProcessType(config.processType);
    }
    if (config.reasoningEnabled !== undefined) {
      setReasoningEnabled(config.reasoningEnabled);
    }
    if (config.reasoningLLM !== undefined) {
      setReasoningLLM(config.reasoningLLM);
    }
    if (config.reasoningConfig !== undefined) {
      setReasoningConfig({ ...config.reasoningConfig, execution_effort: config.reasoningConfig.execution_effort });
    }
    if (config.managerLLM !== undefined) {
      setManagerLLM(config.managerLLM);
    }

    // Reset the restoring flag after a short delay to allow state updates to settle
    setTimeout(() => {
      isRestoringRef.current = false;
    }, 100);
  }, [
    getCanvasExecutionConfig,
    setProcessType,
    setReasoningEnabled,
    setReasoningLLM,
    setReasoningConfig,
    setManagerLLM,
    setSelectedModel
  ]);

  /**
   * Handle tab switches - save current config and restore new tab's config
   */
  useEffect(() => {
    // Skip during crew loading to avoid interfering with crew config restoration
    if (isLoadingCrew) {
      return;
    }

    if (activeCanvasId !== lastActiveTabIdRef.current) {
      console.log('[useBuilderExecutionSync] Tab switch detected:', {
        from: lastActiveTabIdRef.current,
        to: activeCanvasId
      });

      // Save config to the old tab before switching (if there was one)
      if (lastActiveTabIdRef.current && !isInitialMountRef.current) {
        const oldConfig: CanvasExecutionConfig = {
          processType,
          reasoningEnabled,
          reasoningLLM,
          reasoningConfig,
          managerLLM,
          selectedModel
        };
        console.log('[useBuilderExecutionSync] Saving config to previous tab:', lastActiveTabIdRef.current, oldConfig);
        updateCanvasExecutionConfig(lastActiveTabIdRef.current, oldConfig);
      }

      // Restore config from the new tab
      if (activeCanvasId) {
        restoreConfigFromTab(activeCanvasId);
      }

      // Update the reference
      lastActiveTabIdRef.current = activeCanvasId;
      isInitialMountRef.current = false;
    }
  }, [
    activeCanvasId,
    processType,
    reasoningEnabled,
    reasoningLLM,
    reasoningConfig,
    managerLLM,
    selectedModel,
    updateCanvasExecutionConfig,
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
    reasoningEnabled,
    reasoningLLM,
    reasoningConfig,
    managerLLM,
    selectedModel,
    saveConfigToTab,
    isLoadingCrew
  ]);

  return {
    saveConfigToTab,
    restoreConfigFromTab
  };
};
