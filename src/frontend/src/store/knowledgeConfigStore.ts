import { create } from 'zustand';
import { subscribeWithSelector } from 'zustand/middleware';
import { DatabricksService } from '../api/DatabricksService';
import { MemoryBackendService } from '../api/MemoryBackendService';
import { MemoryBackendType } from '../types/memoryBackend';

interface KnowledgeConfigState {
  // Configuration states
  isMemoryBackendConfigured: boolean;
  isKnowledgeSourceEnabled: boolean;
  isLoading: boolean;
  lastChecked: number;

  // Actions
  refreshConfiguration: () => Promise<void>;
  setMemoryBackendConfigured: (configured: boolean) => void;
  setKnowledgeSourceEnabled: (enabled: boolean) => void;
  checkConfiguration: () => Promise<void>;
}

export const useKnowledgeConfigStore = create<KnowledgeConfigState>()(
  subscribeWithSelector((set, get) => ({
    isMemoryBackendConfigured: false,
    isKnowledgeSourceEnabled: false,
    isLoading: false,
    lastChecked: 0,

    setMemoryBackendConfigured: (configured: boolean) => {
      set({ isMemoryBackendConfigured: configured });
    },

    setKnowledgeSourceEnabled: (enabled: boolean) => {
      set({ isKnowledgeSourceEnabled: enabled });
    },

    checkConfiguration: async () => {
      const now = Date.now();
      const state = get();

      // Avoid too frequent checks (minimum 1 second between checks)
      if (now - state.lastChecked < 1000 && !state.isLoading) {
        return;
      }

      set({ isLoading: true, lastChecked: now });

      try {
        // Check memory backend configuration
        const memoryConfig = await MemoryBackendService.getConfig();
        const memoryConfigured = !!(
          memoryConfig?.backend_type === MemoryBackendType.DATABRICKS &&
          memoryConfig.databricks_config?.endpoint_name &&
          memoryConfig.databricks_config?.short_term_index
        );

        // Check Databricks knowledge source configuration
        const databricksConfig = await DatabricksService.getConfiguration();
        const knowledgeSourceEnabled = !!(
          databricksConfig?.knowledge_volume_enabled &&
          databricksConfig?.knowledge_volume_path
        );

        set({
          isMemoryBackendConfigured: memoryConfigured,
          isKnowledgeSourceEnabled: knowledgeSourceEnabled,
          isLoading: false,
        });

        console.log('[KnowledgeConfig] Configuration refreshed:', {
          memoryConfigured,
          knowledgeSourceEnabled,
        });
      } catch (error) {
        console.error('[KnowledgeConfig] Error checking configuration:', error);
        set({
          isMemoryBackendConfigured: false,
          isKnowledgeSourceEnabled: false,
          isLoading: false,
        });
      }
    },

    refreshConfiguration: async () => {
      // Force refresh by resetting lastChecked
      set({ lastChecked: 0 });
      await get().checkConfiguration();
    },
  }))
);

// Auto-refresh configuration periodically
let refreshInterval: NodeJS.Timeout | null = null;

// Start auto-refresh when first subscriber connects
useKnowledgeConfigStore.subscribe(
  (state) => state.isMemoryBackendConfigured,
  () => {
    if (!refreshInterval) {
      // Check configuration immediately
      useKnowledgeConfigStore.getState().checkConfiguration();

      // Then check every 30 seconds
      refreshInterval = setInterval(() => {
        useKnowledgeConfigStore.getState().checkConfiguration();
      }, 30000);
    }
  }
);

// Global event listeners for configuration changes
if (typeof window !== 'undefined') {
  // Listen for custom events when configuration changes
  window.addEventListener('databricks-config-updated', () => {
    console.log('[KnowledgeConfig] Databricks config updated, refreshing...');
    useKnowledgeConfigStore.getState().refreshConfiguration();
  });

  window.addEventListener('memory-backend-updated', () => {
    console.log('[KnowledgeConfig] Memory backend updated, refreshing...');
    useKnowledgeConfigStore.getState().refreshConfiguration();
  });

  // Listen for focus events to refresh when user comes back to the tab
  window.addEventListener('focus', () => {
    useKnowledgeConfigStore.getState().checkConfiguration();
  });
}