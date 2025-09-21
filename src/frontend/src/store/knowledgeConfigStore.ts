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
  hasCheckedOnce: boolean; // Track if we've done an initial check
  lastNotFoundTime: number; // Track when we last got a 404

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
    hasCheckedOnce: false,
    lastNotFoundTime: 0,

    setMemoryBackendConfigured: (configured: boolean) => {
      set({ isMemoryBackendConfigured: configured });
    },

    setKnowledgeSourceEnabled: (enabled: boolean) => {
      set({ isKnowledgeSourceEnabled: enabled });
    },

    checkConfiguration: async () => {
      const now = Date.now();
      const state = get();

      // If we've already checked and got 404s (services not configured),
      // only re-check after 5 minutes or if explicitly refreshed
      const CACHE_DURATION = 5 * 60 * 1000; // 5 minutes
      if (state.hasCheckedOnce &&
          state.lastNotFoundTime > 0 &&
          (now - state.lastNotFoundTime) < CACHE_DURATION) {
        // Skip check - we already know services aren't configured
        return;
      }

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

        // If both services returned null (likely 404s), record this
        if (!memoryConfig && !databricksConfig) {
          set({
            isMemoryBackendConfigured: false,
            isKnowledgeSourceEnabled: false,
            isLoading: false,
            hasCheckedOnce: true,
            lastNotFoundTime: now,
          });
        } else {
          // At least one service is configured
          set({
            isMemoryBackendConfigured: memoryConfigured,
            isKnowledgeSourceEnabled: knowledgeSourceEnabled,
            isLoading: false,
            hasCheckedOnce: true,
            lastNotFoundTime: 0, // Clear the not found time
          });
        }

        // Only log if we found some configuration
        if (memoryConfigured || knowledgeSourceEnabled) {
          console.log('[KnowledgeConfig] Configuration refreshed:', {
            memoryConfigured,
            knowledgeSourceEnabled,
          });
        }
      } catch (error) {
        console.error('[KnowledgeConfig] Error checking configuration:', error);
        set({
          isMemoryBackendConfigured: false,
          isKnowledgeSourceEnabled: false,
          isLoading: false,
          hasCheckedOnce: true,
          lastNotFoundTime: now,
        });
      }
    },

    refreshConfiguration: async () => {
      // Force refresh by resetting lastChecked and cache
      set({ lastChecked: 0, lastNotFoundTime: 0 });
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