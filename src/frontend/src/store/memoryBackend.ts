/**
 * Zustand store for memory backend configuration state management.
 */

import { create } from 'zustand';
import {
  MemoryBackendConfig,
  MemoryBackendType,
  MemoryTuningConfig,
  DatabricksMemoryConfig,
  LakebaseMemoryConfig,
  DEFAULT_MEMORY_BACKEND_CONFIG,
  DEFAULT_DATABRICKS_CONFIG,
  DEFAULT_LAKEBASE_CONFIG,
} from '../types/config/memoryBackend';
import { 
  MemoryBackendService, 
  DatabricksIndex, 
  TestConnectionResult 
} from '../api/memory/MemoryBackendService';

interface MemoryBackendState {
  // State
  config: MemoryBackendConfig;
  isLoading: boolean;
  error: string | null;
  
  // Connection testing
  connectionTestResult: TestConnectionResult | null;
  isTestingConnection: boolean;
  
  // Available indexes
  availableIndexes: DatabricksIndex[];
  isLoadingIndexes: boolean;
  
  // Validation
  validationErrors: string[];

  // Actions
  setConfig: (config: MemoryBackendConfig) => void;
  updateConfig: (updates: Partial<MemoryBackendConfig>) => void;
  updateDatabricksConfig: (updates: Partial<MemoryBackendConfig['databricks_config']>) => void;
  updateLakebaseConfig: (updates: Partial<LakebaseMemoryConfig>) => void;
  updateCognitiveConfig: (updates: Partial<MemoryTuningConfig>) => void;
  
  // API actions
  validateConfig: () => Promise<boolean>;
  saveConfig: () => Promise<boolean>;
  loadConfig: () => Promise<void>;
  
  // Utility actions
  resetConfig: () => void;
  clearError: () => void;
  setError: (error: string) => void;
}

export const useMemoryBackendStore = create<MemoryBackendState>((set, get) => ({
  // Initial state
  config: DEFAULT_MEMORY_BACKEND_CONFIG,
  isLoading: false,
  error: null,
  connectionTestResult: null,
  isTestingConnection: false,
  availableIndexes: [],
  isLoadingIndexes: false,
  validationErrors: [],

  // Basic setters
  setConfig: (config) => set({ config, error: null }),
  
  updateConfig: (updates) => set((state) => ({
    config: { ...state.config, ...updates },
    error: null,
  })),
  
  updateDatabricksConfig: (updates) => set((state) => ({
    config: {
      ...state.config,
      databricks_config: {
        ...(state.config.databricks_config || DEFAULT_DATABRICKS_CONFIG),
        ...updates,
      } as DatabricksMemoryConfig,
    },
    error: null,
  })),

  updateLakebaseConfig: (updates) => set((state) => ({
    config: {
      ...state.config,
      lakebase_config: {
        ...(state.config.lakebase_config || DEFAULT_LAKEBASE_CONFIG),
        ...updates,
      } as LakebaseMemoryConfig,
    },
    error: null,
  })),

  updateCognitiveConfig: (updates) => set((state) => ({
    config: {
      ...state.config,
      cognitive_config: {
        ...(state.config.cognitive_config || {}),
        ...updates,
      } as MemoryTuningConfig,
    },
    error: null,
  })),

  // Validate configuration
  validateConfig: async () => {
    const { config } = get();
    set({ isLoading: true, error: null, validationErrors: [] });
    
    try {
      const result = await MemoryBackendService.validateConfig(config);
      set({ 
        validationErrors: result.errors || [],
        isLoading: false,
      });
      return result.valid;
    } catch (error: unknown) {
      const errorMsg = (error instanceof Error ? error.message : String(error)) || 'Failed to validate configuration';
      set({ 
        error: errorMsg,
        validationErrors: [errorMsg],
        isLoading: false,
      });
      return false;
    }
  },

  // Test Databricks connection

  // Load available indexes

  // Save configuration
  saveConfig: async () => {
    const { config, validateConfig } = get();
    
    // Validate before saving
    const isValid = await validateConfig();
    if (!isValid) {
      return false;
    }
    
    set({ isLoading: true, error: null });
    
    try {
      const result = await MemoryBackendService.saveConfig(config);
      set({ isLoading: false });
      return result.success;
    } catch (error: unknown) {
      set({ 
        error: (error instanceof Error ? error.message : String(error)) || 'Failed to save configuration',
        isLoading: false,
      });
      return false;
    }
  },

  // Load configuration
  loadConfig: async () => {
    set({ isLoading: true, error: null });
    
    try {
      const config = await MemoryBackendService.getConfig();
      if (config) {
        set({ config, isLoading: false });
      } else {
        set({ isLoading: false });
      }
    } catch (error: unknown) {
      set({ 
        error: (error instanceof Error ? error.message : String(error)) || 'Failed to load configuration',
        isLoading: false,
      });
    }
  },

  // Reset to defaults
  resetConfig: () => set({
    config: DEFAULT_MEMORY_BACKEND_CONFIG,
    error: null,
    connectionTestResult: null,
    validationErrors: [],
    availableIndexes: [],
  }),

  // Error handling
  clearError: () => set({ error: null }),
  setError: (error) => set({ error }),
}));

// Selector hooks for specific parts of the state
export const useMemoryBackendConfig = () => useMemoryBackendStore((state) => state.config);
export const useMemoryBackendType = () => useMemoryBackendStore((state) => state.config.backend_type);
export const useDatabricksConfig = () => useMemoryBackendStore((state) => state.config.databricks_config);
export const useLakebaseConfig = () => useMemoryBackendStore((state) => state.config.lakebase_config);
export const useMemoryTuningConfig = () =>
  useMemoryBackendStore((state) => state.config.cognitive_config);