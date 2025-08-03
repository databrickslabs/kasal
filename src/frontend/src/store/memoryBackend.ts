/**
 * Zustand store for memory backend configuration state management.
 */

import { create } from 'zustand';
import { 
  MemoryBackendConfig, 
  MemoryBackendType,
  DatabricksMemoryConfig,
  DEFAULT_MEMORY_BACKEND_CONFIG,
  DEFAULT_DATABRICKS_CONFIG 
} from '../types/memoryBackend';
import { 
  MemoryBackendService, 
  DatabricksIndex, 
  TestConnectionResult 
} from '../api/MemoryBackendService';

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
  
  // API actions
  validateConfig: () => Promise<boolean>;
  testDatabricksConnection: () => Promise<TestConnectionResult>;
  loadAvailableIndexes: () => Promise<void>;
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
    } catch (error: any) {
      const errorMsg = error.message || 'Failed to validate configuration';
      set({ 
        error: errorMsg,
        validationErrors: [errorMsg],
        isLoading: false,
      });
      return false;
    }
  },

  // Test Databricks connection
  testDatabricksConnection: async () => {
    const { config } = get();
    
    if (config.backend_type !== MemoryBackendType.DATABRICKS || !config.databricks_config) {
      const result: TestConnectionResult = {
        success: false,
        message: 'Databricks configuration not set',
      };
      set({ connectionTestResult: result });
      return result;
    }
    
    set({ isTestingConnection: true, connectionTestResult: null, error: null });
    
    try {
      const result = await MemoryBackendService.testDatabricksConnection(config.databricks_config);
      set({ 
        connectionTestResult: result,
        isTestingConnection: false,
      });
      return result;
    } catch (error: any) {
      const result: TestConnectionResult = {
        success: false,
        message: error.message || 'Connection test failed',
      };
      set({ 
        connectionTestResult: result,
        isTestingConnection: false,
        error: result.message,
      });
      return result;
    }
  },

  // Load available indexes
  loadAvailableIndexes: async () => {
    const { config } = get();
    
    if (config.backend_type !== MemoryBackendType.DATABRICKS || !config.databricks_config?.endpoint_name) {
      return;
    }
    
    set({ isLoadingIndexes: true, error: null });
    
    try {
      const response = await MemoryBackendService.getAvailableDatabricksIndexes(
        config.databricks_config.endpoint_name,
        config.databricks_config
      );
      set({ 
        availableIndexes: response.indexes,
        isLoadingIndexes: false,
      });
    } catch (error: any) {
      set({ 
        error: error.message || 'Failed to load indexes',
        availableIndexes: [],
        isLoadingIndexes: false,
      });
    }
  },

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
    } catch (error: any) {
      set({ 
        error: error.message || 'Failed to save configuration',
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
    } catch (error: any) {
      set({ 
        error: error.message || 'Failed to load configuration',
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