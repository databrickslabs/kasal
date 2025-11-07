import { renderHook, act } from '@testing-library/react';
import { useMemoryBackendStore, useMemoryBackendConfig, useMemoryBackendType, useDatabricksConfig } from './memoryBackend';
import { MemoryBackendService } from '../api/MemoryBackendService';
import { 
  MemoryBackendType, 
  DEFAULT_MEMORY_BACKEND_CONFIG, 
  DEFAULT_DATABRICKS_CONFIG,
  DatabricksMemoryConfig 
} from '../types/memoryBackend';

// Mock the MemoryBackendService
jest.mock('../api/MemoryBackendService');

describe('memoryBackendStore', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    // Reset store state before each test
    act(() => {
      useMemoryBackendStore.getState().resetConfig();
    });
  });

  describe('initial state', () => {
    it('should have correct initial state', () => {
      const { result } = renderHook(() => useMemoryBackendStore());
      
      expect(result.current.config).toEqual(DEFAULT_MEMORY_BACKEND_CONFIG);
      expect(result.current.isLoading).toBe(false);
      expect(result.current.error).toBeNull();
      expect(result.current.connectionTestResult).toBeNull();
      expect(result.current.isTestingConnection).toBe(false);
      expect(result.current.availableIndexes).toEqual([]);
      expect(result.current.isLoadingIndexes).toBe(false);
      expect(result.current.validationErrors).toEqual([]);
    });
  });

  describe('setConfig', () => {
    it('should set config and clear error', () => {
      const { result } = renderHook(() => useMemoryBackendStore());
      
      const newConfig = {
        backend_type: MemoryBackendType.DATABRICKS,
        enable_short_term: true,
        enable_long_term: false,
        enable_entity: true,
      };
      
      act(() => {
        result.current.setError('Previous error');
        result.current.setConfig(newConfig);
      });
      
      expect(result.current.config).toEqual(newConfig);
      expect(result.current.error).toBeNull();
    });
  });

  describe('updateConfig', () => {
    it('should update config partially and clear error', () => {
      const { result } = renderHook(() => useMemoryBackendStore());
      
      act(() => {
        result.current.setError('Previous error');
        result.current.updateConfig({ 
          backend_type: MemoryBackendType.DATABRICKS,
          enable_short_term: false,
        });
      });
      
      expect(result.current.config.backend_type).toBe(MemoryBackendType.DATABRICKS);
      expect(result.current.config.enable_short_term).toBe(false);
      expect(result.current.config.enable_long_term).toBe(true); // from default
      expect(result.current.error).toBeNull();
    });
  });

  describe('updateDatabricksConfig', () => {
    it('should update databricks config and preserve existing values', () => {
      const { result } = renderHook(() => useMemoryBackendStore());
      
      const initialDatabricksConfig = {
        endpoint_name: 'test-endpoint',
        short_term_index: 'short-index',
        workspace_url: 'https://example.databricks.com',
      };
      
      act(() => {
        result.current.updateConfig({
          backend_type: MemoryBackendType.DATABRICKS,
          databricks_config: initialDatabricksConfig,
        });
      });
      
      act(() => {
        result.current.updateDatabricksConfig({
          long_term_index: 'long-index',
          entity_index: 'entity-index',
        });
      });
      
      expect(result.current.config.databricks_config).toEqual({
        ...initialDatabricksConfig,
        long_term_index: 'long-index',
        entity_index: 'entity-index',
      });
    });

    it('should use defaults when no databricks config exists', () => {
      const { result } = renderHook(() => useMemoryBackendStore());
      
      act(() => {
        result.current.updateDatabricksConfig({
          endpoint_name: 'new-endpoint',
        });
      });
      
      expect(result.current.config.databricks_config).toEqual({
        ...DEFAULT_DATABRICKS_CONFIG,
        endpoint_name: 'new-endpoint',
      });
    });
  });

  describe('validateConfig', () => {
    it('should validate config successfully', async () => {
      const mockValidationResult = { valid: true };
      (MemoryBackendService.validateConfig as jest.Mock).mockResolvedValue(mockValidationResult);
      
      const { result } = renderHook(() => useMemoryBackendStore());
      
      let isValid = false;
      await act(async () => {
        isValid = await result.current.validateConfig();
      });
      
      expect(isValid).toBe(true);
      expect(result.current.validationErrors).toEqual([]);
      expect(result.current.error).toBeNull();
    });

    it('should handle validation errors', async () => {
      const mockValidationResult = { 
        valid: false, 
        errors: ['Invalid endpoint', 'Missing index'] 
      };
      (MemoryBackendService.validateConfig as jest.Mock).mockResolvedValue(mockValidationResult);
      
      const { result } = renderHook(() => useMemoryBackendStore());
      
      let isValid = true;
      await act(async () => {
        isValid = await result.current.validateConfig();
      });
      
      expect(isValid).toBe(false);
      expect(result.current.validationErrors).toEqual(['Invalid endpoint', 'Missing index']);
    });

    it('should handle validation exceptions', async () => {
      (MemoryBackendService.validateConfig as jest.Mock).mockRejectedValue(
        new Error('Network error')
      );
      
      const { result } = renderHook(() => useMemoryBackendStore());
      
      let isValid = true;
      await act(async () => {
        isValid = await result.current.validateConfig();
      });
      
      expect(isValid).toBe(false);
      expect(result.current.error).toBe('Network error');
      expect(result.current.validationErrors).toEqual(['Network error']);
    });
  });

  describe('testDatabricksConnection', () => {
    it('should test connection successfully', async () => {
      const mockTestResult = {
        success: true,
        message: 'Connection successful',
        details: {
          endpoint_status: 'ONLINE',
          indexes_found: ['index1', 'index2'],
        },
      };
      (MemoryBackendService.testDatabricksConnection as jest.Mock).mockResolvedValue(mockTestResult);
      
      const { result } = renderHook(() => useMemoryBackendStore());
      
      const databricksConfig: DatabricksMemoryConfig = {
        endpoint_name: 'test-endpoint',
        short_term_index: 'short-index',
        workspace_url: 'https://example.databricks.com',
      };
      
      act(() => {
        result.current.updateConfig({
          backend_type: MemoryBackendType.DATABRICKS,
          databricks_config: databricksConfig,
        });
      });
      
      let testResult;
      await act(async () => {
        testResult = await result.current.testDatabricksConnection();
      });
      
      expect(testResult).toEqual(mockTestResult);
      expect(result.current.connectionTestResult).toEqual(mockTestResult);
      expect(result.current.isTestingConnection).toBe(false);
    });

    it('should handle non-databricks backend type', async () => {
      const { result } = renderHook(() => useMemoryBackendStore());
      
      let testResult;
      await act(async () => {
        testResult = await result.current.testDatabricksConnection();
      });
      
      expect(testResult).toEqual({
        success: false,
        message: 'Databricks configuration not set',
      });
      expect(MemoryBackendService.testDatabricksConnection).not.toHaveBeenCalled();
    });

    it('should handle connection test errors', async () => {
      (MemoryBackendService.testDatabricksConnection as jest.Mock).mockRejectedValue(
        new Error('Connection failed')
      );
      
      const { result } = renderHook(() => useMemoryBackendStore());
      
      act(() => {
        result.current.updateConfig({
          backend_type: MemoryBackendType.DATABRICKS,
          databricks_config: {
            endpoint_name: 'test-endpoint',
            short_term_index: 'short-index',
          },
        });
      });
      
      let testResult;
      await act(async () => {
        testResult = await result.current.testDatabricksConnection();
      });
      
      expect(testResult).toEqual({
        success: false,
        message: 'Connection failed',
      });
      expect(result.current.error).toBe('Connection failed');
    });
  });

  describe('loadAvailableIndexes', () => {
    it('should load indexes successfully', async () => {
      const mockIndexes = [
        {
          name: 'index1',
          catalog: 'ml',
          schema: 'agents',
          table: 'memories',
          dimension: 1536,
          total_records: 100,
        },
        {
          name: 'index2',
          catalog: 'ml',
          schema: 'agents',
          table: 'long_term',
          dimension: 1536,
          total_records: 200,
        },
      ];
      (MemoryBackendService.getAvailableDatabricksIndexes as jest.Mock).mockResolvedValue({
        indexes: mockIndexes,
        endpoint_name: 'test-endpoint',
      });
      
      const { result } = renderHook(() => useMemoryBackendStore());
      
      act(() => {
        result.current.updateConfig({
          backend_type: MemoryBackendType.DATABRICKS,
          databricks_config: {
            endpoint_name: 'test-endpoint',
            short_term_index: 'short-index',
          },
        });
      });
      
      await act(async () => {
        await result.current.loadAvailableIndexes();
      });
      
      expect(result.current.availableIndexes).toEqual(mockIndexes);
      expect(result.current.isLoadingIndexes).toBe(false);
    });

    it('should not load indexes for non-databricks backend', async () => {
      const { result } = renderHook(() => useMemoryBackendStore());
      
      await act(async () => {
        await result.current.loadAvailableIndexes();
      });
      
      expect(MemoryBackendService.getAvailableDatabricksIndexes).not.toHaveBeenCalled();
    });

    it('should handle index loading errors', async () => {
      (MemoryBackendService.getAvailableDatabricksIndexes as jest.Mock).mockRejectedValue(
        new Error('Failed to fetch indexes')
      );
      
      const { result } = renderHook(() => useMemoryBackendStore());
      
      act(() => {
        result.current.updateConfig({
          backend_type: MemoryBackendType.DATABRICKS,
          databricks_config: {
            endpoint_name: 'test-endpoint',
            short_term_index: 'short-index',
          },
        });
      });
      
      await act(async () => {
        await result.current.loadAvailableIndexes();
      });
      
      expect(result.current.error).toBe('Failed to fetch indexes');
      expect(result.current.availableIndexes).toEqual([]);
    });
  });

  describe('saveConfig', () => {
    it('should save config successfully after validation', async () => {
      (MemoryBackendService.validateConfig as jest.Mock).mockResolvedValue({ valid: true });
      (MemoryBackendService.saveConfig as jest.Mock).mockResolvedValue({ 
        success: true, 
        message: 'Config saved' 
      });
      
      const { result } = renderHook(() => useMemoryBackendStore());
      
      let success = false;
      await act(async () => {
        success = await result.current.saveConfig();
      });
      
      expect(success).toBe(true);
      expect(MemoryBackendService.validateConfig).toHaveBeenCalled();
      expect(MemoryBackendService.saveConfig).toHaveBeenCalled();
    });

    it('should not save if validation fails', async () => {
      (MemoryBackendService.validateConfig as jest.Mock).mockResolvedValue({ 
        valid: false, 
        errors: ['Invalid config'] 
      });
      
      const { result } = renderHook(() => useMemoryBackendStore());
      
      let success = true;
      await act(async () => {
        success = await result.current.saveConfig();
      });
      
      expect(success).toBe(false);
      expect(MemoryBackendService.saveConfig).not.toHaveBeenCalled();
    });

    it('should handle save errors', async () => {
      (MemoryBackendService.validateConfig as jest.Mock).mockResolvedValue({ valid: true });
      (MemoryBackendService.saveConfig as jest.Mock).mockRejectedValue(
        new Error('Save failed')
      );
      
      const { result } = renderHook(() => useMemoryBackendStore());
      
      let success = true;
      await act(async () => {
        success = await result.current.saveConfig();
      });
      
      expect(success).toBe(false);
      expect(result.current.error).toBe('Save failed');
    });
  });

  describe('loadConfig', () => {
    it('should load config successfully', async () => {
      const mockConfig = {
        backend_type: MemoryBackendType.DATABRICKS,
        enable_short_term: true,
        enable_long_term: true,
        enable_entity: false,
        databricks_config: {
          endpoint_name: 'loaded-endpoint',
          short_term_index: 'loaded-index',
        },
      };
      (MemoryBackendService.getConfig as jest.Mock).mockResolvedValue(mockConfig);
      
      const { result } = renderHook(() => useMemoryBackendStore());
      
      await act(async () => {
        await result.current.loadConfig();
      });
      
      expect(result.current.config).toEqual(mockConfig);
      expect(result.current.isLoading).toBe(false);
    });

    it('should handle null config response', async () => {
      (MemoryBackendService.getConfig as jest.Mock).mockResolvedValue(null);
      
      const { result } = renderHook(() => useMemoryBackendStore());
      
      await act(async () => {
        await result.current.loadConfig();
      });
      
      expect(result.current.config).toEqual(DEFAULT_MEMORY_BACKEND_CONFIG);
      expect(result.current.isLoading).toBe(false);
    });

    it('should handle load errors', async () => {
      (MemoryBackendService.getConfig as jest.Mock).mockRejectedValue(
        new Error('Load failed')
      );
      
      const { result } = renderHook(() => useMemoryBackendStore());
      
      await act(async () => {
        await result.current.loadConfig();
      });
      
      expect(result.current.error).toBe('Load failed');
      expect(result.current.isLoading).toBe(false);
    });
  });

  describe('resetConfig', () => {
    it('should reset to default state', () => {
      const { result } = renderHook(() => useMemoryBackendStore());
      
      // Set some custom state
      act(() => {
        result.current.setConfig({
          backend_type: MemoryBackendType.DATABRICKS,
          enable_short_term: false,
        });
        result.current.setError('Some error');
      });
      
      // Reset
      act(() => {
        result.current.resetConfig();
      });
      
      expect(result.current.config).toEqual(DEFAULT_MEMORY_BACKEND_CONFIG);
      expect(result.current.error).toBeNull();
      expect(result.current.connectionTestResult).toBeNull();
      expect(result.current.validationErrors).toEqual([]);
      expect(result.current.availableIndexes).toEqual([]);
    });
  });

  describe('error handling', () => {
    it('should clear error', () => {
      const { result } = renderHook(() => useMemoryBackendStore());
      
      act(() => {
        result.current.setError('Test error');
      });
      
      expect(result.current.error).toBe('Test error');
      
      act(() => {
        result.current.clearError();
      });
      
      expect(result.current.error).toBeNull();
    });

    it('should set error', () => {
      const { result } = renderHook(() => useMemoryBackendStore());
      
      act(() => {
        result.current.setError('New error');
      });
      
      expect(result.current.error).toBe('New error');
    });
  });

  describe('selector hooks', () => {
    it('useMemoryBackendConfig should return config', () => {
      const { result: storeResult } = renderHook(() => useMemoryBackendStore());
      const { result: configResult } = renderHook(() => useMemoryBackendConfig());
      
      expect(configResult.current).toEqual(storeResult.current.config);
      
      const newConfig = {
        backend_type: MemoryBackendType.DATABRICKS,
        enable_short_term: false,
      };
      
      act(() => {
        storeResult.current.setConfig(newConfig);
      });
      
      expect(configResult.current).toEqual(newConfig);
    });

    it('useMemoryBackendType should return backend type', () => {
      const { result: storeResult } = renderHook(() => useMemoryBackendStore());
      const { result: typeResult } = renderHook(() => useMemoryBackendType());
      
      expect(typeResult.current).toBe(MemoryBackendType.DEFAULT);
      
      act(() => {
        storeResult.current.updateConfig({ backend_type: MemoryBackendType.DATABRICKS });
      });
      
      expect(typeResult.current).toBe(MemoryBackendType.DATABRICKS);
    });

    it('useDatabricksConfig should return databricks config', () => {
      const { result: storeResult } = renderHook(() => useMemoryBackendStore());
      const { result: databricksResult } = renderHook(() => useDatabricksConfig());
      
      expect(databricksResult.current).toBeUndefined();
      
      const databricksConfig = {
        endpoint_name: 'test-endpoint',
        short_term_index: 'short-index',
      };
      
      act(() => {
        storeResult.current.updateConfig({
          backend_type: MemoryBackendType.DATABRICKS,
          databricks_config: databricksConfig,
        });
      });
      
      expect(databricksResult.current).toEqual(databricksConfig);
    });
  });
});