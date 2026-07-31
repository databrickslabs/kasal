import { vi, Mock, beforeEach, afterEach, describe, it, expect } from 'vitest';
import { MemoryBackendService, TestConnectionResult, AvailableIndexesResponse } from './MemoryBackendService';
import { apiClient } from '../../config/api/ApiConfig';
import { MemoryBackendConfig, DatabricksMemoryConfig, MemoryBackendType } from '../../types/config/memoryBackend';
import { AxiosError } from 'axios';

vi.mock('../../config/api/ApiConfig', () => ({
  apiClient: {
    get: vi.fn(),
    post: vi.fn(),
    put: vi.fn(),
    delete: vi.fn(),
    patch: vi.fn(),
  },
  config: {
    apiUrl: 'http://localhost:8000/api/v1',
  },
}));

describe('MemoryBackendService', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.spyOn(console, 'error').mockImplementation(vi.fn());
  });

  afterEach(() => {
    (console.error as Mock).mockRestore();
  });

  describe('validateConfig', () => {
    it('should validate config successfully', async () => {
      const mockConfig: MemoryBackendConfig = {
        backend_type: MemoryBackendType.DATABRICKS,
        enable_short_term: true,
        enable_long_term: true,
        enable_entity: true,
        databricks_config: {
          workspace_url: 'https://example.databricks.com',
          endpoint_name: 'test-endpoint',
          short_term_index: 'short_term_index',
          long_term_index: 'long_term_index',
          entity_index: 'entity_index',
        },
      };

      const mockResponse = { valid: true };
      (apiClient.post as Mock).mockResolvedValue({ data: mockResponse });

      const result = await MemoryBackendService.validateConfig(mockConfig);

      expect(apiClient.post).toHaveBeenCalledWith('/memory-backend/validate', mockConfig);
      expect(result).toEqual(mockResponse);
    });

    it('should handle validation errors', async () => {
      const mockConfig: MemoryBackendConfig = {
        backend_type: MemoryBackendType.DATABRICKS,
        enable_short_term: true,
        databricks_config: {
          workspace_url: '',
          endpoint_name: '',
          short_term_index: '',
        },
      };

      const mockError = new AxiosError('Validation failed');
      mockError.response = {
        data: { detail: 'Invalid workspace URL' },
        status: 400,
        statusText: 'Bad Request',
        headers: {},
        config: { headers: {} } as any,
      };
      (apiClient.post as Mock).mockRejectedValue(mockError);

      const result = await MemoryBackendService.validateConfig(mockConfig);

      expect(result).toEqual({
        valid: false,
        errors: ['Invalid workspace URL'],
      });
    });
  });

  describe('saveConfig', () => {
    it('should save config successfully', async () => {
      const mockConfig: MemoryBackendConfig = {
        backend_type: MemoryBackendType.DATABRICKS,
        enable_short_term: true,
        databricks_config: {
          workspace_url: 'https://example.databricks.com',
          endpoint_name: 'test-endpoint',
          short_term_index: 'short_index',
        },
      };
      const mockResponse = { success: true, message: 'Configuration saved' };
      (apiClient.post as Mock).mockResolvedValue({ data: mockResponse });

      const result = await MemoryBackendService.saveConfig(mockConfig);

      expect(apiClient.post).toHaveBeenCalledWith('/memory-backend/config', mockConfig);
      expect(result).toEqual(mockResponse);
    });

    it('should handle save errors', async () => {
      const mockConfig: MemoryBackendConfig = {
        backend_type: MemoryBackendType.DATABRICKS,
        enable_short_term: true,
      };
      const mockError = new AxiosError('Save failed');
      mockError.response = {
        data: { detail: 'Database error' },
        status: 500,
        statusText: 'Internal Server Error',
        headers: {},
        config: { headers: {} } as any,
      };
      (apiClient.post as Mock).mockRejectedValue(mockError);

      const result = await MemoryBackendService.saveConfig(mockConfig);

      expect(result).toEqual({
        success: false,
        message: 'Database error',
      });
    });
  });

  describe('saveDefaultConfig', () => {
    it('persists local cognitive tuning to the backend default/save-config endpoint', async () => {
      const mockConfig: MemoryBackendConfig = {
        backend_type: MemoryBackendType.DEFAULT,
        cognitive_config: {
          memory_llm_model: 'databricks-claude-haiku-4-5',
          query_analysis_threshold: 99977,
          exploration_budget: 0,
        },
      };
      const mockResponse = { success: true, backend_id: 'abc', message: 'Local memory backend configured successfully' };
      (apiClient.post as Mock).mockResolvedValue({ data: mockResponse });

      const result = await MemoryBackendService.saveDefaultConfig(mockConfig);

      expect(apiClient.post).toHaveBeenCalledWith(
        '/memory-backend/default/save-config',
        { cognitive_config: mockConfig.cognitive_config }
      );
      expect(result).toEqual(mockResponse);
    });

    it('sends null cognitive_config when none is set', async () => {
      const mockConfig: MemoryBackendConfig = { backend_type: MemoryBackendType.DEFAULT };
      (apiClient.post as Mock).mockResolvedValue({ data: { success: true, message: 'ok' } });

      await MemoryBackendService.saveDefaultConfig(mockConfig);

      expect(apiClient.post).toHaveBeenCalledWith(
        '/memory-backend/default/save-config',
        { cognitive_config: null }
      );
    });

    it('returns a failure result on error', async () => {
      const mockConfig: MemoryBackendConfig = { backend_type: MemoryBackendType.DEFAULT };
      const mockError = new AxiosError('Save failed');
      mockError.response = {
        data: { detail: 'Only workspace admins can configure memory backends' },
        status: 403,
        statusText: 'Forbidden',
        headers: {},
        config: { headers: {} } as any,
      };
      (apiClient.post as Mock).mockRejectedValue(mockError);

      const result = await MemoryBackendService.saveDefaultConfig(mockConfig);

      expect(result.success).toBe(false);
      expect(result.message).toBe('Only workspace admins can configure memory backends');
    });
  });

  describe('getConfig', () => {
    it('should fetch config successfully', async () => {
      const mockConfig: MemoryBackendConfig = {
        backend_type: MemoryBackendType.DATABRICKS,
        enable_short_term: true,
        is_default: true,
        is_active: true,
        databricks_config: {
          workspace_url: 'https://example.databricks.com',
          endpoint_name: 'test-endpoint',
          short_term_index: 'short_index',
        },
      };
      // Mock response as array since we're calling /configs endpoint
      (apiClient.get as Mock).mockResolvedValue({ data: [mockConfig] });

      const result = await MemoryBackendService.getConfig();

      expect(apiClient.get).toHaveBeenCalledWith('/memory-backend/configs');
      expect(result).toEqual(mockConfig);
    });

    it('should return null on error', async () => {
      (apiClient.get as Mock).mockRejectedValue(new Error('Network error'));

      const result = await MemoryBackendService.getConfig();

      expect(result).toBeNull();
    });

    it('should return null when no configs exist', async () => {
      (apiClient.get as Mock).mockResolvedValue({ data: [] });

      const result = await MemoryBackendService.getConfig();

      expect(result).toBeNull();
    });

    it('should return null when configs is null', async () => {
      (apiClient.get as Mock).mockResolvedValue({ data: null });

      const result = await MemoryBackendService.getConfig();

      expect(result).toBeNull();
    });
  });

  describe('getMemoryStats', () => {
    it('should fetch memory stats successfully', async () => {
      const mockStats = {
        short_term_count: 10,
        long_term_count: 20,
        entity_count: 5,
        total_size_mb: 1.5,
      };
      (apiClient.get as Mock).mockResolvedValue({ data: mockStats });

      const result = await MemoryBackendService.getMemoryStats('crew-123');

      expect(apiClient.get).toHaveBeenCalledWith('/memory-backend/stats/crew-123');
      expect(result).toEqual(mockStats);
    });

    it('should return empty object on error', async () => {
      (apiClient.get as Mock).mockRejectedValue(new Error('Not found'));

      const result = await MemoryBackendService.getMemoryStats('crew-123');

      expect(result).toEqual({});
    });
  });

  describe('clearMemory', () => {
    it('should clear memory successfully', async () => {
      const mockResponse = { success: true, message: 'Memory cleared' };
      (apiClient.post as Mock).mockResolvedValue({ data: mockResponse });

      const result = await MemoryBackendService.clearMemory('crew-123', ['short_term', 'long_term']);

      expect(apiClient.post).toHaveBeenCalledWith('/memory-backend/clear/crew-123', {
        memory_types: ['short_term', 'long_term'],
      });
      expect(result).toEqual(mockResponse);
    });

    it('should handle clear errors', async () => {
      const mockError = new AxiosError('Clear failed');
      mockError.response = {
        data: { detail: 'Permission denied' },
        status: 403,
        statusText: 'Forbidden',
        headers: {},
        config: { headers: {} } as any,
      };
      (apiClient.post as Mock).mockRejectedValue(mockError);

      const result = await MemoryBackendService.clearMemory('crew-123', ['entity']);

      expect(result).toEqual({
        success: false,
        message: 'Permission denied',
      });
    });
  });

  describe('testLakebaseConnection', () => {
    it('should test connection successfully', async () => {
      const mockResponse = {
        success: true,
        message: 'Connected with pgvector support',
        details: { pgvector_available: true, pg_version: 'PostgreSQL 15.4' },
      };
      (apiClient.post as Mock).mockResolvedValue({ data: mockResponse });

      const result = await MemoryBackendService.testLakebaseConnection('kasal-lakebase1');

      expect(apiClient.post).toHaveBeenCalledWith('/memory-backend/lakebase/test-connection', {
        instance_name: 'kasal-lakebase1',
      });
      expect(result).toEqual(mockResponse);
    });

    it('should call without instance name', async () => {
      const mockResponse = { success: true, message: 'Connected' };
      (apiClient.post as Mock).mockResolvedValue({ data: mockResponse });

      await MemoryBackendService.testLakebaseConnection();

      expect(apiClient.post).toHaveBeenCalledWith('/memory-backend/lakebase/test-connection', {});
    });

    it('should handle connection errors', async () => {
      const mockError = new AxiosError('Connection failed');
      mockError.response = {
        data: { detail: 'Connection refused' },
        status: 500,
        statusText: 'Internal Server Error',
        headers: {},
        config: { headers: {} } as any,
      };
      (apiClient.post as Mock).mockRejectedValue(mockError);

      const result = await MemoryBackendService.testLakebaseConnection();

      expect(result).toEqual({
        success: false,
        message: 'Connection refused',
        details: { error: 'Connection refused' },
      });
    });
  });

  describe('initializeLakebaseTables', () => {
    it('should initialize tables successfully', async () => {
      const mockResponse = {
        success: true,
        message: 'All tables initialized',
        tables: {
          short_term: { success: true, table_name: 'crew_short_term_memory', message: 'OK' },
        },
      };
      (apiClient.post as Mock).mockResolvedValue({ data: mockResponse });

      const result = await MemoryBackendService.initializeLakebaseTables({ embedding_dimension: 1024 });

      expect(apiClient.post).toHaveBeenCalledWith('/memory-backend/lakebase/initialize-tables', {
        embedding_dimension: 1024,
      });
      expect(result).toEqual(mockResponse);
    });

    it('should handle initialization errors', async () => {
      (apiClient.post as Mock).mockRejectedValue(new Error('Failed'));

      const result = await MemoryBackendService.initializeLakebaseTables();

      expect(result).toEqual({
        success: false,
        message: 'Failed to initialize tables',
      });
    });
  });

  describe('getLakebaseTableStats', () => {
    it('should fetch table stats successfully', async () => {
      const mockStats = {
        short_term: { table_name: 'crew_short_term_memory', exists: true, row_count: 10 },
        long_term: { table_name: 'crew_long_term_memory', exists: true, row_count: 5 },
        entity: { table_name: 'crew_entity_memory', exists: true, row_count: 8 },
      };
      (apiClient.get as Mock).mockResolvedValue({ data: mockStats });

      const result = await MemoryBackendService.getLakebaseTableStats('kasal-lakebase1');

      expect(apiClient.get).toHaveBeenCalledWith('/memory-backend/lakebase/table-stats', {
        params: { instance_name: 'kasal-lakebase1' },
      });
      expect(result).toEqual(mockStats);
    });

    it('should return empty object on error', async () => {
      (apiClient.get as Mock).mockRejectedValue(new Error('Not found'));

      const result = await MemoryBackendService.getLakebaseTableStats();

      expect(result).toEqual({});
    });
  });

  describe('getLakebaseTableData', () => {
    it('should fetch table data successfully', async () => {
      const mockResponse = {
        success: true,
        documents: [
          { id: 'doc1', text: 'hello', agent: 'researcher', metadata: {} },
        ],
        total: 1,
      };
      (apiClient.get as Mock).mockResolvedValue({ data: mockResponse });

      const result = await MemoryBackendService.getLakebaseTableData('crew_short_term_memory', 50, 'inst1');

      expect(apiClient.get).toHaveBeenCalledWith('/memory-backend/lakebase/table-data', {
        params: { table_name: 'crew_short_term_memory', limit: 50, instance_name: 'inst1' },
      });
      expect(result).toEqual(mockResponse);
    });

    it('should use default limit', async () => {
      const mockResponse = { success: true, documents: [], total: 0 };
      (apiClient.get as Mock).mockResolvedValue({ data: mockResponse });

      await MemoryBackendService.getLakebaseTableData('crew_long_term_memory');

      expect(apiClient.get).toHaveBeenCalledWith('/memory-backend/lakebase/table-data', {
        params: { table_name: 'crew_long_term_memory', limit: 50 },
      });
    });

    it('should return fallback on error', async () => {
      (apiClient.get as Mock).mockRejectedValue(new Error('Network error'));

      const result = await MemoryBackendService.getLakebaseTableData('crew_short_term_memory');

      expect(result).toEqual({ success: false, documents: [], message: 'Failed to fetch table data' });
    });
  });

  describe('getLakebaseEntityData', () => {
    it('should fetch entity data successfully', async () => {
      const mockResponse = {
        entities: [{ id: 'e1', name: 'Alice', type: 'person', attributes: {} }],
        relationships: [{ source: 'e1', target: 'e2', type: 'knows' }],
      };
      (apiClient.get as Mock).mockResolvedValue({ data: mockResponse });

      const result = await MemoryBackendService.getLakebaseEntityData('crew_memory', 200, 'inst1');

      expect(apiClient.get).toHaveBeenCalledWith('/memory-backend/lakebase/entity-data', {
        params: { memory_table: 'crew_memory', limit: 200, instance_name: 'inst1' },
      });
      expect(result).toEqual(mockResponse);
    });

    it('should use default parameters', async () => {
      const mockResponse = { entities: [], relationships: [] };
      (apiClient.get as Mock).mockResolvedValue({ data: mockResponse });

      await MemoryBackendService.getLakebaseEntityData();

      expect(apiClient.get).toHaveBeenCalledWith('/memory-backend/lakebase/entity-data', {
        params: { memory_table: 'crew_memory', limit: 200 },
      });
    });

    it('should return empty data on error', async () => {
      (apiClient.get as Mock).mockRejectedValue(new Error('Connection refused'));

      const result = await MemoryBackendService.getLakebaseEntityData();

      expect(result).toEqual({ entities: [], relationships: [] });
    });
  });
});