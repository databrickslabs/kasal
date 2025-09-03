import DatabricksVectorSearchService from './DatabricksVectorSearchService';
import { apiClient } from '../config/api/ApiConfig';
import { SetupResult, DatabricksMemoryConfig, MemoryBackendType } from '../types/memoryBackend';

jest.mock('../config/api/ApiConfig');

describe('DatabricksVectorSearchService', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    jest.spyOn(console, 'log').mockImplementation(() => {});
  });

  afterEach(() => {
    (console.log as jest.Mock).mockRestore();
  });

  describe('performOneClickSetup', () => {
    it('should perform one-click setup successfully', async () => {
      const config = {
        workspace_url: 'https://example.databricks.com',
        catalog: 'ml',
        schema: 'agents',
        embedding_dimension: 1536,
      };
      const mockResponse: SetupResult = {
        success: true,
        message: 'Setup completed',
        endpoints: {
          memory: {
            name: 'kasal-memory-endpoint',
            type: 'STANDARD',
            status: 'ONLINE',
          },
        },
        indexes: {
          short_term: {
            name: 'ml.agents.short_term_memories',
            status: 'ONLINE',
          },
          long_term: {
            name: 'ml.agents.long_term_memories',
            status: 'ONLINE',
          },
          entity: {
            name: 'ml.agents.entity_memories',
            status: 'ONLINE',
          },
        },
        config: {
          workspace_url: 'https://example.databricks.com',
          endpoint_name: 'kasal-memory-endpoint',
          short_term_index: 'ml.agents.short_term_memories',
          long_term_index: 'ml.agents.long_term_memories',
          entity_index: 'ml.agents.entity_memories',
        },
        backend_id: 'backend-123',
      };
      (apiClient.post as jest.Mock).mockResolvedValue({ data: mockResponse });

      const result = await DatabricksVectorSearchService.performOneClickSetup(config);

      expect(apiClient.post).toHaveBeenCalledWith('/memory-backend/databricks/one-click-setup', config);
      expect(result).toEqual(mockResponse);
    });
  });

  describe('deleteAllConfigurations', () => {
    it('should delete all configurations', async () => {
      const mockConfigs = [
        { id: 'config-1' },
        { id: 'config-2' },
        { id: 'config-3' },
      ];
      (apiClient.get as jest.Mock).mockResolvedValue({ data: mockConfigs });
      (apiClient.delete as jest.Mock).mockResolvedValue({});

      await DatabricksVectorSearchService.deleteAllConfigurations();

      expect(apiClient.get).toHaveBeenCalledWith('/memory-backend/configs');
      expect(apiClient.delete).toHaveBeenCalledTimes(3);
      expect(apiClient.delete).toHaveBeenCalledWith('/memory-backend/configs/config-1');
      expect(apiClient.delete).toHaveBeenCalledWith('/memory-backend/configs/config-2');
      expect(apiClient.delete).toHaveBeenCalledWith('/memory-backend/configs/config-3');
    });

    it('should handle empty configurations', async () => {
      (apiClient.get as jest.Mock).mockResolvedValue({ data: [] });

      await DatabricksVectorSearchService.deleteAllConfigurations();

      expect(apiClient.get).toHaveBeenCalledWith('/memory-backend/configs');
      expect(apiClient.delete).not.toHaveBeenCalled();
    });

    it('should handle null configurations', async () => {
      (apiClient.get as jest.Mock).mockResolvedValue({ data: null });

      await DatabricksVectorSearchService.deleteAllConfigurations();

      expect(apiClient.get).toHaveBeenCalledWith('/memory-backend/configs');
      expect(apiClient.delete).not.toHaveBeenCalled();
    });
  });

  describe('cleanupDisabledConfigurations', () => {
    it('should cleanup disabled configurations', async () => {
      (apiClient.delete as jest.Mock).mockResolvedValue({});

      await DatabricksVectorSearchService.cleanupDisabledConfigurations();

      expect(apiClient.delete).toHaveBeenCalledWith('/memory-backend/configs/disabled/cleanup');
    });

    it('should handle cleanup errors gracefully', async () => {
      (apiClient.delete as jest.Mock).mockRejectedValue(new Error('Not found'));

      await expect(DatabricksVectorSearchService.cleanupDisabledConfigurations()).resolves.not.toThrow();
      expect(console.log).toHaveBeenCalledWith('No disabled configurations to clean up');
    });
  });

  describe('switchToDisabledMode', () => {
    it('should switch to disabled mode', async () => {
      const mockResponse = { id: 'disabled-config-id', backend_type: 'disabled' };
      (apiClient.post as jest.Mock).mockResolvedValue(mockResponse);

      const result = await DatabricksVectorSearchService.switchToDisabledMode();

      expect(apiClient.post).toHaveBeenCalledWith('/memory-backend/configs/switch-to-disabled');
      expect(result).toEqual(mockResponse);
    });
  });

  describe('updateBackendConfiguration', () => {
    it('should update backend configuration', async () => {
      const backendId = 'backend-123';
      const config = {
        databricks_config: {
          workspace_url: 'https://example.databricks.com',
          endpoint_name: 'updated-endpoint',
          short_term_index: 'short_term_index',
          long_term_index: 'long_term_index',
          entity_index: 'entity_index',
        } as DatabricksMemoryConfig,
      };
      const mockResponse = { id: backendId, backend_type: 'databricks' };
      (apiClient.put as jest.Mock).mockResolvedValue({ data: mockResponse });

      const result = await DatabricksVectorSearchService.updateBackendConfiguration(backendId, config);

      expect(apiClient.put).toHaveBeenCalledWith(`/memory-backend/configs/${backendId}`, config);
      expect(result).toEqual(mockResponse);
    });
  });

  describe('verifyResources', () => {
    it('should verify resources successfully', async () => {
      const workspaceUrl = 'https://example.databricks.com';
      const backendId = 'backend-123';
      const mockResponse = {
        success: true,
        resources: {
          endpoints: {
            memory: { name: 'kasal-memory-endpoint', state: 'ONLINE', ready: true },
          },
          indexes: {
            short_term: { name: 'short_term_index', status: 'ONLINE', index_type: 'DELTA_SYNC' },
          },
        },
      };
      (apiClient.get as jest.Mock).mockResolvedValue({ data: mockResponse });

      const result = await DatabricksVectorSearchService.verifyResources(workspaceUrl, backendId);

      expect(apiClient.get).toHaveBeenCalledWith('/memory-backend/databricks/verify-resources', {
        params: {
          workspace_url: workspaceUrl,
          backend_id: backendId,
        },
      });
      expect(result).toEqual(mockResponse);
    });

    it('should verify resources without backendId', async () => {
      const workspaceUrl = 'https://example.databricks.com';
      const mockResponse = { success: false };
      (apiClient.get as jest.Mock).mockResolvedValue({ data: mockResponse });

      const result = await DatabricksVectorSearchService.verifyResources(workspaceUrl);

      expect(apiClient.get).toHaveBeenCalledWith('/memory-backend/databricks/verify-resources', {
        params: {
          workspace_url: workspaceUrl,
          backend_id: undefined,
        },
      });
      expect(result).toEqual(mockResponse);
    });
  });

  describe('deleteIndex', () => {
    it('should delete index successfully', async () => {
      const config = {
        workspace_url: 'https://example.databricks.com',
        index_name: 'test-index',
        endpoint_name: 'test-endpoint',
      };
      const mockResponse = { success: true, message: 'Index deleted' };
      (apiClient.delete as jest.Mock).mockResolvedValue({ data: mockResponse });

      const result = await DatabricksVectorSearchService.deleteIndex(config);

      expect(apiClient.delete).toHaveBeenCalledWith('/memory-backend/databricks/index', {
        data: config,
      });
      expect(result).toEqual(mockResponse);
    });
  });

  describe('getIndexInfo', () => {
    it('should get index info successfully', async () => {
      const workspaceUrl = 'https://example.databricks.com';
      const indexName = 'test-index';
      const endpointName = 'test-endpoint';
      const mockResponse = {
        success: true,
        doc_count: 100,
        status: 'ONLINE',
        ready: true,
        index_type: 'DELTA_SYNC',
      };
      (apiClient.get as jest.Mock).mockResolvedValue({ data: mockResponse });

      const result = await DatabricksVectorSearchService.getIndexInfo(workspaceUrl, indexName, endpointName);

      expect(apiClient.get).toHaveBeenCalledWith('/memory-backend/databricks/index-info', {
        params: {
          workspace_url: workspaceUrl,
          index_name: indexName,
          endpoint_name: endpointName,
        },
      });
      expect(result).toEqual(mockResponse);
    });
  });

  describe('emptyIndex', () => {
    it('should empty index successfully', async () => {
      const config = {
        workspace_url: 'https://example.databricks.com',
        index_name: 'test-index',
        endpoint_name: 'test-endpoint',
        batch_size: 100,
      };
      const mockResponse = { success: true, message: 'Index emptied', deleted_count: 50 };
      (apiClient.post as jest.Mock).mockResolvedValue({ data: mockResponse });

      const result = await DatabricksVectorSearchService.emptyIndex(config);

      expect(apiClient.post).toHaveBeenCalledWith('/memory-backend/databricks/empty-index', config);
      expect(result).toEqual(mockResponse);
    });

    it('should empty index without batch_size', async () => {
      const config = {
        workspace_url: 'https://example.databricks.com',
        index_name: 'test-index',
        endpoint_name: 'test-endpoint',
      };
      const mockResponse = { success: true, message: 'Index emptied' };
      (apiClient.post as jest.Mock).mockResolvedValue({ data: mockResponse });

      const result = await DatabricksVectorSearchService.emptyIndex(config);

      expect(apiClient.post).toHaveBeenCalledWith('/memory-backend/databricks/empty-index', config);
      expect(result).toEqual(mockResponse);
    });
  });

  describe('reseedDocumentation', () => {
    it('should reseed documentation successfully', async () => {
      const workspaceUrl = 'https://example.databricks.com';
      const mockResponse = {
        success: true,
        message: 'Documentation reseeded',
        uploaded_files: 10,
        total_chunks: 150,
        failed_files: [],
      };
      (apiClient.post as jest.Mock).mockResolvedValue({ data: mockResponse });

      const result = await DatabricksVectorSearchService.reseedDocumentation(workspaceUrl);

      expect(apiClient.post).toHaveBeenCalledWith('/memory-backend/databricks/reseed-documentation', {
        workspace_url: workspaceUrl,
      });
      expect(result).toEqual(mockResponse);
    });
  });

  describe('deleteEndpoint', () => {
    it('should delete endpoint successfully', async () => {
      const config = {
        workspace_url: 'https://example.databricks.com',
        endpoint_name: 'test-endpoint',
      };
      const mockResponse = { success: true, message: 'Endpoint deleted' };
      (apiClient.delete as jest.Mock).mockResolvedValue({ data: mockResponse });

      const result = await DatabricksVectorSearchService.deleteEndpoint(config);

      expect(apiClient.delete).toHaveBeenCalledWith('/memory-backend/databricks/endpoint', {
        data: config,
      });
      expect(result).toEqual(mockResponse);
    });
  });

  describe('saveManualConfiguration', () => {
    const mockConfig = {
      name: 'Test Config',
      description: 'Test Description',
      backend_type: 'databricks' as MemoryBackendType,
      enable_short_term: true,
      enable_long_term: true,
      enable_entity: true,
      databricks_config: {
        workspace_url: 'https://example.databricks.com',
        endpoint_name: 'test-endpoint',
        short_term_index: 'short_term_index',
        long_term_index: 'long_term_index',
        entity_index: 'entity_index',
      } as DatabricksMemoryConfig,
    };

    it('should create new configuration', async () => {
      const mockResponse = { data: { id: 'new-config-id' } };
      (apiClient.post as jest.Mock).mockResolvedValue(mockResponse);

      const result = await DatabricksVectorSearchService.saveManualConfiguration(mockConfig);

      expect(apiClient.post).toHaveBeenCalledWith('/memory-backend/configs', mockConfig);
      expect(result).toEqual(mockResponse);
    });

    it('should update existing configuration', async () => {
      const existingConfigId = 'existing-config-id';
      const mockResponse = { data: { id: existingConfigId } };
      (apiClient.put as jest.Mock).mockResolvedValue(mockResponse);

      const result = await DatabricksVectorSearchService.saveManualConfiguration(mockConfig, existingConfigId);

      expect(apiClient.put).toHaveBeenCalledWith(`/memory-backend/configs/${existingConfigId}`, mockConfig);
      expect(result).toEqual(mockResponse);
    });
  });
});