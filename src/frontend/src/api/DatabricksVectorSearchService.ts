/**
 * Service for Databricks Vector Search operations
 */

import { apiClient } from '../config/api/ApiConfig';
import { SetupResult, DatabricksMemoryConfig, MemoryBackendType } from '../types/memoryBackend';

class DatabricksVectorSearchService {
  /**
   * Performs one-click setup for Databricks Vector Search
   */
  static async performOneClickSetup(config: {
    workspace_url: string;
    catalog: string;
    schema: string;
    embedding_dimension: number;
  }): Promise<SetupResult> {
    const response = await apiClient.post<SetupResult>(
      '/memory-backend/databricks/one-click-setup',
      config
    );
    return response.data;
  }

  /**
   * Deletes all memory backend configurations
   */
  static async deleteAllConfigurations(): Promise<void> {
    const allConfigsResponse = await apiClient.get('/memory-backend/configs');
    
    // Delete each configuration
    for (const config of allConfigsResponse.data || []) {
      await apiClient.delete(`/memory-backend/configs/${config.id}`);
    }
  }

  /**
   * Cleans up disabled configurations
   */
  static async cleanupDisabledConfigurations(): Promise<void> {
    try {
      await apiClient.delete('/memory-backend/configs/disabled/cleanup');
    } catch (error) {
      // It's okay if cleanup fails
      console.log('No disabled configurations to clean up');
    }
  }

  /**
   * Switches to disabled mode
   */
  static async switchToDisabledMode(): Promise<{ id: string; backend_type: string }> {
    return await apiClient.post('/memory-backend/configs/switch-to-disabled');
  }

  /**
   * Updates backend configuration
   */
  static async updateBackendConfiguration(backendId: string, config: {
    databricks_config: DatabricksMemoryConfig
  }): Promise<{ id: string; backend_type: string }> {
    const response = await apiClient.put(
      `/memory-backend/configs/${backendId}`,
      config
    );
    return response.data;
  }

  /**
   * Verifies Databricks resources
   */
  static async verifyResources(workspaceUrl: string, backendId?: string): Promise<{
    success: boolean;
    resources?: {
      endpoints: Record<string, { name: string; state?: string; ready?: boolean }>;
      indexes: Record<string, { name: string; status?: string; index_type?: string }>;
    };
  }> {
    const response = await apiClient.get('/memory-backend/databricks/verify-resources', {
      params: {
        workspace_url: workspaceUrl,
        backend_id: backendId
      }
    });
    return response.data;
  }

  /**
   * Deletes a Vector Search index
   */
  static async deleteIndex(config: {
    workspace_url: string;
    index_name: string;
    endpoint_name: string;
  }): Promise<{ success: boolean; message?: string }> {
    const response = await apiClient.delete('/memory-backend/databricks/index', {
      data: config
    });
    return response.data;
  }

  /**
   * Gets information about a Vector Search index
   */
  static async getIndexInfo(
    workspaceUrl: string,
    indexName: string,
    endpointName: string
  ): Promise<{
    success: boolean;
    doc_count?: number;
    status?: string;
    ready?: boolean;
    index_type?: string;
    message?: string;
  }> {
    const response = await apiClient.get('/memory-backend/databricks/index-info', {
      params: {
        workspace_url: workspaceUrl,
        index_name: indexName,
        endpoint_name: endpointName
      }
    });
    return response.data;
  }

  /**
   * Empties a Vector Search index
   */
  static async emptyIndex(config: {
    workspace_url: string;
    index_name: string;
    endpoint_name: string;
    batch_size?: number;
  }): Promise<{ success: boolean; message?: string; deleted_count?: number }> {
    const response = await apiClient.post('/memory-backend/databricks/empty-index', config);
    return response.data;
  }

  /**
   * Re-seeds documentation in Vector Search
   */
  static async reseedDocumentation(workspaceUrl: string): Promise<{
    success: boolean;
    message?: string;
    uploaded_files?: number;
    total_chunks?: number;
    failed_files?: string[];
  }> {
    const response = await apiClient.post('/memory-backend/databricks/reseed-documentation', {
      workspace_url: workspaceUrl
    });
    return response.data;
  }

  /**
   * Deletes a Vector Search endpoint
   */
  static async deleteEndpoint(config: {
    workspace_url: string;
    endpoint_name: string;
  }): Promise<{ success: boolean; message?: string }> {
    const response = await apiClient.delete('/memory-backend/databricks/endpoint', {
      data: config
    });
    return response.data;
  }

  /**
   * Saves manual configuration
   */
  static async saveManualConfiguration(config: {
    name: string;
    description: string;
    backend_type: MemoryBackendType;
    enable_short_term: boolean;
    enable_long_term: boolean;
    enable_entity: boolean;
    databricks_config: DatabricksMemoryConfig;
  }, existingConfigId?: string): Promise<{ data: { id: string } }> {
    if (existingConfigId) {
      // Update existing
      return await apiClient.put(`/memory-backend/configs/${existingConfigId}`, config);
    } else {
      // Create new
      return await apiClient.post('/memory-backend/configs', config);
    }
  }
}

export default DatabricksVectorSearchService;