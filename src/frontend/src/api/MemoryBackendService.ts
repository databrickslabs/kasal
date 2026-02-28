/**
 * Service for managing memory backend configurations.
 * 
 * This service handles API communication for memory backend settings,
 * including validation, testing connections, and retrieving available indexes.
 */

import { apiClient } from '../config/api/ApiConfig';
import { MemoryBackendConfig, DatabricksMemoryConfig, LakebaseMemoryConfig } from '../types/memoryBackend';
import { AxiosError } from 'axios';

export interface DatabricksIndex {
  name: string;
  catalog: string;
  schema: string;
  table: string;
  dimension: number;
  total_records?: number;
}

export interface TestConnectionResult {
  success: boolean;
  message: string;
  details?: {
    endpoint_status?: string;
    indexes_found?: string[];
    error?: string;
  };
}

export interface AvailableIndexesResponse {
  indexes: DatabricksIndex[];
  endpoint_name: string;
}

export class MemoryBackendService {
  /**
   * Validate memory backend configuration
   */
  static async validateConfig(config: MemoryBackendConfig): Promise<{ valid: boolean; errors?: string[] }> {
    try {
      const response = await apiClient.post<{ valid: boolean; errors?: string[] }>(
        '/memory-backend/validate',
        config
      );
      return response.data;
    } catch (error) {
      console.error('Error validating memory backend config:', error);
      const errorMessage = error instanceof AxiosError 
        ? error.response?.data?.detail 
        : 'Failed to validate configuration';
      return {
        valid: false,
        errors: [errorMessage],
      };
    }
  }

  /**
   * Test connection to Databricks Vector Search
   */
  static async testDatabricksConnection(config: DatabricksMemoryConfig): Promise<TestConnectionResult> {
    try {
      const response = await apiClient.post<TestConnectionResult>(
        '/memory-backend/databricks/test-connection',
        config
      );
      return response.data;
    } catch (error) {
      console.error('Error testing Databricks connection:', error);
      const errorMessage = error instanceof AxiosError
        ? error.response?.data?.detail
        : error instanceof Error ? error.message : 'Failed to test connection';
      return {
        success: false,
        message: errorMessage || 'Failed to test connection',
        details: {
          error: errorMessage,
        },
      };
    }
  }

  /**
   * Get available Databricks indexes for a given endpoint
   */
  static async getAvailableDatabricksIndexes(
    endpointName: string,
    authConfig?: Partial<DatabricksMemoryConfig>
  ): Promise<AvailableIndexesResponse> {
    try {
      const response = await apiClient.post<AvailableIndexesResponse>(
        '/memory-backend/databricks/indexes',
        {
          endpoint_name: endpointName,
          ...authConfig,
        }
      );
      return response.data;
    } catch (error) {
      console.error('Error fetching Databricks indexes:', error);
      const errorMessage = error instanceof AxiosError
        ? error.response?.data?.detail
        : 'Failed to fetch indexes';
      throw new Error(errorMessage || 'Failed to fetch indexes');
    }
  }

  /**
   * Save memory backend configuration
   * Note: This might be saved as part of agent/crew configuration rather than separately
   */
  static async saveConfig(config: MemoryBackendConfig): Promise<{ success: boolean; message: string }> {
    try {
      const response = await apiClient.post<{ success: boolean; message: string }>(
        '/memory-backend/config',
        config
      );
      return response.data;
    } catch (error) {
      console.error('Error saving memory backend config:', error);
      const errorMessage = error instanceof AxiosError
        ? error.response?.data?.detail
        : 'Failed to save configuration';
      return {
        success: false,
        message: errorMessage || 'Failed to save configuration',
      };
    }
  }

  /**
   * Get current memory backend configuration
   */
  static async getConfig(): Promise<MemoryBackendConfig | null> {
    try {
      // Get all configs and find the default one
      const response = await apiClient.get<MemoryBackendConfig[]>('/memory-backend/configs');
      const configs = response.data;

      if (!configs || configs.length === 0) {
        return null;
      }

      // Find the default configuration
      const defaultConfig = configs.find(config => config.is_default && config.is_active);
      return defaultConfig || null;
    } catch (error) {
      console.error('Error fetching memory backend config:', error);
      return null;
    }
  }

  /**
   * Get memory usage statistics for a crew
   */
  static async getMemoryStats(crewId: string): Promise<{
    short_term_count?: number;
    long_term_count?: number;
    entity_count?: number;
    total_size_mb?: number;
  }> {
    try {
      const response = await apiClient.get<{
        short_term_count?: number;
        long_term_count?: number;
        entity_count?: number;
        total_size_mb?: number;
      }>(`/memory-backend/stats/${crewId}`);
      return response.data;
    } catch (error) {
      console.error('Error fetching memory stats:', error);
      return {};
    }
  }

  /**
   * Clear memory for a specific crew
   */
  static async clearMemory(
    crewId: string,
    memoryTypes: ('short_term' | 'long_term' | 'entity')[]
  ): Promise<{ success: boolean; message: string }> {
    try {
      const response = await apiClient.post<{ success: boolean; message: string }>(
        `/memory-backend/clear/${crewId}`,
        { memory_types: memoryTypes }
      );
      return response.data;
    } catch (error) {
      console.error('Error clearing memory:', error);
      const errorMessage = error instanceof AxiosError
        ? error.response?.data?.detail
        : 'Failed to clear memory';
      return {
        success: false,
        message: errorMessage || 'Failed to clear memory',
      };
    }
  }

  /**
   * Create a Databricks Vector Search index
   */
  static async createDatabricksIndex(
    config: DatabricksMemoryConfig,
    indexType: 'short_term' | 'long_term' | 'entity' | 'document',
    catalog: string,
    schema: string,
    tableName: string,
    primaryKey = 'id'
  ): Promise<{
    success: boolean;
    message: string;
    details?: {
      index_name?: string;
      index_type?: string;
      auth_method?: string;
      embedding_dimension?: number;
      error?: string;
    };
  }> {
    try {
      const response = await apiClient.post<{
        success: boolean;
        message: string;
        details?: {
          index_name?: string;
          index_type?: string;
          auth_method?: string;
          embedding_dimension?: number;
          error?: string;
        };
      }>('/memory-backend/databricks/create-index', {
        config,
        index_type: indexType,
        catalog,
        schema,
        table_name: tableName,
        primary_key: primaryKey,
      });
      return response.data;
    } catch (error) {
      console.error('Error creating Databricks index:', error);
      const errorMessage = error instanceof AxiosError
        ? error.response?.data?.detail
        : 'Failed to create index';
      return {
        success: false,
        message: errorMessage || 'Failed to create index',
        details: {
          error: errorMessage,
        },
      };
    }
  }

  /**
   * One-click setup for Databricks Vector Search
   */
  static async oneClickDatabricksSetup(
    workspaceUrl: string,
    catalog = 'ml',
    schema = 'agents'
  ): Promise<{
    success: boolean;
    message: string;
    endpoints?: {
      memory?: {
        name: string;
        type: string;
        status: string;
      };
      document?: {
        name: string;
        type: string;
        status: string;
        error?: string;
      };
    };
    indexes?: {
      short_term?: {
        name: string;
        status: string;
      };
      long_term?: {
        name: string;
        status: string;
      };
      entity?: {
        name: string;
        status: string;
      };
    };
    config?: DatabricksMemoryConfig;
    backend_id?: string;
    error?: string;
    warning?: string;
  }> {
    try {
      const response = await apiClient.post<{
        success: boolean;
        message: string;
        endpoints?: {
          memory?: {
            name: string;
            type: string;
            status: string;
          };
          document?: {
            name: string;
            type: string;
            status: string;
            error?: string;
          };
        };
        indexes?: {
          short_term?: {
            name: string;
            status: string;
          };
          long_term?: {
            name: string;
            status: string;
          };
          entity?: {
            name: string;
            status: string;
          };
        };
        config?: DatabricksMemoryConfig;
        backend_id?: string;
        error?: string;
        warning?: string;
      }>('/memory-backend/databricks/one-click-setup', {
        workspace_url: workspaceUrl,
        catalog,
        schema,
      });
      return response.data;
    } catch (error) {
      console.error('Error in one-click setup:', error);
      const errorMessage = error instanceof AxiosError
        ? error.response?.data?.detail
        : 'Failed to complete setup';
      return {
        success: false,
        message: errorMessage || 'Failed to complete setup',
        error: errorMessage,
      };
    }
  }

  /**
   * Test connection to Lakebase and verify pgvector availability
   */
  static async testLakebaseConnection(instanceName?: string): Promise<TestConnectionResult> {
    try {
      const response = await apiClient.post<TestConnectionResult>(
        '/memory-backend/lakebase/test-connection',
        instanceName ? { instance_name: instanceName } : {}
      );
      return response.data;
    } catch (error) {
      console.error('Error testing Lakebase connection:', error);
      const errorMessage = error instanceof AxiosError
        ? error.response?.data?.detail
        : error instanceof Error ? error.message : 'Failed to test connection';
      return {
        success: false,
        message: errorMessage || 'Failed to test connection',
        details: { error: errorMessage },
      };
    }
  }

  /**
   * Initialize Lakebase pgvector memory tables
   */
  static async initializeLakebaseTables(
    config?: Partial<LakebaseMemoryConfig>
  ): Promise<{ success: boolean; message: string; tables?: Record<string, { success: boolean; table_name: string; message: string }> }> {
    try {
      const response = await apiClient.post<{ success: boolean; message: string; tables?: Record<string, { success: boolean; table_name: string; message: string }> }>(
        '/memory-backend/lakebase/initialize-tables',
        config || {}
      );
      return response.data;
    } catch (error) {
      console.error('Error initializing Lakebase tables:', error);
      const errorMessage = error instanceof AxiosError
        ? error.response?.data?.detail
        : 'Failed to initialize tables';
      return {
        success: false,
        message: errorMessage || 'Failed to initialize tables',
      };
    }
  }

  /**
   * Get Lakebase memory table statistics
   */
  static async getLakebaseTableStats(instanceName?: string): Promise<Record<string, { table_name: string; exists: boolean; row_count: number }>> {
    try {
      const params = instanceName ? { instance_name: instanceName } : {};
      const response = await apiClient.get<Record<string, { table_name: string; exists: boolean; row_count: number }>>(
        '/memory-backend/lakebase/table-stats',
        { params }
      );
      return response.data;
    } catch (error) {
      console.error('Error fetching Lakebase table stats:', error);
      return {};
    }
  }

  /**
   * Get rows from a Lakebase memory table
   */
  static async getLakebaseTableData(
    tableName: string,
    limit = 50,
    instanceName?: string
  ): Promise<{ success: boolean; documents: LakebaseDocument[]; total?: number; message?: string }> {
    try {
      const params: Record<string, string | number> = { table_name: tableName, limit };
      if (instanceName) params.instance_name = instanceName;
      const response = await apiClient.get<{ success: boolean; documents: LakebaseDocument[]; total?: number; message?: string }>(
        '/memory-backend/lakebase/table-data',
        { params }
      );
      return response.data;
    } catch (error) {
      console.error('Error fetching Lakebase table data:', error);
      return { success: false, documents: [], message: 'Failed to fetch table data' };
    }
  }

  /**
   * Get entity data from Lakebase for graph visualization
   */
  static async getLakebaseEntityData(
    entityTable = 'crew_entity_memory',
    limit = 200,
    instanceName?: string
  ): Promise<{ entities: LakebaseEntity[]; relationships: LakebaseRelationship[] }> {
    try {
      const params: Record<string, string | number> = { entity_table: entityTable, limit };
      if (instanceName) params.instance_name = instanceName;
      const response = await apiClient.get<{ entities: LakebaseEntity[]; relationships: LakebaseRelationship[] }>(
        '/memory-backend/lakebase/entity-data',
        { params }
      );
      return response.data;
    } catch (error) {
      console.error('Error fetching Lakebase entity data:', error);
      return { entities: [], relationships: [] };
    }
  }
}

export interface LakebaseDocument {
  id: string;
  crew_id: string;
  group_id: string;
  session_id: string;
  agent: string;
  text: string;
  metadata: Record<string, unknown>;
  score: number | null;
  created_at: string | null;
  updated_at: string | null;
}

export interface LakebaseEntity {
  id: string;
  name: string;
  type: string;
  attributes: Record<string, unknown>;
}

export interface LakebaseRelationship {
  source: string;
  target: string;
  type: string;
}