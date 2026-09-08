/**
 * Service for managing memory backend configurations.
 * 
 * This service handles API communication for memory backend settings,
 * including validation, testing connections, and retrieving available indexes.
 */

import { apiClient } from '../../shared/api/client';
import { MemoryBackendConfig, LakebaseMemoryConfig } from '../../types/config/memoryBackend';
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
    // Lakebase pgvector connection test fields
    pgvector_available?: boolean;
    pg_version?: string;
    // Present when pgvector is not yet enabled: guidance + exact SQL the
    // Lakebase instance owner must run (the app SP cannot create the extension).
    pgvector_setup_instructions?: string;
    pgvector_setup_sql?: string;
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
   * Persist the local (DEFAULT / SQLite) memory backend as an ACTIVE config so
   * crew execution loads its memory tuning via ``get_active_config``. Saving
   * to localStorage alone never reaches the backend runtime, so the memory LLM /
   * recall thresholds were silently ignored for local memory. Mirrors the
   * Lakebase save flow on the backend.
   */
  static async saveDefaultConfig(
    config: MemoryBackendConfig
  ): Promise<{ success: boolean; backend_id?: string; message: string }> {
    try {
      const response = await apiClient.post<{ success: boolean; backend_id?: string; message: string }>(
        '/memory-backend/default/save-config',
        { cognitive_config: config.cognitive_config ?? null }
      );
      return response.data;
    } catch (error) {
      console.error('Error saving default memory backend config:', error);
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
      const response = await apiClient.get<MemoryBackendConfig | null>(
        '/memory-backend/configs/effective',
      );
      return response.data;
    } catch (error) {
      console.error('Error fetching memory backend config:', error);
      return null;
    }
  }


  /**
   * Switch the workspace to the disabled memory mode.
   */
  static async switchToDisabledMode(): Promise<{ id: string; backend_type: string }> {
    return await apiClient.post('/memory-backend/configs/switch-to-disabled');
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


}
