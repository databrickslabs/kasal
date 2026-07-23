/**
 * Converter Service
 * API service for measure conversion operations
 */

import { apiClient } from '../config/api/ApiConfig';
import type {
  ConversionHistory,
  ConversionHistoryCreate,
  ConversionHistoryUpdate,
  ConversionHistoryFilter,
  ConversionHistoryListResponse,
  ConversionStatistics,
  ConversionJob,
  ConversionJobCreate,
  ConversionJobUpdate,
  ConversionJobStatusUpdate,
  ConversionJobListResponse,
  SavedConverterConfiguration,
  SavedConfigurationCreate,
  SavedConfigurationUpdate,
  SavedConfigurationFilter,
  SavedConfigurationListResponse,
} from '../types/converter';

export class ConverterService {
  private static readonly BASE_PATH = '/converters';

  // ===== Conversion History Methods =====

  /**
   * Create a new conversion history entry
   */
  static async createHistory(data: ConversionHistoryCreate): Promise<ConversionHistory> {
    const response = await apiClient.post<ConversionHistory>(
      `${this.BASE_PATH}/history`,
      data
    );
    return response.data;
  }

  /**
   * Get conversion history by ID
   */
  static async getHistory(historyId: number): Promise<ConversionHistory> {
    const response = await apiClient.get<ConversionHistory>(
      `${this.BASE_PATH}/history/${historyId}`
    );
    return response.data;
  }

  /**
   * Update conversion history
   */
  static async updateHistory(
    historyId: number,
    data: ConversionHistoryUpdate
  ): Promise<ConversionHistory> {
    const response = await apiClient.patch<ConversionHistory>(
      `${this.BASE_PATH}/history/${historyId}`,
      data
    );
    return response.data;
  }

  /**
   * List conversion history with filters
   */
  static async listHistory(
    filters?: ConversionHistoryFilter
  ): Promise<ConversionHistoryListResponse> {
    const params = new URLSearchParams();

    if (filters) {
      if (filters.source_format) params.append('source_format', filters.source_format);
      if (filters.target_format) params.append('target_format', filters.target_format);
      if (filters.status) params.append('status', filters.status);
      if (filters.execution_id) params.append('execution_id', filters.execution_id);
      if (filters.limit) params.append('limit', filters.limit.toString());
      if (filters.offset) params.append('offset', filters.offset.toString());
    }

    const response = await apiClient.get<ConversionHistoryListResponse>(
      `${this.BASE_PATH}/history?${params.toString()}`
    );
    return response.data;
  }

  /**
   * Get conversion statistics
   */
  static async getStatistics(days = 30): Promise<ConversionStatistics> {
    const response = await apiClient.get<ConversionStatistics>(
      `${this.BASE_PATH}/history/statistics?days=${days}`
    );
    return response.data;
  }

  // ===== Conversion Job Methods =====

  /**
   * Create a new conversion job
   */
  static async createJob(data: ConversionJobCreate): Promise<ConversionJob> {
    const response = await apiClient.post<ConversionJob>(
      `${this.BASE_PATH}/jobs`,
      data
    );
    return response.data;
  }

  /**
   * Get conversion job by ID
   */
  static async getJob(jobId: string): Promise<ConversionJob> {
    const response = await apiClient.get<ConversionJob>(
      `${this.BASE_PATH}/jobs/${jobId}`
    );
    return response.data;
  }

  /**
   * Update conversion job
   */
  static async updateJob(
    jobId: string,
    data: ConversionJobUpdate
  ): Promise<ConversionJob> {
    const response = await apiClient.patch<ConversionJob>(
      `${this.BASE_PATH}/jobs/${jobId}`,
      data
    );
    return response.data;
  }

  /**
   * Update job status and progress
   */
  static async updateJobStatus(
    jobId: string,
    data: ConversionJobStatusUpdate
  ): Promise<ConversionJob> {
    const response = await apiClient.patch<ConversionJob>(
      `${this.BASE_PATH}/jobs/${jobId}/status`,
      data
    );
    return response.data;
  }

  /**
   * List conversion jobs with optional status filter
   */
  static async listJobs(
    status?: string,
    limit = 50
  ): Promise<ConversionJobListResponse> {
    const params = new URLSearchParams();
    if (status) params.append('status', status);
    params.append('limit', limit.toString());

    const response = await apiClient.get<ConversionJobListResponse>(
      `${this.BASE_PATH}/jobs?${params.toString()}`
    );
    return response.data;
  }

  /**
   * Cancel a conversion job
   */
  static async cancelJob(jobId: string): Promise<ConversionJob> {
    const response = await apiClient.post<ConversionJob>(
      `${this.BASE_PATH}/jobs/${jobId}/cancel`
    );
    return response.data;
  }

  // ===== Saved Configuration Methods =====

  /**
   * Save a converter configuration
   */
  static async saveConfiguration(
    data: SavedConfigurationCreate
  ): Promise<SavedConverterConfiguration> {
    const response = await apiClient.post<SavedConverterConfiguration>(
      `${this.BASE_PATH}/configs`,
      data
    );
    return response.data;
  }

  /**
   * Get saved configuration by ID
   */
  static async getConfiguration(configId: number): Promise<SavedConverterConfiguration> {
    const response = await apiClient.get<SavedConverterConfiguration>(
      `${this.BASE_PATH}/configs/${configId}`
    );
    return response.data;
  }

  /**
   * Update saved configuration
   */
  static async updateConfiguration(
    configId: number,
    data: SavedConfigurationUpdate
  ): Promise<SavedConverterConfiguration> {
    const response = await apiClient.patch<SavedConverterConfiguration>(
      `${this.BASE_PATH}/configs/${configId}`,
      data
    );
    return response.data;
  }

  /**
   * Delete saved configuration
   */
  static async deleteConfiguration(configId: number): Promise<void> {
    await apiClient.delete(`${this.BASE_PATH}/configs/${configId}`);
  }

  /**
   * List saved configurations with filters
   */
  static async listConfigurations(
    filters?: SavedConfigurationFilter
  ): Promise<SavedConfigurationListResponse> {
    const params = new URLSearchParams();

    if (filters) {
      if (filters.source_format) params.append('source_format', filters.source_format);
      if (filters.target_format) params.append('target_format', filters.target_format);
      if (filters.is_public !== undefined) params.append('is_public', filters.is_public.toString());
      if (filters.is_template !== undefined) params.append('is_template', filters.is_template.toString());
      if (filters.search) params.append('search', filters.search);
      if (filters.limit) params.append('limit', filters.limit.toString());
    }

    const response = await apiClient.get<SavedConfigurationListResponse>(
      `${this.BASE_PATH}/configs?${params.toString()}`
    );
    return response.data;
  }

  /**
   * Mark configuration as used (increment use count)
   */
  static async trackConfigurationUsage(configId: number): Promise<SavedConverterConfiguration> {
    const response = await apiClient.post<SavedConverterConfiguration>(
      `${this.BASE_PATH}/configs/${configId}/use`
    );
    return response.data;
  }

  /**
   * Health check
   */
  static async healthCheck(): Promise<{ status: string; service: string; version: string }> {
    const response = await apiClient.get<{ status: string; service: string; version: string }>(
      `${this.BASE_PATH}/health`
    );
    return response.data;
  }
}

export default ConverterService;
