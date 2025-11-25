import apiClient from '../config/api/ApiConfig';
import { AxiosError } from 'axios';

export interface DatabricksConfig {
  workspace_url: string;
  warehouse_id: string;
  catalog: string;
  schema: string;

  enabled: boolean;

  // MLflow configuration
  mlflow_enabled?: boolean;
  mlflow_experiment_name?: string; // MLflow experiment name
  // MLflow Evaluation configuration
  evaluation_enabled?: boolean;
  evaluation_judge_model?: string; // Databricks judge endpoint route, e.g., "databricks:/<endpoint>"

  // Volume configuration fields
  volume_enabled?: boolean;
  volume_path?: string;
  volume_file_format?: 'json' | 'csv' | 'txt';
  volume_create_date_dirs?: boolean;
  // Knowledge source volume configuration
  knowledge_volume_enabled?: boolean;
  knowledge_volume_path?: string;
  knowledge_chunk_size?: number;
  knowledge_chunk_overlap?: number;
}

export interface DatabricksTokenStatus {
  personal_token_required: boolean;
  message: string;
}

export interface DatabricksConnectionStatus {
  status: string;
  message: string;
  connected: boolean;
}

export interface DatabricksEnvironment {
  is_databricks_apps: boolean;
  databricks_app_name: string | null;
  databricks_host: string | null;
  workspace_id: string | null;
  has_oauth_credentials: boolean;
  message: string;
}

export class DatabricksService {
  private static instance: DatabricksService;

  public static getInstance(): DatabricksService {
    if (!DatabricksService.instance) {
      DatabricksService.instance = new DatabricksService();
    }
    return DatabricksService.instance;
  }

  public async setDatabricksConfig(config: DatabricksConfig): Promise<DatabricksConfig> {
    try {
      const response = await apiClient.post<{status: string, message: string, config: DatabricksConfig}>(
        `/databricks/config`,
        config
      );
      return response.data.config;
    } catch (error) {
      if (error instanceof AxiosError) {
        throw new Error(error.response?.data?.detail || 'Failed to set Databricks configuration');
      }
      throw new Error('Failed to connect to the server');
    }
  }

  public async getDatabricksConfig(): Promise<DatabricksConfig | null> {
    try {
      const response = await apiClient.get<DatabricksConfig>(`/databricks/config`);
      return response.data;
    } catch (error) {
      if (error instanceof AxiosError) {
        if (error.response?.status === 404) {
          console.log('Databricks configuration not found - this is expected if Databricks integration is not set up');
          return null;
        }
        throw new Error(error.response?.data?.detail || 'Failed to get Databricks configuration');
      }
      throw new Error('Failed to connect to the server');
    }
  }

  public async checkPersonalTokenRequired(): Promise<DatabricksTokenStatus> {
    try {
      const response = await apiClient.get<DatabricksTokenStatus>(`/databricks/status/personal-token-required`);
      return response.data;
    } catch (error) {
      if (error instanceof AxiosError) {
        throw new Error(error.response?.data?.detail || 'Failed to check personal token status');
      }
      throw new Error('Failed to connect to the server');
    }
  }

  public async checkDatabricksConnection(): Promise<DatabricksConnectionStatus> {
    try {
      const response = await apiClient.get<DatabricksConnectionStatus>(`/databricks/connection`);
      return response.data;
    } catch (error) {
      if (error instanceof AxiosError) {
        throw new Error(error.response?.data?.detail || 'Failed to check Databricks connection');
      }
      throw new Error('Failed to connect to the server');
    }
  }

  public async getDatabricksEnvironment(): Promise<DatabricksEnvironment> {
    try {
      const response = await apiClient.get<DatabricksEnvironment>(`/databricks/environment`);
      return response.data;
    } catch (error) {
      if (error instanceof AxiosError) {
        throw new Error(error.response?.data?.detail || 'Failed to get Databricks environment');
      }
      throw new Error('Failed to connect to the server');
    }
  }

  // Static methods for DatabricksVolumeConfiguration component
  public static async getConfiguration(): Promise<DatabricksConfig | null> {
    const instance = DatabricksService.getInstance();
    return instance.getDatabricksConfig();
  }

  public static async updateConfiguration(config: DatabricksConfig): Promise<DatabricksConfig> {
    const instance = DatabricksService.getInstance();
    return instance.setDatabricksConfig(config);
  }
} 