import apiClient from '../config/api/ApiConfig';
import { AxiosError } from 'axios';

export interface DatabricksConfig {
  workspace_url: string;
  warehouse_id: string;
  catalog: string;
  schema: string;
  secret_scope: string;
  enabled: boolean;
  apps_enabled: boolean;
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
} 