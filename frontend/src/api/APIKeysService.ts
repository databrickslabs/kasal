import apiClient from '../config/api/ApiConfig';
import { AxiosError } from 'axios';
import { 
  ApiKey, 
  ApiKeyCreate, 
  ApiKeyUpdate, 
  DatabricksSecret, 
  DatabricksSecretCreate, 
  DatabricksSecretUpdate,
  DatabricksTokenRequest 
} from '../types/apiKeys';
import { DatabricksService } from './DatabricksService';

export type { 
  ApiKey, 
  ApiKeyCreate, 
  ApiKeyUpdate, 
  DatabricksSecret, 
  DatabricksSecretCreate, 
  DatabricksSecretUpdate,
  DatabricksTokenRequest 
};

export class APIKeysService {
  private static instance: APIKeysService;

  public static getInstance(): APIKeysService {
    if (!APIKeysService.instance) {
      APIKeysService.instance = new APIKeysService();
    }
    return APIKeysService.instance;
  }

  public async isDatabricksEnabled(): Promise<boolean> {
    try {
      const databricksService = DatabricksService.getInstance();
      const config = await databricksService.getDatabricksConfig();
      return config?.enabled ?? false;
    } catch (error) {
      console.error('Error checking Databricks enabled state:', error);
      return false;
    }
  }

  public async getAPIKeys(): Promise<ApiKey[]> {
    try {
      const response = await apiClient.get<ApiKey[]>(`/api-keys`);
      return response.data;
    } catch (error) {
      if (error instanceof AxiosError) {
        const errorMessage = error.response?.data?.detail || 'Failed to load API keys';
        throw new Error(errorMessage);
      }
      throw new Error('Failed to connect to the server');
    }
  }

  public async getDatabricksSecrets(): Promise<DatabricksSecret[]> {
    try {
      // Check if Databricks is enabled
      const databricksEnabled = await this.isDatabricksEnabled();
      
      if (!databricksEnabled) {
        return [];
      }

      const response = await apiClient.get<DatabricksSecret[]>(`/databricks-secrets`);
      return response.data;
    } catch (error) {
      if (error instanceof AxiosError) {
        const errorMessage = error.response?.data?.detail || 'Failed to load Databricks secrets';
        throw new Error(errorMessage);
      }
      throw new Error('Failed to connect to the server');
    }
  }

  public async createAPIKey(apiKey: ApiKeyCreate): Promise<{ message: string }> {
    try {
      const response = await apiClient.post<{ message: string }>(`/api-keys`, apiKey);
      return response.data;
    } catch (error) {
      if (error instanceof AxiosError) {
        const errorMessage = error.response?.data?.detail || 'Failed to create API key';
        throw new Error(errorMessage);
      }
      throw new Error('Failed to connect to the server');
    }
  }

  public async createDatabricksSecret(secret: DatabricksSecretCreate): Promise<{ message: string }> {
    try {
      const response = await apiClient.post<{ message: string }>(`/databricks-secrets`, secret);
      return response.data;
    } catch (error) {
      if (error instanceof AxiosError) {
        const errorMessage = error.response?.data?.detail || 'Failed to create Databricks secret';
        throw new Error(errorMessage);
      }
      throw new Error('Failed to connect to the server');
    }
  }

  public async updateAPIKey(name: string, data: ApiKeyUpdate): Promise<{ message: string }> {
    try {
      const response = await apiClient.put<{ message: string }>(`/api-keys/${name}`, data);
      return response.data;
    } catch (error) {
      if (error instanceof AxiosError) {
        const errorMessage = error.response?.data?.detail || 'Failed to update API key';
        throw new Error(errorMessage);
      }
      throw new Error('Failed to connect to the server');
    }
  }

  public async updateDatabricksSecret(name: string, data: DatabricksSecretUpdate): Promise<{ message: string }> {
    try {
      const response = await apiClient.put<{ message: string }>(`/databricks-secrets/${name}`, data);
      return response.data;
    } catch (error) {
      if (error instanceof AxiosError) {
        const errorMessage = error.response?.data?.detail || 'Failed to update Databricks secret';
        throw new Error(errorMessage);
      }
      throw new Error('Failed to connect to the server');
    }
  }

  public async deleteAPIKey(name: string): Promise<{ message: string }> {
    try {
      const response = await apiClient.delete<{ message: string }>(`/api-keys/${name}`);
      return response.data;
    } catch (error) {
      if (error instanceof AxiosError) {
        const errorMessage = error.response?.data?.detail || 'Failed to delete API key';
        throw new Error(errorMessage);
      }
      throw new Error('Failed to connect to the server');
    }
  }

  public async deleteDatabricksSecret(name: string): Promise<{ message: string }> {
    try {
      const response = await apiClient.delete<{ message: string }>(`/databricks-secrets/${name}`);
      return response.data;
    } catch (error) {
      if (error instanceof AxiosError) {
        const errorMessage = error.response?.data?.detail || 'Failed to delete Databricks secret';
        throw new Error(errorMessage);
      }
      throw new Error('Failed to connect to the server');
    }
  }

  public async setDatabricksToken(workspace_url: string, token: string): Promise<void> {
    try {
      if (!workspace_url) {
        throw new Error('Databricks workspace URL is required');
      }

      const request: DatabricksTokenRequest = {
        workspace_url,
        token
      };

      await apiClient.post(`/databricks/token`, request);
    } catch (error) {
      if (error instanceof AxiosError) {
        const errorMessage = error.response?.data?.detail || 'Failed to set Databricks token';
        throw new Error(errorMessage);
      }
      throw new Error('Failed to connect to the server');
    }
  }
}