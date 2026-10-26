import apiClient from '../../shared/api/client';
import { AxiosError } from 'axios';
import {
  ApiKey,
  ApiKeyCreate,
  ApiKeyUpdate
} from '../../types/config/apiKeys';

export type { ApiKey, ApiKeyCreate, ApiKeyUpdate };

export class APIKeysService {
  private static instance: APIKeysService;

  public static getInstance(): APIKeysService {
    if (!APIKeysService.instance) {
      APIKeysService.instance = new APIKeysService();
    }
    return APIKeysService.instance;
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


}
