import { AxiosError } from 'axios';
import { ConfigValue } from '../types/tool';
import { apiClient } from '../config/api/ApiConfig';

export interface Tool {
  id: number;
  title: string;
  description: string;
  icon: string;
  config?: Record<string, ConfigValue>;
  category?: 'PreBuilt' | 'Custom' | 'UnityCatalog';
  enabled?: boolean;
}

// Define a type for the error response
interface ErrorResponse {
  detail?: string;
}

export class ToolService {
  static async getTool(id: number): Promise<Tool | null> {
    try {
      const response = await apiClient.get<Tool>(`/tools/${id}`);
      console.log('Fetched tool:', response.data);
      return response.data;
    } catch (error) {
      console.error('Error fetching tool:', error);
      return null;
    }
  }

  static async createTool(tool: Omit<Tool, 'id'>): Promise<Tool> {
    try {
      const response = await apiClient.post<Tool>('/tools', tool);
      console.log('Created tool:', response.data);
      return response.data;
    } catch (error) {
      console.error('Error creating tool:', error);
      const axiosError = error as AxiosError<ErrorResponse>;
      throw new Error(axiosError.response?.data?.detail || 'Error creating tool');
    }
  }

  static async updateTool(id: number, tool: Partial<Tool>): Promise<Tool> {
    try {
      console.log('UPDATE TOOL - Input:', {
        id,
        tool
      });

      // Create the update data with the config
      const updateData = {
        title: tool.title,
        description: tool.description,
        icon: tool.icon,
        config: tool.config
      };

      console.log('UPDATE TOOL - Final request data:', JSON.stringify(updateData, null, 2));

      const response = await apiClient.put<Tool>(
        `/tools/${id}`,
        updateData
      );

      console.log('UPDATE TOOL - Response:', JSON.stringify(response.data, null, 2));
      return response.data;
    } catch (error) {
      console.error('Error updating tool:', error);
      const axiosError = error as AxiosError<ErrorResponse>;
      throw new Error(axiosError.response?.data?.detail || 'Error updating tool');
    }
  }

  static async deleteTool(id: number): Promise<void> {
    try {
      const response = await apiClient.delete<{ message: string }>(`/tools/${id}`);
      console.log('Deleted tool:', response.data.message);
    } catch (error) {
      console.error('Error deleting tool:', error);
      const axiosError = error as AxiosError<ErrorResponse>;
      throw new Error(axiosError.response?.data?.detail || 'Error deleting tool');
    }
  }

  static async listTools(): Promise<Tool[]> {
    try {
      const response = await apiClient.get<Tool[]>('/tools');
      return response.data;
    } catch (error) {
      console.error('Error fetching tools:', error);
      const axiosError = error as AxiosError<ErrorResponse>;
      throw new Error(axiosError.response?.data?.detail || 'Error fetching tools');
    }
  }

  static async toggleToolEnabled(id: number): Promise<{ enabled: boolean }> {
    try {
      const response = await apiClient.patch<{ message: string, enabled: boolean }>(
        `/tools/${id}/toggle-enabled`
      );
      console.log('Toggled tool enabled state:', response.data);
      return { enabled: response.data.enabled };
    } catch (error) {
      console.error('Error toggling tool enabled state:', error);
      const axiosError = error as AxiosError<ErrorResponse>;
      throw new Error(axiosError.response?.data?.detail || 'Error toggling tool enabled state');
    }
  }

  static async enableAllTools(): Promise<Tool[]> {
    try {
      const response = await apiClient.patch<Tool[]>('/tools/enable-all');
      console.log('Enabled all tools:', response.data);
      return response.data;
    } catch (error) {
      console.error('Error enabling all tools:', error);
      const axiosError = error as AxiosError<ErrorResponse>;
      throw new Error(axiosError.response?.data?.detail || 'Error enabling all tools');
    }
  }

  static async disableAllTools(): Promise<Tool[]> {
    try {
      const response = await apiClient.patch<Tool[]>('/tools/disable-all');
      console.log('Disabled all tools:', response.data);
      return response.data;
    } catch (error) {
      console.error('Error disabling all tools:', error);
      const axiosError = error as AxiosError<ErrorResponse>;
      throw new Error(axiosError.response?.data?.detail || 'Error disabling all tools');
    }
  }
} 