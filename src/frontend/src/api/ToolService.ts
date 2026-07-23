import { AxiosError } from 'axios';
import { ConfigValue } from '../types/tool';
import { apiClient } from '../config/api/ApiConfig';

export interface Tool {
  id: number;
  title: string;
  description: string;
  icon: string;
  config?: Record<string, ConfigValue>;
  category?: 'PreBuilt' | 'Custom';
  enabled?: boolean;
  group_id?: string; // present when this is a workspace-specific override
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

  // Returns only tools that are enabled for the current workspace (group)
  static async listEnabledTools(): Promise<Tool[]> {
    try {
      const response = await apiClient.get<{ tools: Tool[]; count: number }>('/tools/enabled');
      return response.data?.tools ?? [];
    } catch (error) {
      console.error('Error fetching enabled tools:', error);
      const axiosError = error as AxiosError<ErrorResponse>;
      throw new Error(axiosError.response?.data?.detail || 'Error fetching enabled tools');
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


  static async getAllToolConfigurations(): Promise<Record<string, unknown>> {
    try {
      const response = await apiClient.get<Record<string, unknown>>('/tools/configurations/all');
      return response.data || {};
    } catch (error) {
      console.error('Error fetching all tool configurations:', error);
      const axiosError = error as AxiosError<ErrorResponse>;
      throw new Error(axiosError.response?.data?.detail || 'Error fetching tool configurations');
    }
  }

  static async getToolConfiguration(title: string): Promise<Record<string, unknown>> {
    try {
      const response = await apiClient.get<Record<string, unknown>>(`/tools/configurations/${encodeURIComponent(title)}`);
      return response.data || {};
    } catch (error) {
      console.error('Error fetching tool configuration:', error);
      const axiosError = error as AxiosError<ErrorResponse>;
      throw new Error(axiosError.response?.data?.detail || 'Error fetching tool configuration');
    }
  }

  static async updateToolConfigurationForGroup(title: string, config: Record<string, unknown>): Promise<Record<string, unknown>> {
    try {
      const response = await apiClient.put<Record<string, unknown>>(`/tools/configurations/${encodeURIComponent(title)}`, config);
      return response.data || {};
    } catch (error) {
      console.error('Error updating group-scoped tool configuration:', error);
      const axiosError = error as AxiosError<ErrorResponse>;
      throw new Error(axiosError.response?.data?.detail || 'Error updating tool configuration');
    }
  }

  static async updateToolConfigurationInMemory(title: string, config: Record<string, unknown>): Promise<Record<string, unknown>> {
    try {
      const response = await apiClient.patch<Record<string, unknown>>(`/tools/configurations/${encodeURIComponent(title)}/in-memory`, config);
      return response.data || {};
    } catch (error) {
      console.error('Error updating in-memory tool configuration:', error);
      const axiosError = error as AxiosError<ErrorResponse>;
      throw new Error(axiosError.response?.data?.detail || 'Error updating in-memory tool configuration');
    }
  }

  static async listGlobal(): Promise<Tool[]> {
    try {
      const response = await apiClient.get<{ tools: Tool[]; count: number }>(`/tools/global`);
      return response.data?.tools ?? [];
    } catch (error) {
      const axiosError = error as AxiosError<ErrorResponse>;
      throw new Error(axiosError.response?.data?.detail || 'Error loading global tools');
    }
  }

  static async setGlobalAvailability(toolId: number, enabled: boolean): Promise<Tool> {
    try {
      const response = await apiClient.patch<Tool>(`/tools/${toolId}/global-availability`, { enabled });
      return response.data;
    } catch (error) {
      const axiosError = error as AxiosError<ErrorResponse>;
      throw new Error(axiosError.response?.data?.detail || 'Error updating global availability');
    }
  }

  // Removed enableAllTools and disableAllTools methods for security reasons
  // Individual tool enabling now requires security disclaimer confirmation
}