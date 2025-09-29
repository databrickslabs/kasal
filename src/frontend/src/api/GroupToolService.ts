import { AxiosError } from 'axios';
import { apiClient } from '../config/api/ApiConfig';
import type { Tool } from './ToolService';

export interface GroupToolMapping {
  id: number;
  tool_id: number;
  group_id: string;
  enabled: boolean;
  config?: Record<string, unknown>;
  credentials_status: string;
  created_at: string;
  updated_at: string;
}

interface ErrorResponse { detail?: string }

export class GroupToolService {
  static async listAvailable(): Promise<Tool[]> {
    try {
      const res = await apiClient.get<{ tools: Tool[]; count: number }>(`/group-tools/available`);
      return res.data?.tools ?? [];
    } catch (error) {
      const axiosError = error as AxiosError<ErrorResponse>;
      throw new Error(axiosError.response?.data?.detail || 'Error loading available tools');
    }
  }

  static async listAdded(): Promise<GroupToolMapping[]> {
    try {
      const res = await apiClient.get<{ items: GroupToolMapping[]; count: number }>(`/group-tools`);
      return res.data?.items ?? [];
    } catch (error) {
      const axiosError = error as AxiosError<ErrorResponse>;
      throw new Error(axiosError.response?.data?.detail || 'Error loading added tools');
    }
  }

  static async addTool(toolId: number): Promise<GroupToolMapping> {
    try {
      const res = await apiClient.post<GroupToolMapping>(`/group-tools/${toolId}`);
      return res.data;
    } catch (error) {
      const axiosError = error as AxiosError<ErrorResponse>;
      throw new Error(axiosError.response?.data?.detail || 'Error adding tool to workspace');
    }
  }

  static async setEnabled(toolId: number, enabled: boolean): Promise<GroupToolMapping> {
    try {
      const res = await apiClient.patch<GroupToolMapping>(`/group-tools/${toolId}/enabled`, { enabled });
      return res.data;
    } catch (error) {
      const axiosError = error as AxiosError<ErrorResponse>;
      throw new Error(axiosError.response?.data?.detail || 'Error updating enabled state');
    }
  }

  static async updateConfig(toolId: number, config: Record<string, unknown>): Promise<GroupToolMapping> {
    try {
      const res = await apiClient.patch<GroupToolMapping>(`/group-tools/${toolId}/config`, config);
      return res.data;
    } catch (error) {
      const axiosError = error as AxiosError<ErrorResponse>;
      throw new Error(axiosError.response?.data?.detail || 'Error updating tool configuration');
    }
  }

  static async remove(toolId: number): Promise<void> {
    try {
      await apiClient.delete(`/group-tools/${toolId}`);
    } catch (error) {
      const axiosError = error as AxiosError<ErrorResponse>;
      throw new Error(axiosError.response?.data?.detail || 'Error removing tool from workspace');
    }
  }
}

