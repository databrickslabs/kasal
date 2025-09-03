import { AxiosError } from 'axios';
import { apiClient } from '../config/api/ApiConfig';
import { MCPServerConfig } from '../components/Configuration/MCP/MCPConfiguration';

// Error response type
interface ErrorResponse {
  detail?: string;
}

// Add the MCPServerListResponse type
interface MCPServerListResponse {
  servers: MCPServerConfig[];
  count: number;
}

/**
 * Service for managing MCP (Model Context Protocol) server configurations
 */
export class MCPService {
  private static instance: MCPService;

  /**
   * Get singleton instance of MCPService
   */
  public static getInstance(): MCPService {
    if (!MCPService.instance) {
      MCPService.instance = new MCPService();
    }
    return MCPService.instance;
  }

  /**
   * Get all MCP server configurations
   * @returns List of MCP server configurations
   */
  async getMcpServers(): Promise<MCPServerListResponse> {
    try {
      const response = await apiClient.get<MCPServerListResponse>('/mcp/servers');
      console.log('Fetched MCP servers:', response.data);
      return response.data;
    } catch (error) {
      console.error('Error fetching MCP servers:', error);
      const axiosError = error as AxiosError<ErrorResponse>;
      throw new Error(axiosError.response?.data?.detail || 'Error fetching MCP servers');
    }
  }

  /**
   * Get a specific MCP server configuration by ID
   * @param id Server ID
   * @returns MCP server configuration or null if not found
   */
  async getMcpServer(id: string): Promise<MCPServerConfig | null> {
    try {
      const response = await apiClient.get<MCPServerConfig>(`/mcp/servers/${id}`);
      return response.data;
    } catch (error) {
      console.error(`Error fetching MCP server with ID ${id}:`, error);
      if ((error as AxiosError).response?.status === 404) {
        return null;
      }
      const axiosError = error as AxiosError<ErrorResponse>;
      throw new Error(axiosError.response?.data?.detail || `Error fetching MCP server with ID ${id}`);
    }
  }

  /**
   * Create a new MCP server configuration
   * @param server Server configuration
   * @returns Created server configuration
   */
  async createMcpServer(server: Omit<MCPServerConfig, 'id'>): Promise<MCPServerConfig> {
    try {
      const response = await apiClient.post<MCPServerConfig>('/mcp/servers', server);
      console.log('Created MCP server:', response.data);
      return response.data;
    } catch (error) {
      console.error('Error creating MCP server:', error);
      const axiosError = error as AxiosError<ErrorResponse>;
      throw new Error(axiosError.response?.data?.detail || 'Error creating MCP server');
    }
  }

  /**
   * Update an existing MCP server configuration
   * @param id Server ID
   * @param server Updated server configuration
   * @returns Updated server configuration
   */
  async updateMcpServer(id: string, server: Partial<MCPServerConfig>): Promise<MCPServerConfig> {
    try {
      const response = await apiClient.put<MCPServerConfig>(`/mcp/servers/${id}`, server);
      console.log('Updated MCP server:', response.data);
      return response.data;
    } catch (error) {
      console.error(`Error updating MCP server with ID ${id}:`, error);
      const axiosError = error as AxiosError<ErrorResponse>;
      throw new Error(axiosError.response?.data?.detail || `Error updating MCP server with ID ${id}`);
    }
  }

  /**
   * Delete an MCP server configuration
   * @param id Server ID
   * @returns True if deleted successfully
   */
  async deleteMcpServer(id: string): Promise<boolean> {
    try {
      await apiClient.delete(`/mcp/servers/${id}`);
      console.log(`Deleted MCP server with ID ${id}`);
      return true;
    } catch (error) {
      console.error(`Error deleting MCP server with ID ${id}:`, error);
      const axiosError = error as AxiosError<ErrorResponse>;
      throw new Error(axiosError.response?.data?.detail || `Error deleting MCP server with ID ${id}`);
    }
  }

  /**
   * Toggle the enabled state of an MCP server
   * @param id Server ID
   * @returns Updated enabled state
   */
  async toggleMcpServerEnabled(id: string): Promise<{ enabled: boolean }> {
    try {
      const response = await apiClient.patch<{ message: string; enabled: boolean }>(
        `/mcp/servers/${id}/toggle-enabled`
      );
      console.log(`Toggled MCP server ${id} enabled state:`, response.data);
      return { enabled: response.data.enabled };
    } catch (error) {
      console.error(`Error toggling MCP server ${id} enabled state:`, error);
      const axiosError = error as AxiosError<ErrorResponse>;
      throw new Error(axiosError.response?.data?.detail || `Error toggling MCP server ${id} enabled state`);
    }
  }

  /**
   * Toggle the global enabled state of an MCP server
   * @param id Server ID
   * @returns Updated global enabled state
   */
  async toggleMcpServerGlobalEnabled(id: string): Promise<{ enabled: boolean }> {
    try {
      const response = await apiClient.patch<{ message: string; enabled: boolean }>(
        `/mcp/servers/${id}/toggle-global-enabled`
      );
      console.log(`Toggled MCP server ${id} global enabled state:`, response.data);
      return { enabled: response.data.enabled };
    } catch (error) {
      console.error(`Error toggling MCP server ${id} global enabled state:`, error);
      const axiosError = error as AxiosError<ErrorResponse>;
      throw new Error(axiosError.response?.data?.detail || `Error toggling MCP server ${id} global enabled state`);
    }
  }

  /**
   * Test connection to an MCP server
   * @param serverConfig Server configuration to test
   * @returns Connection status
   */
  async testConnection(serverConfig: MCPServerConfig): Promise<{ success: boolean; message: string }> {
    try {
      const response = await apiClient.post<{ success: boolean; message: string }>(
        '/mcp/test-connection',
        serverConfig
      );
      return response.data;
    } catch (error) {
      console.error('Error testing MCP server connection:', error);
      const axiosError = error as AxiosError<ErrorResponse>;
      return {
        success: false,
        message: axiosError.response?.data?.detail || 'Error testing connection'
      };
    }
  }

  /**
   * Get global MCP settings
   * @returns Global MCP configuration
   */
  async getGlobalSettings(): Promise<{ global_enabled: boolean }> {
    try {
      const response = await apiClient.get<{ global_enabled: boolean }>('/mcp/settings');
      return response.data;
    } catch (error) {
      console.error('Error fetching MCP global settings:', error);
      const axiosError = error as AxiosError<ErrorResponse>;
      throw new Error(axiosError.response?.data?.detail || 'Error fetching MCP global settings');
    }
  }

  /**
   * Update global MCP settings
   * @param settings Global settings to update
   * @returns Updated global settings
   */
  async updateGlobalSettings(settings: { global_enabled: boolean }): Promise<{ global_enabled: boolean }> {
    try {
      const response = await apiClient.put<{ global_enabled: boolean }>('/mcp/settings', settings);
      return response.data;
    } catch (error) {
      console.error('Error updating MCP global settings:', error);
      const axiosError = error as AxiosError<ErrorResponse>;
      throw new Error(axiosError.response?.data?.detail || 'Error updating MCP global settings');
    }
  }
} 