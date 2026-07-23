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
 * A directly-selectable Databricks MCP server (an external UC-connection MCP, a
 * managed leaf like Databricks SQL, a Genie space, or an AI Search index).
 */
export interface DatabricksMcpOption {
  id: string;
  kind: string;
  name: string;
  description?: string | null;
  server_url: string;
}

/** A managed MCP TYPE: leaves carry a server_url; expandable types drill into a
 *  second step (Genie spaces / AI Search indexes). */
export interface DatabricksManagedMcpType {
  id: string;
  kind: string;
  name: string;
  description?: string | null;
  server_url?: string;
  expandable: boolean;
}

/** The workspace's Databricks MCP catalog, grouped for the two-step picker. */
export interface DatabricksMcpCatalog {
  workspace_url: string;
  external: DatabricksMcpOption[];
  managed: DatabricksManagedMcpType[];
}

/**
 * The Kasal MCP server name a Databricks option registers under.
 * Always LOWERCASE: server resolution matches by exact name, so one canonical
 * casing prevents duplicate registrations of the same server. This is the
 * single source of truth for that mapping — both registration
 * (MCPService.ensureDatabricksServer) and "is it already enabled?" matching
 * must agree, or the catalog toggles desync from the registered servers.
 */
export function databricksMcpServerName(
  option: Pick<DatabricksMcpOption, 'kind' | 'name'>,
): string {
  if (option.kind === 'genie') return `databricks genie: ${option.name}`.toLowerCase();
  if (option.kind === 'ai-search') return `databricks ai search: ${option.name}`.toLowerCase();
  return option.name.toLowerCase();
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

  /**
   * Create or update a workspace override for a server and enable it
   */
  async enableForWorkspace(id: string): Promise<MCPServerConfig> {
    try {
      const response = await apiClient.post<MCPServerConfig>(`/mcp/servers/${id}/enable-for-workspace`);
      return response.data;
    } catch (error) {
      const axiosError = error as AxiosError<ErrorResponse>;
      throw new Error(axiosError.response?.data?.detail || `Error enabling server ${id} for teamspace`);
    }
  }

  /**
   * The workspace's Databricks MCP catalog (admin-only on the backend): external
   * UC-connection MCPs and managed types (Databricks SQL, Unity Catalog
   * Functions, plus expandable Genie / AI Search). Drives the Configuration → MCP
   * "Databricks MCP Catalog" section where admins enable/disable them.
   */
  async getDatabricksCatalog(): Promise<DatabricksMcpCatalog> {
    try {
      const response = await apiClient.get<Partial<DatabricksMcpCatalog>>('/mcp/databricks/available');
      return {
        workspace_url: response.data.workspace_url ?? '',
        external: response.data.external ?? [],
        managed: response.data.managed ?? [],
      };
    } catch (error) {
      const axiosError = error as AxiosError<ErrorResponse>;
      throw new Error(axiosError.response?.data?.detail || 'Error fetching Databricks MCP catalog');
    }
  }

  /** Second step of the Genie picker: searchable, paginated spaces. */
  async listGenieSpaces(
    search?: string,
    pageToken?: string,
  ): Promise<{ options: DatabricksMcpOption[]; next_page_token: string | null }> {
    try {
      const response = await apiClient.get<{
        options?: DatabricksMcpOption[];
        next_page_token?: string | null;
      }>('/mcp/databricks/genie-spaces', {
        params: {
          ...(search ? { search } : {}),
          ...(pageToken ? { page_token: pageToken } : {}),
        },
      });
      return {
        options: response.data.options ?? [],
        next_page_token: response.data.next_page_token ?? null,
      };
    } catch (error) {
      const axiosError = error as AxiosError<ErrorResponse>;
      throw new Error(axiosError.response?.data?.detail || 'Error fetching Genie spaces');
    }
  }

  /** Second step of the AI Search picker: the workspace's vector search indexes. */
  async listAiSearchIndexes(): Promise<DatabricksMcpOption[]> {
    try {
      const response = await apiClient.get<{ options?: DatabricksMcpOption[] }>(
        '/mcp/databricks/ai-search-indexes',
      );
      return response.data.options ?? [];
    } catch (error) {
      const axiosError = error as AxiosError<ErrorResponse>;
      throw new Error(axiosError.response?.data?.detail || 'Error fetching AI Search indexes');
    }
  }

  /**
   * Get the base/global MCP servers (group_id IS NULL) — the system-admin
   * catalog. A base server is "available to all workspaces" when enabled.
   */
  async getBaseServers(): Promise<MCPServerListResponse> {
    try {
      const response = await apiClient.get<MCPServerListResponse>('/mcp/servers/base');
      return response.data;
    } catch (error) {
      const axiosError = error as AxiosError<ErrorResponse>;
      throw new Error(axiosError.response?.data?.detail || 'Error fetching global MCP servers');
    }
  }

  /** Create a base/global MCP server (available to all workspaces). System admin only. */
  async createGlobalServer(server: Omit<MCPServerConfig, 'id'>): Promise<MCPServerConfig> {
    try {
      const response = await apiClient.post<MCPServerConfig>('/mcp/servers/global', server);
      return response.data;
    } catch (error) {
      const axiosError = error as AxiosError<ErrorResponse>;
      throw new Error(axiosError.response?.data?.detail || 'Error creating global MCP server');
    }
  }

  /** System admin: set whether a base/global server is available to all workspaces. */
  async setGlobalAvailability(id: string, enabled: boolean): Promise<MCPServerConfig> {
    try {
      const response = await apiClient.patch<MCPServerConfig>(
        `/mcp/servers/${id}/global-availability`,
        { enabled },
      );
      return response.data;
    } catch (error) {
      const axiosError = error as AxiosError<ErrorResponse>;
      throw new Error(axiosError.response?.data?.detail || 'Error setting global availability');
    }
  }

  /**
   * Workspace admin: enable/disable a server for THIS workspace only. Disabling a
   * globally-available (base) server creates a workspace-scoped override; toggling
   * the workspace's own row flips it in place.
   */
  async setWorkspaceEnabled(id: string, enabled: boolean): Promise<MCPServerConfig> {
    try {
      const response = await apiClient.patch<MCPServerConfig>(
        `/mcp/servers/${id}/workspace-enabled`,
        { enabled },
      );
      return response.data;
    } catch (error) {
      const axiosError = error as AxiosError<ErrorResponse>;
      throw new Error(axiosError.response?.data?.detail || 'Error updating MCP server for teamspace');
    }
  }

  /**
   * Idempotently register a Databricks MCP option as a Kasal MCP server and
   * return the registered name. Reuses an existing registration when the
   * canonical name or URL already exists, re-enabling it if disabled and
   * normalizing legacy mixed-case names. Registration requires admin (403
   * otherwise). Used by the Configuration → MCP catalog toggle.
   *
   * ``scope`` controls whether the server is registered as a base/global server
   * ('global', system admin — available to all workspaces) or a workspace-scoped
   * one ('workspace', the default).
   */
  async ensureDatabricksServer(
    option: DatabricksMcpOption,
    scope: 'workspace' | 'global' = 'workspace',
  ): Promise<string> {
    const name = databricksMcpServerName(option);
    const { servers } =
      scope === 'global' ? await this.getBaseServers() : await this.getMcpServers();
    // Match case-insensitively so legacy mixed-case registrations are reused
    // instead of duplicated.
    const match = servers.find(
      (s) =>
        s.name.toLowerCase() === name ||
        (!!s.server_url && s.server_url === option.server_url),
    );
    if (match) {
      if (!match.enabled) {
        if (scope === 'global') await this.setGlobalAvailability(match.id, true);
        else await this.setWorkspaceEnabled(match.id, true);
      }
      // Normalize legacy mixed-case names to the lowercase canonical name. If the
      // rename is not permitted, keep the stored name — crews resolve by it.
      if (match.name !== name) {
        try {
          await this.updateMcpServer(match.id, { name });
          return name;
        } catch {
          return match.name;
        }
      }
      return match.name;
    }
    const payload: Omit<MCPServerConfig, 'id'> = {
      name,
      server_url: option.server_url,
      server_type: 'streamable',
      // Managed Databricks MCP (Genie, UC functions) authenticates on-behalf-of
      // the requesting user (OBO) so per-user resources like Genie spaces resolve
      // against the user's own permissions, not the app service principal.
      auth_type: 'databricks_obo',
      enabled: true,
      global_enabled: false,
      api_key: '',
      timeout_seconds: 30,
      max_retries: 3,
      rate_limit: 60,
    };
    if (scope === 'global') await this.createGlobalServer(payload);
    else await this.createMcpServer(payload);
    return name;
  }

}
