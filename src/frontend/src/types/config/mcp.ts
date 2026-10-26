/** MCP server configuration shared by API clients and configuration views. */
export interface MCPServerConfig {
  id: string;
  name: string;
  enabled: boolean;
  global_enabled: boolean;  // NEW: Enable across all agents/tasks
  server_url: string;
  api_key: string;
  /** A key is stored server-side; the key itself is never returned. */
  has_api_key?: boolean;
  server_type: string;  // "sse" or "streamable"
  auth_type?: string;  // "api_key", "databricks_obo", or "databricks_spn"
  timeout_seconds: number;
  max_retries: number;
  rate_limit: number;
  command?: string;  // Command for stdio server type
  args?: string[];   // Arguments for stdio server type
  session_id?: string;  // Session ID for streamable server type
  additional_config?: Record<string, unknown>;  // Additional configuration parameters
  group_id?: string | null; // Workspace override identifier when present
}
