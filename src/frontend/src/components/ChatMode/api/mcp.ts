/**
 * MCP servers for the chat's "+" picker.
 *
 * The picker lists ONLY the MCP servers configured for the current workspace —
 * the curated, group-scoped allow-list returned by GET /mcp/servers. Browsing
 * and registering the full Databricks catalog (external connections, Databricks
 * SQL, Unity Catalog Functions, Genie spaces, AI Search indexes) lives in
 * Configuration → MCP and is served by MCPService (src/api/MCPService.ts).
 */
import { getClient } from './client';

export interface KasalMcpServer {
  id: string | number;
  name: string;
  enabled: boolean;
  server_url?: string;
}

export async function listKasalMcpServers(): Promise<KasalMcpServer[]> {
  const res = await getClient().get<{ servers?: KasalMcpServer[] }>('/mcp/servers');
  return res.data.servers ?? [];
}
