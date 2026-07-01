/**
 * MCP servers for the chat's "+" picker.
 *
 * The picker lists ONLY the MCP servers ENABLED for the current workspace — the
 * curated, group-scoped allow-list returned by GET /mcp/servers, filtered to the
 * usable (enabled) ones. Disabled servers are omitted entirely: the picker is for
 * attaching a server to a run, and disabled servers can't be used. (Workspace
 * admins manage disabled servers in Configuration → MCP, not here.) Browsing and
 * registering the full Databricks catalog also lives in Configuration → MCP
 * (served by MCPService, src/api/MCPService.ts).
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
  // Only enabled servers are usable in a run, so the picker never shows disabled
  // ones (the endpoint returns disabled servers too for workspace admins, who
  // manage them in Configuration → MCP).
  return (res.data.servers ?? []).filter((s) => s.enabled);
}
