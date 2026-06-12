/**
 * MCP servers for the chat's "+" picker. Two sources:
 *  - Kasal: the MCP servers configured in Kasal itself.
 *  - Databricks: the workspace's MCP servers — EXTERNAL ones registered as UC
 *    connections, and MANAGED types (Databricks SQL, Unity Catalog Functions
 *    from the configured catalog.schema, plus two-step Genie / AI Search
 *    pickers, since spaces and indexes can number in the thousands).
 *
 * Selecting a Databricks entry registers it as a Kasal MCP server (streamable
 * transport, databricks_spn auth on the workspace host) so crews can resolve
 * it by name at execution time.
 */
import { getClient } from './client';

export interface KasalMcpServer {
  id: string | number;
  name: string;
  enabled: boolean;
  server_url?: string;
}

export interface DatabricksMcpOption {
  id: string;
  kind: string;
  name: string;
  description?: string | null;
  server_url: string;
}

/** A managed MCP TYPE: leaves carry a server_url; expandable types drill into
 *  a second step (Genie spaces / AI Search indexes). */
export interface DatabricksManagedMcpType {
  id: string;
  kind: string;
  name: string;
  description?: string | null;
  server_url?: string;
  expandable: boolean;
}

export interface DatabricksMcpCatalog {
  workspace_url: string;
  external: DatabricksMcpOption[];
  managed: DatabricksManagedMcpType[];
}

export async function listKasalMcpServers(): Promise<KasalMcpServer[]> {
  const res = await getClient().get<{ servers?: KasalMcpServer[] }>('/mcp/servers');
  return res.data.servers ?? [];
}

export async function getDatabricksMcpCatalog(): Promise<DatabricksMcpCatalog> {
  const res = await getClient().get<Partial<DatabricksMcpCatalog>>(
    '/mcp/databricks/available',
  );
  return {
    workspace_url: res.data.workspace_url ?? '',
    external: res.data.external ?? [],
    managed: res.data.managed ?? [],
  };
}

/** Second step of the Genie picker: searchable, paginated spaces. */
export async function listGenieMcpSpaces(
  search?: string,
  pageToken?: string,
): Promise<{ options: DatabricksMcpOption[]; next_page_token: string | null }> {
  const res = await getClient().get<{
    options?: DatabricksMcpOption[];
    next_page_token?: string | null;
  }>('/mcp/databricks/genie-spaces', {
    params: {
      ...(search ? { search } : {}),
      ...(pageToken ? { page_token: pageToken } : {}),
    },
  });
  return {
    options: res.data.options ?? [],
    next_page_token: res.data.next_page_token ?? null,
  };
}

/** Second step of the AI Search picker: the workspace's indexes. */
export async function listAiSearchMcpIndexes(): Promise<DatabricksMcpOption[]> {
  const res = await getClient().get<{ options?: DatabricksMcpOption[] }>(
    '/mcp/databricks/ai-search-indexes',
  );
  return res.data.options ?? [];
}

/** The Kasal MCP server name a Databricks option registers under. */
export function databricksMcpServerName(
  option: Pick<DatabricksMcpOption, 'kind' | 'name'>,
): string {
  if (option.kind === 'genie') return `Databricks Genie: ${option.name}`;
  if (option.kind === 'ai-search') return `Databricks AI Search: ${option.name}`;
  return option.name;
}

/**
 * Idempotently register a Databricks MCP server as a Kasal MCP server and
 * return the (Kasal) server name crews reference in tool_configs.MCP_SERVERS.
 * Reuses an existing registration when the name or URL already exists,
 * re-enabling it if needed. Registration requires the admin role — a 403
 * surfaces to the caller.
 */
export async function ensureDatabricksMcpServer(
  option: DatabricksMcpOption,
): Promise<string> {
  const name = databricksMcpServerName(option);
  const existing = await listKasalMcpServers();
  const match = existing.find(
    (s) => s.name === name || (s.server_url && s.server_url === option.server_url),
  );
  if (match) {
    if (!match.enabled) {
      await getClient().patch(`/mcp/servers/${match.id}/toggle-enabled`);
    }
    return match.name;
  }
  await getClient().post('/mcp/servers', {
    name,
    server_url: option.server_url,
    server_type: 'streamable',
    auth_type: 'databricks_spn',
    enabled: true,
  });
  return name;
}
