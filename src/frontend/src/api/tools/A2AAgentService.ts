import { apiClient } from '../../config/api/ApiConfig';

/**
 * Remote agents this workspace can delegate to over A2A.
 *
 * The OUTBOUND half of the A2A surface. Publishing (PublicationService) is the
 * inbound half — what this workspace offers to others. The two are deliberately
 * separate services because they are separate decisions: an admin can attach a
 * remote without publishing anything, and vice versa.
 */

/** A skill the remote advertises on its Agent Card. */
export interface A2ASkill {
  id: string;
  name: string;
  description?: string;
}

export interface A2AAgent {
  id: number;
  /**
   * NULL for a globally-registered agent, set for a workspace's own opt-in
   * copy. In the workspace view a null `group_id` means "inherited — toggle
   * only, edited in the global view".
   */
  group_id?: string | null;
  name: string;
  card_url: string;
  description?: string | null;
  auth_type: 'obo' | 'api_key' | 'none';
  enabled: boolean;
  global_enabled: boolean;
  timeout_seconds: number;
  /** Whether a key is stored — never the key itself. */
  has_api_key: boolean;
  skills: A2ASkill[];
  card_fetched_at?: string | null;
  /** Why the last card fetch failed. Null when the remote is reachable. */
  last_error?: string | null;
}

export interface A2AAgentInput {
  name: string;
  card_url: string;
  description?: string | null;
  auth_type?: 'obo' | 'api_key' | 'none';
  enabled?: boolean;
  global_enabled?: boolean;
  timeout_seconds?: number;
  /** Write-only. '' clears a stored key; omit to leave it untouched. */
  api_key?: string;
}

export interface A2AConnectionTest {
  connected: boolean;
  message: string;
  agent_name?: string | null;
  skills: A2ASkill[];
}

const BASE = '/a2a-agents';

export const A2AAgentService = {
  async list(): Promise<A2AAgent[]> {
    const { data } = await apiClient.get<{ agents: A2AAgent[]; count: number }>(BASE);
    return data.agents ?? [];
  },

  /**
   * The Kasal-admin catalogue: globally registered agents.
   *
   * A remote agent carries an outbound URL and a credential, so registering one
   * is a system-administration act — the same split MCP servers have. 403s for
   * anyone who is not a Kasal admin.
   */
  async listBase(): Promise<A2AAgent[]> {
    const { data } = await apiClient.get<{ agents: A2AAgent[]; count: number }>(
      `${BASE}/base`,
    );
    return data.agents ?? [];
  },

  /** Kasal admin: offer an agent to all workspaces, or withdraw it. */
  async setGlobalAvailability(id: number, enabled: boolean): Promise<A2AAgent> {
    const { data } = await apiClient.patch<A2AAgent>(
      `${BASE}/${id}/global-availability`,
      { enabled },
    );
    return data;
  },

  /**
   * Workspace admin: turn an agent on or off for THIS workspace.
   *
   * Toggling an inherited global agent creates a workspace-scoped copy carrying
   * that choice; the global row is never touched.
   */
  async setWorkspaceEnabled(id: number, enabled: boolean): Promise<A2AAgent> {
    const { data } = await apiClient.patch<A2AAgent>(
      `${BASE}/${id}/workspace-enabled`,
      { enabled },
    );
    return data;
  },

  async create(input: A2AAgentInput): Promise<A2AAgent> {
    const { data } = await apiClient.post<A2AAgent>(BASE, input);
    return data;
  },

  async update(id: number, input: Partial<A2AAgentInput>): Promise<A2AAgent> {
    const { data } = await apiClient.put<A2AAgent>(`${BASE}/${id}`, input);
    return data;
  },

  async remove(id: number): Promise<void> {
    await apiClient.delete(`${BASE}/${id}`);
  },

  /**
   * Fetch the remote's card now.
   *
   * Answers 200 with `connected: false` for an unreachable remote — that is a
   * result the operator asked for, not a failed request — so callers should read
   * the body rather than relying on a rejected promise.
   */
  async test(id: number): Promise<A2AConnectionTest> {
    const { data } = await apiClient.post<A2AConnectionTest>(`${BASE}/${id}/test`);
    return data;
  },
};

export default A2AAgentService;
