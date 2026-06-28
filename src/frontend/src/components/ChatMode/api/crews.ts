import { getClient } from './client';
import { GenerationCompleteData } from '../types/dispatcher';

/**
 * Save a chat-generated crew "plan" (its agents + tasks) to the catalog.
 *
 * The chat's crew generation already persists the agents and tasks to the DB
 * (see /create-crew-streaming → create_crew_progressive), so each generated
 * agent/task carries a real DB id. Saving the plan is therefore just creating a
 * crew record that references those ids — no entity creation, no backend change.
 * We also synthesize ReactFlow nodes/edges so the saved crew opens cleanly in
 * Crew mode later.
 *
 * Only the plan is saved; the run's output already lives in job history.
 */

interface GenAgent {
  id?: string;
  name?: string;
  role?: string;
  goal?: string;
  backstory?: string;
  tools?: string[];
  llm?: string;
  memory?: boolean;
  // Generated agents are full DB records; keep any extra config (advanced
  // settings) so the saved crew round-trips faithfully.
  [key: string]: unknown;
}

interface GenTask {
  id?: string;
  name?: string;
  description?: string;
  expected_output?: string;
  tools?: string[];
  agent_id?: string;
}

export interface SavedCrew {
  id: string;
  name: string;
}

/**
 * Thrown when a crew with the same name already exists in the catalog and the
 * caller didn't ask to overwrite. Lets the UI offer an "Overwrite" choice
 * instead of surfacing a raw 409.
 */
export class CrewNameConflictError extends Error {
  constructor(public crewName: string) {
    super(`A crew named "${crewName}" already exists.`);
    this.name = 'CrewNameConflictError';
  }
}

/**
 * Pull agents/tasks out of the generation payload, tolerating the few shapes
 * the backend uses (top-level, or nested under result/data/generation_result).
 */
export function normalizeGeneration(
  data: GenerationCompleteData | Record<string, unknown> | null | undefined,
): { agents: GenAgent[]; tasks: GenTask[] } {
  if (!data || typeof data !== 'object') return { agents: [], tasks: [] };
  const obj = data as Record<string, unknown>;
  const pick = (key: string): Record<string, unknown>[] => {
    if (Array.isArray(obj[key]) && (obj[key] as unknown[]).length > 0) {
      return obj[key] as Record<string, unknown>[];
    }
    for (const wrap of ['result', 'data', 'generation_result']) {
      const nested = obj[wrap];
      if (nested && typeof nested === 'object') {
        const arr = (nested as Record<string, unknown>)[key];
        if (Array.isArray(arr) && arr.length > 0) return arr as Record<string, unknown>[];
      }
    }
    return [];
  };
  return {
    agents: pick('agents') as GenAgent[],
    tasks: pick('tasks') as GenTask[],
  };
}

/**
 * Whether any agent or task in the generated crew references GenieTool (by name
 * or by a tool id that resolves to "GenieTool" via the tool-name map). Genie
 * crews need a Genie space picked before they can run, so the chat must not
 * auto-run them.
 */
// GenieTool can be referenced by its display name, one of its aliases, or its
// numeric tool id. Detection must NOT depend solely on toolNameMap being loaded:
// in a chat-only deployment (e.g. Databricks Apps) the tools list may still be
// loading — or have failed — when a crew finishes generating, leaving the map
// empty. Relying on it then makes usesGenieTool return false, so the Genie-space
// selector never appears and the crew auto-runs with NO space — Genie then runs
// blind ("space ID not configured"). Matching the known name/aliases/id directly
// makes detection work regardless of toolNameMap hydration.
const GENIE_IDENTIFIERS = new Set([
  'GenieTool', 'Genie', 'DatabricksGenie', 'DataSearch', '35',
]);

/**
 * Whether a single tool reference (a name OR a tool id) is GenieTool. Robust to
 * an unloaded toolNameMap: matches the known name/aliases/id directly. Shared by
 * usesGenieTool (auto-run gate) and the card's space-selector gate so they agree
 * regardless of the chosen output format.
 */
export function isGenieToolRef(
  tool: unknown,
  toolNameMap: Record<string, string> = {},
): boolean {
  const raw = String(tool);
  return GENIE_IDENTIFIERS.has(raw) || (toolNameMap[raw] || raw) === 'GenieTool';
}

export function usesGenieTool(
  data: GenerationCompleteData | Record<string, unknown> | null | undefined,
  toolNameMap: Record<string, string>,
): boolean {
  const { agents, tasks } = normalizeGeneration(data);
  const anyGenie = (items: { tools?: unknown }[]): boolean =>
    items.some((it) => Array.isArray(it.tools) && it.tools.some((t) => isGenieToolRef(t, toolNameMap)));
  return anyGenie(agents) || anyGenie(tasks);
}

/**
 * A copy of the generation with every Genie tool reference removed — used when
 * the user SKIPS the Genie-space prompt: the crew runs with its remaining
 * tools instead of blocking on a space pick. Strips GenieTool (name, alias or
 * id) from agent/task tool lists and drops any GenieTool tool_configs entry.
 */
export function stripGenieTools(
  data: GenerationCompleteData,
  toolNameMap: Record<string, string> = {},
): GenerationCompleteData {
  const stripItem = (item: Record<string, unknown>): Record<string, unknown> => {
    const next = { ...item };
    if (Array.isArray(next.tools)) {
      next.tools = (next.tools as unknown[]).filter((t) => !isGenieToolRef(t, toolNameMap));
    }
    const cfgs = next.tool_configs;
    if (cfgs && typeof cfgs === 'object') {
      next.tool_configs = Object.fromEntries(
        Object.entries(cfgs as Record<string, unknown>).filter(
          ([key]) => !isGenieToolRef(key, toolNameMap),
        ),
      );
    }
    return next;
  };
  return {
    ...data,
    agents: (data.agents || []).map(stripItem),
    tasks: (data.tasks || []).map(stripItem),
  };
}

/** Derive a sensible default crew name when the user doesn't supply one. */
export function deriveCrewName(
  data: GenerationCompleteData | Record<string, unknown>,
): string {
  const { agents, tasks } = normalizeGeneration(data);
  const taskName = tasks[0]?.name?.trim();
  if (taskName) return taskName.length > 60 ? `${taskName.slice(0, 60).trim()}…` : taskName;
  const agentLabel = (agents[0]?.role || agents[0]?.name)?.trim();
  if (agentLabel) return `${agentLabel} crew`;
  return 'Generated crew';
}

/**
 * Persist the generated crew to the catalog. Resolves to the created crew's
 * id + name. Throws if the plan has no DB-backed agents/tasks to reference.
 */
export async function saveGeneratedCrew(
  data: GenerationCompleteData | Record<string, unknown>,
  name?: string,
  opts?: {
    overwrite?: boolean;
    memoryEnabled?: boolean;
    spaceId?: string;
    mcpServers?: string[];
    agentBricksEndpoints?: string[];
    // Persist the answer-mode crew config so a saved Research/Deep crew reloads
    // WITH reasoning (and planning + planning_llm for Deep) instead of as a plain crew.
    reasoning?: boolean;
    planning?: boolean;
    planningLlm?: string;
  },
): Promise<SavedCrew> {
  // When a Genie space was picked in chat, persist it as a GenieTool override on
  // any agent/task that uses GenieTool so the saved crew runs against that space.
  const genieToolConfig = opts?.spaceId ? { GenieTool: { spaceId: opts.spaceId } } : undefined;
  const usesGenie = (tools: unknown): boolean =>
    Array.isArray(tools) && tools.some((t) => isGenieToolRef(t));

  // Agent Bricks: the user picked an endpoint in the chat "+" — persist it so the
  // saved crew reloads WITH the agent assigned (the editor's AgentBricks selector
  // shows it populated) and runs against it. Mirrors the Genie/MCP persistence:
  // equip AgentBricksTool (catalog id 71) and store its endpointName on each node.
  const ABT = '71';
  const agentBricksEndpoints = opts?.agentBricksEndpoints ?? [];
  const agentBricksPicked = agentBricksEndpoints.length > 0;
  const equipAgentBricks = (tools: unknown): string[] => {
    const arr = Array.isArray(tools) ? [...(tools as unknown[])].map(String) : [];
    if (agentBricksPicked && !arr.some((t) => t === ABT || t === 'AgentBricksTool')) {
      arr.push(ABT);
    }
    return arr;
  };

  // Persist the MCP servers selected for the run onto EVERY agent and task
  // (mirrors execution's buildCrewConfigFromGenerated injection), so the saved
  // crew reloads with the same MCP tools instead of losing them.
  const mcpServers = opts?.mcpServers ?? [];
  const toolConfigsFor = (tools: unknown): Record<string, unknown> | undefined => {
    const cfg: Record<string, unknown> = {};
    if (genieToolConfig && usesGenie(tools)) Object.assign(cfg, genieToolConfig);
    if (mcpServers.length > 0) cfg.MCP_SERVERS = { servers: mcpServers };
    if (agentBricksPicked) cfg.AgentBricksTool = { endpointName: agentBricksEndpoints };
    return Object.keys(cfg).length > 0 ? cfg : undefined;
  };

  const { agents, tasks } = normalizeGeneration(data);
  const agent_ids = agents.map((a) => a.id).filter((id): id is string => Boolean(id));
  const task_ids = tasks.map((t) => t.id).filter((id): id is string => Boolean(id));

  if (agent_ids.length === 0 || task_ids.length === 0) {
    throw new Error('This crew has no saved agents or tasks to reference yet.');
  }

  const nodes: Record<string, unknown>[] = [];
  const edges: Record<string, unknown>[] = [];

  agents.forEach((a, i) => {
    nodes.push({
      id: `agent-${a.id}`,
      type: 'agentNode',
      position: { x: 80, y: 100 + i * 220 },
      data: {
        // Carry the full agent record (llm + advanced behaviour settings) so the
        // saved crew round-trips faithfully instead of falling back to defaults.
        ...a,
        label: a.name || a.role || `Agent ${i + 1}`,
        agentId: String(a.id),
        id: String(a.id),
        type: 'agent',
        role: a.role || '',
        goal: a.goal || '',
        backstory: a.backstory || '',
        tools: equipAgentBricks(a.tools),
        // Honour the chat's memory choice when provided, else keep the agent's own.
        ...(opts?.memoryEnabled !== undefined ? { memory: opts.memoryEnabled } : {}),
        // Persist the Genie space, selected MCP servers, and/or Agent Bricks endpoint.
        ...(toolConfigsFor(a.tools) ? { tool_configs: toolConfigsFor(a.tools) } : {}),
      },
    });
  });

  tasks.forEach((t, i) => {
    nodes.push({
      id: `task-${t.id}`,
      type: 'taskNode',
      position: { x: 480, y: 100 + i * 220 },
      data: {
        label: t.name || `Task ${i + 1}`,
        taskId: String(t.id),
        description: t.description || '',
        expected_output: t.expected_output || '',
        tools: equipAgentBricks(t.tools),
        // Persist the Genie space, selected MCP servers, and/or Agent Bricks endpoint.
        ...(toolConfigsFor(t.tools) ? { tool_configs: toolConfigsFor(t.tools) } : {}),
      },
    });

    // Connect a task to its owning agent (fall back to positional pairing),
    // and chain tasks sequentially so dependencies render in Crew mode.
    const ownerAgentId = t.agent_id || agents[i]?.id || agents[0]?.id;
    if (ownerAgentId) {
      edges.push({
        id: `e-agent-${ownerAgentId}-task-${t.id}`,
        source: `agent-${ownerAgentId}`,
        target: `task-${t.id}`,
      });
    }
    if (i > 0 && tasks[i - 1]?.id) {
      edges.push({
        id: `e-task-${tasks[i - 1].id}-task-${t.id}`,
        source: `task-${tasks[i - 1].id}`,
        target: `task-${t.id}`,
      });
    }
  });

  const payload = {
    name: name?.trim() || deriveCrewName(data),
    agent_ids,
    task_ids,
    nodes,
    edges,
    // Crew-level memory mirrors the chat's memory choice when provided.
    ...(opts?.memoryEnabled !== undefined ? { memory: opts.memoryEnabled } : {}),
    // Answer-mode crew config (snake_case — CrewBase columns). Send reasoning/
    // planning explicitly: the columns default to false, so omitting them would
    // silently reload a Research/Deep crew as a plain crew. planning_llm only
    // rides along when planning is on (avoids the OpenAI-default 401 on reload).
    ...(opts?.reasoning !== undefined ? { reasoning: opts.reasoning } : {}),
    ...(opts?.planning !== undefined ? { planning: opts.planning } : {}),
    ...(opts?.planning && opts?.planningLlm ? { planning_llm: opts.planningLlm } : {}),
  };

  try {
    const res = await getClient().post<{ id: string; name: string }>(
      opts?.overwrite ? '/crews?overwrite=true' : '/crews',
      payload,
    );
    return { id: res.data.id, name: res.data.name };
  } catch (err) {
    // Surface a name clash as a typed error so the UI can offer "Overwrite".
    const status = (err as { response?: { status?: number } })?.response?.status;
    if (status === 409) {
      throw new CrewNameConflictError(payload.name);
    }
    throw err;
  }
}

/** A saved catalog entry (crew or flow) for the library rail. */
export interface CatalogItem {
  id: string;
  name: string;
}

/**
 * List the workspace's saved crews for the library rail. Group/tenant scoping is
 * applied by the shared client interceptors, same as the rest of chat mode.
 */
export async function listSavedCrews(): Promise<CatalogItem[]> {
  const res = await getClient().get<Array<{ id?: string; name?: string }>>('/crews');
  return (res.data || [])
    .filter((c) => c && c.id && c.name)
    .map((c) => ({ id: String(c.id), name: String(c.name) }));
}

/** List the workspace's saved flows for the library rail. */
export async function listSavedFlows(): Promise<CatalogItem[]> {
  const res = await getClient().get<Array<{ id?: string; name?: string }>>('/flows');
  return (res.data || [])
    .filter((f) => f && f.id && f.name)
    .map((f) => ({ id: String(f.id), name: String(f.name) }));
}

// --- Crew thumbs feedback (surfaced in the Agent Builder catalog) -----------

export interface CrewFeedbackEntry {
  id: string;
  crew_id: string;
  rating: 'up' | 'down';
  comment?: string | null;
  created_at: string;
  group_email?: string | null;
}

/** Record a thumbs vote on a cataloged crew. Thumbs-down requires a comment. */
export async function postCrewFeedback(
  crewId: string,
  rating: 'up' | 'down',
  comment?: string,
): Promise<CrewFeedbackEntry> {
  const res = await getClient().post<CrewFeedbackEntry>(`/crews/${crewId}/feedback`, {
    rating,
    ...(comment ? { comment } : {}),
  });
  return res.data;
}
