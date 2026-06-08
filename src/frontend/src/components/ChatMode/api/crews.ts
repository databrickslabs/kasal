import { getClient } from './client';
import { GenerationCompleteData } from '../hooks/useGenerationStream';

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
): Promise<SavedCrew> {
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
        label: a.name || a.role || `Agent ${i + 1}`,
        agentId: String(a.id),
        role: a.role || '',
        goal: a.goal || '',
        backstory: a.backstory || '',
        tools: Array.isArray(a.tools) ? a.tools : [],
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
        tools: Array.isArray(t.tools) ? t.tools : [],
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
  };

  const res = await getClient().post<{ id: string; name: string }>('/crews', payload);
  return { id: res.data.id, name: res.data.name };
}
