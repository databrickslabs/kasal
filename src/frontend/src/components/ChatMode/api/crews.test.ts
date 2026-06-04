import { describe, it, expect, beforeEach, vi } from 'vitest';
import { saveGeneratedCrew, deriveCrewName, normalizeGeneration, usesGenieTool } from './crews';
import { getClient } from './client';

vi.mock('./client', () => ({
  getClient: vi.fn(),
}));

const mockedGetClient = vi.mocked(getClient);

const data = {
  agents: [
    { id: 'a1', name: 'Researcher', role: 'Analyst', goal: 'g', backstory: 'b', tools: ['T1'] },
    { id: 'a2', role: 'Writer' },
  ],
  tasks: [
    { id: 't1', name: 'Gather', description: 'd', expected_output: 'e', tools: ['T1'], agent_id: 'a1' },
    { id: 't2', name: 'Write' },
  ],
};

describe('ChatMode crews api', () => {
  let post: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    post = vi.fn().mockResolvedValue({ data: { id: 'crew-1', name: 'Gather' } });
    mockedGetClient.mockReturnValue({ post } as unknown as ReturnType<typeof getClient>);
  });

  describe('normalizeGeneration', () => {
    it('returns empty arrays for null / non-object', () => {
      expect(normalizeGeneration(null)).toEqual({ agents: [], tasks: [] });
      expect(normalizeGeneration(undefined)).toEqual({ agents: [], tasks: [] });
      expect(normalizeGeneration(42 as unknown as Record<string, unknown>)).toEqual({ agents: [], tasks: [] });
    });
    it('reads top-level agents/tasks', () => {
      const out = normalizeGeneration(data as never);
      expect(out.agents).toHaveLength(2);
      expect(out.tasks).toHaveLength(2);
    });
    it('reads agents/tasks nested under result/data/generation_result', () => {
      expect(normalizeGeneration({ result: { agents: [{ id: 'x' }], tasks: [{ id: 'y' }] } }).agents).toHaveLength(1);
      expect(normalizeGeneration({ data: { agents: [{ id: 'x' }], tasks: [{ id: 'y' }] } }).tasks).toHaveLength(1);
      expect(normalizeGeneration({ generation_result: { agents: [{ id: 'z' }], tasks: [] } }).agents).toHaveLength(1);
    });
  });

  describe('usesGenieTool', () => {
    it('detects GenieTool by name on an agent', () => {
      expect(usesGenieTool({ agents: [{ id: 'a', tools: ['GenieTool'] }], tasks: [] }, {})).toBe(true);
    });
    it('detects GenieTool by a tool id resolved through the name map (on a task)', () => {
      expect(usesGenieTool({ agents: [], tasks: [{ id: 't', tools: ['5'] }] }, { '5': 'GenieTool' })).toBe(true);
    });
    it('returns false when no tool resolves to GenieTool', () => {
      expect(usesGenieTool({ agents: [{ id: 'a', tools: ['7'] }], tasks: [{ id: 't', tools: ['Other'] }] }, { '7': 'Perplexity' })).toBe(false);
    });
    it('returns false when there are no tools at all', () => {
      expect(usesGenieTool({ agents: [{ id: 'a' }], tasks: [{ id: 't' }] }, {})).toBe(false);
    });
  });

  describe('deriveCrewName', () => {
    it('uses the first task name', () => {
      expect(deriveCrewName(data as never)).toBe('Gather');
    });
    it('truncates a very long task name', () => {
      const long = { tasks: [{ id: 't', name: 'x'.repeat(90) }], agents: [] };
      expect(deriveCrewName(long).endsWith('…')).toBe(true);
    });
    it('falls back to the first agent role/name', () => {
      expect(deriveCrewName({ agents: [{ id: 'a', role: 'Planner' }], tasks: [] })).toBe('Planner crew');
      expect(deriveCrewName({ agents: [{ id: 'a', name: 'Bob' }], tasks: [] })).toBe('Bob crew');
    });
    it('falls back to a generic name when nothing is usable', () => {
      expect(deriveCrewName({ agents: [], tasks: [] })).toBe('Generated crew');
    });
  });

  describe('saveGeneratedCrew', () => {
    it('POSTs /crews with agent_ids/task_ids, nodes, edges and returns id+name', async () => {
      const result = await saveGeneratedCrew(data as never);
      expect(post).toHaveBeenCalledTimes(1);
      const [url, payload] = post.mock.calls[0] as [string, Record<string, unknown>];
      expect(url).toBe('/crews');
      expect(payload.name).toBe('Gather');
      expect(payload.agent_ids).toEqual(['a1', 'a2']);
      expect(payload.task_ids).toEqual(['t1', 't2']);
      // one node per agent + one per task
      expect((payload.nodes as unknown[]).length).toBe(4);
      // agent→task edges (2) + sequential task edge (1)
      const edges = payload.edges as Array<{ source: string; target: string }>;
      expect(edges).toContainEqual(expect.objectContaining({ source: 'agent-a1', target: 'task-t1' }));
      expect(edges).toContainEqual(expect.objectContaining({ source: 'task-t1', target: 'task-t2' }));
      // t2 has no agent_id → falls back to positional agent (a2)
      expect(edges).toContainEqual(expect.objectContaining({ source: 'agent-a2', target: 'task-t2' }));
      expect(result).toEqual({ id: 'crew-1', name: 'Gather' });
    });

    it('uses an explicit name over the derived one', async () => {
      await saveGeneratedCrew(data as never, '  My Crew  ');
      const payload = post.mock.calls[0][1] as Record<string, unknown>;
      expect(payload.name).toBe('My Crew');
    });

    it('falls back to the derived name when the supplied name is blank', async () => {
      await saveGeneratedCrew(data as never, '   ');
      const payload = post.mock.calls[0][1] as Record<string, unknown>;
      expect(payload.name).toBe('Gather');
    });

    it('throws when there are no agent/task ids to reference', async () => {
      await expect(saveGeneratedCrew({ agents: [{ name: 'no-id' }], tasks: [] } as never)).rejects.toThrow(
        /no saved agents or tasks/i,
      );
      expect(post).not.toHaveBeenCalled();
    });

    it('labels nodes by index when agents/tasks lack a name/role', async () => {
      const unnamed = {
        agents: [{ id: 'a1' }], // no name, no role → "Agent 1"
        tasks: [{ id: 't1', agent_id: 'a1' }], // no name → "Task 1"
      };
      await saveGeneratedCrew(unnamed as never);
      const nodes = post.mock.calls[0][1].nodes as Array<{ type: string; data: { label: string } }>;
      expect(nodes.find((n) => n.type === 'agentNode')?.data.label).toBe('Agent 1');
      expect(nodes.find((n) => n.type === 'taskNode')?.data.label).toBe('Task 1');
    });

    it('skips the agent→task edge when no owning agent id can be resolved', async () => {
      // agents[0] has no id (but a later agent does, so the save still proceeds);
      // the first task has no agent_id → no owner edge is emitted for it.
      const data2 = {
        agents: [{ name: 'NoId' }, { id: 'a2', name: 'HasId' }],
        tasks: [{ id: 't1', name: 'First' }],
      };
      await saveGeneratedCrew(data2 as never);
      const edges = post.mock.calls[0][1].edges as Array<{ target: string }>;
      expect(edges.some((e) => e.target === 'task-t1')).toBe(false);
    });

    it('falls back to the first agent for a task when no positional match exists', async () => {
      // More tasks than agents: task index 1 has no agents[1], so it uses agents[0].
      const lopsided = {
        agents: [{ id: 'a1', name: 'Solo' }],
        tasks: [{ id: 't1', name: 'One' }, { id: 't2', name: 'Two' }],
      };
      await saveGeneratedCrew(lopsided as never);
      const edges = post.mock.calls[0][1].edges as Array<{ source: string; target: string }>;
      expect(edges).toContainEqual(expect.objectContaining({ source: 'agent-a1', target: 'task-t2' }));
    });
  });
});
