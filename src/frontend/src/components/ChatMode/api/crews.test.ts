import { describe, it, expect, beforeEach, vi } from 'vitest';
import { saveGeneratedCrew, deriveCrewName, normalizeGeneration, usesGenieTool, stripGenieTools, postCrewFeedback, CrewNameConflictError, listSavedCrews, listSavedFlows } from './crews';
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

    it('persists the answer-mode crew config (reasoning/planning/planning_llm)', async () => {
      // Deep mode → reasoning + planning + an explicit planning_llm so the saved
      // crew round-trips (and never defaults planning to OpenAI on reload).
      await saveGeneratedCrew(data as never, undefined, {
        reasoning: true,
        planning: true,
        planningLlm: 'databricks-claude-sonnet-4-5',
      });
      const payload = post.mock.calls[0][1] as Record<string, unknown>;
      expect(payload.reasoning).toBe(true);
      expect(payload.planning).toBe(true);
      expect(payload.planning_llm).toBe('databricks-claude-sonnet-4-5');
    });

    it('persists reasoning without planning for research mode (no planning_llm)', async () => {
      await saveGeneratedCrew(data as never, undefined, { reasoning: true, planning: false });
      const payload = post.mock.calls[0][1] as Record<string, unknown>;
      expect(payload.reasoning).toBe(true);
      expect(payload.planning).toBe(false);
      expect(payload).not.toHaveProperty('planning_llm');
    });

    it('omits answer-mode config when not provided (chat/light agent save)', async () => {
      await saveGeneratedCrew(data as never);
      const payload = post.mock.calls[0][1] as Record<string, unknown>;
      expect(payload).not.toHaveProperty('reasoning');
      expect(payload).not.toHaveProperty('planning');
      expect(payload).not.toHaveProperty('planning_llm');
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

    describe('overwrite + name-conflict handling', () => {
      it('POSTs to /crews?overwrite=true when opts.overwrite is set', async () => {
        await saveGeneratedCrew(data as never, undefined, { overwrite: true });
        expect(post.mock.calls[0][0]).toBe('/crews?overwrite=true');
      });

      it('POSTs to /crews by default (no overwrite)', async () => {
        await saveGeneratedCrew(data as never);
        expect(post.mock.calls[0][0]).toBe('/crews');
      });

      it('throws CrewNameConflictError with the payload name on a 409', async () => {
        post.mockRejectedValue({ response: { status: 409 } });
        const err = await saveGeneratedCrew(data as never, 'Taken Name').catch((e) => e);
        expect(err).toBeInstanceOf(CrewNameConflictError);
        expect((err as CrewNameConflictError).crewName).toBe('Taken Name');
      });

      it('rethrows a non-409 error as-is', async () => {
        const boom = { response: { status: 500 }, message: 'server blew up' };
        post.mockRejectedValueOnce(boom);
        await expect(saveGeneratedCrew(data as never)).rejects.toBe(boom);
      });
    });

    describe('agent config + memory carry-through', () => {
      const withLlm = {
        agents: [{ id: 'a1', name: 'Researcher', role: 'Analyst', llm: 'gpt-4o', allow_delegation: true }],
        tasks: [{ id: 't1', name: 'Gather', agent_id: 'a1' }],
      };

      it('carries the full agent record (llm + advanced config) into the node data', async () => {
        await saveGeneratedCrew(withLlm as never);
        const nodes = post.mock.calls[0][1].nodes as Array<{ type: string; data: Record<string, unknown> }>;
        const agentNode = nodes.find((n) => n.type === 'agentNode')!;
        expect(agentNode.data.llm).toBe('gpt-4o');
        expect(agentNode.data.allow_delegation).toBe(true);
      });

      it('sets agent + crew memory to true when opts.memoryEnabled is true', async () => {
        await saveGeneratedCrew(withLlm as never, undefined, { memoryEnabled: true });
        const payload = post.mock.calls[0][1] as Record<string, unknown>;
        expect(payload.memory).toBe(true);
        const nodes = payload.nodes as Array<{ type: string; data: Record<string, unknown> }>;
        expect(nodes.find((n) => n.type === 'agentNode')!.data.memory).toBe(true);
      });

      it('sets agent + crew memory to false when opts.memoryEnabled is false', async () => {
        await saveGeneratedCrew(withLlm as never, undefined, { memoryEnabled: false });
        const payload = post.mock.calls[0][1] as Record<string, unknown>;
        expect(payload.memory).toBe(false);
        const nodes = payload.nodes as Array<{ type: string; data: Record<string, unknown> }>;
        expect(nodes.find((n) => n.type === 'agentNode')!.data.memory).toBe(false);
      });

      it('omits crew-level memory when opts.memoryEnabled is not provided', async () => {
        await saveGeneratedCrew(withLlm as never);
        const payload = post.mock.calls[0][1] as Record<string, unknown>;
        expect('memory' in payload).toBe(false);
      });
    });

    describe('Genie space injection', () => {
      const genieData = {
        agents: [{ id: 'a1', name: 'GenieAgent', tools: ['35'] }],
        tasks: [{ id: 't1', name: 'Ask', tools: ['GenieTool'], agent_id: 'a1' }],
      };

      it('injects tool_configs on Genie-using agent and task nodes when spaceId is set', async () => {
        await saveGeneratedCrew(genieData as never, undefined, { spaceId: 'space-xyz' });
        const nodes = post.mock.calls[0][1].nodes as Array<{ type: string; data: Record<string, unknown> }>;
        const agentNode = nodes.find((n) => n.type === 'agentNode')!;
        const taskNode = nodes.find((n) => n.type === 'taskNode')!;
        expect(agentNode.data.tool_configs).toEqual({ GenieTool: { spaceId: 'space-xyz' } });
        expect(taskNode.data.tool_configs).toEqual({ GenieTool: { spaceId: 'space-xyz' } });
      });

      it('does not inject tool_configs when no spaceId is provided', async () => {
        await saveGeneratedCrew(genieData as never);
        const nodes = post.mock.calls[0][1].nodes as Array<{ type: string; data: Record<string, unknown> }>;
        expect(nodes.find((n) => n.type === 'agentNode')!.data.tool_configs).toBeUndefined();
        expect(nodes.find((n) => n.type === 'taskNode')!.data.tool_configs).toBeUndefined();
      });

      it('does not inject tool_configs on nodes that do not use GenieTool, even with a spaceId', async () => {
        const mixed = {
          agents: [{ id: 'a1', name: 'Plain', tools: ['SomeOtherTool'] }],
          tasks: [{ id: 't1', name: 'Plain task', tools: [], agent_id: 'a1' }],
        };
        await saveGeneratedCrew(mixed as never, undefined, { spaceId: 'space-xyz' });
        const nodes = post.mock.calls[0][1].nodes as Array<{ type: string; data: Record<string, unknown> }>;
        expect(nodes.find((n) => n.type === 'agentNode')!.data.tool_configs).toBeUndefined();
        expect(nodes.find((n) => n.type === 'taskNode')!.data.tool_configs).toBeUndefined();
      });
    });

    describe('MCP server persistence', () => {
      const data = {
        agents: [{ id: 'a1', name: 'A', tools: ['SomeTool'] }],
        tasks: [{ id: 't1', name: 'T', tools: [], agent_id: 'a1' }],
      };

      it('persists the selected MCP servers on EVERY agent and task node', async () => {
        await saveGeneratedCrew(data as never, undefined, {
          mcpServers: ['databricks sql', 'databricks genie: sales'],
        });
        const nodes = post.mock.calls[0][1].nodes as Array<{ type: string; data: Record<string, unknown> }>;
        const expected = { MCP_SERVERS: { servers: ['databricks sql', 'databricks genie: sales'] } };
        expect(nodes.find((n) => n.type === 'agentNode')!.data.tool_configs).toEqual(expected);
        expect(nodes.find((n) => n.type === 'taskNode')!.data.tool_configs).toEqual(expected);
      });

      it('merges the Genie space override with the MCP servers on Genie nodes', async () => {
        const genieData = {
          agents: [{ id: 'a1', name: 'G', tools: ['GenieTool'] }],
          tasks: [{ id: 't1', name: 'Ask', tools: ['GenieTool'], agent_id: 'a1' }],
        };
        await saveGeneratedCrew(genieData as never, undefined, {
          spaceId: 'space-xyz',
          mcpServers: ['my mcp'],
        });
        const nodes = post.mock.calls[0][1].nodes as Array<{ type: string; data: Record<string, unknown> }>;
        const expected = {
          GenieTool: { spaceId: 'space-xyz' },
          MCP_SERVERS: { servers: ['my mcp'] },
        };
        expect(nodes.find((n) => n.type === 'agentNode')!.data.tool_configs).toEqual(expected);
        expect(nodes.find((n) => n.type === 'taskNode')!.data.tool_configs).toEqual(expected);
      });

      it('adds no tool_configs when no MCP servers are selected and no spaceId is set', async () => {
        await saveGeneratedCrew(data as never, undefined, { mcpServers: [] });
        const nodes = post.mock.calls[0][1].nodes as Array<{ type: string; data: Record<string, unknown> }>;
        expect(nodes.find((n) => n.type === 'agentNode')!.data.tool_configs).toBeUndefined();
        expect(nodes.find((n) => n.type === 'taskNode')!.data.tool_configs).toBeUndefined();
      });
    });

    describe('Agent Bricks persistence', () => {
      const data = {
        agents: [{ id: 'a1', name: 'A', tools: ['SomeTool'] }],
        tasks: [{ id: 't1', name: 'T', tools: [], agent_id: 'a1' }],
      };

      it('equips tool 71 on every agent and task node when endpoints are picked', async () => {
        await saveGeneratedCrew(data as never, undefined, {
          agentBricksEndpoints: ['ep-1', 'ep-2'],
        });
        const nodes = post.mock.calls[0][1].nodes as Array<{ type: string; data: Record<string, unknown> }>;
        const agentNode = nodes.find((n) => n.type === 'agentNode')!;
        const taskNode = nodes.find((n) => n.type === 'taskNode')!;
        expect(agentNode.data.tools).toContain('71');
        expect(taskNode.data.tools).toContain('71');
      });

      it('sets tool_configs.AgentBricksTool.endpointName on every agent and task node', async () => {
        await saveGeneratedCrew(data as never, undefined, {
          agentBricksEndpoints: ['ep-1', 'ep-2'],
        });
        const nodes = post.mock.calls[0][1].nodes as Array<{ type: string; data: Record<string, unknown> }>;
        const expected = { AgentBricksTool: { endpointName: ['ep-1', 'ep-2'] } };
        expect(nodes.find((n) => n.type === 'agentNode')!.data.tool_configs).toEqual(expected);
        expect(nodes.find((n) => n.type === 'taskNode')!.data.tool_configs).toEqual(expected);
      });

      it('adds no AgentBricksTool config (and no tool 71) when no endpoints are picked', async () => {
        // empty array → nothing added
        await saveGeneratedCrew(data as never, undefined, { agentBricksEndpoints: [] });
        let nodes = post.mock.calls[0][1].nodes as Array<{ type: string; data: Record<string, unknown> }>;
        expect(nodes.find((n) => n.type === 'agentNode')!.data.tool_configs).toBeUndefined();
        expect(nodes.find((n) => n.type === 'taskNode')!.data.tool_configs).toBeUndefined();
        expect(nodes.find((n) => n.type === 'agentNode')!.data.tools).not.toContain('71');
        expect(nodes.find((n) => n.type === 'taskNode')!.data.tools).not.toContain('71');

        // undefined → nothing added
        post.mockClear();
        await saveGeneratedCrew(data as never);
        nodes = post.mock.calls[0][1].nodes as Array<{ type: string; data: Record<string, unknown> }>;
        expect(nodes.find((n) => n.type === 'agentNode')!.data.tool_configs).toBeUndefined();
        expect(nodes.find((n) => n.type === 'taskNode')!.data.tool_configs).toBeUndefined();
        expect(nodes.find((n) => n.type === 'agentNode')!.data.tools).not.toContain('71');
        expect(nodes.find((n) => n.type === 'taskNode')!.data.tools).not.toContain('71');
      });
    });
  });

  describe('listSavedCrews', () => {
    let get: ReturnType<typeof vi.fn>;

    beforeEach(() => {
      get = vi.fn();
      mockedGetClient.mockReturnValue({ get } as unknown as ReturnType<typeof getClient>);
    });

    it('GETs /crews and maps valid rows to {id,name}', async () => {
      get.mockResolvedValue({ data: [{ id: 1, name: 'Alpha' }, { id: 'c2', name: 'Beta' }] });
      const result = await listSavedCrews();
      expect(get).toHaveBeenCalledWith('/crews');
      expect(result).toEqual([
        { id: '1', name: 'Alpha' },
        { id: 'c2', name: 'Beta' },
      ]);
    });

    it('filters out rows missing id, name, or the row itself', async () => {
      get.mockResolvedValue({
        data: [
          { id: 'c1', name: 'Keep' },
          { id: 'c2' }, // missing name
          { name: 'NoId' }, // missing id
          null, // falsy row
        ],
      });
      const result = await listSavedCrews();
      expect(result).toEqual([{ id: 'c1', name: 'Keep' }]);
    });

    it('returns [] for an empty or undefined response data', async () => {
      get.mockResolvedValue({ data: undefined });
      expect(await listSavedCrews()).toEqual([]);
      get.mockResolvedValue({ data: [] });
      expect(await listSavedCrews()).toEqual([]);
    });
  });

  describe('listSavedFlows', () => {
    let get: ReturnType<typeof vi.fn>;

    beforeEach(() => {
      get = vi.fn();
      mockedGetClient.mockReturnValue({ get } as unknown as ReturnType<typeof getClient>);
    });

    it('GETs /flows and maps valid rows to {id,name}', async () => {
      get.mockResolvedValue({ data: [{ id: 7, name: 'Flow A' }] });
      const result = await listSavedFlows();
      expect(get).toHaveBeenCalledWith('/flows');
      expect(result).toEqual([{ id: '7', name: 'Flow A' }]);
    });

    it('filters out rows missing id, name, or the row itself', async () => {
      get.mockResolvedValue({
        data: [
          { id: 'f1', name: 'Keep' },
          { id: 'f2' }, // missing name
          { name: 'NoId' }, // missing id
          undefined, // falsy row
        ],
      });
      const result = await listSavedFlows();
      expect(result).toEqual([{ id: 'f1', name: 'Keep' }]);
    });

    it('returns [] for an empty or undefined response data', async () => {
      get.mockResolvedValue({ data: undefined });
      expect(await listSavedFlows()).toEqual([]);
      get.mockResolvedValue({ data: [] });
      expect(await listSavedFlows()).toEqual([]);
    });
  });
});

describe('stripGenieTools', () => {
  it('removes Genie refs (name, alias, id) from agent and task tool lists', () => {
    const genieData = {
      agents: [{ name: 'A', tools: ['GenieTool', 'SerperDevTool', 'Genie'] }],
      tasks: [{ name: 'T', tools: ['35', 'DataSearch', 'PerplexityTool'] }],
    };
    const out = stripGenieTools(genieData as never);
    expect(out.agents[0].tools).toEqual(['SerperDevTool']);
    expect(out.tasks[0].tools).toEqual(['PerplexityTool']);
  });

  it('resolves Genie via the toolNameMap for non-canonical ids', () => {
    const out = stripGenieTools(
      { agents: [{ tools: ['99'] }], tasks: [] } as never,
      { '99': 'GenieTool' },
    );
    expect(out.agents[0].tools).toEqual([]);
  });

  it('drops GenieTool tool_configs entries but keeps the rest', () => {
    const out = stripGenieTools({
      agents: [],
      tasks: [{ tool_configs: { GenieTool: { spaceId: 's' }, Reducer: { q: '{x}' } } }],
    } as never);
    expect(out.tasks[0].tool_configs).toEqual({ Reducer: { q: '{x}' } });
  });

  it('leaves items without tools or tool_configs untouched and does not mutate the input', () => {
    const input = {
      agents: [{ name: 'A' }],
      tasks: [{ name: 'T', tools: ['GenieTool'] }],
    };
    const out = stripGenieTools(input as never);
    expect(out.agents[0]).toEqual({ name: 'A' });
    expect(out.tasks[0].tools).toEqual([]);
    expect((input.tasks[0].tools as string[])).toEqual(['GenieTool']); // input untouched
  });

  it('tolerates generations without agents or tasks arrays', () => {
    const out = stripGenieTools({} as never);
    expect(out.agents).toEqual([]);
    expect(out.tasks).toEqual([]);
  });
});

describe('postCrewFeedback', () => {
  let post: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    post = vi.fn().mockResolvedValue({
      data: { id: 'fb-1', crew_id: 'crew-1', rating: 'up', created_at: 'now' },
    });
    mockedGetClient.mockReturnValue({ post } as unknown as ReturnType<typeof getClient>);
  });

  it('posts a thumbs-up without a comment', async () => {
    const entry = await postCrewFeedback('crew-1', 'up');
    expect(post).toHaveBeenCalledWith('/crews/crew-1/feedback', { rating: 'up' });
    expect(entry.id).toBe('fb-1');
  });

  it('posts a thumbs-down with its comment', async () => {
    await postCrewFeedback('crew-1', 'down', 'images were broken');
    expect(post).toHaveBeenCalledWith('/crews/crew-1/feedback', {
      rating: 'down',
      comment: 'images were broken',
    });
  });
});
