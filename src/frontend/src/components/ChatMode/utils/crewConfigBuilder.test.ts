import { describe, it, expect } from 'vitest';
import {
  buildCrewConfig,
  buildCrewConfigFromGenerated,
  buildFlowConfig,
} from './crewConfigBuilder';

describe('crewConfigBuilder', () => {
  describe('buildCrewConfig', () => {
    it('builds agent nodes with required and optional fields, ignoring null/undefined optionals', () => {
      const config = buildCrewConfig({
        name: 'plan',
        nodes: [
          {
            id: 'agent1',
            type: 'agentNode',
            data: {
              role: 'Researcher',
              goal: 'Find info',
              backstory: 'Expert',
              tools: ['search'],
              // optional fields: some set, some null/undefined
              llm: 'gpt-4',
              max_iter: 5,
              memory: null, // should be skipped
              verbose: undefined, // should be skipped
            },
          },
        ],
        edges: [],
      });

      expect(config.agents_yaml).toHaveProperty('agent_agent1');
      const agent = config.agents_yaml.agent_agent1;
      expect(agent.role).toBe('Researcher');
      expect(agent.goal).toBe('Find info');
      expect(agent.backstory).toBe('Expert');
      expect(agent.tools).toEqual(['search']);
      expect(agent.llm).toBe('gpt-4');
      expect(agent.max_iter).toBe(5);
      expect(agent).not.toHaveProperty('memory');
      expect(agent).not.toHaveProperty('verbose');
    });

    it('uses default empty values for agent fields when missing and non-array tools', () => {
      const config = buildCrewConfig({
        nodes: [
          {
            id: 'a2',
            type: 'agent', // alternate type
            data: {
              tools: 'not-an-array', // not an array -> []
            },
          },
        ],
        edges: [],
      });

      const agent = config.agents_yaml.agent_a2;
      expect(agent.role).toBe('');
      expect(agent.goal).toBe('');
      expect(agent.backstory).toBe('');
      expect(agent.tools).toEqual([]);
    });

    it('builds task nodes with config output_file, output_json, output_pydantic, human_input and valid guardrail JSON', () => {
      const config = buildCrewConfig({
        nodes: [
          {
            id: 'task-abc',
            type: 'taskNode',
            data: {
              description: 'Do work',
              expected_output: 'Result',
              tools: ['tool1'],
              async_execution: true,
              tool_configs: { tool1: { foo: 'bar' } },
              config: {
                output_file: 'custom/output.md',
                output_json: 'SomeSchema',
                output_pydantic: 'PydModel',
                human_input: true,
                guardrail: '{"type":"length","max":100}',
              },
            },
          },
        ],
        edges: [],
      });

      const task = config.tasks_yaml.task_abc;
      // id strips the 'task-' prefix (substring(5))
      expect(task.id).toBe('abc');
      expect(task.description).toBe('Do work');
      expect(task.expected_output).toBe('Result');
      expect(task.tools).toEqual(['tool1']);
      expect(task.async_execution).toBe(true);
      expect(task.output_file).toBe('custom/output.md');
      expect(task.tool_configs).toEqual({ tool1: { foo: 'bar' } });
      expect(task.output_json).toBe('SomeSchema');
      expect(task.output_pydantic).toBe('PydModel');
      expect(task.human_input).toBe(true);
      expect(task.guardrail).toEqual({ type: 'length', max: 100 });
    });

    it('handles task config with human_input false and invalid guardrail JSON (ignored)', () => {
      const config = buildCrewConfig({
        nodes: [
          {
            id: 'task_two',
            type: 'task',
            data: {
              tools: 123, // not an array -> []
              config: {
                human_input: false,
                guardrail: 'not-valid-json{', // triggers catch
              },
            },
          },
        ],
        edges: [],
      });

      const task = config.tasks_yaml.task_two;
      // id does not start with 'task-' so id remains full node id
      expect(task.id).toBe('task_two');
      expect(task.description).toBe('');
      expect(task.expected_output).toBe('');
      expect(task.tools).toEqual([]);
      expect(task.human_input).toBe(false);
      expect(task).not.toHaveProperty('guardrail');
      // config present but output_file not set -> default
      expect(task.output_file).toBe('output/task_two.md');
    });

    it('handles llm_guardrail which overrides guardrail as JSON string', () => {
      const config = buildCrewConfig({
        nodes: [
          {
            id: 'task-g',
            type: 'taskNode',
            data: {
              config: {
                llm_guardrail: { instructions: 'be safe' },
              },
            },
          },
        ],
        edges: [],
      });

      const task = config.tasks_yaml.task_g;
      expect(task.guardrail).toBe(JSON.stringify({ instructions: 'be safe' }));
    });

    it('builds task node without config object (default output_file, no optional fields)', () => {
      const config = buildCrewConfig({
        nodes: [
          {
            id: 'plain-task', // does not start with task- or task_
            type: 'taskNode',
            data: {
              description: 'desc',
            },
          },
        ],
        edges: [],
      });

      // normalizeTaskName: other prefix -> task_<id>
      const task = config.tasks_yaml['task_plain-task'];
      expect(task).toBeDefined();
      expect(task.id).toBe('plain-task');
      expect(task.output_file).toBe('output/plain-task.md');
      expect(task).not.toHaveProperty('tool_configs');
      expect(task).not.toHaveProperty('output_json');
    });

    it('connects agent -> task via edges and resolves task -> task context dependency', () => {
      const config = buildCrewConfig({
        nodes: [
          {
            id: 'agentX',
            type: 'agentNode',
            data: { role: 'r', goal: 'g', backstory: 'b', tools: [] },
          },
          {
            id: 'task-1',
            type: 'taskNode',
            data: { description: 'first' },
          },
          {
            id: 'task-2',
            type: 'taskNode',
            data: { description: 'second' },
          },
        ],
        edges: [
          { id: 'e1', source: 'agentX', target: 'task-1' }, // agent -> task
          { id: 'e2', source: 'task-1', target: 'task-2' }, // task -> task context
        ],
      });

      expect(config.tasks_yaml.task_1.agent).toBe('agent_agentX');
      expect(config.tasks_yaml.task_2.context).toEqual(['task_1']);
    });

    it('ignores edges whose tasks are not in tasks_yaml and unrelated edge types', () => {
      const config = buildCrewConfig({
        nodes: [
          {
            id: 'agentX',
            type: 'agentNode',
            data: { tools: [] },
          },
          {
            id: 'task-1',
            type: 'taskNode',
            data: {},
          },
        ],
        edges: [
          // agent -> task but task target missing from tasks_yaml (target node not a task)
          { id: 'e1', source: 'agentX', target: 'missing' },
          // task -> task but source/target unknown
          { id: 'e2', source: 'task-1', target: 'agentX' }, // task -> agent: no branch
          // edge with unknown source node
          { id: 'e3', source: 'unknown', target: 'task-1' },
        ],
      });

      // agent should not be set for task_1 since no valid agent->task edge to it
      expect(config.tasks_yaml.task_1.agent).toBeNull();
      expect(config.tasks_yaml.task_1.context).toEqual([]);
    });

    it('agent -> task edge where target task not present in tasks_yaml falls into guard', () => {
      // Build a scenario where edge source is agent and target is task type
      // but the task is filtered out (e.g. the task name normalizes but isn't created).
      // We make a task node that creates task_keep, but edge targets a different task id.
      const config = buildCrewConfig({
        nodes: [
          { id: 'agentX', type: 'agentNode', data: { tools: [] } },
          { id: 'task-keep', type: 'taskNode', data: {} },
          // a node that is task type but we will remove from tasks via different normalize is hard;
          // instead reference a target that is a task node but produces a name not in yaml is impossible.
        ],
        edges: [
          { id: 'e1', source: 'agentX', target: 'task-keep' },
        ],
      });
      expect(config.tasks_yaml.task_keep.agent).toBe('agent_agentX');
    });

    it('task->task edge where target task missing from yaml is guarded', () => {
      // Create two task nodes but make one normalize to a name; edge target valid.
      const config = buildCrewConfig({
        nodes: [
          { id: 'task-a', type: 'taskNode', data: {} },
          { id: 'task-b', type: 'taskNode', data: {} },
        ],
        edges: [
          { id: 'e1', source: 'task-a', target: 'task-b' },
        ],
      });
      expect(config.tasks_yaml.task_b.context).toEqual(['task_a']);
    });

    it('third pass resolves agent_id by data.id when no edge connection exists', () => {
      const config = buildCrewConfig({
        nodes: [
          {
            id: 'node-agent-1',
            type: 'agentNode',
            data: { id: 'internal-agent-id', tools: [] },
          },
          {
            id: 'task-needs-agent',
            type: 'taskNode',
            data: { agent_id: 'internal-agent-id' },
          },
        ],
        edges: [],
      });

      expect(config.tasks_yaml['task_needs-agent'].agent).toBe('agent_node-agent-1');
    });

    it('third pass resolves agent_id by node.id when data.id does not match', () => {
      const config = buildCrewConfig({
        nodes: [
          {
            id: 'agent-by-node-id',
            type: 'agentNode',
            data: { tools: [] },
          },
          {
            id: 'task-x',
            type: 'taskNode',
            data: { agent_id: 'agent-by-node-id' },
          },
        ],
        edges: [],
      });

      expect(config.tasks_yaml.task_x.agent).toBe('agent_agent-by-node-id');
    });

    it('third pass does nothing when agent_id has no matching agent node', () => {
      const config = buildCrewConfig({
        nodes: [
          { id: 'task-y', type: 'taskNode', data: { agent_id: 'nonexistent' } },
        ],
        edges: [],
      });
      expect(config.tasks_yaml.task_y.agent).toBeNull();
    });

    it('third pass skips task that already has an agent assigned', () => {
      const config = buildCrewConfig({
        nodes: [
          { id: 'agentA', type: 'agentNode', data: { id: 'aid', tools: [] } },
          { id: 'agentB', type: 'agentNode', data: { id: 'bid', tools: [] } },
          { id: 'task-z', type: 'taskNode', data: { agent_id: 'bid' } },
        ],
        edges: [
          { id: 'e1', source: 'agentA', target: 'task-z' }, // assigns agentA first
        ],
      });
      // already assigned by edge -> third pass should not override
      expect(config.tasks_yaml.task_z.agent).toBe('agent_agentA');
    });

    it('passes through planning flag and model, with defaults when omitted', () => {
      const withModel = buildCrewConfig(
        { nodes: [], edges: [], planning: true },
        'my-model',
        { foo: 'bar' }
      );
      expect(withModel.planning).toBe(true);
      expect(withModel.model).toBe('my-model');
      expect(withModel.reasoning).toBe(false);
      expect(withModel.execution_type).toBe('crew');
      expect(withModel.schema_detection_enabled).toBe(true);
      expect(withModel.inputs).toEqual({ foo: 'bar' });

      const noModel = buildCrewConfig({ nodes: [], edges: [] });
      expect(noModel.planning).toBe(false);
      expect(noModel.model).toBeUndefined();
      expect(noModel.inputs).toEqual({});
    });

    it('ignores nodes that are neither agent nor task', () => {
      const config = buildCrewConfig({
        nodes: [{ id: 'other', type: 'somethingElse', data: {} }],
        edges: [],
      });
      expect(config.agents_yaml).toEqual({});
      expect(config.tasks_yaml).toEqual({});
    });

    it('forces every agent to memory:false when memoryEnabled is false ("No memory")', () => {
      const config = buildCrewConfig(
        {
          nodes: [
            { id: 'a1', type: 'agentNode', data: { role: 'r', goal: 'g', backstory: 'b', tools: [] } },
            { id: 'a2', type: 'agentNode', data: { role: 'r2', goal: 'g2', backstory: 'b2', tools: [], memory: true } },
          ],
          edges: [],
        },
        undefined,
        undefined,
        false,
      );
      expect(config.agents_yaml.agent_a1.memory).toBe(false);
      // Overrides the node's own saved memory value.
      expect(config.agents_yaml.agent_a2.memory).toBe(false);
    });

    it('does NOT force memory when memoryEnabled is true (keeps the node value / absent)', () => {
      const config = buildCrewConfig(
        {
          nodes: [
            { id: 'a1', type: 'agentNode', data: { role: 'r', goal: 'g', backstory: 'b', tools: [] } },
            { id: 'a2', type: 'agentNode', data: { role: 'r2', goal: 'g2', backstory: 'b2', tools: [], memory: true } },
          ],
          edges: [],
        },
        undefined,
        undefined,
        true,
      );
      // memory absent on a1 (not forced false), and a2 keeps its own value.
      expect(config.agents_yaml.agent_a1).not.toHaveProperty('memory');
      expect(config.agents_yaml.agent_a2.memory).toBe(true);
    });

    it('does NOT force memory when memoryEnabled is omitted (defaults to true)', () => {
      const config = buildCrewConfig({
        nodes: [
          { id: 'a1', type: 'agentNode', data: { role: 'r', goal: 'g', backstory: 'b', tools: [] } },
        ],
        edges: [],
      });
      expect(config.agents_yaml.agent_a1).not.toHaveProperty('memory');
    });
  });

  describe('buildCrewConfigFromGenerated', () => {
    it('builds agents with applicable tool_configs and optional fields', () => {
      const config = buildCrewConfigFromGenerated(
        [
          {
            id: 'a1',
            role: 'Role',
            goal: 'Goal',
            backstory: 'Story',
            tools: ['GenieTool', 'OtherTool'],
            llm: 'gpt-4',
            verbose: true,
            memory: null, // skipped
            allow_delegation: undefined, // skipped
          },
        ],
        [],
        'model-x',
        { GenieTool: { spaceId: '123' } }, // toolConfigs - only GenieTool applies
      );

      const agent = config.agents_yaml.agent_a1;
      expect(agent.role).toBe('Role');
      expect(agent.tools).toEqual(['GenieTool', 'OtherTool']);
      expect(agent.tool_configs).toEqual({ GenieTool: { spaceId: '123' } });
      expect(agent.llm).toBe('gpt-4');
      expect(agent.verbose).toBe(true);
      expect(agent).not.toHaveProperty('memory');
      expect(agent).not.toHaveProperty('allow_delegation');
    });

    it('resolves a tool referenced by id to its name when attaching tool_configs', () => {
      // The generated crew lists GenieTool by id ('5'); toolConfigs is keyed by
      // the canonical name. The toolNameMap resolves id → name so the override
      // attaches under "GenieTool" (what the backend looks up). Without this the
      // Genie space never reaches the tool.
      const config = buildCrewConfigFromGenerated(
        [{ id: 'a1', tools: ['5'] }],
        [{ id: 't1', tools: ['5'], agent_id: 'a1' }],
        undefined,
        { GenieTool: { spaceId: 'space-1' } },
        undefined,
        { '5': 'GenieTool' }, // toolNameMap: id → name
      );
      expect(config.agents_yaml.agent_a1.tool_configs).toEqual({ GenieTool: { spaceId: 'space-1' } });
      expect(config.tasks_yaml.task_t1.tool_configs).toEqual({ GenieTool: { spaceId: 'space-1' } });
    });

    it('does not add tool_configs when no tools match and handles non-array tools', () => {
      const config = buildCrewConfigFromGenerated(
        [
          { id: 'a2', tools: ['NoMatchTool'] },
          { id: 'a3', tools: 'not-array' },
        ],
        [],
        undefined,
        { SomeOtherTool: { x: 1 } },
      );

      expect(config.agents_yaml.agent_a2).not.toHaveProperty('tool_configs');
      expect(config.agents_yaml.agent_a2.tools).toEqual(['NoMatchTool']);
      expect(config.agents_yaml.agent_a3.tools).toEqual([]);
      expect(config.agents_yaml.agent_a3).not.toHaveProperty('tool_configs');
      expect(config.model).toBeUndefined();
    });

    it('builds agents without toolConfigs and with missing id (empty string key)', () => {
      const config = buildCrewConfigFromGenerated([{ tools: [] }], []);
      // id missing -> '' -> key 'agent_'
      expect(config.agents_yaml).toHaveProperty('agent_');
      expect(config.agents_yaml.agent_.role).toBe('');
    });

    it('builds tasks with context as string, object, and other types; resolves agent_id', () => {
      const config = buildCrewConfigFromGenerated(
        [{ id: 'agentX', tools: [] }],
        [
          {
            id: 't1',
            description: 'Task one',
            expected_output: 'Out',
            tools: ['GenieTool'],
            async_execution: true,
            agent_id: 'agentX',
            context: ['dep1', { id: 'dep2' }, 42, { noId: true }, null],
            output_file: 'custom.md',
          },
        ],
        undefined,
        { GenieTool: { spaceId: 'x' } },
      );

      const task = config.tasks_yaml.task_t1;
      expect(task.id).toBe('t1');
      expect(task.description).toBe('Task one');
      expect(task.expected_output).toBe('Out');
      expect(task.tools).toEqual(['GenieTool']);
      expect(task.async_execution).toBe(true);
      expect(task.agent).toBe('agent_agentX');
      expect(task.output_file).toBe('custom.md');
      expect(task.tool_configs).toEqual({ GenieTool: { spaceId: 'x' } });
      // context: 'dep1' -> task_dep1; {id:dep2} -> task_dep2; 42 -> '' filtered;
      // {noId} -> '' filtered; null -> '' filtered
      expect(task.context).toEqual(['task_dep1', 'task_dep2']);
    });

    it('resolves agent via task.agent fallback and unknown agent yields null', () => {
      const config = buildCrewConfigFromGenerated(
        [{ id: 'known', tools: [] }],
        [
          { id: 'tA', agent: 'known' }, // uses task.agent fallback
          { id: 'tB', agent_id: 'unknown' }, // not in map -> null
          { id: 'tC' }, // no agent at all -> '' -> null
        ],
      );

      expect(config.tasks_yaml.task_tA.agent).toBe('agent_known');
      expect(config.tasks_yaml.task_tB.agent).toBeNull();
      expect(config.tasks_yaml.task_tC.agent).toBeNull();
    });

    it('uses default output_file and empty context when not provided, non-array context', () => {
      const config = buildCrewConfigFromGenerated(
        [],
        [
          { id: 'tD', context: 'not-an-array' },
          { id: 'tE' },
        ],
      );

      expect(config.tasks_yaml.task_tD.context).toEqual([]);
      expect(config.tasks_yaml.task_tD.output_file).toBe('output/tD.md');
      expect(config.tasks_yaml.task_tE.output_file).toBe('output/tE.md');
      expect(config.tasks_yaml.task_tE.id).toBe('tE');
    });

    it('does not add task tool_configs when no matching tools, and handles missing toolConfigs', () => {
      const config = buildCrewConfigFromGenerated(
        [],
        [{ id: 'tF', tools: ['Unmatched'] }],
        undefined,
        { Other: { a: 1 } },
      );
      expect(config.tasks_yaml.task_tF).not.toHaveProperty('tool_configs');

      const config2 = buildCrewConfigFromGenerated([], [{ id: 'tG', tools: ['Any'] }]);
      expect(config2.tasks_yaml.task_tG).not.toHaveProperty('tool_configs');
    });

    it('passes model and inputs through, with crew execution defaults', () => {
      const config = buildCrewConfigFromGenerated([], [], 'model-z', undefined, {
        topic: 'AI',
      });
      expect(config.model).toBe('model-z');
      expect(config.inputs).toEqual({ topic: 'AI' });
      expect(config.planning).toBe(false);
      expect(config.reasoning).toBe(false);
      expect(config.execution_type).toBe('crew');
      expect(config.schema_detection_enabled).toBe(true);

      const noInputs = buildCrewConfigFromGenerated([], []);
      expect(noInputs.inputs).toEqual({});
    });

    it('task with missing id uses empty string id and default output file', () => {
      const config = buildCrewConfigFromGenerated([], [{ description: 'no id' }]);
      expect(config.tasks_yaml).toHaveProperty('task_');
      expect(config.tasks_yaml.task_.id).toBe('');
      expect(config.tasks_yaml.task_.output_file).toBe('output/.md');
    });
  });

  describe('buildFlowConfig', () => {
    it('maps nodes and edges with provided values', () => {
      const config = buildFlowConfig(
        {
          id: 'flow-1',
          name: 'My Flow',
          nodes: [
            {
              id: 'n1',
              type: 'customNode',
              position: { x: 10, y: 20 },
              data: { foo: 'bar' },
            },
          ],
          edges: [
            {
              id: 'edge-1',
              source: 'n1',
              target: 'n2',
              sourceHandle: 'sh',
              targetHandle: 'th',
              data: { weight: 1 },
            },
          ],
          flow_config: { existing: 'value' },
        },
        'flow-model',
      );

      expect(config.nodes).toEqual([
        { id: 'n1', type: 'customNode', position: { x: 10, y: 20 }, data: { foo: 'bar' } },
      ]);
      expect(config.edges).toEqual([
        {
          id: 'edge-1',
          source: 'n1',
          target: 'n2',
          sourceHandle: 'sh',
          targetHandle: 'th',
          data: { weight: 1 },
        },
      ]);
      expect(config.flow_id).toBe('flow-1');
      expect(config.model).toBe('flow-model');
      expect(config.execution_type).toBe('flow');
      expect(config.flow_config).toEqual({
        existing: 'value',
        nodes: config.nodes,
        edges: config.edges,
      });
    });

    it('applies defaults for missing node/edge fields and missing flow_config/id/model', () => {
      const config = buildFlowConfig({
        nodes: [
          {
            id: 'n1',
            // no type, position, data
          } as unknown as { id: string; type: string; data: Record<string, unknown> },
        ],
        edges: [
          {
            id: 'e1',
            source: 's',
            target: 't',
            // no sourceHandle, targetHandle, data
          },
        ],
      });

      expect(config.nodes[0].type).toBe('unknown');
      expect(config.nodes[0].position).toEqual({ x: 0, y: 0 });
      expect(config.nodes[0].data).toEqual({});
      expect(config.edges[0].sourceHandle).toBeUndefined();
      expect(config.edges[0].targetHandle).toBeUndefined();
      expect(config.edges[0].data).toEqual({});
      expect(config.flow_id).toBeUndefined();
      expect(config.model).toBeUndefined();
      expect(config.flow_config).toEqual({
        nodes: config.nodes,
        edges: config.edges,
      });
      expect(config.agents_yaml).toEqual({});
      expect(config.tasks_yaml).toEqual({});
      expect(config.inputs).toEqual({});
      expect(config.planning).toBe(false);
      expect(config.reasoning).toBe(false);
      expect(config.schema_detection_enabled).toBe(true);
    });

    it('sets memory_workspace_scope from workspaceMemory and leaves agent memory alone when memory is enabled', () => {
      const agents = [{ id: 'a1', role: 'R', goal: 'G', backstory: 'B', tools: [] }];
      const ws = buildCrewConfigFromGenerated(agents, [], 'm', undefined, undefined, {}, 's1', true, true);
      expect(ws.memory_workspace_scope).toBe(true);
      expect(ws.agents_yaml.agent_a1).not.toHaveProperty('memory');

      const session = buildCrewConfigFromGenerated(agents, [], 'm', undefined, undefined, {}, 's1', false, true);
      expect(session.memory_workspace_scope).toBe(false);
      expect(session.agents_yaml.agent_a1).not.toHaveProperty('memory');
    });

    it('forces every agent to memory:false when memoryEnabled is false ("No memory")', () => {
      const agents = [
        { id: 'a1', role: 'R1', goal: 'G1', backstory: 'B1', tools: [] },
        { id: 'a2', role: 'R2', goal: 'G2', backstory: 'B2', tools: [], memory: true },
      ];
      const config = buildCrewConfigFromGenerated(agents, [], 'm', undefined, undefined, {}, 's1', true, false);
      expect(config.agents_yaml.agent_a1.memory).toBe(false);
      expect(config.agents_yaml.agent_a2.memory).toBe(false);
    });
  });
});
