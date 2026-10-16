/**
 * A partial task update sends only what the caller passed.
 *
 * `updateTask` used to fill every omitted field with a default — a config with
 * human_input false and no guardrail or output schema, agent_id "", context [],
 * a root llm_guardrail null — so the Tools chip and the knowledge upload
 * switched off a task's approval gate, LLM guardrail and output schema and
 * unassigned its agent, while the node still showed them. The backend keeps
 * whatever the body leaves out, so leaving it out is the whole fix.
 */
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { TaskService } from './TaskService';
import type { Task } from '../../types/workflow/task';

const api = vi.hoisted(() => ({ put: vi.fn() }));
vi.mock('../../shared/api/client', () => ({ apiClient: api }));

beforeEach(() => {
  vi.clearAllMocks();
  api.put.mockResolvedValue({ data: { id: 't1' } });
});

const sentBody = () => {
  expect(api.put).toHaveBeenCalledOnce();
  const [url, body] = api.put.mock.calls[0];
  expect(url).toBe('/tasks/t1');
  return body as Record<string, unknown>;
};

describe('TaskService.updateTask sends a true partial update', () => {
  it('Tools/MCP chip: sends tools and tool_configs and nothing else', async () => {
    // TaskNode.handleQuickDialogApply
    await TaskService.updateTask('t1', {
      tools: ['SerperDevTool'],
      tool_configs: { MCP_SERVERS: { servers: ['docs'] } },
    });

    expect(sentBody()).toEqual({
      tools: ['SerperDevTool'],
      tool_configs: { MCP_SERVERS: { servers: ['docs'] } },
    });
  });

  it('Tools/MCP chip: clearing every tool still sends the empty list', async () => {
    await TaskService.updateTask('t1', { tools: [], tool_configs: {} });
    expect(sentBody()).toEqual({ tools: [], tool_configs: {} });
  });

  it('knowledge upload: sends tools and tool_configs and nothing else', async () => {
    // WorkflowChat knowledge-file upload, once per task node
    const toolConfigs = {
      DatabricksKnowledgeSearchTool: { file_paths: ['/Volumes/main/docs/a.pdf'], agent_id: 'writer' },
    };
    await TaskService.updateTask('t1', {
      tools: ['DatabricksKnowledgeSearchTool'],
      tool_configs: toolConfigs,
    });

    const body = sentBody();
    expect(body).toEqual({ tools: ['DatabricksKnowledgeSearchTool'], tool_configs: toolConfigs });
    for (const key of ['config', 'agent_id', 'context', 'llm_guardrail', 'markdown', 'async_execution']) {
      expect(body).not.toHaveProperty(key);
    }
  });

  it('GEPA Apply shape: a prompt-text write sends only the prompt text', async () => {
    // The optimizer's own write is server-side ({description, expected_output});
    // the same shape from the client must stay that small too.
    await TaskService.updateTask('t1', {
      description: '  sharper prompt ',
      expected_output: ' a tighter report ',
    });

    expect(sentBody()).toEqual({ description: 'sharper prompt', expected_output: 'a tighter report' });
  });

  it('Genie prompt: patches tool_configs alone', async () => {
    // GenieSpaceConfigPrompt
    await TaskService.patchTaskToolConfigs('t1', { GenieTool: { spaceId: 'space-1' } });
    expect(sentBody()).toEqual({ tool_configs: { GenieTool: { spaceId: 'space-1' } } });
  });

  it.each(['', null])('never sends agent_id %j — the backend would unassign the task', async agentId => {
    // The canvas task editor passed agent_id "" on every save.
    await TaskService.updateTask('t1', { description: 'edited', agent_id: agentId });
    expect(sentBody()).toEqual({ description: 'edited' });
  });

  it('still sends a real agent_id', async () => {
    await TaskService.updateTask('t1', { agent_id: 'writer' });
    expect(sentBody()).toEqual({ agent_id: 'writer' });
  });

  it('a full form save still sends a complete, normalized config', async () => {
    // TaskForm passes the whole task; a config that is sent replaces the
    // stored one, so it has to be complete and clears what it leaves out.
    const guardrail = { description: 'cites sources' };
    await TaskService.updateTask('t1', {
      name: ' Draft ',
      tools: [],
      context: ['t0'],
      config: { human_input: true, output_pydantic: 'Report', llm_guardrail: guardrail } as Task['config'],
    });

    const body = sentBody();
    expect(body).toMatchObject({
      name: 'Draft',
      tools: [],
      context: ['t0'],
      markdown: false,
      llm_guardrail: guardrail,
      config: expect.objectContaining({
        human_input: true,
        output_pydantic: 'Report',
        llm_guardrail: guardrail,
        callback: null,
        retry_on_fail: false,
        markdown: false,
      }),
    });
    expect(body).not.toHaveProperty('agent_id');
  });
});
