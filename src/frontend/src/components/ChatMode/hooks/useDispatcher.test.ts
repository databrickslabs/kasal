import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { act, renderHook } from '@testing-library/react';
import { useDispatcher } from './useDispatcher';
import { dispatch } from '../api/dispatcher';
import { generateId } from '../utils/markdown';
import { useExecutionStore } from '../store/executionStore';
import type { DispatchResult, GenerationCompleteData } from '../types/dispatcher';

vi.mock('../api/dispatcher', () => ({
  dispatch: vi.fn(),
}));

vi.mock('../utils/markdown', () => ({
  generateId: vi.fn(),
}));

const mockedDispatch = vi.mocked(dispatch);
const mockedGenerateId = vi.mocked(generateId);

const ASSISTANT_ID = 'assistant-id-1';

function makeOptions(overrides: Record<string, unknown> = {}) {
  return {
    addMessage: vi.fn((_role: string, _content: string) => 'user-msg-id'),
    addMessageToTargetSession: vi.fn(() => 'targeted-msg-id'),
    updateMessage: vi.fn(),
    updateMessageInTargetSession: vi.fn(),
    onStartGenerationStream: vi.fn(),
    onStartExecutionStream: vi.fn(),
    onExecuteCrew: vi.fn(),
    onExecuteFlow: vi.fn(),
    onExecuteGenerated: vi.fn(),
    getCurrentSessionId: vi.fn(() => 'session-1'),
    ensureSession: vi.fn(async () => 'session-1'),
    ...overrides,
  };
}

function result(
  intent: DispatchResult['dispatcher']['intent'],
  generation_result: unknown,
): DispatchResult {
  return {
    dispatcher: { intent, confidence: 1, extracted_info: {} },
    generation_result,
    service_called: null,
  };
}

// ChatMode run settings the hook passes to dispatch() as the 4th arg. Defaults:
// session-1 (getCurrentSessionId), and the execution store's own defaults
// (workspace-wide memory recall, memory on, no MCP servers). The 5th arg is the
// CLEAN user message (before the intent-steering prefix is added to dispatch).
const RUN_SETTINGS = {
  auto_execute: true,
  session_id: 'session-1',
  memory_workspace_scope: true,
  disable_memory: false,
  mcp_servers: [],
  agentbricks_endpoints: [],
  // Default answer mode is 'chat' (single light agent) — also skips the
  // crew-plan steering prefix (research/deep keep it).
  chat_mode_type: 'chat',
};

beforeEach(() => {
  vi.clearAllMocks();
  mockedGenerateId.mockReturnValue(ASSISTANT_ID);
  // Reset the shared store's answer mode so each test starts at the 'chat'
  // default (tests that need crew augmentation opt into research explicitly).
  useExecutionStore.getState().setChatModeType('chat');
  // Silence console noise from the hook.
  vi.spyOn(console, 'log').mockImplementation(() => undefined);
});

afterEach(() => {
  vi.restoreAllMocks();
});

describe('useDispatcher', () => {
  it('exposes sendMessage, isDispatching ref, and setLastGenerated', () => {
    const { result: hook } = renderHook(() => useDispatcher(makeOptions()));
    expect(typeof hook.current.sendMessage).toBe('function');
    expect(hook.current.isDispatching).toHaveProperty('current', false);
    expect(typeof hook.current.setLastGenerated).toBe('function');
  });

  it('re-entrancy guard: ignores a second call while dispatching', async () => {
    const opts = makeOptions();
    let resolveDispatch: (v: DispatchResult) => void = () => undefined;
    mockedDispatch.mockReturnValue(
      new Promise<DispatchResult>((res) => {
        resolveDispatch = res;
      }),
    );

    const { result: hook } = renderHook(() => useDispatcher(opts));

    let firstCall: Promise<void>;
    act(() => {
      firstCall = hook.current.sendMessage('hello there');
    });

    // Second call while the first is still in-flight is ignored.
    await act(async () => {
      await hook.current.sendMessage('second message');
    });

    expect(mockedDispatch).toHaveBeenCalledTimes(1);

    await act(async () => {
      resolveDispatch(result('conversation', null));
      await firstCall;
    });
  });

  describe('first-turn session binding (fresh chat, no current session yet)', () => {
    it('awaits ensureSession and registers the generation stream under the REAL id, not empty', async () => {
      // Regression: on a fresh chat the session is created lazily, so
      // getCurrentSessionId() is still null when the first prompt is sent (a
      // window that's ~zero locally but wide under Databricks Apps' remote
      // Postgres). The run must be registered under the id ensureSession
      // resolves — not '' — otherwise the completed result is owner-routed to
      // nowhere and the first prompt renders empty (second prompt works).
      const opts = makeOptions({
        getCurrentSessionId: vi.fn(() => null),
        ensureSession: vi.fn(async () => 'created-session'),
      });
      useExecutionStore.getState().setChatModeType('chat');
      mockedDispatch.mockResolvedValue(
        result('generate_crew', { type: 'streaming', generation_id: 'g1' }),
      );
      const { result: hook } = renderHook(() => useDispatcher(opts));

      await act(async () => {
        await hook.current.sendMessage('build me a crew');
      });

      expect(opts.ensureSession).toHaveBeenCalled();
      // The generation stream owner is the resolved session id, never ''.
      expect(opts.onStartGenerationStream).toHaveBeenCalledWith('g1', 'created-session');
      // And the run settings carry the real session id.
      expect(mockedDispatch).toHaveBeenCalledWith(
        expect.any(String),
        undefined,
        undefined,
        expect.objectContaining({ session_id: 'created-session' }),
        'build me a crew',
      );
    });
  });

  describe('ec/execute crew local intercept', () => {
    it.each(['ec', 'execute crew', 'run crew', 'start crew', '  EC  '])(
      'intercepts %s when lastGenerated is set',
      async (cmd) => {
        const opts = makeOptions();
        const { result: hook } = renderHook(() => useDispatcher(opts));

        const data: GenerationCompleteData = { agents: [{ role: 'r' }], tasks: [] };
        act(() => {
          hook.current.setLastGenerated(data);
        });

        await act(async () => {
          await hook.current.sendMessage(cmd);
        });

        expect(opts.addMessage).toHaveBeenCalledWith('user', cmd);
        expect(opts.onExecuteGenerated).toHaveBeenCalledWith(data);
        expect(mockedDispatch).not.toHaveBeenCalled();
        expect(hook.current.isDispatching.current).toBe(false);
      },
    );

    it('does NOT intercept when lastGenerated is not set', async () => {
      const opts = makeOptions();
      mockedDispatch.mockResolvedValue(result('conversation', null));
      const { result: hook } = renderHook(() => useDispatcher(opts));

      await act(async () => {
        await hook.current.sendMessage('ec');
      });

      // Falls through to normal dispatch path.
      expect(mockedDispatch).toHaveBeenCalledTimes(1);
      expect(opts.onExecuteGenerated).not.toHaveBeenCalled();
    });

    it('does NOT intercept when onExecuteGenerated is undefined', async () => {
      const opts = makeOptions({ onExecuteGenerated: undefined });
      mockedDispatch.mockResolvedValue(result('conversation', null));
      const { result: hook } = renderHook(() => useDispatcher(opts));

      act(() => {
        hook.current.setLastGenerated({ agents: [{ role: 'r' }], tasks: [] });
      });

      await act(async () => {
        await hook.current.sendMessage('ec');
      });

      expect(mockedDispatch).toHaveBeenCalledTimes(1);
    });
  });

  describe('prompt augmentation', () => {
    it('does NOT augment slash commands', async () => {
      const opts = makeOptions();
      mockedDispatch.mockResolvedValue(result('catalog_help', { message: 'help' }));
      const { result: hook } = renderHook(() => useDispatcher(opts));

      await act(async () => {
        await hook.current.sendMessage('/help');
      });

      expect(mockedDispatch).toHaveBeenCalledWith('/help', undefined, undefined, RUN_SETTINGS, '/help');
    });

    it('does NOT augment messages already containing a crew hint', async () => {
      const opts = makeOptions();
      mockedDispatch.mockResolvedValue(result('generate_crew', { type: 'streaming', generation_id: 'g1' }));
      const { result: hook } = renderHook(() => useDispatcher(opts));

      await act(async () => {
        await hook.current.sendMessage('build me a crew', 'my-model');
      });

      expect(mockedDispatch).toHaveBeenCalledWith('build me a crew', 'my-model', undefined, RUN_SETTINGS, 'build me a crew');
    });

    it('augments plain messages with the crew steering prefix (research/deep)', async () => {
      const opts = makeOptions();
      mockedDispatch.mockResolvedValue(result('conversation', null));
      // Augmentation only happens for crew-building modes; chat (the default)
      // sends the literal message to a single light agent.
      useExecutionStore.getState().setChatModeType('research');
      const { result: hook } = renderHook(() => useDispatcher(opts));

      await act(async () => {
        await hook.current.sendMessage('do something cool');
      });

      expect(mockedDispatch).toHaveBeenCalledWith(
        'create a crew plan with agents and tasks: do something cool',
        undefined,
        undefined,
        { ...RUN_SETTINGS, chat_mode_type: 'research' },
        'do something cool',
      );
    });

    it('does NOT augment in chat (light agent) mode — sends the literal message', async () => {
      const opts = makeOptions();
      mockedDispatch.mockResolvedValue(result('conversation', null));
      // 'chat' is the default (reset in beforeEach); the prefix must be skipped.
      const { result: hook } = renderHook(() => useDispatcher(opts));

      await act(async () => {
        await hook.current.sendMessage('do something cool');
      });

      expect(mockedDispatch).toHaveBeenCalledWith(
        'do something cool',
        undefined,
        undefined,
        RUN_SETTINGS,
        'do something cool',
      );
    });

    it('appends the hidden dispatchSuffix to the payload but shows the clean message', async () => {
      const opts = makeOptions();
      mockedDispatch.mockResolvedValue(result('generate_crew', { type: 'streaming', generation_id: 'g1' }));
      const { result: hook } = renderHook(() => useDispatcher(opts));

      await act(async () => {
        await hook.current.sendMessage(
          'build me a crew',
          'm',
          ['DatabricksKnowledgeSearchTool'],
          '\n\n[Knowledge files attached: a.txt.]',
        );
      });

      // The chat shows the clean message (no suffix, no attachments).
      expect(opts.addMessage).toHaveBeenCalledWith('user', 'build me a crew', undefined);
      // The dispatch payload carries the suffix + the tool.
      expect(mockedDispatch).toHaveBeenCalledWith(
        'build me a crew\n\n[Knowledge files attached: a.txt.]',
        'm',
        ['DatabricksKnowledgeSearchTool'],
        RUN_SETTINGS,
        'build me a crew',
      );
    });

    it('forwards the selected Agent Bricks endpoints in the run settings', async () => {
      // The execution store's picked endpoints must ride along in the dispatch
      // run settings as agentbricks_endpoints so the backend equips the tool.
      const opts = makeOptions();
      mockedDispatch.mockResolvedValue(result('conversation', null));
      useExecutionStore.getState().setSelectedAgentBricksEndpoints(['ep-1']);
      // Research mode so the crew prefix is applied (matches the expectation).
      useExecutionStore.getState().setChatModeType('research');
      const { result: hook } = renderHook(() => useDispatcher(opts));

      try {
        await act(async () => {
          await hook.current.sendMessage('do something cool');
        });

        expect(mockedDispatch).toHaveBeenCalledWith(
          'create a crew plan with agents and tasks: do something cool',
          undefined,
          undefined,
          { ...RUN_SETTINGS, agentbricks_endpoints: ['ep-1'], chat_mode_type: 'research' },
          'do something cool',
        );
      } finally {
        // Reset so the shared store doesn't leak into other tests.
        useExecutionStore.getState().setSelectedAgentBricksEndpoints([]);
      }
    });

    it('records attachment names on the displayed user message', async () => {
      const opts = makeOptions();
      mockedDispatch.mockResolvedValue(result('generate_crew', { type: 'streaming', generation_id: 'g1' }));
      const { result: hook } = renderHook(() => useDispatcher(opts));

      await act(async () => {
        await hook.current.sendMessage('build me a crew', 'm', undefined, undefined, ['a.txt', 'b.pdf']);
      });

      expect(opts.addMessage).toHaveBeenCalledWith('user', 'build me a crew', {
        attachments: ['a.txt', 'b.pdf'],
      });
    });

    it('forwards attachment paths as knowledge_file_paths in the dispatch run settings', async () => {
      const opts = makeOptions();
      mockedDispatch.mockResolvedValue(result('generate_crew', { type: 'streaming', generation_id: 'g1' }));
      const { result: hook } = renderHook(() => useDispatcher(opts));

      await act(async () => {
        // sendMessage(message, model, tools, dispatchSuffix, attachments, displayAs, knowledgeFilePaths)
        await hook.current.sendMessage(
          'summarize this', 'm', ['DatabricksKnowledgeSearchTool'], undefined,
          ['doc.pdf'], undefined, ['uploads/g/e/doc.pdf'],
        );
      });

      const runSettings = mockedDispatch.mock.calls[0][3];
      expect(runSettings.knowledge_file_paths).toEqual(['uploads/g/e/doc.pdf']);
    });
  });

  describe('getAssistantResponse intents', () => {
    async function runIntent(
      intent: DispatchResult['dispatcher']['intent'],
      genResult: unknown,
      optsOverride: Record<string, unknown> = {},
    ) {
      const opts = makeOptions(optsOverride);
      mockedDispatch.mockResolvedValue(result(intent, genResult));
      const { result: hook } = renderHook(() => useDispatcher(opts));
      await act(async () => {
        await hook.current.sendMessage('hi crew', 'm');
      });
      return opts;
    }

    it('unknown intent with message in generation_result returns the message', async () => {
      const opts = await runIntent('unknown', { message: 'custom msg' });
      const update = opts.updateMessageInTargetSession.mock.calls[0][2];
      expect(update.content).toBe('custom msg');
      expect(update.resultType).toBeUndefined();
    });

    it('conversation intent with message returns it', async () => {
      const opts = await runIntent('conversation', { message: 'hello back' });
      const update = opts.updateMessageInTargetSession.mock.calls[0][2];
      expect(update.content).toBe('hello back');
    });

    it('conversation intent without message returns the help fallback', async () => {
      const opts = await runIntent('conversation', null);
      const update = opts.updateMessageInTargetSession.mock.calls[0][2];
      expect(update.content).toContain("I'm not sure what you want to do");
    });

    it('unknown intent with non-object generation_result returns fallback', async () => {
      const opts = await runIntent('unknown', 'just a string');
      const update = opts.updateMessageInTargetSession.mock.calls[0][2];
      expect(update.content).toContain("I'm not sure what you want to do");
    });

    it('non-conversation intent with null generation_result returns generic could-not-generate', async () => {
      const opts = await runIntent('generate_crew', null);
      const update = opts.updateMessageInTargetSession.mock.calls[0][2];
      expect(update.content).toContain('could not generate a result');
    });

    it('generate_* streaming result returns progressive message', async () => {
      const opts = await runIntent('generate_crew', { type: 'streaming', generation_id: 'g1' });
      const update = opts.updateMessageInTargetSession.mock.calls[0][2];
      expect(update.content).toBe(''); // status text removed — shown by the run-activity container
      expect(update.resultType).toBe('streaming');
    });

    it('generate_* non-streaming with agents and tasks builds full response', async () => {
      const genResult = {
        agents: [
          { name: 'Alice', role: 'Researcher' },
          { role: 'Writer' }, // no name -> uses role
          {}, // no name/role -> Agent N
        ],
        tasks: [
          { name: 'T1', description: 'a'.repeat(120) },
          {}, // Task N, no description
        ],
      };
      const opts = await runIntent('generate_agent', genResult);
      const update = opts.updateMessageInTargetSession.mock.calls[0][2];
      expect(update.content).toContain('Crew generation complete!');
      expect(update.content).toContain('**Alice** (Researcher)');
      expect(update.content).toContain('**Writer**');
      expect(update.content).toContain('Agent 3');
      expect(update.content).toContain('Task: **T1**');
      expect(update.content).toContain('5. Task: **Task 2**');
      expect(update.resultType).toBe('generation_complete');
    });

    it('generate_* non-streaming with empty crew returns short complete message', async () => {
      const opts = await runIntent('generate_task', { agents: [], tasks: [] });
      const update = opts.updateMessageInTargetSession.mock.calls[0][2];
      expect(update.content).toBe('Crew generation complete!');
    });

    it('generate_plan non-streaming with non-object generation_result returns short complete', async () => {
      // generation_result is truthy but not object: crewToGenerationData returns empty.
      const opts = await runIntent('generate_plan', 42);
      const update = opts.updateMessageInTargetSession.mock.calls[0][2];
      expect(update.content).toBe('Crew generation complete!');
    });

    it('catalog_list returns the list message', async () => {
      const opts = await runIntent('catalog_list', { type: 'catalog_list', plans: [], message: 'Found plans' });
      const update = opts.updateMessageInTargetSession.mock.calls[0][2];
      expect(update.content).toBe('Found plans');
      expect(update.resultType).toBe('catalog_list');
    });

    it('catalog_load with catalog_list type + plans array returns message', async () => {
      const opts = await runIntent('catalog_load', { type: 'catalog_list', plans: [{ id: '1' }], message: 'Multiple' });
      const update = opts.updateMessageInTargetSession.mock.calls[0][2];
      expect(update.content).toBe('Multiple');
      expect(update.resultType).toBe('catalog_list');
    });

    it('catalog_load with catalog_list type + plans array but no message uses default', async () => {
      const opts = await runIntent('catalog_load', { type: 'catalog_list', plans: [{ id: '1' }] });
      const update = opts.updateMessageInTargetSession.mock.calls[0][2];
      expect(update.content).toBe('Multiple matches found.');
    });

    it('catalog_load single load with message returns message', async () => {
      const opts = await runIntent('catalog_load', { type: 'catalog_load', message: 'Loaded it', plan: null });
      const update = opts.updateMessageInTargetSession.mock.calls[0][2];
      expect(update.content).toBe('Loaded it');
      expect(update.resultType).toBe('catalog_load');
    });

    it('catalog_load single load without message uses default', async () => {
      const opts = await runIntent('catalog_load', { type: 'catalog_load', plan: null });
      const update = opts.updateMessageInTargetSession.mock.calls[0][2];
      expect(update.content).toBe('Plan loaded.');
    });

    it('flow_list returns the flow list message', async () => {
      const opts = await runIntent('flow_list', { type: 'flow_list', flows: [], message: 'Flows here' });
      const update = opts.updateMessageInTargetSession.mock.calls[0][2];
      expect(update.content).toBe('Flows here');
      expect(update.resultType).toBe('flow_list');
    });

    it('flow_load with flow_list type + flows array returns message', async () => {
      const opts = await runIntent('flow_load', { type: 'flow_list', flows: [{ id: '1' }], message: 'Many flows' });
      const update = opts.updateMessageInTargetSession.mock.calls[0][2];
      expect(update.content).toBe('Many flows');
      expect(update.resultType).toBe('flow_list');
    });

    it('flow_load with flow_list type + flows array but no message uses default', async () => {
      const opts = await runIntent('flow_load', { type: 'flow_list', flows: [{ id: '1' }] });
      const update = opts.updateMessageInTargetSession.mock.calls[0][2];
      expect(update.content).toBe('Multiple flows found.');
    });

    it('flow_load single load with message returns message', async () => {
      const opts = await runIntent('flow_load', { type: 'flow_load', message: 'Flow loaded ok', flow: null });
      const update = opts.updateMessageInTargetSession.mock.calls[0][2];
      expect(update.content).toBe('Flow loaded ok');
      expect(update.resultType).toBe('flow_load');
    });

    it('flow_load single load without message uses default', async () => {
      const opts = await runIntent('flow_load', { type: 'flow_load', flow: null });
      const update = opts.updateMessageInTargetSession.mock.calls[0][2];
      expect(update.content).toBe('Flow loaded.');
    });

    it('configure_crew with string message returns it', async () => {
      const opts = await runIntent('configure_crew', { message: 'configured' });
      const update = opts.updateMessageInTargetSession.mock.calls[0][2];
      expect(update.content).toBe('configured');
      expect(update.resultType).toBeUndefined();
    });

    it('catalog_save with object message returns JSON stringified', async () => {
      const opts = await runIntent('catalog_save', { message: { a: 1 } });
      const update = opts.updateMessageInTargetSession.mock.calls[0][2];
      expect(update.content).toBe(JSON.stringify({ a: 1 }));
    });

    it('flow_save with no message returns processed default', async () => {
      const opts = await runIntent('flow_save', { somethingElse: true });
      const update = opts.updateMessageInTargetSession.mock.calls[0][2];
      expect(update.content).toBe('Your request has been processed.');
    });

    it('catalog_help returns help resultType and message', async () => {
      const opts = await runIntent('catalog_help', { message: 'help text' });
      const update = opts.updateMessageInTargetSession.mock.calls[0][2];
      expect(update.content).toBe('help text');
      expect(update.resultType).toBe('help');
    });

    it('catalog_schedule, catalog_delete, flow_delete return their message strings', async () => {
      for (const intent of ['catalog_schedule', 'catalog_delete', 'flow_delete'] as const) {
        const opts = await runIntent(intent, { message: `msg-${intent}` });
        const update = opts.updateMessageInTargetSession.mock.calls[0][2];
        expect(update.content).toBe(`msg-${intent}`);
      }
    });

    it('catalog_save resultType falls back to undefined (default branch)', async () => {
      const opts = await runIntent('catalog_save', { message: 'saved' });
      const update = opts.updateMessageInTargetSession.mock.calls[0][2];
      expect(update.resultType).toBeUndefined();
    });

    it('unrecognised intent (default switch branch) returns the generic processed message', async () => {
      // Craft an intent outside the known union with a truthy generation_result to
      // reach the default branch in getAssistantResponse.
      const opts = await runIntent(
        'some_future_intent' as DispatchResult['dispatcher']['intent'],
        { foo: 'bar' },
      );
      const update = opts.updateMessageInTargetSession.mock.calls[0][2];
      expect(update.content).toBe('Your request has been processed.');
      expect(update.resultType).toBeUndefined();
    });
  });

  describe('crewToGenerationData shapes', () => {
    async function runGen(genResult: unknown) {
      const opts = makeOptions();
      mockedDispatch.mockResolvedValue(result('generate_crew', genResult));
      const { result: hook } = renderHook(() => useDispatcher(opts));
      await act(async () => {
        await hook.current.sendMessage('hi crew');
      });
      const update = opts.updateMessageInTargetSession.mock.calls[0][2];
      return update.resultData as GenerationCompleteData;
    }

    it('arrays shape', async () => {
      const data = await runGen({ agents: [{ role: 'a' }], tasks: [{ name: 't' }] });
      expect(data.agents).toHaveLength(1);
      expect(data.tasks).toHaveLength(1);
    });

    it('arrays shape where only agents is an array (tasks not array)', async () => {
      const data = await runGen({ agents: [{ role: 'a' }], tasks: 'nope' });
      expect(data.agents).toHaveLength(1);
      expect(data.tasks).toHaveLength(0);
    });

    it('arrays shape where only tasks is an array (agents not array)', async () => {
      const data = await runGen({ agents: undefined, tasks: [{ name: 't' }] });
      expect(data.agents).toHaveLength(0);
      expect(data.tasks).toHaveLength(1);
    });

    it('nested wrapper (result key)', async () => {
      const data = await runGen({ result: { agents: [{ role: 'a' }], tasks: [] } });
      expect(data.agents).toHaveLength(1);
    });

    it('nested wrapper data key with non-object skipped, falls through', async () => {
      // data is an array (skipped by !Array.isArray guard), no other shape matches => empty
      const data = await runGen({ data: [1, 2, 3] });
      expect(data.agents).toHaveLength(0);
      expect(data.tasks).toHaveLength(0);
    });

    it('nested wrapper crew key that yields empty does not short-circuit', async () => {
      // crew is an object but produces empty -> loop continues, ends with empty
      const data = await runGen({ crew: { foo: 'bar' } });
      expect(data.agents).toHaveLength(0);
      expect(data.tasks).toHaveLength(0);
    });

    it('single agent shape (role + goal)', async () => {
      const data = await runGen({ role: 'Researcher', goal: 'find', backstory: 'b' });
      expect(data.agents).toHaveLength(1);
      expect(data.tasks).toHaveLength(0);
    });

    it('single task shape (description + expected_output)', async () => {
      const data = await runGen({ description: 'd', expected_output: 'e' });
      expect(data.tasks).toHaveLength(1);
      expect(data.agents).toHaveLength(0);
    });

    it('wrapped agent under agent key', async () => {
      const data = await runGen({ agent: { role: 'r' } });
      expect(data.agents).toHaveLength(1);
    });

    it('wrapped task under task key', async () => {
      const data = await runGen({ task: { name: 't' } });
      expect(data.tasks).toHaveLength(1);
    });

    it('empty object yields empty data', async () => {
      const data = await runGen({ notRelevant: true });
      expect(data.agents).toHaveLength(0);
      expect(data.tasks).toHaveLength(0);
    });

    it('null result yields empty data', async () => {
      // generation_result null -> resultType not generation_complete, but exercise via getAssistantResponse
      const opts = makeOptions();
      mockedDispatch.mockResolvedValue(result('generate_crew', null));
      const { result: hook } = renderHook(() => useDispatcher(opts));
      await act(async () => {
        await hook.current.sendMessage('hi crew');
      });
      // No generation_result so resultData stays null.
      const update = opts.updateMessageInTargetSession.mock.calls[0][2];
      expect(update.resultData).toBeNull();
    });

    it('stores lastGenerated when generation produces agents/tasks (enables ec command)', async () => {
      const opts = makeOptions();
      mockedDispatch.mockResolvedValue(result('generate_crew', { agents: [{ role: 'a' }], tasks: [] }));
      const { result: hook } = renderHook(() => useDispatcher(opts));
      await act(async () => {
        await hook.current.sendMessage('hi crew');
      });
      // Now 'ec' should be intercepted with the stored data.
      await act(async () => {
        await hook.current.sendMessage('ec');
      });
      expect(opts.onExecuteGenerated).toHaveBeenCalledWith({ agents: [{ role: 'a' }], tasks: [] });
    });

    it('does NOT store lastGenerated when generation is empty', async () => {
      const opts = makeOptions();
      mockedDispatch.mockResolvedValue(result('generate_crew', { agents: [], tasks: [] }));
      const { result: hook } = renderHook(() => useDispatcher(opts));
      await act(async () => {
        await hook.current.sendMessage('hi crew');
      });
      mockedDispatch.mockResolvedValue(result('conversation', null));
      await act(async () => {
        await hook.current.sendMessage('ec');
      });
      // Not intercepted -> falls through to dispatch.
      expect(opts.onExecuteGenerated).not.toHaveBeenCalled();
    });
  });

  describe('streaming start', () => {
    it('starts generation stream for streaming generate_crew result', async () => {
      const opts = makeOptions();
      mockedDispatch.mockResolvedValue(result('generate_crew', { type: 'streaming', generation_id: 'gen-99' }));
      const { result: hook } = renderHook(() => useDispatcher(opts));
      await act(async () => {
        await hook.current.sendMessage('hi crew');
      });
      expect(opts.onStartGenerationStream).toHaveBeenCalledWith('gen-99', 'session-1');
    });

    it('does not start generation stream when generate result is non-streaming', async () => {
      const opts = makeOptions();
      mockedDispatch.mockResolvedValue(result('generate_crew', { agents: [{ role: 'a' }], tasks: [] }));
      const { result: hook } = renderHook(() => useDispatcher(opts));
      await act(async () => {
        await hook.current.sendMessage('hi crew');
      });
      expect(opts.onStartGenerationStream).not.toHaveBeenCalled();
    });

    it('passes empty session id when ensureSession yields none for streaming (defensive)', async () => {
      const opts = makeOptions({ ensureSession: vi.fn(async () => '') });
      mockedDispatch.mockResolvedValue(result('generate_plan', { type: 'streaming', generation_id: 'gen-x' }));
      const { result: hook } = renderHook(() => useDispatcher(opts));
      await act(async () => {
        await hook.current.sendMessage('hi crew');
      });
      expect(opts.onStartGenerationStream).toHaveBeenCalledWith('gen-x', '');
    });
  });

  describe('execute_crew / execute_flow setTimeout branches', () => {
    beforeEach(() => {
      vi.useFakeTimers();
    });
    afterEach(() => {
      vi.useRealTimers();
    });

    it('execute_crew with plan calls onExecuteCrew after timeout', async () => {
      const opts = makeOptions();
      const plan = { id: 'p1', name: 'Plan', nodes: [], edges: [] };
      mockedDispatch.mockResolvedValue(result('execute_crew', { plan, message: 'running' }));
      const { result: hook } = renderHook(() => useDispatcher(opts));

      await act(async () => {
        const p = hook.current.sendMessage('hi crew');
        await vi.runAllTimersAsync();
        await p;
      });

      expect(opts.onExecuteCrew).toHaveBeenCalledWith(plan);
    });

    it('execute_crew with no plan does not schedule callback', async () => {
      const opts = makeOptions();
      mockedDispatch.mockResolvedValue(result('execute_crew', { message: 'no plan' }));
      const { result: hook } = renderHook(() => useDispatcher(opts));

      await act(async () => {
        const p = hook.current.sendMessage('hi crew');
        await vi.runAllTimersAsync();
        await p;
      });

      expect(opts.onExecuteCrew).not.toHaveBeenCalled();
    });

    it('execute_crew with plan but no onExecuteCrew callback does nothing', async () => {
      const opts = makeOptions({ onExecuteCrew: undefined });
      const plan = { id: 'p1', name: 'Plan', nodes: [], edges: [] };
      mockedDispatch.mockResolvedValue(result('execute_crew', { plan, message: 'running' }));
      const { result: hook } = renderHook(() => useDispatcher(opts));

      await act(async () => {
        const p = hook.current.sendMessage('hi crew');
        await vi.runAllTimersAsync();
        await p;
      });
      // No throw; nothing scheduled.
      expect(true).toBe(true);
    });

    it('execute_flow with flow calls onExecuteFlow after timeout', async () => {
      const opts = makeOptions();
      const flow = { id: 'f1', name: 'Flow', nodes: [], edges: [] };
      mockedDispatch.mockResolvedValue(result('execute_flow', { flow, message: 'running flow' }));
      const { result: hook } = renderHook(() => useDispatcher(opts));

      await act(async () => {
        const p = hook.current.sendMessage('hi crew');
        await vi.runAllTimersAsync();
        await p;
      });

      expect(opts.onExecuteFlow).toHaveBeenCalledWith(flow);
    });

    it('execute_flow with no flow does not schedule callback', async () => {
      const opts = makeOptions();
      mockedDispatch.mockResolvedValue(result('execute_flow', { message: 'no flow' }));
      const { result: hook } = renderHook(() => useDispatcher(opts));

      await act(async () => {
        const p = hook.current.sendMessage('hi crew');
        await vi.runAllTimersAsync();
        await p;
      });

      expect(opts.onExecuteFlow).not.toHaveBeenCalled();
    });

    it('execute_flow with flow but no onExecuteFlow callback does nothing', async () => {
      const opts = makeOptions({ onExecuteFlow: undefined });
      const flow = { id: 'f1', name: 'Flow', nodes: [], edges: [] };
      mockedDispatch.mockResolvedValue(result('execute_flow', { flow, message: 'running flow' }));
      const { result: hook } = renderHook(() => useDispatcher(opts));

      await act(async () => {
        const p = hook.current.sendMessage('hi crew');
        await vi.runAllTimersAsync();
        await p;
      });
      expect(true).toBe(true);
    });
  });

  describe('crew/flow loaded (pending run) branches', () => {
    it('catalog_load with a plan invokes onCrewLoaded with the plan and origin session', async () => {
      const onCrewLoaded = vi.fn();
      const opts = makeOptions({ onCrewLoaded });
      const plan = { id: 'p1', name: 'Loaded plan', nodes: [], edges: [] };
      mockedDispatch.mockResolvedValue(
        result('catalog_load', { type: 'catalog_load', message: 'Plan loaded.', plan }),
      );
      const { result: hook } = renderHook(() => useDispatcher(opts));

      await act(async () => {
        await hook.current.sendMessage('open crew x');
      });

      expect(onCrewLoaded).toHaveBeenCalledWith(plan, 'session-1');
    });

    it('catalog_load without a plan does not invoke onCrewLoaded', async () => {
      const onCrewLoaded = vi.fn();
      const opts = makeOptions({ onCrewLoaded });
      mockedDispatch.mockResolvedValue(
        result('catalog_load', { type: 'catalog_load', message: 'Plan loaded.', plan: null }),
      );
      const { result: hook } = renderHook(() => useDispatcher(opts));

      await act(async () => {
        await hook.current.sendMessage('open crew x');
      });

      expect(onCrewLoaded).not.toHaveBeenCalled();
    });

    it('catalog_load with a plan but no onCrewLoaded callback does nothing', async () => {
      const opts = makeOptions({ onCrewLoaded: undefined });
      const plan = { id: 'p1', name: 'Loaded plan', nodes: [], edges: [] };
      mockedDispatch.mockResolvedValue(
        result('catalog_load', { type: 'catalog_load', message: 'Plan loaded.', plan }),
      );
      const { result: hook } = renderHook(() => useDispatcher(opts));

      await act(async () => {
        await hook.current.sendMessage('open crew x');
      });
      // No throw; nothing to assert beyond completion.
      expect(hook.current.isDispatching.current).toBe(false);
    });

    it('flow_load with a flow invokes onFlowLoaded with the flow and origin session', async () => {
      const onFlowLoaded = vi.fn();
      const opts = makeOptions({ onFlowLoaded });
      const flow = { id: 'f1', name: 'Loaded flow', nodes: [], edges: [] };
      mockedDispatch.mockResolvedValue(
        result('flow_load', { type: 'flow_load', message: 'Flow loaded.', flow }),
      );
      const { result: hook } = renderHook(() => useDispatcher(opts));

      await act(async () => {
        await hook.current.sendMessage('open flow x');
      });

      expect(onFlowLoaded).toHaveBeenCalledWith(flow, 'session-1');
    });

    it('flow_load without a flow does not invoke onFlowLoaded', async () => {
      const onFlowLoaded = vi.fn();
      const opts = makeOptions({ onFlowLoaded });
      mockedDispatch.mockResolvedValue(
        result('flow_load', { type: 'flow_load', message: 'Flow loaded.', flow: null }),
      );
      const { result: hook } = renderHook(() => useDispatcher(opts));

      await act(async () => {
        await hook.current.sendMessage('open flow x');
      });

      expect(onFlowLoaded).not.toHaveBeenCalled();
    });

    it('flow_load with a flow but no onFlowLoaded callback does nothing', async () => {
      const opts = makeOptions({ onFlowLoaded: undefined });
      const flow = { id: 'f1', name: 'Loaded flow', nodes: [], edges: [] };
      mockedDispatch.mockResolvedValue(
        result('flow_load', { type: 'flow_load', message: 'Flow loaded.', flow }),
      );
      const { result: hook } = renderHook(() => useDispatcher(opts));

      await act(async () => {
        await hook.current.sendMessage('open flow x');
      });
      expect(hook.current.isDispatching.current).toBe(false);
    });
  });

  describe('displayAs label', () => {
    it('shows the displayAs label as the user message instead of the raw message', async () => {
      const opts = makeOptions();
      mockedDispatch.mockResolvedValue(result('catalog_load', { type: 'catalog_load', message: 'Plan loaded.', plan: null }));
      const { result: hook } = renderHook(() => useDispatcher(opts));

      await act(async () => {
        // raw dispatch message is the command; the chat should show the friendly label.
        await hook.current.sendMessage('/crew open p1', undefined, undefined, undefined, undefined, 'Open crew: My Plan');
      });

      // The displayed user message uses displayAs, not the raw message.
      expect(opts.addMessage).toHaveBeenCalledWith('user', 'Open crew: My Plan', undefined);
      // The raw message is still dispatched (slash command -> not augmented).
      expect(mockedDispatch).toHaveBeenCalledWith('/crew open p1', undefined, undefined, RUN_SETTINGS, '/crew open p1');
    });
  });

  describe('session targeting', () => {
    it('uses updateMessage fallback when there is no origin session (defensive)', async () => {
      const opts = makeOptions({ ensureSession: vi.fn(async () => '') });
      mockedDispatch.mockResolvedValue(result('conversation', { message: 'hi' }));
      const { result: hook } = renderHook(() => useDispatcher(opts));

      await act(async () => {
        await hook.current.sendMessage('hello');
      });

      expect(opts.updateMessage).toHaveBeenCalledWith(ASSISTANT_ID, expect.objectContaining({ content: 'hi' }));
      expect(opts.updateMessageInTargetSession).not.toHaveBeenCalled();
    });

    it('uses targeted update when origin session exists', async () => {
      const opts = makeOptions();
      mockedDispatch.mockResolvedValue(result('conversation', { message: 'hi' }));
      const { result: hook } = renderHook(() => useDispatcher(opts));

      await act(async () => {
        await hook.current.sendMessage('hello');
      });

      expect(opts.updateMessageInTargetSession).toHaveBeenCalledWith(
        'session-1',
        ASSISTANT_ID,
        expect.objectContaining({ content: 'hi' }),
      );
      expect(opts.updateMessage).not.toHaveBeenCalled();
    });
  });

  describe('error / catch path', () => {
    it('handles Error instance with targeted session update', async () => {
      const opts = makeOptions();
      mockedDispatch.mockRejectedValue(new Error('boom'));
      const { result: hook } = renderHook(() => useDispatcher(opts));

      await act(async () => {
        await hook.current.sendMessage('hello');
      });

      expect(opts.updateMessageInTargetSession).toHaveBeenCalledWith(
        'session-1',
        ASSISTANT_ID,
        { content: 'Failed to process your request: boom', isStreaming: false },
      );
      expect(hook.current.isDispatching.current).toBe(false);
    });

    it('handles non-Error rejection with fallback message and no origin session', async () => {
      const opts = makeOptions({ ensureSession: vi.fn(async () => '') });
      mockedDispatch.mockRejectedValue('a string error');
      const { result: hook } = renderHook(() => useDispatcher(opts));

      await act(async () => {
        await hook.current.sendMessage('hello');
      });

      expect(opts.updateMessage).toHaveBeenCalledWith(ASSISTANT_ID, {
        content: 'Failed to process your request: Unknown error',
        isStreaming: false,
      });
    });
  });
});
