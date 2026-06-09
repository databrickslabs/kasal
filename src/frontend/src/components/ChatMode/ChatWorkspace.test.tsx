import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, act } from '@testing-library/react';

// ---------------------------------------------------------------------------
// Shared, mutable mock state + captured callbacks (hoisted before vi.mock).
// ---------------------------------------------------------------------------
const h = vi.hoisted(() => {
  const fn = () => {
    const f: { (...a: unknown[]): unknown; calls: unknown[][] } = ((...args: unknown[]) => {
      f.calls.push(args);
      return undefined;
    }) as never;
    f.calls = [];
    return f;
  };
  return {
    session: {
      sessions: [
        { id: 's1', title: 'One', updatedAt: new Date(), createdAt: new Date() },
        { id: 's2', title: 'Two', updatedAt: new Date(), createdAt: new Date() },
      ],
      currentSessionId: 's1',
      messages: [] as unknown[],
      addMessage: vi.fn(() => 'mid'),
      addMessageToTargetSession: vi.fn(() => 'mid'),
      updateMessage: vi.fn(),
      updateMessageInTargetSession: vi.fn(),
      clearMessages: vi.fn(),
      init: vi.fn(async () => {}),
      reloadForGroup: vi.fn(async () => {}),
      switchSession: vi.fn(async () => {}),
      createNewSession: vi.fn(async () => 's-new'),
      deleteSession: vi.fn(async () => {}),
      renameSession: vi.fn(async () => {}),
    },
    exec: {
      isExecuting: false,
      isGenerating: false,
      isLoading: false,
      executionContext: null as unknown,
      previewContent: null as unknown,
      previewOwnerSessionId: null as unknown,
      previewHistory: [] as unknown[],
      previewIndex: 0,
      navigatePreview: vi.fn(),
      chatCollapsed: false,
      executionOwnerSessionId: 's1',
      activeExecution: null as unknown,
      hasActiveExecution: vi.fn(() => false),
      setIsLoading: vi.fn(),
      setExecutionContext: vi.fn(),
      setPreviewContent: vi.fn(),
      startExecution: vi.fn(),
      startGeneration: vi.fn(),
      completeExecution: vi.fn(),
      failExecution: vi.fn(),
      completeGeneration: vi.fn(),
      failGeneration: vi.fn(),
      updateExecutionStatus: vi.fn(),
      saveSessionState: vi.fn(),
      restoreSessionState: vi.fn(),
      resetForSession: vi.fn(),
      reopenPreview: vi.fn(),
      clearPreview: vi.fn(),
      toggleChatCollapsed: vi.fn(),
    },
    app: {
      models: [{ key: 'm1', name: 'Model 1' }],
      selectedModel: 'm1',
      sidebarOpen: true,
      toolNameMap: {} as Record<string, string>,
      savedCrews: [] as { id: string; name: string }[],
      savedFlows: [] as { id: string; name: string }[],
      init: vi.fn(),
      setTheme: vi.fn(),
      loadModels: vi.fn(),
      loadTools: vi.fn(),
      loadCatalog: vi.fn(async () => {}),
      setSelectedModel: vi.fn(),
    },
    theme: { isDarkMode: false },
    streamOpts: {} as Record<string, (...a: unknown[]) => void>,
    genOpts: {} as Record<string, (...a: unknown[]) => void>,
    dispatcherOpts: {} as Record<string, (...a: unknown[]) => unknown>,
    dispatcherSend: vi.fn(async () => {}),
    setLastGenerated: vi.fn(),
    startStream: vi.fn(),
    stopStream: vi.fn(),
    createExecution: vi.fn(async () => ({ job_id: 'job-1' })),
    listExecutions: vi.fn(async () => []),
    stopExecution: vi.fn(async () => {}),
    getExecutionStatus: vi.fn(async () => ({ status: 'running' })),
    saveGeneratedCrew: vi.fn(async () => ({ id: 'crew-1', name: 'Saved Crew' })),
    saveSessionPreview: vi.fn(),
    getSessionPreview: vi.fn(async () => null),
    setSessionRunningJob: vi.fn(async () => {}),
    getSessionRunningJob: vi.fn(async () => null),
    clearSessionRunningJob: vi.fn(async () => {}),
    parsePreview: vi.fn(() => null),
    detectVars: vi.fn(() => [] as unknown[]),
    fn,
  };
});

function storeHook(obj: Record<string, unknown>) {
  const hook = ((sel: (s: unknown) => unknown) => sel(obj)) as unknown as {
    (sel: (s: unknown) => unknown): unknown;
    getState: () => unknown;
    setState: (patch: unknown) => void;
  };
  hook.getState = () => obj;
  hook.setState = (patch: unknown) => {
    const next = typeof patch === 'function'
      ? (patch as (s: unknown) => Record<string, unknown>)(obj)
      : patch;
    Object.assign(obj, next as Record<string, unknown>);
  };
  return hook;
}

vi.mock('./store/sessionStore', () => ({ useSessionStore: storeHook(h.session) }));
vi.mock('./store/executionStore', () => ({ useExecutionStore: storeHook(h.exec) }));
vi.mock('./store/appStore', () => ({ useAppStore: storeHook(h.app) }));
vi.mock('../../store/theme', () => ({ useThemeStore: storeHook(h.theme) }));

vi.mock('./hooks/useDispatcher', () => ({
  useDispatcher: (opts: Record<string, (...a: unknown[]) => unknown>) => {
    Object.assign(h.dispatcherOpts, opts);
    return { sendMessage: h.dispatcherSend, setLastGenerated: h.setLastGenerated };
  },
}));
vi.mock('./hooks/useExecutionStream', () => ({
  useExecutionStream: (opts: Record<string, (...a: unknown[]) => void>) => {
    Object.assign(h.streamOpts, opts);
    return { startStream: h.startStream, stopStream: h.stopStream };
  },
}));
vi.mock('./hooks/useGenerationStream', () => ({
  useGenerationStream: (opts: Record<string, (...a: unknown[]) => void>) => {
    Object.assign(h.genOpts, opts);
    return { startStream: h.startStream, stopStream: h.stopStream };
  },
}));
vi.mock('./api/executions', () => ({
  createExecution: (...a: unknown[]) => h.createExecution(...a),
  listExecutions: (...a: unknown[]) => h.listExecutions(...a),
  stopExecution: (...a: unknown[]) => h.stopExecution(...a),
  getExecutionStatus: (...a: unknown[]) => h.getExecutionStatus(...a),
}));
vi.mock('./api/crews', () => ({
  saveGeneratedCrew: (...a: unknown[]) => h.saveGeneratedCrew(...a),
  CrewNameConflictError: class CrewNameConflictError extends Error {
    crewName: string;
    constructor(crewName: string) {
      super(`A crew named "${crewName}" already exists.`);
      this.name = 'CrewNameConflictError';
      this.crewName = crewName;
    }
  },
  // Faithful-enough stand-in (the real impl is covered in crews.test.ts):
  // true when any agent/task lists the GenieTool by name.
  usesGenieTool: (data: { agents?: { tools?: unknown }[]; tasks?: { tools?: unknown }[] }) => {
    const items = [...(data?.agents ?? []), ...(data?.tasks ?? [])];
    return items.some((x) => Array.isArray(x?.tools) && x.tools.includes('GenieTool'));
  },
}));
vi.mock('./db/sessionDb', () => ({
  saveSessionPreview: (...a: unknown[]) => h.saveSessionPreview(...a),
  getSessionPreview: (...a: unknown[]) => h.getSessionPreview(...a),
  setSessionRunningJob: (...a: unknown[]) => h.setSessionRunningJob(...a),
  getSessionRunningJob: (...a: unknown[]) => h.getSessionRunningJob(...a),
  clearSessionRunningJob: (...a: unknown[]) => h.clearSessionRunningJob(...a),
}));
vi.mock('./components/Preview/PreviewPanel', () => ({
  default: (props: { onClose: () => void; onToggleChat: () => void; onRefine?: (i: string) => void }) => (
    <div data-testid="preview-panel">
      <button data-testid="preview-close" onClick={props.onClose}>x</button>
      <button data-testid="preview-toggle" onClick={props.onToggleChat}>t</button>
      <button data-testid="preview-refine" onClick={() => props.onRefine?.((globalThis as { __refineMsg?: string }).__refineMsg ?? 'make it pop')}>r</button>
    </div>
  ),
  parsePreviewContent: (...a: unknown[]) => h.parsePreview(...a),
}));
vi.mock('./components/Chat/ChatContainer', () => ({
  default: (props: Record<string, (...a: unknown[]) => void>) => (
    <div data-testid="chat-container">
      <button data-testid="cc-send" onClick={() => props.onSend((globalThis as { __ccMsg?: string }).__ccMsg ?? 'hello world')}>send</button>
      <button data-testid="cc-stop" onClick={() => props.onStopExecution?.()}>stop</button>
      <button data-testid="cc-exec-crew" onClick={() => props.onExecuteCrew?.((globalThis as { __crewPlan?: unknown }).__crewPlan ?? { name: 'P', nodes: [], edges: [] })}>crew</button>
      <button data-testid="cc-exec-flow" onClick={() => props.onExecuteFlow?.({ name: 'F', nodes: [], edges: [] })}>flow</button>
      <button data-testid="cc-exec-gen" onClick={() => props.onExecuteGenerated?.((globalThis as { __genData?: unknown }).__genData ?? { agents: [{ id: 'a1' }], tasks: [{ id: 't1' }] })}>gen</button>
      <button data-testid="cc-save" onClick={() => props.onSaveCrew?.({ agents: [{ id: 'a1' }], tasks: [{ id: 't1' }] })}>save</button>
      <button data-testid="cc-model" onClick={() => props.onModelChange?.('m2')}>model</button>
      {/* Surfaces the pending-run affordance: the label only renders when armed
          for the current session, and the button drives onRunPending. */}
      <span data-testid="cc-pending-label">{(props as { pendingRunLabel?: string }).pendingRunLabel ?? ''}</span>
      <button data-testid="cc-run-pending" onClick={() => props.onRunPending?.()}>run-pending</button>
    </div>
  ),
}));
// Default: no detected variables (dialog stays closed). Overridable per test.
vi.mock('./utils/variableDetector', () => ({
  detectVariablesFromNodes: (...a: unknown[]) => h.detectVars(...a),
  detectVariablesFromGenerated: (...a: unknown[]) => h.detectVars(...a),
}));
vi.mock('./utils/crewConfigBuilder', () => ({
  buildCrewConfig: vi.fn(() => ({ cfg: 'crew' })),
  buildFlowConfig: vi.fn(() => ({ cfg: 'flow' })),
  buildCrewConfigFromGenerated: vi.fn(() => ({ cfg: 'gen' })),
}));
vi.mock('./components/InputVariablesDialog', () => ({
  default: (props: { open: boolean; onConfirm: (v: Record<string, string>) => void; onCancel: () => void }) => {
    // Capture onConfirm so a test can invoke it directly (e.g. with no pending execution).
    (h as { varsConfirm?: (v: Record<string, string>) => void }).varsConfirm = props.onConfirm;
    return props.open ? (
      <div data-testid="vars-dialog">
        <button data-testid="vars-confirm" onClick={() => props.onConfirm({ topic: 'AI' })}>ok</button>
        <button data-testid="vars-cancel" onClick={props.onCancel}>cancel</button>
      </div>
    ) : null;
  },
}));

import ChatWorkspace, {
  toolMatchKey,
  summarizeArgs,
  buildTraceEntry,
  summarizeTaskOutput,
  cleanTaskLabel,
} from './ChatWorkspace';
import { buildCrewConfigFromGenerated } from './utils/crewConfigBuilder';

const mockedBuildGenerated = buildCrewConfigFromGenerated as unknown as ReturnType<typeof vi.fn>;

// ===========================================================================
// Pure helper tests
// ===========================================================================
describe('toolMatchKey', () => {
  it('normalizes name and joins string/number arg values', () => {
    expect(toolMatchKey('My Tool', '{"q":"hello","n":3}')).toBe('mytool::hello|3');
  });
  it('handles object args', () => {
    expect(toolMatchKey('T', { a: 'x' })).toBe('t::x');
  });
  it('handles invalid JSON args and nullish name', () => {
    expect(toolMatchKey(null, 'not json')).toBe('::');
    expect(toolMatchKey('T', '')).toBe('t::');
  });
});

describe('summarizeArgs', () => {
  it('returns undefined for falsy args', () => {
    expect(summarizeArgs('')).toBeUndefined();
    expect(summarizeArgs(undefined)).toBeUndefined();
  });
  it('joins string values from JSON', () => {
    expect(summarizeArgs('{"q":"hello"}')).toBe('hello');
  });
  it('falls back to the raw string on invalid JSON', () => {
    expect(summarizeArgs('plainstring')).toBe('plainstring');
  });
  it('returns undefined when invalid JSON is not a string', () => {
    // an object that JSON.parse path skips; no string values -> undefined
    expect(summarizeArgs({ n: 5 })).toBeUndefined();
  });
  it('truncates long values', () => {
    const long = 'a'.repeat(100);
    expect(summarizeArgs(JSON.stringify({ q: long }))?.endsWith('…')).toBe(true);
  });
  it('returns undefined when no string values present', () => {
    expect(summarizeArgs('{"n":1}')).toBeUndefined();
  });
  it('returns undefined for args that are neither string nor object', () => {
    expect(summarizeArgs(5 as unknown)).toBeUndefined();
  });
});

describe('buildTraceEntry', () => {
  it('filters llm_retry and task_started as noise', () => {
    expect(buildTraceEntry('x', { event_type: 'llm_retry' })).toBeNull();
    expect(buildTraceEntry('x', { event_type: 'task_started' })).toBeNull();
  });
  it('builds a tool_call from tool_usage', () => {
    const e = buildTraceEntry('', {
      event_type: 'tool_usage',
      event_source: 'agent',
      output: { extra_data: { tool_name: 'Search', tool_args: '{"q":"hi"}' } },
    });
    expect(e?.kind).toBe('tool_call');
    expect(e?.label).toBe('Search');
  });
  it('uses default tool label when missing', () => {
    const e = buildTraceEntry('', { event_type: 'tool_usage', output: {} });
    expect(e?.label).toBe('tool');
  });
  it('tolerates a non-JSON string output (asObject parse failure)', () => {
    const e = buildTraceEntry('', { event_type: 'tool_usage', output: 'not valid json {' });
    expect(e?.label).toBe('tool'); // parse failed → treated as empty object
  });
  it('parses a JSON-string output into an object (asObject string→object)', () => {
    const e = buildTraceEntry('', {
      event_type: 'tool_usage',
      output: '{"extra_data":{"tool_name":"Zed","tool_args":"{}"}}',
    });
    expect(e?.label).toBe('Zed'); // parsed object is used
  });
  it('treats a JSON-string output that parses to a non-object as empty', () => {
    const e = buildTraceEntry('', { event_type: 'tool_usage', output: '42' });
    expect(e?.label).toBe('tool'); // 42 is not an object → {}
  });
  it('builds a tool_result from a *_run event', () => {
    const e = buildTraceEntry('', {
      event_type: 'perplexitytool_run',
      output: { tool_name: 'Perplexity', content: 'result text', input: '{"q":"x"}', duration_ms: 1200 },
    });
    expect(e?.kind).toBe('tool_result');
    expect(e?.durationMs).toBe(1200);
  });
  it('derives tool name from event_type when output.tool_name missing', () => {
    const e = buildTraceEntry('', { event_type: 'scrapetool_run', output: {} });
    expect(e?.label).toBe('scrapetool');
  });
  it('filters empty messages', () => {
    expect(buildTraceEntry('   ', {})).toBeNull();
  });
  it('filters raw JSON id payloads', () => {
    expect(buildTraceEntry('{"id": 42, "x": 1}', {})).toBeNull();
  });
  it('filters short single-token fragments', () => {
    expect(buildTraceEntry('_usage', {})).toBeNull();
  });
  it('filters generic "Calling tools" pings', () => {
    expect(buildTraceEntry('Calling tools.', {})).toBeNull();
  });
  it('builds an event with truncation for long messages', () => {
    const long = 'word '.repeat(40);
    const e = buildTraceEntry(long, {});
    expect(e?.kind).toBe('event');
    expect(e?.detail).toBe(long.trim());
    expect(e?.label.endsWith('…')).toBe(true);
  });
  it('builds a short event without truncation', () => {
    const e = buildTraceEntry('a readable status line', {});
    expect(e?.kind).toBe('event');
    expect(e?.detail).toBeUndefined();
  });
  it('surfaces retrieved memory context as a Memory pill', () => {
    const e = buildTraceEntry('', {
      event_type: 'memory_retrieval',
      event_source: 'agent',
      output: { content: 'remembered: the latest Swiss news', duration_ms: 7 },
    });
    expect(e?.kind).toBe('tool_result');
    expect(e?.label).toBe('Memory');
    expect(e?.detail).toContain('Swiss news');
  });
  it('surfaces a memory pill with no event_source (source falls back to undefined)', () => {
    const e = buildTraceEntry('', {
      event_type: 'memory_retrieval',
      output: { content: 'remembered fact' },
    });
    expect(e?.label).toBe('Memory');
    expect(e?.source).toBeUndefined();
  });
  it('drops a memory_retrieval that found nothing (no redundant pill)', () => {
    expect(
      buildTraceEntry('', { event_type: 'memory_retrieval_completed', output: { content: 'No relevant memories found' } }),
    ).toBeNull();
    expect(
      buildTraceEntry('', { event_type: 'memory_retrieval', output: {} }),
    ).toBeNull();
  });
});

describe('summarizeTaskOutput', () => {
  it('returns null for empty', () => {
    expect(summarizeTaskOutput('   ', null)).toBeNull();
  });
  it('returns null for short status noise', () => {
    expect(summarizeTaskOutput('Calling tools now', null)).toBeNull();
    expect(summarizeTaskOutput('Thinking...', null)).toBeNull();
  });
  it('describes a preview when present (html/markdown/other)', () => {
    expect(summarizeTaskOutput('x', { type: 'html', data: 'd' })).toMatch(/HTML output/);
    expect(summarizeTaskOutput('x', { type: 'markdown', data: 'd' })).toMatch(/report/);
    expect(summarizeTaskOutput('x', { type: 'ui', data: 'd' })).toMatch(/app/);
    expect(summarizeTaskOutput('x', { type: 'json', data: 'd' })).toMatch(/result/);
  });
  it('truncates long plain text', () => {
    const long = 'z'.repeat(500);
    expect(summarizeTaskOutput(long, null)?.endsWith('…')).toBe(true);
  });
  it('returns normal short text unchanged', () => {
    expect(summarizeTaskOutput('a normal result', null)).toBe('a normal result');
  });
});

describe('cleanTaskLabel', () => {
  it('falls back to "Task" for empty input', () => {
    expect(cleanTaskLabel('')).toBe('Task');
    expect(cleanTaskLabel('   ')).toBe('Task');
  });
  it('collapses the refine prompt to a clean label instead of dumping the artifact', () => {
    const refinePrompt =
      'Improve the artifact below based on this instruction.\n\nINSTRUCTION:\n' +
      'remove this from the title Executive Dashboard\n\nCURRENT ARTIFACT:\n<!DOCTYPE html><html>...';
    expect(cleanTaskLabel(refinePrompt)).toBe('Refined artifact');
  });
  it('keeps a short single-line task name as-is', () => {
    expect(cleanTaskLabel('Gather Latest Swiss News')).toBe('Gather Latest Swiss News');
  });
  it('uses the first line and truncates an over-long description', () => {
    const long = 'Research and compile the most recent data '.repeat(5);
    const label = cleanTaskLabel(`${long}\nsecond line`);
    expect(label.endsWith('…')).toBe(true);
    expect(label).not.toContain('second line');
    expect(label.length).toBeLessThanOrEqual(81);
  });
});

// ===========================================================================
// Component tests
// ===========================================================================
describe('ChatWorkspace component', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    h.session.currentSessionId = 's1';
    h.session.messages = [];
    h.exec.previewContent = null;
    h.exec.previewOwnerSessionId = null;
    h.exec.executionOwnerSessionId = 's1';
    h.app.sidebarOpen = true;
    h.parsePreview.mockReturnValue(null);
    h.detectVars.mockReturnValue([]);
    h.createExecution.mockResolvedValue({ job_id: 'job-1' });
    h.getSessionPreview.mockResolvedValue(null);
    h.stopExecution.mockResolvedValue(undefined);
    h.listExecutions.mockResolvedValue([]);
    h.saveGeneratedCrew.mockResolvedValue({ id: 'crew-1', name: 'Saved Crew' });
    // Reset shared execution flags so per-test toggles don't leak across tests.
    h.exec.isExecuting = false;
    h.exec.isGenerating = false;
    h.exec.isLoading = false;
    h.exec.chatCollapsed = false;
    h.exec.activeExecution = null;
    h.exec.hasActiveExecution = vi.fn(() => false);
    h.app.selectedModel = 'm1';
    h.theme.isDarkMode = false;
    (globalThis as { __ccMsg?: string }).__ccMsg = 'hello world';
    delete (globalThis as { __crewPlan?: unknown }).__crewPlan;
    delete (globalThis as { __genData?: unknown }).__genData;
    delete (globalThis as { __refineMsg?: string }).__refineMsg;
    Element.prototype.scrollIntoView = vi.fn();
  });

  it('renders the chat root, sidebar and container; runs init + theme effects', () => {
    render(<ChatWorkspace />);
    expect(document.getElementById('kasal-chat-root')).toBeInTheDocument();
    expect(screen.getByTestId('chat-container')).toBeInTheDocument();
    expect(h.app.init).toHaveBeenCalled();
    expect(h.app.setTheme).toHaveBeenCalledWith('light');
    expect(h.session.init).toHaveBeenCalled();
  });

  it('shows the preview panel only when previewOwnerSessionId matches the current session', () => {
    h.exec.previewContent = { type: 'html', data: '<p>x</p>' };
    h.exec.previewOwnerSessionId = 's2'; // different session
    const { rerender } = render(<ChatWorkspace />);
    expect(screen.queryByTestId('preview-panel')).not.toBeInTheDocument();
    // now matching
    h.exec.previewOwnerSessionId = 's1';
    rerender(<ChatWorkspace />);
    expect(screen.getByTestId('preview-panel')).toBeInTheDocument();
  });

  it('routes a normal message through the dispatcher', async () => {
    render(<ChatWorkspace />);
    await act(async () => {
      fireEvent.click(screen.getByTestId('cc-send'));
    });
    // dispatcher signature: (message, model, tools?, dispatchSuffix?, attachments?)
    expect(h.dispatcherSend).toHaveBeenCalledWith('hello world', 'm1', undefined, undefined, undefined, undefined);
  });

  it('invokes execution-stream callbacks (trace/taskOutput/status/complete/error)', () => {
    render(<ChatWorkspace />);
    // trace: a tool_call then its tool_result with the same matchKey
    act(() => {
      h.streamOpts.onTrace('', {
        event_type: 'tool_usage',
        output: { extra_data: { tool_name: 'S', tool_args: '{"q":"a"}' } },
      });
      h.streamOpts.onTrace('', {
        event_type: 's_run',
        output: { tool_name: 'S', input: '{"q":"a"}', content: 'done', duration_ms: 10 },
      });
      // a plain event trace
      h.streamOpts.onTrace('a readable event', {});
      h.streamOpts.onStatusChange('running');
      h.streamOpts.onComplete({ result: 'final text' });
      h.streamOpts.onError('boom');
    });
    expect(h.exec.completeExecution).toHaveBeenCalled();
    expect(h.exec.failExecution).toHaveBeenCalledWith('boom');
  });

  it('onTaskOutput persists a preview and summarizes output', () => {
    h.parsePreview.mockReturnValue({ type: 'html', data: '<p>x</p>' });
    render(<ChatWorkspace />);
    act(() => {
      h.streamOpts.onTrace('', { event_type: 'task_completed', trace_metadata: { task_name: 'Build' }, output: '<p>x</p>' });
    });
    expect(h.saveSessionPreview).toHaveBeenCalled();
  });

  it('generation onComplete adds a message and auto-runs the crew', async () => {
    render(<ChatWorkspace />);
    await act(async () => {
      h.genOpts.onComplete({ agents: [{ id: 'a1', role: 'r' }], tasks: [{ id: 't1' }] });
    });
    expect(h.exec.completeGeneration).toHaveBeenCalled();
    expect(h.createExecution).toHaveBeenCalled();
  });

  it('generation onFailed marks the generation failed', () => {
    render(<ChatWorkspace />);
    act(() => h.genOpts.onFailed('gen error'));
    expect(h.exec.failGeneration).toHaveBeenCalledWith('gen error');
  });

  // --- execution handlers ---
  it('executes a crew (success path -> startExecution)', async () => {
    render(<ChatWorkspace />);
    await act(async () => { fireEvent.click(screen.getByTestId('cc-exec-crew')); });
    expect(h.createExecution).toHaveBeenCalled();
    expect(h.exec.startExecution).toHaveBeenCalledWith('job-1', 's1', undefined);
  });

  it('crew execution with no job id reports an error message', async () => {
    h.createExecution.mockResolvedValue({});
    render(<ChatWorkspace />);
    await act(async () => { fireEvent.click(screen.getByTestId('cc-exec-crew')); });
    expect(h.session.addMessage).toHaveBeenCalledWith('assistant', expect.stringContaining('no job ID'));
  });

  it('crew execution that throws reports a failure message', async () => {
    h.createExecution.mockRejectedValue(new Error('api down'));
    render(<ChatWorkspace />);
    await act(async () => { fireEvent.click(screen.getByTestId('cc-exec-crew')); });
    expect(h.session.addMessage).toHaveBeenCalledWith('assistant', expect.stringContaining('api down'));
  });

  it('executes a flow', async () => {
    render(<ChatWorkspace />);
    await act(async () => { fireEvent.click(screen.getByTestId('cc-exec-flow')); });
    expect(h.createExecution).toHaveBeenCalled();
  });

  it('flow execution that throws reports a failure', async () => {
    h.createExecution.mockRejectedValue(new Error('flow boom'));
    render(<ChatWorkspace />);
    await act(async () => { fireEvent.click(screen.getByTestId('cc-exec-flow')); });
    expect(h.session.addMessage).toHaveBeenCalledWith('assistant', expect.stringContaining('flow boom'));
  });

  it('flow execution with no job id reports an error message', async () => {
    h.createExecution.mockResolvedValue({});
    render(<ChatWorkspace />);
    await act(async () => { fireEvent.click(screen.getByTestId('cc-exec-flow')); });
    expect(h.session.addMessage).toHaveBeenCalledWith('assistant', expect.stringContaining('no job ID'));
  });

  it('executes a generated crew directly when no variables are detected', async () => {
    render(<ChatWorkspace />);
    await act(async () => { fireEvent.click(screen.getByTestId('cc-exec-gen')); });
    expect(h.createExecution).toHaveBeenCalled();
  });

  it('generated execution with no job id and with throw', async () => {
    h.createExecution.mockResolvedValue({});
    const { rerender } = render(<ChatWorkspace />);
    await act(async () => { fireEvent.click(screen.getByTestId('cc-exec-gen')); });
    expect(h.session.addMessage).toHaveBeenCalledWith('assistant', expect.stringContaining('no job ID'));
    h.createExecution.mockRejectedValue(new Error('gen boom'));
    rerender(<ChatWorkspace />);
    await act(async () => { fireEvent.click(screen.getByTestId('cc-exec-gen')); });
    expect(h.session.addMessage).toHaveBeenCalledWith('assistant', expect.stringContaining('gen boom'));
  });

  // --- variables dialog ---
  it('opens the variables dialog when a crew needs variables, then confirms', async () => {
    h.detectVars.mockReturnValue([{ name: 'topic', required: true }]);
    render(<ChatWorkspace />);
    fireEvent.click(screen.getByTestId('cc-exec-crew'));
    expect(screen.getByTestId('vars-dialog')).toBeInTheDocument();
    await act(async () => { fireEvent.click(screen.getByTestId('vars-confirm')); });
    expect(h.createExecution).toHaveBeenCalled();
  });

  it('opens the variables dialog for a generated crew, then cancels', () => {
    h.detectVars.mockReturnValue([{ name: 'topic', required: true }]);
    render(<ChatWorkspace />);
    fireEvent.click(screen.getByTestId('cc-exec-gen'));
    expect(screen.getByTestId('vars-dialog')).toBeInTheDocument();
    fireEvent.click(screen.getByTestId('vars-cancel'));
    expect(screen.queryByTestId('vars-dialog')).not.toBeInTheDocument();
  });

  // --- stop execution ---
  it('stops an active execution', async () => {
    h.exec.activeExecution = { jobId: 'job-9', status: 'running' };
    render(<ChatWorkspace />);
    await act(async () => { fireEvent.click(screen.getByTestId('cc-stop')); });
    expect(h.stopExecution).toHaveBeenCalledWith('job-9');
    expect(h.exec.failExecution).toHaveBeenCalled();
  });

  it('stop is a no-op when there is no active execution', async () => {
    h.exec.activeExecution = null;
    render(<ChatWorkspace />);
    await act(async () => { fireEvent.click(screen.getByTestId('cc-stop')); });
    expect(h.stopExecution).not.toHaveBeenCalled();
  });

  it('stop reports a failure message when stopExecution throws', async () => {
    h.exec.activeExecution = { jobId: 'job-9', status: 'running' };
    h.stopExecution.mockRejectedValue(new Error('cant stop'));
    render(<ChatWorkspace />);
    await act(async () => { fireEvent.click(screen.getByTestId('cc-stop')); });
    expect(h.session.addMessage).toHaveBeenCalledWith('assistant', expect.stringContaining('cant stop'));
  });

  // --- local slash commands via handleSend ---
  async function send(msg: string) {
    (globalThis as { __ccMsg?: string }).__ccMsg = msg;
    await act(async () => { fireEvent.click(screen.getByTestId('cc-send')); });
  }

  it('/clear clears messages and resets', async () => {
    render(<ChatWorkspace />);
    await send('/clear');
    expect(h.session.clearMessages).toHaveBeenCalled();
    expect(h.exec.resetForSession).toHaveBeenCalled();
  });

  it('/jobs lists executions (empty and populated)', async () => {
    render(<ChatWorkspace />);
    await send('/jobs');
    expect(h.listExecutions).toHaveBeenCalled();
    h.listExecutions.mockResolvedValue([{ job_id: 'abcdef12', status: 'completed', created_at: new Date().toISOString() }]);
    await send('/list jobs');
    expect(h.session.addMessage).toHaveBeenCalled();
  });

  it('/jobs reports an error when listing fails', async () => {
    h.listExecutions.mockRejectedValue(new Error('list fail'));
    render(<ChatWorkspace />);
    await send('/jobs');
    expect(h.session.addMessage).toHaveBeenCalledWith('assistant', expect.stringContaining('list fail'));
  });

  it('/stop <id> stops; /stop without id shows usage', async () => {
    h.exec.activeExecution = { jobId: 'job-77', status: 'running' };
    render(<ChatWorkspace />);
    await send('/stop job-77');
    expect(h.stopExecution).toHaveBeenCalledWith('job-77');
    h.session.addMessage.mockClear();
    await send('/stop'); // bare command, no id -> usage
    expect(h.session.addMessage).toHaveBeenCalledWith('assistant', expect.stringContaining('Usage'));
  });

  it('/stop reports a failure when it throws', async () => {
    h.stopExecution.mockRejectedValue(new Error('nope'));
    render(<ChatWorkspace />);
    await send('/stop job-5');
    expect(h.session.addMessage).toHaveBeenCalledWith('assistant', expect.stringContaining('nope'));
  });

  it('/dismiss and /close reset the session', async () => {
    render(<ChatWorkspace />);
    await send('/dismiss');
    await send('/close');
    expect(h.exec.resetForSession).toHaveBeenCalled();
  });

  it('/refine runs an editor crew on the current artifact', async () => {
    const artifact = '<html><body>old</body></html>';
    h.exec.previewContent = { type: 'html', data: artifact };
    render(<ChatWorkspace />);
    await send('/refine make the header blue');
    expect(h.session.addMessage).toHaveBeenCalledWith('user', 'Refine: make the header blue');
    expect(h.createExecution).toHaveBeenCalled();
    // The editor agent is pinned to the selected model (avoids the gpt-4o default
    // that fails with no OpenAI key); the crew-level model arg is set too.
    const [agents, tasks, model, , inputs] = mockedBuildGenerated.mock.calls.at(-1) as [
      Array<{ llm?: string; memory?: boolean; allow_delegation?: boolean }>,
      Array<{ description: string }>,
      string | undefined,
      unknown,
      Record<string, string> | undefined,
    ];
    expect(model).toBe('m1');
    expect(agents[0].llm).toBe('m1');
    // A refine is a single-shot edit: memory off (skips the cognitive-memory
    // search/save flow) and no delegation keeps it to one lightweight pass.
    expect(agents[0].memory).toBe(false);
    expect(agents[0].allow_delegation).toBe(false);
    // The instruction + artifact are passed as inputs and referenced via
    // {instruction}/{artifact} placeholders — NOT inlined. Inlining an artifact
    // whose HTML/JS contains a brace token (e.g. `${spread}` -> `{spread}`) makes
    // CrewAI's {var} interpolation fail ("template variable not found").
    expect(inputs).toEqual({ instruction: 'make the header blue', artifact });
    expect(tasks[0].description).toContain('{instruction}');
    expect(tasks[0].description).toContain('{artifact}');
    expect(tasks[0].description).not.toContain(artifact);
    // The refine preserves the existing preview + history (continuation, not a
    // fresh run that would wipe the artifact lineage).
    expect(h.exec.startExecution).toHaveBeenCalledWith('job-1', 's1', { preservePreview: true });
  });

  it('/refine omits the agent llm when no model is selected', async () => {
    const prevModel = h.app.selectedModel;
    h.app.selectedModel = '';
    h.exec.previewContent = { type: 'html', data: '<html><body>old</body></html>' };
    try {
      render(<ChatWorkspace />);
      await send('/refine make it pop');
      const [agents] = mockedBuildGenerated.mock.calls.at(-1) as [Array<{ llm?: string }>];
      expect(agents[0].llm).toBeUndefined();
    } finally {
      h.app.selectedModel = prevModel;
    }
  });

  it('/refine falls back to the persisted preview when none is live', async () => {
    h.exec.previewContent = null;
    h.getSessionPreview.mockResolvedValue({ type: 'html', data: '<html>stored</html>' });
    render(<ChatWorkspace />);
    await send('/refine tweak it');
    expect(h.createExecution).toHaveBeenCalled();
  });

  it('/refine with no artifact tells the user to run a crew first', async () => {
    h.exec.previewContent = null;
    h.getSessionPreview.mockResolvedValue(null);
    render(<ChatWorkspace />);
    await send('/refine improve this');
    expect(h.session.addMessage).toHaveBeenCalledWith('assistant', expect.stringContaining('no result to refine'));
    expect(h.createExecution).not.toHaveBeenCalled();
  });

  it('/refine with no instruction shows usage', async () => {
    render(<ChatWorkspace />);
    await send('/refine');
    expect(h.session.addMessage).toHaveBeenCalledWith('assistant', expect.stringContaining('Usage'));
  });

  it('refines via the preview pane onRefine handler', async () => {
    h.exec.previewContent = { type: 'html', data: '<html><body>x</body></html>' };
    h.exec.previewOwnerSessionId = 's1';
    render(<ChatWorkspace />);
    await act(async () => { fireEvent.click(screen.getByTestId('preview-refine')); });
    expect(h.createExecution).toHaveBeenCalled();
  });

  // --- save crew to catalog ---
  it('/save tells the user when there is no generated crew yet', async () => {
    render(<ChatWorkspace />);
    await send('/save');
    expect(h.saveGeneratedCrew).not.toHaveBeenCalled();
    expect(h.session.addMessage).toHaveBeenCalledWith(
      'assistant',
      expect.stringContaining('no generated crew to save'),
    );
  });

  it('/save persists the last generated crew and confirms by name', async () => {
    render(<ChatWorkspace />);
    // a generation completes → becomes the /save target
    await act(async () => { h.genOpts.onComplete({ agents: [{ id: 'a1' }], tasks: [{ id: 't1' }] }); });
    await send('/save');
    expect(h.saveGeneratedCrew).toHaveBeenCalledWith({ agents: [{ id: 'a1' }], tasks: [{ id: 't1' }] }, undefined, expect.anything());
    expect(h.session.addMessage).toHaveBeenCalledWith(
      'assistant',
      expect.stringContaining('Saved **Saved Crew** to the catalog'),
    );
  });

  it('/save <name> passes the explicit name through', async () => {
    render(<ChatWorkspace />);
    await act(async () => { h.genOpts.onComplete({ agents: [{ id: 'a1' }], tasks: [{ id: 't1' }] }); });
    await send('/save Oil Crew');
    expect(h.saveGeneratedCrew).toHaveBeenCalledWith(expect.anything(), 'Oil Crew', expect.anything());
  });

  it('/save reports an error when the save fails', async () => {
    h.saveGeneratedCrew.mockRejectedValueOnce(new Error('nope'));
    render(<ChatWorkspace />);
    await act(async () => { h.genOpts.onComplete({ agents: [{ id: 'a1' }], tasks: [{ id: 't1' }] }); });
    await send('/save');
    expect(h.session.addMessage).toHaveBeenCalledWith(
      'assistant',
      expect.stringContaining('Failed to save crew: nope'),
    );
  });

  it('saves a crew via the card bookmark (onSaveCrew handler)', async () => {
    render(<ChatWorkspace />);
    await act(async () => { fireEvent.click(screen.getByTestId('cc-save')); });
    expect(h.saveGeneratedCrew).toHaveBeenCalledWith({ agents: [{ id: 'a1' }], tasks: [{ id: 't1' }] }, undefined, expect.anything());
  });

  // --- Genie crews are not auto-run ---
  it('does NOT auto-run a Genie crew and prompts to pick a space', async () => {
    render(<ChatWorkspace />);
    await act(async () => {
      h.genOpts.onComplete({ agents: [{ id: 'a1', tools: ['GenieTool'] }], tasks: [] });
    });
    // generation finalized, but no execution kicked off
    expect(h.exec.completeGeneration).toHaveBeenCalled();
    expect(h.createExecution).not.toHaveBeenCalled();
    expect(h.session.addMessageToTargetSession).toHaveBeenCalledWith(
      's1',
      'assistant',
      '', // status text removed — the crew card (with the Genie space selector) carries it
      expect.objectContaining({ resultType: 'generation_complete' }),
    );
  });

  // --- sidebar interactions ---
  it('New Chat saves + creates + restores session state', async () => {
    render(<ChatWorkspace />);
    await act(async () => { fireEvent.click(screen.getByText('New Chat')); });
    expect(h.session.createNewSession).toHaveBeenCalled();
    expect(h.exec.restoreSessionState).toHaveBeenCalled();
  });

  it('clicking a session switches to it', async () => {
    render(<ChatWorkspace />);
    await act(async () => { fireEvent.click(screen.getByTitle('Two')); });
    expect(h.session.switchSession).toHaveBeenCalledWith('s2');
  });

  it('opens the context menu and renames a session', async () => {
    render(<ChatWorkspace />);
    // kebab buttons have title "Options"
    fireEvent.click(screen.getAllByTitle('Options')[0]);
    fireEvent.click(screen.getByText('Rename'));
    const input = document.querySelector('input[autofocus], input') as HTMLInputElement;
    fireEvent.change(input, { target: { value: 'Renamed' } });
    await act(async () => { fireEvent.keyDown(input, { key: 'Enter' }); });
    expect(h.session.renameSession).toHaveBeenCalledWith('s1', 'Renamed');
  });

  it('rename can be cancelled with Escape', () => {
    render(<ChatWorkspace />);
    fireEvent.click(screen.getAllByTitle('Options')[0]);
    fireEvent.click(screen.getByText('Rename'));
    const input = document.querySelector('input') as HTMLInputElement;
    fireEvent.keyDown(input, { key: 'Escape' });
    expect(h.session.renameSession).not.toHaveBeenCalled();
  });

  it('deletes a session from the context menu', async () => {
    render(<ChatWorkspace />);
    fireEvent.click(screen.getAllByTitle('Options')[0]);
    await act(async () => { fireEvent.click(screen.getByText('Delete')); });
    expect(h.session.deleteSession).toHaveBeenCalledWith('s1');
  });

  it('renders the sidebar-closed minimal layout when sidebarOpen is false', () => {
    h.app.sidebarOpen = false;
    render(<ChatWorkspace />);
    expect(screen.getByTestId('chat-container')).toBeInTheDocument();
  });

  // --- preview reopen + panel controls ---
  it('shows the "Show preview" reopen button when a hidden preview exists', async () => {
    h.exec.previewContent = null;
    h.getSessionPreview.mockResolvedValue({ type: 'html', data: '<p>x</p>' });
    render(<ChatWorkspace />);
    expect(await screen.findByText('Show preview')).toBeInTheDocument();
    fireEvent.click(screen.getByText('Show preview'));
    expect(h.exec.reopenPreview).toHaveBeenCalled();
  });

  it('preview panel close + toggle-chat buttons call the store', () => {
    h.exec.previewContent = { type: 'html', data: '<p>x</p>' };
    h.exec.previewOwnerSessionId = 's1';
    render(<ChatWorkspace />);
    fireEvent.click(screen.getByTestId('preview-close'));
    expect(h.exec.clearPreview).toHaveBeenCalled();
    fireEvent.click(screen.getByTestId('preview-toggle'));
    expect(h.exec.toggleChatCollapsed).toHaveBeenCalled();
  });

  // --- onTrace pairing + onComplete extraction shapes ---
  it('onTrace pairs a tool_result arriving before its tool_call', () => {
    render(<ChatWorkspace />);
    act(() => {
      h.streamOpts.onTrace('', { event_type: 's_run', output: { tool_name: 'S', input: '{"q":"a"}', content: 'r', duration_ms: 5 } });
      // a second tool_call with the same matchKey should be dropped (already resolved)
      h.streamOpts.onTrace('', { event_type: 'tool_usage', output: { extra_data: { tool_name: 'S', tool_args: '{"q":"a"}' } } });
    });
    // owner session is set, so traces are routed to the target session
    expect(h.session.addMessageToTargetSession).toHaveBeenCalled();
  });

  it.each([
    ['string result', { result: 'plain' }],
    ['json string result', { result: '{"result":"inner"}' }],
    ['json string content', { result: '{"content":"c"}' }],
    ['object nested result', { result: { result: 'deep' } }],
    ['object nested content', { result: { content: 'deepc' } }],
    ['object deep content', { result: { result: { content: 'x' } } }],
    ['top-level content', { content: 'topc' }],
    ['output field', { output: 'outp' }],
    ['unparseable string', { result: 'not json {' }],
  ])('onComplete extracts result from %s', (_label, data) => {
    render(<ChatWorkspace />);
    act(() => { h.streamOpts.onComplete(data); });
    expect(h.exec.completeExecution).toHaveBeenCalled();
  });

  it('onTaskOutput never auto-completes the execution on a timer (banner persists until real completion)', () => {
    vi.useFakeTimers();
    h.parsePreview.mockReturnValue(null);
    h.exec.executionOwnerSessionId = 's1';
    render(<ChatWorkspace />);
    act(() => {
      // intermediate task output, then a later one — neither schedules a timer
      h.streamOpts.onTrace('', { event_type: 'task_completed', trace_metadata: { task_name: 'Task 1' }, output: JSON.stringify({ content: 'intermediate markdown brief' }) });
      h.streamOpts.onTrace('', { event_type: 'task_completed', trace_metadata: { task_name: 'Task 2' }, output: 'a later task output' });
    });
    // advancing far past any old window must NOT trigger a completion
    act(() => { vi.advanceTimersByTime(120000); });
    vi.useRealTimers();
    // the task output still routes its summary message to the owner session
    expect(h.session.addMessageToTargetSession).toHaveBeenCalled();
    // ...but the crew is only "done" when a real onComplete/onError arrives
    expect(h.exec.completeExecution).not.toHaveBeenCalled();
  });

  it('each previewable task output is pushed to the preview store (history accumulates)', () => {
    const first = { type: 'markdown' as const, data: '# first' };
    const second = { type: 'html' as const, data: '<p>second</p>' };
    h.parsePreview.mockReturnValueOnce(first).mockReturnValueOnce(second);
    h.exec.executionOwnerSessionId = 's1';
    h.session.currentSessionId = 's1';
    render(<ChatWorkspace />);
    act(() => {
      h.streamOpts.onTrace('', { event_type: 'task_completed', trace_metadata: { task_name: 'Task 1' }, output: '# first' });
      h.streamOpts.onTrace('', { event_type: 'task_completed', trace_metadata: { task_name: 'Task 2' }, output: '<p>second</p>' });
    });
    expect(h.exec.setPreviewContent).toHaveBeenCalledWith(first);
    expect(h.exec.setPreviewContent).toHaveBeenCalledWith(second);
  });

  it('a real onComplete completes the execution', () => {
    h.exec.executionOwnerSessionId = 's1';
    render(<ChatWorkspace />);
    act(() => {
      h.streamOpts.onTrace('', { event_type: 'task_completed', trace_metadata: { task_name: 'Task' }, output: 'some intermediate output' });
      h.streamOpts.onComplete({ result: 'real final' });
    });
    expect(h.exec.completeExecution).toHaveBeenCalledWith('real final');
  });

  it('a real onError fails the execution and never completes it', () => {
    h.exec.executionOwnerSessionId = 's1';
    render(<ChatWorkspace />);
    act(() => {
      h.streamOpts.onTrace('', { event_type: 'task_completed', trace_metadata: { task_name: 'Task' }, output: 'some intermediate output' });
      h.streamOpts.onError('stream failed');
    });
    expect(h.exec.failExecution).toHaveBeenCalledWith('stream failed');
    expect(h.exec.completeExecution).not.toHaveBeenCalled();
  });

  it('executes a crew built from real agent/task nodes (name-mapping arms)', async () => {
    (globalThis as { __crewPlan?: unknown }).__crewPlan = {
      name: 'Rich',
      nodes: [
        { type: 'agentNode', data: { role: 'Researcher' } },
        { type: 'agent', data: { name: 'NamedAgent' } },
        { type: 'agentNode', data: {} },
        { type: 'taskNode', data: { name: 'T1' } },
        { type: 'task', data: { description: 'a description that is quite long indeed for slicing' } },
        { type: 'taskNode', data: {} },
      ],
      edges: [],
    };
    render(<ChatWorkspace />);
    await act(async () => { fireEvent.click(screen.getByTestId('cc-exec-crew')); });
    expect(h.exec.setExecutionContext).toHaveBeenCalled();
    expect(h.createExecution).toHaveBeenCalled();
  });

  it('executes a generated crew with named/role agents and named/desc tasks', async () => {
    (globalThis as { __genData?: unknown }).__genData = {
      agents: [{ id: 'a1', name: 'Alice', role: 'Lead' }, { id: 'a2', role: 'Helper' }],
      tasks: [{ id: 't1', name: 'Task One' }, { id: 't2', description: 'desc only' }],
    };
    render(<ChatWorkspace />);
    await act(async () => { fireEvent.click(screen.getByTestId('cc-exec-gen')); });
    expect(h.exec.setExecutionContext).toHaveBeenCalled();
    expect(h.createExecution).toHaveBeenCalled();
  });

  it('backfills a hidden preview from an assistant message containing previewable content', async () => {
    h.exec.previewContent = null;
    h.getSessionPreview.mockResolvedValue(null);
    h.parsePreview.mockReturnValue({ type: 'html', data: '<p>x</p>' });
    h.session.messages = [
      { id: 'u', role: 'user', content: 'hi', timestamp: new Date() },
      { id: 'a', role: 'assistant', content: '<html>doc</html>', timestamp: new Date() },
    ] as unknown[];
    render(<ChatWorkspace />);
    await screen.findByText('Show preview');
    expect(h.saveSessionPreview).toHaveBeenCalled();
  });

  it('does not show a reopen button when no stored or message preview exists', async () => {
    h.exec.previewContent = null;
    h.getSessionPreview.mockResolvedValue(null);
    h.parsePreview.mockReturnValue(null);
    h.session.messages = [{ id: 'a', role: 'assistant', content: 'plain', timestamp: new Date() }] as unknown[];
    render(<ChatWorkspace />);
    // allow the effect's promise chain to settle
    await act(async () => { await Promise.resolve(); });
    expect(screen.queryByText('Show preview')).not.toBeInTheDocument();
  });

  it('onComplete handles a deeply nested object whose inner is stringified', () => {
    render(<ChatWorkspace />);
    act(() => { h.streamOpts.onComplete({ result: { result: { foo: 'bar' } } }); });
    act(() => { h.streamOpts.onComplete({ result: {} }); });
    act(() => { h.streamOpts.onComplete({}); });
    expect(h.exec.completeExecution).toHaveBeenCalled();
  });

  it('onTaskOutput sets the live preview when viewing the owner session', () => {
    h.parsePreview.mockReturnValue({ type: 'html', data: '<p>x</p>' });
    h.session.currentSessionId = 's1';
    h.exec.executionOwnerSessionId = 's1';
    render(<ChatWorkspace />);
    act(() => { h.streamOpts.onTrace('', { event_type: 'task_completed', trace_metadata: { task_name: 'Build' }, output: '<p>x</p>' }); });
    expect(h.exec.setPreviewContent).toHaveBeenCalled();
  });

  it('/stop stops the matching active execution stream', async () => {
    h.exec.activeExecution = { jobId: 'job-match', status: 'running' };
    render(<ChatWorkspace />);
    await send('/stop job-match');
    expect(h.stopExecution).toHaveBeenCalledWith('job-match');
    expect(h.exec.updateExecutionStatus).toHaveBeenCalledWith('stopped');
  });

  it('preview-check effect no-ops when there is no current session', async () => {
    h.session.currentSessionId = null;
    h.exec.previewContent = null;
    render(<ChatWorkspace />);
    await act(async () => { await Promise.resolve(); });
    expect(screen.queryByText('Show preview')).not.toBeInTheDocument();
  });

  it('routes trace/taskOutput/generation messages to addMessage when no owner session', () => {
    h.exec.executionOwnerSessionId = null;
    h.parsePreview.mockReturnValue(null);
    render(<ChatWorkspace />);
    act(() => {
      // matched pill update with no owner -> updateMessage
      h.streamOpts.onTrace('', { event_type: 'tool_usage', output: { extra_data: { tool_name: 'S', tool_args: '{"q":"a"}' } } });
      h.streamOpts.onTrace('', { event_type: 's_run', output: { tool_name: 'S', input: '{"q":"a"}', content: 'r', duration_ms: 5 } });
      // task output with no owner -> addMessage
      h.streamOpts.onTrace('', { event_type: 'task_completed', trace_metadata: { task_name: 'Task' }, output: 'a normal textual result' });
    });
    expect(h.session.addMessage).toHaveBeenCalled();
    expect(h.session.updateMessage).toHaveBeenCalled();
  });

  it('resolves trace ownership from the trace job_id when present', () => {
    h.exec.executionOwnerSessionId = 's1';
    h.session.addMessageToTargetSession.mockClear();
    render(<ChatWorkspace />);
    act(() => {
      h.streamOpts.onTrace('', {
        event_type: 'tool_usage',
        job_id: 'job-x', // present → exercises the `jobId && jobOwnerRef.get(jobId)` branch
        output: { extra_data: { tool_name: 'S', tool_args: '{"q":"a"}' } },
      });
    });
    expect(h.session.addMessageToTargetSession).toHaveBeenCalled();
  });

  it('generation onComplete adds to addMessage when no owner session', async () => {
    h.exec.executionOwnerSessionId = null;
    render(<ChatWorkspace />);
    await act(async () => { h.genOpts.onComplete({ agents: [], tasks: [] }); });
    expect(h.session.addMessage).toHaveBeenCalledWith('assistant', expect.any(String), expect.any(Object));
  });

  it('onComplete handles a string result that JSON-parses to a non-object', () => {
    render(<ChatWorkspace />);
    act(() => { h.streamOpts.onComplete({ result: '123' }); });
    expect(h.exec.completeExecution).toHaveBeenCalledWith('123');
  });

  it('onComplete swallows extraction errors and completes with empty text', () => {
    render(<ChatWorkspace />);
    act(() => {
      h.streamOpts.onComplete({ get result() { throw new Error('boom'); } } as unknown as Record<string, unknown>);
    });
    expect(h.exec.completeExecution).toHaveBeenCalledWith('');
  });

  it('confirms the variables dialog for a generated crew', async () => {
    h.detectVars.mockReturnValue([{ name: 'topic', required: true }]);
    render(<ChatWorkspace />);
    fireEvent.click(screen.getByTestId('cc-exec-gen'));
    await act(async () => { fireEvent.click(screen.getByTestId('vars-confirm')); });
    expect(h.createExecution).toHaveBeenCalled();
  });

  it('right-clicking a session opens its context menu, backdrop click closes it', () => {
    render(<ChatWorkspace />);
    fireEvent.contextMenu(screen.getByTitle('One'));
    expect(screen.getByText('Rename')).toBeInTheDocument();
    // backdrop is the fixed inset-0 overlay
    const backdrop = document.querySelector('.fixed.inset-0.z-40') as HTMLElement;
    fireEvent.click(backdrop);
    expect(screen.queryByText('Rename')).not.toBeInTheDocument();
  });

  it('forwards model selection to the app store', () => {
    render(<ChatWorkspace />);
    fireEvent.click(screen.getByTestId('cc-model'));
    expect(h.app.setSelectedModel).toHaveBeenCalledWith('m2');
  });

  it('shows a spinner for sessions with an active execution', () => {
    h.exec.hasActiveExecution = vi.fn(() => true);
    render(<ChatWorkspace />);
    // SessionSpinner renders a spinning dot inside the session button (no crash)
    expect(screen.getByTitle('One')).toBeInTheDocument();
  });

  it('dispatcher option callbacks start the generation/execution streams', () => {
    render(<ChatWorkspace />);
    // these options are wired into useDispatcher; invoke them directly
    act(() => { h.dispatcherOpts.onStartGenerationStream('gen-1', 's1'); });
    act(() => { h.dispatcherOpts.onStartExecutionStream('job-x', 's1'); });
    expect(h.exec.startGeneration).toHaveBeenCalled();
    expect(h.exec.startExecution).toHaveBeenCalledWith('job-x', 's1', undefined);
    expect(h.dispatcherOpts.getCurrentSessionId()).toBe('s1');
  });

  it('dispatcher stream starts fall back to the current session when no id passed', () => {
    render(<ChatWorkspace />);
    act(() => { h.dispatcherOpts.onStartGenerationStream('gen-2', ''); });
    act(() => { h.dispatcherOpts.onStartExecutionStream('job-y'); });
    expect(h.startStream).toHaveBeenCalled();
  });

  // =========================================================================
  // Full branch coverage — owned-run flags, fallbacks, and error arms
  // =========================================================================

  it('reflects owned execution/generation/loading flags and hides chat when collapsed with a preview', () => {
    h.exec.isExecuting = true;
    h.exec.isGenerating = true;
    h.exec.isLoading = true;
    h.exec.chatCollapsed = true;
    h.exec.previewContent = { type: 'html', data: '<p>x</p>' };
    h.exec.previewOwnerSessionId = 's1';
    h.exec.executionOwnerSessionId = 's1';
    render(<ChatWorkspace />);
    // chatCollapsed && previewContent -> the chat main panel is hidden
    expect(screen.queryByTestId('chat-container')).not.toBeInTheDocument();
    expect(screen.getByTestId('preview-panel')).toBeInTheDocument();
  });

  it('applies the dark theme when Kasal is in dark mode', () => {
    h.theme.isDarkMode = true;
    render(<ChatWorkspace />);
    expect(h.app.setTheme).toHaveBeenCalledWith('dark');
  });

  it('onTaskOutput persists a preview but does not set the live one when viewing another session', () => {
    h.parsePreview.mockReturnValue({ type: 'html', data: '<p>x</p>' });
    h.session.currentSessionId = 's1';
    h.exec.executionOwnerSessionId = 's2';
    render(<ChatWorkspace />);
    act(() => { h.streamOpts.onTrace('', { event_type: 'task_completed', trace_metadata: { task_name: 'Build' }, output: '<p>x</p>' }); });
    expect(h.saveSessionPreview).toHaveBeenCalledWith('s2', expect.anything());
    expect(h.exec.setPreviewContent).not.toHaveBeenCalled();
  });

  it('onTaskOutput with a preview but no owner session neither sets nor persists it', () => {
    h.parsePreview.mockReturnValue({ type: 'html', data: '<p>x</p>' });
    h.exec.executionOwnerSessionId = null;
    h.session.currentSessionId = 's1';
    render(<ChatWorkspace />);
    act(() => { h.streamOpts.onTrace('', { event_type: 'task_completed', trace_metadata: { task_name: 'Build' }, output: '<p>x</p>' }); });
    expect(h.exec.setPreviewContent).not.toHaveBeenCalled();
    expect(h.saveSessionPreview).not.toHaveBeenCalled();
  });

  it('onTaskOutput skips the chat summary when the output is pure status noise', () => {
    h.parsePreview.mockReturnValue(null);
    h.exec.executionOwnerSessionId = 's1';
    render(<ChatWorkspace />);
    h.session.addMessageToTargetSession.mockClear();
    act(() => { h.streamOpts.onTrace('', { event_type: 'task_completed', trace_metadata: { task_name: 'Task' }, output: 'Calling tools.' }); });
    expect(h.session.addMessageToTargetSession).not.toHaveBeenCalled();
  });

  it('onComplete falls back to the raw string when parsed JSON has neither result nor content', () => {
    render(<ChatWorkspace />);
    act(() => { h.streamOpts.onComplete({ result: '{"foo":"bar"}' }); });
    expect(h.exec.completeExecution).toHaveBeenCalledWith('{"foo":"bar"}');
  });

  it('wires the generation no-op callbacks (onPlanReady/onAgentDetail/onTaskDetail)', () => {
    render(<ChatWorkspace />);
    act(() => {
      h.genOpts.onPlanReady();
      h.genOpts.onAgentDetail();
      h.genOpts.onTaskDetail();
    });
    expect(h.genOpts.onPlanReady).toBeTypeOf('function');
  });

  it('starts generation/execution streams with an undefined origin when there is no session at all', () => {
    h.session.currentSessionId = null;
    render(<ChatWorkspace />);
    act(() => { h.dispatcherOpts.onStartGenerationStream('g', ''); });
    act(() => { h.dispatcherOpts.onStartExecutionStream('j', ''); });
    expect(h.exec.startGeneration).toHaveBeenCalledWith(undefined);
    expect(h.exec.startExecution).toHaveBeenCalledWith('j', undefined, undefined);
  });

  it('executes a crew with missing nodes/name, no model, and an execution_id fallback', async () => {
    h.app.selectedModel = '';
    h.createExecution.mockResolvedValue({ execution_id: 'exec-9' });
    (globalThis as { __crewPlan?: unknown }).__crewPlan = { edges: [] };
    render(<ChatWorkspace />);
    await act(async () => { fireEvent.click(screen.getByTestId('cc-exec-crew')); });
    expect(h.exec.setExecutionContext).toHaveBeenCalledWith(expect.objectContaining({ crewName: 'Crew' }));
    expect(h.exec.startExecution).toHaveBeenCalledWith('exec-9', 's1', undefined);
  });

  it('crew execution that throws a non-Error reports the generic failure', async () => {
    h.createExecution.mockRejectedValue('weird');
    render(<ChatWorkspace />);
    await act(async () => { fireEvent.click(screen.getByTestId('cc-exec-crew')); });
    expect(h.session.addMessage).toHaveBeenCalledWith('assistant', expect.stringContaining('Failed to start execution'));
  });

  it('executes a generated crew with a Genie space, missing agents/tasks, and undefined origin', async () => {
    h.session.currentSessionId = null;
    render(<ChatWorkspace />);
    await act(async () => { await h.dispatcherOpts.onExecuteGenerated({}, 'space-1'); });
    expect(h.createExecution).toHaveBeenCalled();
    expect(h.exec.startExecution).toHaveBeenCalledWith('job-1', undefined, { originSession: undefined });
  });

  it('generated execution that throws a non-Error reports the generic failure', async () => {
    h.createExecution.mockRejectedValue('boom-str');
    render(<ChatWorkspace />);
    await act(async () => { fireEvent.click(screen.getByTestId('cc-exec-gen')); });
    expect(h.session.addMessage).toHaveBeenCalledWith('assistant', expect.stringContaining('Failed to start execution'));
  });

  it('refining via the preview pane with an empty instruction is a no-op', async () => {
    (globalThis as { __refineMsg?: string }).__refineMsg = '   ';
    h.exec.previewContent = { type: 'html', data: 'x' };
    h.exec.previewOwnerSessionId = 's1';
    render(<ChatWorkspace />);
    await act(async () => { fireEvent.click(screen.getByTestId('preview-refine')); });
    expect(h.createExecution).not.toHaveBeenCalled();
  });

  it('refining with no artifact and no current session tells the user to run a crew first', async () => {
    // No live preview AND no session id -> handleRefine skips the persisted-preview
    // lookup (sid is falsy) and reports there is nothing to refine.
    h.exec.previewContent = null;
    h.session.currentSessionId = null;
    render(<ChatWorkspace />);
    await send('/refine do it');
    expect(h.session.addMessage).toHaveBeenCalledWith('assistant', expect.stringContaining('no result to refine'));
    expect(h.getSessionPreview).not.toHaveBeenCalled();
  });

  it('executes a flow with no name/model, falling back to execution_id', async () => {
    h.app.selectedModel = '';
    h.createExecution.mockResolvedValue({ execution_id: 'flow-exec' });
    render(<ChatWorkspace />);
    await act(async () => { await h.dispatcherOpts.onExecuteFlow({}); });
    expect(h.exec.setExecutionContext).toHaveBeenCalledWith(expect.objectContaining({ crewName: 'Flow' }));
    expect(h.exec.startExecution).toHaveBeenCalledWith('flow-exec', 's1', undefined);
  });

  it('flow execution that throws a non-Error reports the generic failure', async () => {
    h.createExecution.mockRejectedValue('flow-str');
    render(<ChatWorkspace />);
    await act(async () => { await h.dispatcherOpts.onExecuteFlow({ name: 'F' }); });
    expect(h.session.addMessage).toHaveBeenCalledWith('assistant', expect.stringContaining('Failed to start execution'));
  });

  it('/jobs renders rows using id / unknown / dash fallbacks', async () => {
    h.listExecutions.mockResolvedValue([{ id: 'fallbackid123' }, {}]);
    render(<ChatWorkspace />);
    await send('/jobs');
    expect(h.session.addMessage).toHaveBeenCalledWith('assistant', expect.stringContaining('unknown'));
  });

  it('/jobs reports a generic error on a non-Error rejection', async () => {
    h.listExecutions.mockRejectedValue('list-str');
    render(<ChatWorkspace />);
    await send('/jobs');
    expect(h.session.addMessage).toHaveBeenCalledWith('assistant', expect.stringContaining('Failed to list executions'));
  });

  it('/stop matches by jobId prefix, and ignores a non-matching active execution', async () => {
    h.exec.activeExecution = { jobId: 'job-prefix-123', status: 'running' };
    render(<ChatWorkspace />);
    await send('/stop job-prefix'); // startsWith arm (not strict equality)
    expect(h.exec.updateExecutionStatus).toHaveBeenCalledWith('stopped');
    h.exec.activeExecution = { jobId: 'totally-different', status: 'running' };
    h.exec.updateExecutionStatus = vi.fn();
    await send('/stop nomatch'); // neither equals nor prefixes -> no status change
    expect(h.exec.updateExecutionStatus).not.toHaveBeenCalled();
  });

  it('/stop reports a generic error on a non-Error rejection', async () => {
    h.stopExecution.mockRejectedValue('stop-str');
    render(<ChatWorkspace />);
    await send('/stop job-x');
    expect(h.session.addMessage).toHaveBeenCalledWith('assistant', expect.stringContaining('Failed to stop'));
  });

  it('/save reports a generic error on a non-Error rejection', async () => {
    render(<ChatWorkspace />);
    await act(async () => { h.genOpts.onComplete({ agents: [{ id: 'a1' }], tasks: [{ id: 't1' }] }); });
    h.saveGeneratedCrew.mockRejectedValueOnce('save-str'); // non-Error -> generic message
    await send('/save');
    expect(h.session.addMessage).toHaveBeenCalledWith('assistant', expect.stringContaining('Failed to save crew'));
  });

  it('sends a message with an undefined model when none is selected', async () => {
    h.app.selectedModel = '';
    render(<ChatWorkspace />);
    await send('hello there');
    expect(h.dispatcherSend).toHaveBeenCalledWith('hello there', undefined, undefined, undefined, undefined, undefined);
  });

  it('handleStopExecution reports a generic error on a non-Error rejection', async () => {
    h.exec.activeExecution = { jobId: 'j', status: 'running' };
    h.stopExecution.mockRejectedValue('hs-str');
    render(<ChatWorkspace />);
    await act(async () => { fireEvent.click(screen.getByTestId('cc-stop')); });
    expect(h.session.addMessage).toHaveBeenCalledWith('assistant', expect.stringContaining('Failed to stop'));
  });

  it('New Chat and session switch skip saving when there is no current session', async () => {
    h.session.currentSessionId = null;
    render(<ChatWorkspace />);
    await act(async () => { fireEvent.click(screen.getByText('New Chat')); });
    expect(h.exec.saveSessionState).not.toHaveBeenCalled();
    expect(h.session.createNewSession).toHaveBeenCalled();
    await act(async () => { fireEvent.click(screen.getByTitle('Two')); });
    expect(h.session.switchSession).toHaveBeenCalledWith('s2');
    expect(h.exec.saveSessionState).not.toHaveBeenCalled();
  });

  it('finishing a rename with a blank value does not call renameSession', async () => {
    render(<ChatWorkspace />);
    fireEvent.click(screen.getAllByTitle('Options')[0]);
    fireEvent.click(screen.getByText('Rename'));
    const input = document.querySelector('input') as HTMLInputElement;
    fireEvent.change(input, { target: { value: '   ' } });
    await act(async () => { fireEvent.blur(input); });
    expect(h.session.renameSession).not.toHaveBeenCalled();
  });

  it('crew and flow executions started with no current session pass an undefined origin', async () => {
    h.session.currentSessionId = null;
    render(<ChatWorkspace />);
    await act(async () => { fireEvent.click(screen.getByTestId('cc-exec-crew')); });
    expect(h.exec.startExecution).toHaveBeenCalledWith('job-1', undefined, undefined);
    h.exec.startExecution = vi.fn();
    await act(async () => { await h.dispatcherOpts.onExecuteFlow({ name: 'F' }); });
    expect(h.exec.startExecution).toHaveBeenCalledWith('job-1', undefined, undefined);
  });

  it('the preview backfill scan skips non-assistant and empty-content messages', async () => {
    h.exec.previewContent = null;
    h.getSessionPreview.mockResolvedValue(null);
    h.parsePreview.mockReturnValue(null); // no message is previewable
    h.session.messages = [
      { id: 'u', role: 'user', content: 'hi', timestamp: new Date() },
      { id: 'e', role: 'assistant', content: '', timestamp: new Date() },
      { id: 'a', role: 'assistant', content: 'plain text', timestamp: new Date() },
    ] as unknown[];
    render(<ChatWorkspace />);
    await act(async () => { await Promise.resolve(); });
    expect(screen.queryByText('Show preview')).not.toBeInTheDocument();
  });

  it('onTrace ignores noise events that yield no trace entry', () => {
    render(<ChatWorkspace />);
    h.session.addMessage.mockClear();
    h.session.addMessageToTargetSession.mockClear();
    act(() => { h.streamOpts.onTrace('', { event_type: 'llm_retry' }); });
    expect(h.session.addMessage).not.toHaveBeenCalled();
    expect(h.session.addMessageToTargetSession).not.toHaveBeenCalled();
  });

  it('confirming the variables dialog with no pending execution is a no-op', async () => {
    render(<ChatWorkspace />);
    // Invoke the captured onConfirm directly without ever opening a pending run.
    await act(async () => {
      (h as { varsConfirm?: (v: Record<string, string>) => void }).varsConfirm?.({ topic: 'AI' });
    });
    expect(h.createExecution).not.toHaveBeenCalled();
  });

  it('the context-menu Rename is a no-op when its session no longer exists', () => {
    const { rerender } = render(<ChatWorkspace />);
    // Open the context menu for session s1...
    fireEvent.contextMenu(screen.getByTitle('One'));
    expect(screen.getByText('Rename')).toBeInTheDocument();
    // ...then the session list loses s1 (re-render) before Rename is clicked, so the
    // find() inside the handler returns undefined and the guard short-circuits.
    h.session.sessions = [{ id: 's2', title: 'Two', updatedAt: new Date(), createdAt: new Date() }] as unknown[];
    rerender(<ChatWorkspace />);
    fireEvent.click(screen.getByText('Rename'));
    expect(h.session.renameSession).not.toHaveBeenCalled();
    // restore for later tests
    h.session.sessions = [
      { id: 's1', title: 'One', updatedAt: new Date(), createdAt: new Date() },
      { id: 's2', title: 'Two', updatedAt: new Date(), createdAt: new Date() },
    ] as unknown[];
  });

  // --- REST polling fallback (Job-History style) ---------------------------
  // ChatMode renders trace pills / completion from the live SSE stream, but the
  // Databricks Apps HTTP/2 proxy frequently kills SSE. So it also announces each
  // job via a 'jobCreated' window event (picked up by the globally-mounted
  // useTracePolling) and consumes the poller's 'traceUpdate' / 'jobCompleted' /
  // 'jobFailed' / 'jobStopped' window events — the same path crew-mode Job
  // History uses. These tests cover that wiring.
  it('announces the job via a jobCreated window event so the global poller polls it', async () => {
    const seen: (string | undefined)[] = [];
    const onCreated = (e: Event) => seen.push((e as CustomEvent).detail?.jobId);
    window.addEventListener('jobCreated', onCreated as EventListener);
    render(<ChatWorkspace />);
    await act(async () => { fireEvent.click(screen.getByTestId('cc-exec-crew')); });
    window.removeEventListener('jobCreated', onCreated as EventListener);
    expect(seen).toContain('job-1');
  });

  it('renders a polled trace (traceUpdate) for the active job through the same pipeline', () => {
    h.exec.activeExecution = { jobId: 'job-poll', status: 'running' };
    render(<ChatWorkspace />);
    act(() => {
      window.dispatchEvent(new CustomEvent('traceUpdate', {
        detail: {
          jobId: 'job-poll',
          trace: { id: 1, event_type: 'tool_usage', output: { extra_data: { tool_name: 'S', tool_args: '{"q":"a"}' } } },
        },
      }));
    });
    expect(h.session.addMessageToTargetSession).toHaveBeenCalled();
  });

  it('ignores a traceUpdate whose jobId is not the active execution', () => {
    h.exec.activeExecution = { jobId: 'job-poll', status: 'running' };
    render(<ChatWorkspace />);
    act(() => {
      window.dispatchEvent(new CustomEvent('traceUpdate', {
        detail: { jobId: 'other-job', trace: { id: 2, event_type: 'tool_usage', output: { extra_data: { tool_name: 'S', tool_args: '{}' } } } },
      }));
    });
    expect(h.session.addMessageToTargetSession).not.toHaveBeenCalled();
  });

  it('ignores a traceUpdate when there is no active execution', () => {
    h.exec.activeExecution = null;
    render(<ChatWorkspace />);
    act(() => {
      window.dispatchEvent(new CustomEvent('traceUpdate', {
        detail: { jobId: 'x', trace: { id: 3, event_type: 'tool_usage', output: {} } },
      }));
    });
    expect(h.session.addMessageToTargetSession).not.toHaveBeenCalled();
  });

  it('completes the run from the polling-fallback jobCompleted event', () => {
    h.exec.isExecuting = true;
    h.exec.activeExecution = { jobId: 'job-done', status: 'running' };
    render(<ChatWorkspace />);
    act(() => {
      window.dispatchEvent(new CustomEvent('jobCompleted', { detail: { jobId: 'job-done', result: 'final answer' } }));
    });
    expect(h.exec.completeExecution).toHaveBeenCalledWith('final answer');
  });

  it('ignores jobCompleted when it is not the active job', () => {
    h.exec.isExecuting = true;
    h.exec.activeExecution = { jobId: 'job-A', status: 'running' };
    render(<ChatWorkspace />);
    act(() => {
      window.dispatchEvent(new CustomEvent('jobCompleted', { detail: { jobId: 'job-B', result: 'x' } }));
    });
    expect(h.exec.completeExecution).not.toHaveBeenCalled();
  });

  it('completes a run only once even if jobCompleted is delivered twice (SSE + poll)', () => {
    h.exec.isExecuting = true;
    h.exec.activeExecution = { jobId: 'job-dupe', status: 'running' };
    render(<ChatWorkspace />);
    act(() => {
      window.dispatchEvent(new CustomEvent('jobCompleted', { detail: { jobId: 'job-dupe', result: 'one' } }));
      window.dispatchEvent(new CustomEvent('jobCompleted', { detail: { jobId: 'job-dupe', result: 'one' } }));
    });
    expect(h.exec.completeExecution).toHaveBeenCalledTimes(1);
  });

  it('fails the run from the polling-fallback jobFailed event', () => {
    h.exec.isExecuting = true;
    h.exec.activeExecution = { jobId: 'job-bad', status: 'running' };
    render(<ChatWorkspace />);
    act(() => {
      window.dispatchEvent(new CustomEvent('jobFailed', { detail: { jobId: 'job-bad', error: 'kaboom' } }));
    });
    expect(h.exec.failExecution).toHaveBeenCalledWith('kaboom');
  });

  it('jobFailed with no error message falls back to a default', () => {
    h.exec.isExecuting = true;
    h.exec.activeExecution = { jobId: 'job-bad2', status: 'running' };
    render(<ChatWorkspace />);
    act(() => {
      window.dispatchEvent(new CustomEvent('jobFailed', { detail: { jobId: 'job-bad2' } }));
    });
    expect(h.exec.failExecution).toHaveBeenCalledWith('Execution failed');
  });

  it('ignores jobFailed when not executing', () => {
    h.exec.isExecuting = false;
    h.exec.activeExecution = { jobId: 'job-bad3', status: 'running' };
    render(<ChatWorkspace />);
    act(() => {
      window.dispatchEvent(new CustomEvent('jobFailed', { detail: { jobId: 'job-bad3', error: 'x' } }));
    });
    expect(h.exec.failExecution).not.toHaveBeenCalled();
  });

  it('stops the run from the polling-fallback jobStopped event', () => {
    h.exec.isExecuting = true;
    h.exec.activeExecution = { jobId: 'job-stop', status: 'running' };
    render(<ChatWorkspace />);
    act(() => {
      window.dispatchEvent(new CustomEvent('jobStopped', { detail: { jobId: 'job-stop', status: 'stopped' } }));
    });
    expect(h.exec.failExecution).toHaveBeenCalledWith('Execution stopped');
  });

  it('ignores jobStopped when it is not the active job', () => {
    h.exec.isExecuting = true;
    h.exec.activeExecution = { jobId: 'job-stop-A', status: 'running' };
    render(<ChatWorkspace />);
    act(() => {
      window.dispatchEvent(new CustomEvent('jobStopped', { detail: { jobId: 'job-stop-B' } }));
    });
    expect(h.exec.failExecution).not.toHaveBeenCalled();
  });

  it('renders a polled trace only once even if delivered twice (dedup by trace id)', () => {
    h.exec.activeExecution = { jobId: 'job-dd', status: 'running' };
    render(<ChatWorkspace />);
    const trace = { id: 99, event_type: 'tool_usage', output: { extra_data: { tool_name: 'S', tool_args: '{"q":"a"}' } } };
    act(() => {
      window.dispatchEvent(new CustomEvent('traceUpdate', { detail: { jobId: 'job-dd', trace } }));
      window.dispatchEvent(new CustomEvent('traceUpdate', { detail: { jobId: 'job-dd', trace } }));
    });
    expect(h.session.addMessageToTargetSession).toHaveBeenCalledTimes(1);
  });

  it('handles polling-fallback events dispatched without a detail payload', () => {
    h.exec.activeExecution = null;
    h.exec.isExecuting = false;
    render(<ChatWorkspace />);
    act(() => {
      window.dispatchEvent(new Event('traceUpdate'));
      window.dispatchEvent(new Event('jobCompleted'));
      window.dispatchEvent(new Event('jobFailed'));
      window.dispatchEvent(new Event('jobStopped'));
    });
    expect(h.session.addMessageToTargetSession).not.toHaveBeenCalled();
    expect(h.exec.completeExecution).not.toHaveBeenCalled();
    expect(h.exec.failExecution).not.toHaveBeenCalled();
  });

  it('a polled task_completed falls back to event_context name and stringifies non-string output', () => {
    h.exec.activeExecution = { jobId: 'job-tc', status: 'running' };
    render(<ChatWorkspace />);
    act(() => {
      window.dispatchEvent(new CustomEvent('traceUpdate', {
        detail: { jobId: 'job-tc', trace: { id: 7, event_type: 'task_completed', event_context: 'My Task', result: { foo: 'bar' } } },
      }));
    });
    expect(h.session.addMessageToTargetSession).toHaveBeenCalled();
  });

  it('a task_completed with no output/result uses the message and a default task name', () => {
    render(<ChatWorkspace />);
    act(() => {
      h.streamOpts.onTrace('the message body', { event_type: 'task_completed' });
    });
    expect(h.session.addMessageToTargetSession).toHaveBeenCalled();
  });

  // --- group switching + reconnect-after-refresh (window-driven) ---
  it('group-changed reloads the workspace sessions and restores the active one', async () => {
    h.session.currentSessionId = 's1';
    render(<ChatWorkspace />);
    await act(async () => {
      window.dispatchEvent(new Event('group-changed'));
      await new Promise((r) => setTimeout(r, 0));
    });
    expect(h.session.reloadForGroup).toHaveBeenCalled();
    expect(h.exec.restoreSessionState).toHaveBeenCalledWith('s1');
  });

  it('group-changed with no active session resets per-session state', async () => {
    h.session.currentSessionId = null;
    render(<ChatWorkspace />);
    await act(async () => {
      window.dispatchEvent(new Event('group-changed'));
      await new Promise((r) => setTimeout(r, 0));
    });
    expect(h.exec.resetForSession).toHaveBeenCalled();
  });

  it('reconnects to a still-running job after refresh, then clears it once finished', async () => {
    h.getSessionRunningJob.mockResolvedValueOnce('job-rc');
    h.exec.activeExecution = null;
    h.exec.startExecution.mockImplementationOnce((jobId: string) => {
      h.exec.activeExecution = { jobId, status: 'running' };
      h.exec.isExecuting = true;
    });
    h.getExecutionStatus.mockResolvedValueOnce({ status: 'completed' });
    h.session.currentSessionId = 's1';
    await act(async () => {
      render(<ChatWorkspace />);
      await new Promise((r) => setTimeout(r, 0));
    });
    expect(h.exec.startExecution).toHaveBeenCalledWith('job-rc', 's1', { preservePreview: true });
    expect(h.getExecutionStatus).toHaveBeenCalledWith('job-rc');
    expect(h.clearSessionRunningJob).toHaveBeenCalledWith('s1');
  });

  it('reconnect keeps the optimistic running state when status is missing/not terminal', async () => {
    h.getSessionRunningJob.mockResolvedValueOnce('job-rc2');
    h.exec.activeExecution = null;
    h.exec.startExecution.mockImplementationOnce((jobId: string) => {
      h.exec.activeExecution = { jobId, status: 'running' };
      h.exec.isExecuting = true;
    });
    // No status field -> String(exec?.status || '') falls back to '' (not finished).
    h.getExecutionStatus.mockResolvedValueOnce({});
    h.session.currentSessionId = 's1';
    await act(async () => {
      render(<ChatWorkspace />);
      await new Promise((r) => setTimeout(r, 0));
    });
    expect(h.getExecutionStatus).toHaveBeenCalledWith('job-rc2');
    // not finished -> the marker is NOT cleared
    expect(h.clearSessionRunningJob).not.toHaveBeenCalled();
  });

  it('reconnect bails when a run is already active (no hijack)', async () => {
    h.getSessionRunningJob.mockResolvedValueOnce('job-other');
    h.exec.activeExecution = { jobId: 'already-running', status: 'running' };
    h.session.currentSessionId = 's1';
    await act(async () => {
      render(<ChatWorkspace />);
      await new Promise((r) => setTimeout(r, 0));
    });
    expect(h.exec.startExecution).not.toHaveBeenCalledWith('job-other', 's1', { preservePreview: true });
  });

  // =========================================================================
  // Rail catalog library + pending-run (loaded crew/flow) wiring
  // =========================================================================

  it('refreshes the rail catalog library on mount', () => {
    render(<ChatWorkspace />);
    expect(h.app.loadCatalog).toHaveBeenCalled();
  });

  it('refreshes the catalog library when the workspace (group) changes', async () => {
    render(<ChatWorkspace />);
    h.app.loadCatalog.mockClear();
    await act(async () => {
      window.dispatchEvent(new Event('group-changed'));
      await new Promise((r) => setTimeout(r, 0));
    });
    expect(h.app.loadCatalog).toHaveBeenCalled();
  });

  it('refreshes the catalog after a card-bookmark save (onSaveCrew)', async () => {
    render(<ChatWorkspace />);
    h.app.loadCatalog.mockClear();
    await act(async () => { fireEvent.click(screen.getByTestId('cc-save')); });
    expect(h.app.loadCatalog).toHaveBeenCalled();
  });

  it('passes the chat memory toggle through to buildCrewConfig when executing a loaded crew', async () => {
    const { buildCrewConfig } = await import('./utils/crewConfigBuilder');
    const mockedBuildCrew = buildCrewConfig as unknown as ReturnType<typeof vi.fn>;
    (h.exec as { memoryEnabled?: boolean }).memoryEnabled = false;
    render(<ChatWorkspace />);
    await act(async () => { fireEvent.click(screen.getByTestId('cc-exec-crew')); });
    // signature: (plan, model, inputs, memoryEnabled)
    expect(mockedBuildCrew.mock.calls.at(-1)?.[3]).toBe(false);
    delete (h.exec as { memoryEnabled?: boolean }).memoryEnabled;
  });

  it('arms a pending run when a crew is loaded for the current session, then runs it', async () => {
    render(<ChatWorkspace />);
    // dispatcher reports a loaded crew scoped to the current session
    await act(async () => {
      h.dispatcherOpts.onCrewLoaded({ name: 'Loaded Crew', nodes: [], edges: [] }, 's1');
    });
    // the label surfaces in the chat container for the matching session
    expect(screen.getByTestId('cc-pending-label')).toHaveTextContent('Loaded Crew');
    // running it executes the crew and clears the pending state
    await act(async () => { fireEvent.click(screen.getByTestId('cc-run-pending')); });
    expect(h.createExecution).toHaveBeenCalled();
    expect(screen.getByTestId('cc-pending-label')).toHaveTextContent('');
  });

  it('falls back to the "crew"/"flow" label when the loaded plan has no name', async () => {
    render(<ChatWorkspace />);
    await act(async () => { h.dispatcherOpts.onCrewLoaded({ nodes: [], edges: [] }, 's1'); });
    expect(screen.getByTestId('cc-pending-label')).toHaveTextContent('crew');
    await act(async () => { h.dispatcherOpts.onFlowLoaded({ nodes: [], edges: [] }, 's1'); });
    expect(screen.getByTestId('cc-pending-label')).toHaveTextContent('flow');
  });

  it('arms a pending run for a loaded flow and runs it', async () => {
    render(<ChatWorkspace />);
    await act(async () => { h.dispatcherOpts.onFlowLoaded({ name: 'Loaded Flow', nodes: [], edges: [] }, 's1'); });
    expect(screen.getByTestId('cc-pending-label')).toHaveTextContent('Loaded Flow');
    await act(async () => { fireEvent.click(screen.getByTestId('cc-run-pending')); });
    expect(h.createExecution).toHaveBeenCalled();
  });

  it('hides the pending-run label when it was armed for a different session', async () => {
    render(<ChatWorkspace />);
    // armed for s2 while viewing s1 -> not surfaced, and running is a no-op
    await act(async () => { h.dispatcherOpts.onCrewLoaded({ name: 'Other Crew', nodes: [], edges: [] }, 's2'); });
    expect(screen.getByTestId('cc-pending-label')).toHaveTextContent('');
    await act(async () => { fireEvent.click(screen.getByTestId('cc-run-pending')); });
    expect(h.createExecution).not.toHaveBeenCalled();
  });

  it('a genuine user message clears a pending loaded run', async () => {
    render(<ChatWorkspace />);
    await act(async () => { h.dispatcherOpts.onCrewLoaded({ name: 'Armed', nodes: [], edges: [] }, 's1'); });
    expect(screen.getByTestId('cc-pending-label')).toHaveTextContent('Armed');
    await send('just chatting');
    expect(screen.getByTestId('cc-pending-label')).toHaveTextContent('');
  });

  it('switching sessions clears a pending loaded run', async () => {
    render(<ChatWorkspace />);
    await act(async () => { h.dispatcherOpts.onCrewLoaded({ name: 'Armed', nodes: [], edges: [] }, 's1'); });
    expect(screen.getByTestId('cc-pending-label')).toHaveTextContent('Armed');
    await act(async () => { fireEvent.click(screen.getByTitle('Two')); });
    expect(h.session.switchSession).toHaveBeenCalledWith('s2');
    expect(screen.getByTestId('cc-pending-label')).toHaveTextContent('');
  });

  it('loads a saved crew from the rail library into a fresh session', async () => {
    h.app.savedCrews = [{ id: 'c1', name: 'My Saved Crew' }];
    h.app.savedFlows = [];
    render(<ChatWorkspace />);
    // open the collapsible "Agents Catalog" section
    fireEvent.click(screen.getByText('Agents Catalog'));
    await act(async () => { fireEvent.click(screen.getByTitle('Open crew “My Saved Crew”')); });
    // saves current state, spins up a new session, restores it, and sends /load
    expect(h.exec.saveSessionState).toHaveBeenCalledWith('s1');
    expect(h.session.createNewSession).toHaveBeenCalled();
    expect(h.exec.restoreSessionState).toHaveBeenCalledWith('s-new');
    expect(h.dispatcherSend).toHaveBeenCalledWith(
      '/load crew My Saved Crew', 'm1', undefined, undefined, undefined, 'Open crew: My Saved Crew',
    );
    h.app.savedCrews = [];
  });

  it('loads a saved flow from the rail library (and skips save when no current session)', async () => {
    h.session.currentSessionId = null;
    h.app.savedCrews = [];
    h.app.savedFlows = [{ id: 'f1', name: 'My Saved Flow' }];
    render(<ChatWorkspace />);
    fireEvent.click(screen.getByText('Agents Catalog'));
    await act(async () => { fireEvent.click(screen.getByTitle('Open flow “My Saved Flow”')); });
    expect(h.exec.saveSessionState).not.toHaveBeenCalled();
    expect(h.dispatcherSend).toHaveBeenCalledWith(
      '/load flow My Saved Flow', 'm1', undefined, undefined, undefined, 'Open flow: My Saved Flow',
    );
    h.app.savedFlows = [];
  });

  // --- /save overwrite + name-conflict ------------------------------------
  it('/save overwrite replaces an existing crew and confirms with "Updated … in"', async () => {
    render(<ChatWorkspace />);
    await act(async () => { h.genOpts.onComplete({ agents: [{ id: 'a1' }], tasks: [{ id: 't1' }] }); });
    h.app.loadCatalog.mockClear();
    await send('/save overwrite');
    expect(h.saveGeneratedCrew).toHaveBeenCalledWith(
      expect.anything(), undefined, expect.objectContaining({ overwrite: true }),
    );
    expect(h.app.loadCatalog).toHaveBeenCalled();
    expect(h.session.addMessage).toHaveBeenCalledWith(
      'assistant',
      expect.stringContaining('Updated **Saved Crew** in the catalog'),
    );
  });

  it('/save overwrite <name> forwards the explicit name', async () => {
    render(<ChatWorkspace />);
    await act(async () => { h.genOpts.onComplete({ agents: [{ id: 'a1' }], tasks: [{ id: 't1' }] }); });
    await send('/save overwrite Renamed Crew');
    expect(h.saveGeneratedCrew).toHaveBeenCalledWith(
      expect.anything(), 'Renamed Crew', expect.objectContaining({ overwrite: true }),
    );
  });

  it('/save surfaces a name-conflict message when the crew already exists', async () => {
    const { CrewNameConflictError } = await import('./api/crews');
    h.saveGeneratedCrew.mockRejectedValueOnce(new CrewNameConflictError('Dup Crew'));
    render(<ChatWorkspace />);
    await act(async () => { h.genOpts.onComplete({ agents: [{ id: 'a1' }], tasks: [{ id: 't1' }] }); });
    await send('/save');
    expect(h.session.addMessage).toHaveBeenCalledWith(
      'assistant',
      expect.stringContaining('**Dup Crew** is already in the catalog'),
    );
  });
});
