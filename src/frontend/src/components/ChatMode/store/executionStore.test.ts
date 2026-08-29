import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import { rememberTaskOutputMessage, useExecutionStore } from './executionStore';
import { useSessionStore } from './sessionStore';
import { saveSessionPreview, getSessionPreview, clearSessionRunningJob } from '../db/sessionApi';
import { parsePreviewContent } from '../components/Preview/PreviewPanel';
import { deriveSessionPreviews } from '../utils/sessionPreview';

// --- Mocks for sibling modules ---
vi.mock('./sessionStore', () => {
  const state = {
    currentSessionId: null as string | null,
    addMessage: vi.fn(),
    addMessageToTargetSession: vi.fn(),
    updateMessageInTargetSession: vi.fn(),
    updateMessage: vi.fn(),
    messages: [] as { id: string; content: string }[],
  };
  return {
    useSessionStore: {
      getState: vi.fn(() => state),
    },
  };
});

vi.mock('../db/sessionApi', () => ({
  saveSessionPreview: vi.fn(),
  getSessionPreview: vi.fn(() => Promise.resolve(undefined)),
  getSessionMessages: vi.fn(() => Promise.resolve([])),
  setSessionRunningJob: vi.fn(() => Promise.resolve()),
  getSessionRunningJob: vi.fn(() => Promise.resolve(null)),
  clearSessionRunningJob: vi.fn(() => Promise.resolve()),
}));

vi.mock('../components/Preview/PreviewPanel', () => ({
  parsePreviewContent: vi.fn(),
}));

vi.mock('../utils/sessionPreview', () => ({
  deriveSessionPreviews: vi.fn(() => Promise.resolve({ history: [], current: null })),
}));

// Typed helpers to access the mocked sessionStore state
const sessionState = () => (useSessionStore as unknown as { getState: () => any }).getState();
const setCurrentSessionId = (id: string | null) => {
  sessionState().currentSessionId = id;
};

const mockedSave = saveSessionPreview as unknown as ReturnType<typeof vi.fn>;
const mockedGet = getSessionPreview as unknown as ReturnType<typeof vi.fn>;
const mockedClearMarker = clearSessionRunningJob as unknown as ReturnType<typeof vi.fn>;
const mockedParse = parsePreviewContent as unknown as ReturnType<typeof vi.fn>;
const mockedDerive = deriveSessionPreviews as unknown as ReturnType<typeof vi.fn>;

// Capture the pristine initial state so each test resets cleanly
const initialState = useExecutionStore.getState();

const resetStore = () => {
  useExecutionStore.setState({
    activeExecution: null,
    isExecuting: false,
    isGenerating: false,
    isLoading: false,
    executionContext: null,
    previewContent: null,
    previewOwnerSessionId: null,
    previewHistory: [],
    previewIndex: 0,
    chatCollapsed: false,
    executionOwnerSessionId: null,
  });
};

beforeEach(() => {
  vi.clearAllMocks();
  setCurrentSessionId(null);
  mockedGet.mockResolvedValue(undefined);
  mockedParse.mockReset();
  resetStore();
});

afterEach(() => {
  vi.restoreAllMocks();
});

const preview = { type: 'ui' as const, data: '<p>hi</p>', title: 'T' };

describe('executionStore - basic setters & log', () => {
  it('chatModeType defaults to chat (single light agent) and setChatModeType updates it', () => {
    // Default answer mode is the fast single-agent path.
    expect(useExecutionStore.getState().chatModeType).toBe('chat');
    useExecutionStore.getState().setChatModeType('research');
    expect(useExecutionStore.getState().chatModeType).toBe('research');
    useExecutionStore.getState().setChatModeType('deep');
    expect(useExecutionStore.getState().chatModeType).toBe('deep');
    // restore default so other tests start clean
    useExecutionStore.getState().setChatModeType('chat');
  });

  it('setIsLoading / setExecutionContext / setChatCollapsed / toggleChatCollapsed', () => {
    const store = useExecutionStore.getState();
    store.setIsLoading(true);
    expect(useExecutionStore.getState().isLoading).toBe(true);

    const ctx = { foo: 'bar' } as any;
    store.setExecutionContext(ctx);
    expect(useExecutionStore.getState().executionContext).toBe(ctx);

    store.setChatCollapsed(true);
    expect(useExecutionStore.getState().chatCollapsed).toBe(true);

    store.toggleChatCollapsed();
    expect(useExecutionStore.getState().chatCollapsed).toBe(false);
    store.toggleChatCollapsed();
    expect(useExecutionStore.getState().chatCollapsed).toBe(true);
  });

  it('setWorkspaceMemory toggles the recall scope (default workspace-wide)', () => {
    // Recall scope defaults to workspace-wide; it only matters when memory is on
    // (the composer defaults the memory pill to Session — memoryEnabled=false).
    expect(useExecutionStore.getState().workspaceMemory).toBe(true);
    useExecutionStore.getState().setWorkspaceMemory(false);
    expect(useExecutionStore.getState().workspaceMemory).toBe(false);
    useExecutionStore.getState().setWorkspaceMemory(true);
    expect(useExecutionStore.getState().workspaceMemory).toBe(true);
  });

  it('memoryEnabled defaults to false so the composer shows "Session memory"', () => {
    expect(useExecutionStore.getState().memoryEnabled).toBe(false);
  });

  it('toggleMcpServer adds and removes MCP selections; setSelectedMcpServers replaces them', () => {
    useExecutionStore.setState({ selectedMcpServers: [] });
    useExecutionStore.getState().toggleMcpServer('My MCP');
    useExecutionStore.getState().toggleMcpServer('Databricks Genie: Sales');
    expect(useExecutionStore.getState().selectedMcpServers).toEqual([
      'My MCP',
      'Databricks Genie: Sales',
    ]);
    useExecutionStore.getState().toggleMcpServer('My MCP');
    expect(useExecutionStore.getState().selectedMcpServers).toEqual([
      'Databricks Genie: Sales',
    ]);
    useExecutionStore.getState().setSelectedMcpServers(['Only This']);
    expect(useExecutionStore.getState().selectedMcpServers).toEqual(['Only This']);
    useExecutionStore.getState().setSelectedMcpServers([]);
  });

  it('toggleAgentBricksEndpoint adds and removes selections; setSelectedAgentBricksEndpoints replaces them', () => {
    // Mirrors the MCP server selection behaviour for Agent Bricks endpoints.
    useExecutionStore.setState({ selectedAgentBricksEndpoints: [] });
    useExecutionStore.getState().toggleAgentBricksEndpoint('mas-81a3c6bb-endpoint');
    useExecutionStore.getState().toggleAgentBricksEndpoint('ka-9f2-endpoint');
    expect(useExecutionStore.getState().selectedAgentBricksEndpoints).toEqual([
      'mas-81a3c6bb-endpoint',
      'ka-9f2-endpoint',
    ]);
    // Toggling a present one removes it.
    useExecutionStore.getState().toggleAgentBricksEndpoint('mas-81a3c6bb-endpoint');
    expect(useExecutionStore.getState().selectedAgentBricksEndpoints).toEqual([
      'ka-9f2-endpoint',
    ]);
    // setSelectedAgentBricksEndpoints replaces the whole list.
    useExecutionStore.getState().setSelectedAgentBricksEndpoints(['only-this']);
    expect(useExecutionStore.getState().selectedAgentBricksEndpoints).toEqual(['only-this']);
    useExecutionStore.getState().setSelectedAgentBricksEndpoints([]);
    expect(useExecutionStore.getState().selectedAgentBricksEndpoints).toEqual([]);
  });

  it('persists ONLY the "+" picker selections to localStorage so a refresh/new chat keeps the connected MCP', () => {
    // The store is wrapped in zustand `persist`; selecting servers/endpoints must
    // survive a page reload (users complained when the connection reset), while
    // volatile per-run state must NOT be persisted (it would resurrect a stale
    // "running" banner / dead preview on refresh).
    useExecutionStore.getState().setSelectedMcpServers(['My MCP', 'Databricks Genie: Sales']);
    useExecutionStore.getState().setSelectedAgentBricksEndpoints(['mas-1-endpoint']);
    // The memory mode is a user preference and must survive a refresh too.
    useExecutionStore.getState().setMemoryEnabled(false);
    useExecutionStore.getState().setWorkspaceMemory(false);
    // Dirty some volatile state that must be excluded from the persisted blob.
    useExecutionStore.setState({
      activeExecution: { jobId: 'job-1', status: 'running' },
      isExecuting: true,
      previewContent: preview,
    });

    const raw = window.localStorage.getItem('kasal-chatmode-mcp-selection');
    expect(raw).toBeTruthy();
    const persisted = JSON.parse(raw as string).state;

    // Selections are persisted...
    expect(persisted.selectedMcpServers).toEqual(['My MCP', 'Databricks Genie: Sales']);
    expect(persisted.selectedAgentBricksEndpoints).toEqual(['mas-1-endpoint']);
    // ...the memory-mode preference is persisted so "No memory" survives a refresh...
    expect(persisted.memoryEnabled).toBe(false);
    expect(persisted.workspaceMemory).toBe(false);
    // ...and nothing volatile is.
    expect(persisted).not.toHaveProperty('activeExecution');
    expect(persisted).not.toHaveProperty('isExecuting');
    expect(persisted).not.toHaveProperty('previewContent');

    useExecutionStore.getState().setSelectedMcpServers([]);
    useExecutionStore.getState().setSelectedAgentBricksEndpoints([]);
    useExecutionStore.getState().setMemoryEnabled(true);
    useExecutionStore.getState().setWorkspaceMemory(true);
  });

  it('setMemoryEnabled toggles whether crews run with memory (default enabled)', () => {
    // Defaults to enabled so crews keep memory unless the user picks "No memory".
    expect(useExecutionStore.getState().memoryEnabled).toBe(true);
    useExecutionStore.getState().setMemoryEnabled(false);
    expect(useExecutionStore.getState().memoryEnabled).toBe(false);
    useExecutionStore.getState().setMemoryEnabled(true);
    expect(useExecutionStore.getState().memoryEnabled).toBe(true);
  });

  it('setPreviewContent stamps owner with current session when content provided', () => {
    setCurrentSessionId('sess-A');
    useExecutionStore.getState().setPreviewContent(preview as any);
    const s = useExecutionStore.getState();
    expect(s.previewContent).toEqual(preview);
    expect(s.previewOwnerSessionId).toBe('sess-A');
  });

  it('setPreviewContent clears owner when content is null', () => {
    setCurrentSessionId('sess-A');
    useExecutionStore.getState().setPreviewContent(null);
    const s = useExecutionStore.getState();
    expect(s.previewContent).toBeNull();
    expect(s.previewOwnerSessionId).toBeNull();
  });

  it('clearPreview closes the pane and uncollapses chat but keeps the content for reopen', () => {
    useExecutionStore.setState({
      previewContent: preview as any,
      previewOwnerSessionId: 'sess-A',
      previewPaneOpen: true,
      chatCollapsed: true,
    });
    useExecutionStore.getState().clearPreview();
    const s = useExecutionStore.getState();
    // Pane closes, but the deliverable is kept (renders inline; instant reopen).
    expect(s.previewPaneOpen).toBe(false);
    expect(s.previewContent).toEqual(preview);
    expect(s.previewOwnerSessionId).toBe('sess-A');
    expect(s.chatCollapsed).toBe(false);
  });
});

describe('executionStore - preview history', () => {
  const a = { type: 'ui' as const, data: '# A', title: 'A' };
  const b = { type: 'ui' as const, data: '<p>B</p>', title: 'B' };

  it('setPreviewContent appends each distinct preview and points index at the latest', () => {
    setCurrentSessionId('sess-A');
    const store = useExecutionStore.getState();
    store.setPreviewContent(a as any);
    store.setPreviewContent(b as any);
    const s = useExecutionStore.getState();
    expect(s.previewHistory).toEqual([a, b]);
    expect(s.previewIndex).toBe(1);
    expect(s.previewContent).toEqual(b);
  });

  it('setPreviewContent dedupes consecutive identical previews', () => {
    setCurrentSessionId('sess-A');
    const store = useExecutionStore.getState();
    store.setPreviewContent(a as any);
    store.setPreviewContent({ ...a } as any);
    const s = useExecutionStore.getState();
    expect(s.previewHistory).toEqual([a]);
    expect(s.previewIndex).toBe(0);
  });

  it('setPreviewContent(null) clears content/owner but leaves history untouched', () => {
    setCurrentSessionId('sess-A');
    const store = useExecutionStore.getState();
    store.setPreviewContent(a as any);
    store.setPreviewContent(null);
    const s = useExecutionStore.getState();
    expect(s.previewContent).toBeNull();
    expect(s.previewOwnerSessionId).toBeNull();
    expect(s.previewHistory).toEqual([a]);
  });

  it('navigatePreview switches the shown preview to an earlier entry', () => {
    setCurrentSessionId('sess-A');
    const store = useExecutionStore.getState();
    store.setPreviewContent(a as any);
    store.setPreviewContent(b as any);
    useExecutionStore.getState().navigatePreview(0);
    const s = useExecutionStore.getState();
    expect(s.previewIndex).toBe(0);
    expect(s.previewContent).toEqual(a);
  });

  it('navigatePreview ignores out-of-range indices', () => {
    setCurrentSessionId('sess-A');
    useExecutionStore.getState().setPreviewContent(a as any);
    useExecutionStore.getState().navigatePreview(5);
    expect(useExecutionStore.getState().previewIndex).toBe(0);
    useExecutionStore.getState().navigatePreview(-1);
    expect(useExecutionStore.getState().previewIndex).toBe(0);
    expect(useExecutionStore.getState().previewContent).toEqual(a);
  });

  it('updatePreviewData replaces the current version in place (no new history entry) and persists', () => {
    setCurrentSessionId('sess-A');
    const store = useExecutionStore.getState();
    store.setPreviewContent(a as any);
    store.setPreviewContent(b as any); // history: [a, b], index 1
    mockedSave.mockClear();
    useExecutionStore.getState().updatePreviewData('<p>B restyled</p>');
    const s = useExecutionStore.getState();
    // history length unchanged; the viewed entry's data swapped in place
    expect(s.previewHistory).toHaveLength(2);
    expect(s.previewIndex).toBe(1);
    expect(s.previewContent?.data).toBe('<p>B restyled</p>');
    expect(s.previewContent?.title).toBe('B'); // other fields preserved
    expect(s.previewHistory[1].data).toBe('<p>B restyled</p>');
    expect(s.previewHistory[0]).toEqual(a); // earlier version untouched
    // persisted to the owning session
    expect(mockedSave).toHaveBeenCalledWith('sess-A', { type: 'ui', data: '<p>B restyled</p>', title: 'B' });
  });

  it('updatePreviewData is a no-op when there is no current preview', () => {
    const store = useExecutionStore.getState();
    store.updatePreviewData('anything');
    expect(useExecutionStore.getState().previewContent).toBeNull();
    expect(mockedSave).not.toHaveBeenCalled();
  });

  it('updatePreviewData swaps content but skips history when the index has no slot', () => {
    useExecutionStore.setState({
      previewContent: a as any,
      previewOwnerSessionId: 'sess-A',
      previewHistory: [a as any],
      previewIndex: 5, // out of range — no slot to replace
    });
    mockedSave.mockClear();
    useExecutionStore.getState().updatePreviewData('NEW');
    const s = useExecutionStore.getState();
    expect(s.previewContent?.data).toBe('NEW');
    expect(s.previewHistory[0]).toEqual(a); // history untouched
    expect(mockedSave).toHaveBeenCalledWith('sess-A', expect.objectContaining({ data: 'NEW' }));
  });

  it('updatePreviewData does not persist when no session owns the preview', () => {
    setCurrentSessionId(null);
    const store = useExecutionStore.getState();
    store.setPreviewContent(a as any); // owner resolves to null
    mockedSave.mockClear();
    store.updatePreviewData('X');
    expect(useExecutionStore.getState().previewContent?.data).toBe('X');
    expect(mockedSave).not.toHaveBeenCalled();
  });

  it('updatePreviewData round-trips a UI restyle to the owning message resultData', () => {
    // Regression: a pane "Customize → Look" restyle must persist onto the
    // source message (session-API round-trip), or the palette is lost on the
    // next session switch (deriveSessionPreviews reads message.resultData first).
    setCurrentSessionId('sess-A');
    const surface = { surfaceKind: 'presentation', root: 'd', components: [], theme: { accent: '#f00' } };
    useExecutionStore.getState().setPreviewContent({
      type: 'ui',
      data: '{}',
      sourceMessageId: 'msg-42',
    } as any);
    useExecutionStore.getState().updatePreviewData(JSON.stringify(surface));
    expect(sessionState().updateMessageInTargetSession).toHaveBeenCalledWith(
      'sess-A',
      'msg-42',
      { resultData: surface },
    );
  });

  it('updatePreviewData skips the message round-trip without a source message or on non-JSON data', () => {
    setCurrentSessionId('sess-A');
    useExecutionStore.getState().setPreviewContent({ type: 'ui', data: '{}' } as any);
    useExecutionStore.getState().updatePreviewData('{"ok":true}');
    expect(sessionState().updateMessageInTargetSession).not.toHaveBeenCalled();
    // Non-JSON data with a source message: swallow, never throw.
    useExecutionStore.getState().setPreviewContent({ type: 'ui', data: '{}', sourceMessageId: 'm1' } as any);
    expect(() => useExecutionStore.getState().updatePreviewData('not json')).not.toThrow();
    expect(sessionState().updateMessageInTargetSession).not.toHaveBeenCalled();
  });

  it('completeExecution appends the final preview to history when viewing owner', () => {
    setCurrentSessionId('sess-O');
    mockedParse.mockReturnValue(b);
    useExecutionStore.setState({ executionOwnerSessionId: 'sess-O' });
    // seed an earlier intermediate output
    useExecutionStore.getState().setPreviewContent(a as any);
    useExecutionStore.getState().completeExecution('final');
    const s = useExecutionStore.getState();
    expect(s.previewHistory).toEqual([a, b]);
    expect(s.previewIndex).toBe(1);
    expect(s.previewContent).toEqual(b);
  });

  it('completeExecution dedupes when the final preview matches the last intermediate', () => {
    setCurrentSessionId('sess-O');
    mockedParse.mockReturnValue(a);
    useExecutionStore.setState({ executionOwnerSessionId: 'sess-O' });
    useExecutionStore.getState().setPreviewContent(a as any);
    useExecutionStore.getState().completeExecution('same');
    expect(useExecutionStore.getState().previewHistory).toEqual([a]);
  });

  it('startExecution CLOSES the pane on a fresh run — opening is always a user click', () => {
    setCurrentSessionId('sess-X');
    useExecutionStore.setState({ previewPaneOpen: true, chatCollapsed: true });
    useExecutionStore.getState().startExecution('job-p', 'sess-X');
    expect(useExecutionStore.getState().previewPaneOpen).toBe(false);
    expect(useExecutionStore.getState().chatCollapsed).toBe(false);
  });

  it('startExecution with preservePreview keeps an open pane open (refine flow)', () => {
    setCurrentSessionId('sess-X');
    useExecutionStore.setState({ previewPaneOpen: true });
    useExecutionStore.getState().startExecution('job-q', 'sess-X', { preservePreview: true });
    expect(useExecutionStore.getState().previewPaneOpen).toBe(true);
  });

  it('startExecution clears preview history', () => {
    setCurrentSessionId('sess-X'); // viewing the run's owner → drives the live slot
    useExecutionStore.setState({ previewHistory: [a, b] as any, previewIndex: 1 });
    useExecutionStore.getState().startExecution('job-1', 'sess-X');
    const s = useExecutionStore.getState();
    expect(s.previewHistory).toEqual([]);
    expect(s.previewIndex).toBe(0);
  });

  it('startExecution with preservePreview keeps the existing preview + history (refine continuation)', () => {
    setCurrentSessionId('sess-X'); // viewing the run's owner → drives the live slot
    useExecutionStore.setState({
      previewContent: b as any,
      previewOwnerSessionId: 'sess-X',
      previewHistory: [a, b] as any,
      previewIndex: 1,
    });
    useExecutionStore.getState().startExecution('job-2', 'sess-X', { preservePreview: true });
    const s = useExecutionStore.getState();
    expect(s.isExecuting).toBe(true);
    expect(s.previewContent).toEqual(b);
    expect(s.previewOwnerSessionId).toBe('sess-X');
    expect(s.previewHistory).toEqual([a, b]);
    expect(s.previewIndex).toBe(1);
  });

  it('resetForSession clears preview history', () => {
    useExecutionStore.setState({ previewHistory: [a, b] as any, previewIndex: 1 });
    useExecutionStore.getState().resetForSession();
    const s = useExecutionStore.getState();
    expect(s.previewHistory).toEqual([]);
    expect(s.previewIndex).toBe(0);
  });

  it('saveSessionState/restoreSessionState round-trips preview history', () => {
    useExecutionStore.setState({
      previewContent: b as any,
      previewOwnerSessionId: 'sess-H',
      previewHistory: [a, b] as any,
      previewIndex: 1,
    });
    useExecutionStore.getState().saveSessionState('sess-H');
    useExecutionStore.setState({ previewContent: null, previewHistory: [], previewIndex: 0 });
    useExecutionStore.getState().restoreSessionState('sess-H');
    const s = useExecutionStore.getState();
    expect(s.previewHistory).toEqual([a, b]);
    expect(s.previewIndex).toBe(1);
    expect(s.previewContent).toEqual(b);
  });
});

describe('executionStore - reopenPreview', () => {
  it('returns early when no current session', async () => {
    setCurrentSessionId(null);
    useExecutionStore.getState().reopenPreview();
    expect(mockedGet).not.toHaveBeenCalled();
  });

  it('applies stored preview when still on same session', async () => {
    setCurrentSessionId('sess-A');
    mockedGet.mockResolvedValue({ type: 'ui', data: '{}', title: 'JT' });
    useExecutionStore.getState().reopenPreview();
    await vi.waitFor(() => {
      expect(useExecutionStore.getState().previewContent).toEqual({
        type: 'ui',
        data: '{}',
        title: 'JT',
      });
    });
    expect(useExecutionStore.getState().previewOwnerSessionId).toBe('sess-A');
  });

  it('seeds preview history when empty', async () => {
    setCurrentSessionId('sess-A');
    mockedGet.mockResolvedValue({ type: 'ui', data: '{}', title: 'JT' });
    useExecutionStore.getState().reopenPreview();
    await vi.waitFor(() => {
      expect(useExecutionStore.getState().previewHistory).toHaveLength(1);
    });
    const s = useExecutionStore.getState();
    expect(s.previewHistory[0]).toEqual({ type: 'ui', data: '{}', title: 'JT' });
    expect(s.previewIndex).toBe(0);
  });

  it('reopens the viewed entry from in-memory history without refetching', async () => {
    // Fast path: history is still in memory (clearPreview keeps it), so reopen
    // restores the entry the user was on (previewIndex) and never hits the
    // persisted-preview fallback.
    setCurrentSessionId('sess-A');
    const existing = [
      { type: 'ui', data: '# x', title: 'X' },
      { type: 'ui', data: '<p>y</p>', title: 'Y' },
    ];
    useExecutionStore.setState({ previewHistory: existing as any, previewIndex: 1 });
    mockedGet.mockResolvedValue({ type: 'ui', data: '{}', title: 'JT' });
    useExecutionStore.getState().reopenPreview();
    expect(useExecutionStore.getState().previewContent).toEqual(existing[1]);
    expect(useExecutionStore.getState().previewHistory).toEqual(existing);
    expect(mockedGet).not.toHaveBeenCalled(); // no refetch when history is present
  });

  it('ignores stored preview if session switched away', async () => {
    setCurrentSessionId('sess-A');
    mockedGet.mockImplementation(() => {
      // switch session before promise resolves
      setCurrentSessionId('sess-B');
      return Promise.resolve({ type: 'ui', data: 'x', title: 't' });
    });
    useExecutionStore.getState().reopenPreview();
    await Promise.resolve();
    await Promise.resolve();
    expect(useExecutionStore.getState().previewContent).toBeNull();
  });

  it('does nothing when no stored preview', async () => {
    setCurrentSessionId('sess-A');
    mockedGet.mockResolvedValue(undefined);
    useExecutionStore.getState().reopenPreview();
    await Promise.resolve();
    await Promise.resolve();
    expect(useExecutionStore.getState().previewContent).toBeNull();
  });
});

describe('executionStore - startExecution & updateExecutionStatus', () => {
  it('startExecution uses provided sessionId', () => {
    setCurrentSessionId('sess-X'); // viewing the owner → live slot reflects it
    useExecutionStore.getState().startExecution('job-1', 'sess-X');
    const s = useExecutionStore.getState();
    expect(s.executionOwnerSessionId).toBe('sess-X');
    expect(s.isExecuting).toBe(true);
    expect(s.isLoading).toBe(true);
    expect(s.activeExecution).toEqual({ jobId: 'job-1', status: 'running' });
    expect(s.previewContent).toBeNull();
    expect(s.previewOwnerSessionId).toBeNull();
  });

  it('startExecution falls back to current session', () => {
    setCurrentSessionId('sess-current');
    useExecutionStore.getState().startExecution('job-2');
    expect(useExecutionStore.getState().executionOwnerSessionId).toBe('sess-current');
  });

  it('startExecution with no session at all skips persisting an owner marker', () => {
    setCurrentSessionId(null);
    useExecutionStore.getState().startExecution('job-noowner');
    expect(useExecutionStore.getState().executionOwnerSessionId).toBeNull(); // owner falsy → no persist
  });

  it('startExecution for a BACKGROUNDED run snapshots it without seizing the live slot', () => {
    // Viewing sess-current, but a run starts for sess-bg (e.g. its generation
    // finished on the backend while you're elsewhere). It must NOT take over the
    // viewed session's live slot — it parks a running snapshot for sess-bg.
    setCurrentSessionId('sess-current');
    useExecutionStore.setState({
      executionOwnerSessionId: 'sess-current',
      isExecuting: false,
      activeExecution: null,
    });
    useExecutionStore.getState().startExecution('job-bg', 'sess-bg');
    const s = useExecutionStore.getState();
    // Live slot untouched (still the viewed session, not executing here).
    expect(s.executionOwnerSessionId).toBe('sess-current');
    expect(s.activeExecution).toBeNull();
    // But the job is owned by sess-bg, and switching to it restores the run.
    expect(s.jobOwnerOf('job-bg')).toBe('sess-bg');
    setCurrentSessionId('sess-bg');
    useExecutionStore.setState({ executionOwnerSessionId: null });
    useExecutionStore.getState().restoreSessionState('sess-bg');
    const r = useExecutionStore.getState();
    expect(r.isExecuting).toBe(true);
    expect(r.activeExecution).toEqual({ jobId: 'job-bg', status: 'running' });
    expect(r.executionOwnerSessionId).toBe('sess-bg');
  });

  it('updateExecutionStatus updates when active execution exists', () => {
    useExecutionStore.setState({ activeExecution: { jobId: 'j', status: 'running' } });
    useExecutionStore.getState().updateExecutionStatus('completed');
    expect(useExecutionStore.getState().activeExecution).toEqual({
      jobId: 'j',
      status: 'completed',
    });
  });

  it('updateExecutionStatus is a no-op when no active execution', () => {
    useExecutionStore.setState({ activeExecution: null });
    useExecutionStore.getState().updateExecutionStatus('failed');
    expect(useExecutionStore.getState().activeExecution).toBeNull();
  });
});

describe('executionStore - completeExecution', () => {
  it('viewing owner with preview: surfaces preview, persists, finalizes, deletes snapshot', () => {
    setCurrentSessionId('sess-O');
    mockedParse.mockReturnValue(preview);
    useExecutionStore.setState({
      executionOwnerSessionId: 'sess-O',
      activeExecution: { jobId: 'j', status: 'running' },
      isExecuting: true,
      isLoading: true,
    });
    useExecutionStore.getState().completeExecution('some result');
    const s = useExecutionStore.getState();
    expect(s.previewContent).toEqual(preview);
    expect(s.previewOwnerSessionId).toBe('sess-O');
    expect(mockedSave).toHaveBeenCalledWith('sess-O', preview);
    expect(s.activeExecution).toEqual({ jobId: 'j', status: 'completed' });
    expect(s.isExecuting).toBe(false);
    expect(s.executionContext).toBeNull();
    expect(s.isLoading).toBe(false);
    expect(s.executionOwnerSessionId).toBeNull();
  });

  it('composed surface + previewable text: renders the a2ui surface INLINE (empty body), not the opt-in pane', () => {
    // Regression: deep-mode runs return a structured deck whose `text` trips
    // parsePreviewContent. The composed A2UI surface must still ride inline on the
    // message; it must NOT be diverted to the (hidden) preview pane, which dropped
    // it entirely (no presentation showed for deep while research worked).
    const surface = { surfaceKind: 'presentation', root: 'r', components: [], dataModel: {} } as never;
    setCurrentSessionId('sess-O');
    mockedParse.mockReturnValue(preview); // the raw text looks previewable
    useExecutionStore.getState().startExecution('job-S', 'sess-O');
    useExecutionStore.getState().completeExecution('# Deck\n## Slide 1', 'job-S', surface);
    // The inline a2ui message is posted with an EMPTY body (the surface renders the
    // deck; the raw markdown is suppressed to avoid printing it twice).
    expect(sessionState().addMessageToTargetSession).toHaveBeenCalledWith(
      'sess-O',
      'assistant',
      '',
      { executionId: 'job-S', resultType: 'a2ui', resultData: surface },
    );
    // The previewable text did NOT hijack the opt-in pane.
    expect(useExecutionStore.getState().previewContent).toBeNull();
  });

  it('composed surface + NON-previewable markdown text: still drops the raw text (no double render)', () => {
    // Regression: a Genie answer is plain markdown (a 100-row restaurant table),
    // so parsePreviewContent returns null (it is A2UI-only). The composed surface
    // renders that data as an interactive Table — the raw markdown must NOT also
    // print in the bubble above it. Formerly `surface && preview` kept the text.
    const surface = { surfaceKind: 'dashboard', root: 'r', components: [], dataModel: {} } as never;
    setCurrentSessionId('sess-O');
    mockedParse.mockReturnValue(null); // plain markdown is not A2UI-previewable
    useExecutionStore.getState().startExecution('job-T', 'sess-O');
    useExecutionStore
      .getState()
      .completeExecution('| # | Restaurant |\n|---|---|\n| 1 | Kronenhalle |', 'job-T', surface);
    expect(sessionState().addMessageToTargetSession).toHaveBeenCalledWith(
      'sess-O',
      'assistant',
      '',
      { executionId: 'job-T', resultType: 'a2ui', resultData: surface },
    );
  });

  it('streamed text SURVIVES a composed surface: the reader was already reading it', () => {
    // Regression: composition takes tens of seconds, and chat streams the answer
    // meanwhile precisely so there is something to read. Completion assumed "a
    // surface arrived, so nothing rendered" and blanked the bubble — the answer
    // the user had been reading vanished, and for a dashboard carrying only
    // headline numbers most of the content went with it.
    const surface = { surfaceKind: 'dashboard', root: 'r', components: [], dataModel: {} } as never;
    setCurrentSessionId('sess-O');
    mockedParse.mockReturnValue(null);
    useExecutionStore.getState().startExecution('job-ST', 'sess-O');
    // The reader watched this arrive.
    useExecutionStore.getState().appendStreamChunk('job-ST', '# Comparative Analysis');

    useExecutionStore
      .getState()
      .completeExecution('# Comparative Analysis\n\nLangChain scores 4.25…', 'job-ST', surface);

    // Finalized in place: the full answer AND the surface on one message.
    expect(sessionState().updateMessageInTargetSession).toHaveBeenCalledWith(
      'sess-O',
      expect.stringContaining('stream-job-ST'),
      expect.objectContaining({
        content: '# Comparative Analysis\n\nLangChain scores 4.25…',
        isStreaming: false,
        resultType: 'a2ui',
        resultData: surface,
      }),
    );
  });

  it('nothing streamed: the surface is still the only rendering', () => {
    // The other half of the rule — never ADD something the reader never saw.
    // A fast compose shows no text at any point, so printing the prose beside
    // the surface would be the duplication the gate exists to stop.
    const surface = { surfaceKind: 'dashboard', root: 'r', components: [], dataModel: {} } as never;
    setCurrentSessionId('sess-O');
    mockedParse.mockReturnValue(null);
    useExecutionStore.getState().startExecution('job-NS', 'sess-O');

    useExecutionStore.getState().completeExecution('| # | Restaurant |', 'job-NS', surface);

    expect(sessionState().addMessageToTargetSession).toHaveBeenCalledWith(
      'sess-O',
      'assistant',
      '',
      { executionId: 'job-NS', resultType: 'a2ui', resultData: surface },
    );
  });

  it('a truncated task line is upgraded to the full answer EVEN when a surface exists', () => {
    // The reported bug. Every task output is posted capped at 300 chars + "…"
    // (summarizeTaskOutput), on the understanding that completion replaces it
    // with the full text. The fold was gated on `body`, which is '' whenever a
    // surface exists — so a run that composed one never attempted it and the
    // only thing left on screen was 300 characters ending mid-word ("4 = St…").
    const answer =
      '# Comparative Analysis: Core Features of Leading Agentic AI Frameworks\n\n' +
      'x'.repeat(2000);
    const truncated = `${answer.slice(0, 300).trim()}…`;
    const surface = { surfaceKind: 'dashboard', root: 'r', components: [], dataModel: {} } as never;

    setCurrentSessionId('sess-O');
    mockedParse.mockReturnValue(null);
    sessionState().messages = [{ id: 'task-msg-1', role: 'assistant', content: truncated }];
    useExecutionStore.getState().startExecution('job-TR', 'sess-O');

    useExecutionStore.getState().completeExecution(answer, 'job-TR', surface);

    // The capped line becomes the whole answer…
    expect(sessionState().updateMessageInTargetSession).toHaveBeenCalledWith(
      'sess-O',
      'task-msg-1',
      expect.objectContaining({ content: answer, isStreaming: false }),
    );
    // …and the surface attaches to that same message rather than being lost.
    expect(sessionState().updateMessageInTargetSession).toHaveBeenCalledWith(
      'sess-O',
      'task-msg-1',
      { executionId: 'job-TR', resultType: 'a2ui', resultData: surface },
    );
    // No second copy of the answer posted underneath it.
    expect(sessionState().addMessageToTargetSession).not.toHaveBeenCalled();
  });

  it('folds into the capped line even when trace activity was posted after it', () => {
    // The reason the previous fix still showed the stub. Every trace — tool
    // pill, memory read, memory write, LLM call — is an assistant message too.
    // A run that works for half a minute puts dozens of them after its own task
    // output, so a fixed "last 4 entries" window stopped reaching the capped
    // line and the full answer was posted as a second message beneath it.
    const answer = '# Catalog of Agentic AI Frameworks\n\n' + 'y'.repeat(2000);
    const capped = `${answer.slice(0, 300).trim()}…`;

    setCurrentSessionId('sess-O');
    mockedParse.mockReturnValue(null);
    sessionState().messages = [
      { id: 'task-msg-1', role: 'assistant', content: capped },
      // Activity that arrived while the task kept working.
      { id: 't1', role: 'assistant', content: '', resultType: 'trace' },
      { id: 't2', role: 'assistant', content: '', resultType: 'trace' },
      { id: 't3', role: 'assistant', content: '', resultType: 'trace' },
      { id: 't4', role: 'assistant', content: '', resultType: 'trace' },
      { id: 't5', role: 'assistant', content: '', resultType: 'trace' },
      { id: 't6', role: 'assistant', content: '', resultType: 'trace' },
    ];
    useExecutionStore.getState().startExecution('job-TW', 'sess-O');

    useExecutionStore.getState().completeExecution(answer, 'job-TW');

    expect(sessionState().updateMessageInTargetSession).toHaveBeenCalledWith(
      'sess-O',
      'task-msg-1',
      expect.objectContaining({ content: answer, isStreaming: false }),
    );
    // Not posted a second time underneath the stub.
    expect(sessionState().addMessageToTargetSession).not.toHaveBeenCalled();
  });

  it('replaces the registered task-output message, without scanning for it', () => {
    // The scan kept missing for reasons unrelated to the answer: trace pills
    // crowding its window, and a backgrounded session's messages never entering
    // the in-memory array at all (addMessageToTargetSession only appends when
    // that session is the one on screen). The producer knows the id, so
    // completion updates THAT message.
    const answer = '# Database of Successful Agentic AI Projects\n\n' + 'q'.repeat(3000);

    setCurrentSessionId('sess-O');
    mockedParse.mockReturnValue(null);
    // Deliberately EMPTY: a scan would find nothing here.
    sessionState().messages = [];
    useExecutionStore.getState().startExecution('job-REG', 'sess-O');
    rememberTaskOutputMessage('job-REG', 'task-msg-42');

    useExecutionStore.getState().completeExecution(answer, 'job-REG');

    expect(sessionState().updateMessageInTargetSession).toHaveBeenCalledWith(
      'sess-O',
      'task-msg-42',
      expect.objectContaining({ content: answer, isStreaming: false }),
    );
    expect(sessionState().addMessageToTargetSession).not.toHaveBeenCalled();
  });

  it('carries the surface onto the registered message too', () => {
    const answer = '# Report\n\n' + 'w'.repeat(3000);
    const surface = { surfaceKind: 'dashboard', root: 'r', components: [], dataModel: {} } as never;

    setCurrentSessionId('sess-O');
    mockedParse.mockReturnValue(null);
    sessionState().messages = [];
    useExecutionStore.getState().startExecution('job-REG2', 'sess-O');
    rememberTaskOutputMessage('job-REG2', 'task-msg-43');

    useExecutionStore.getState().completeExecution(answer, 'job-REG2', surface);

    expect(sessionState().updateMessageInTargetSession).toHaveBeenCalledWith(
      'sess-O',
      'task-msg-43',
      { executionId: 'job-REG2', resultType: 'a2ui', resultData: surface },
    );
  });

  it('a registration is consumed once, so a later run cannot reuse it', () => {
    const answer = 'x'.repeat(3000);
    setCurrentSessionId('sess-O');
    mockedParse.mockReturnValue(null);
    sessionState().messages = [];
    rememberTaskOutputMessage('job-REG3', 'task-msg-44');

    useExecutionStore.getState().startExecution('job-REG3', 'sess-O');
    useExecutionStore.getState().completeExecution(answer, 'job-REG3');
    vi.clearAllMocks();

    // Same job id finalizing again must not rewrite a message it no longer owns.
    useExecutionStore.getState().startExecution('job-REG3', 'sess-O');
    useExecutionStore.getState().completeExecution('a different answer', 'job-REG3');

    expect(sessionState().updateMessageInTargetSession).not.toHaveBeenCalledWith(
      'sess-O',
      'task-msg-44',
      expect.anything(),
    );
  });

  it('stops the typing indicator when the answer is folded into the live bubble', () => {
    // The regression this pins: superseding skips the branch that cleared
    // `isStreaming`, and the message the fold lands on is usually the live
    // bubble itself — its streamed text IS a prefix of the final answer. The
    // run finished and the three dots kept bouncing under it.
    const answer = 'I do not have live web access in this conversation, so I cannot pull todays news.';

    setCurrentSessionId('sess-O');
    mockedParse.mockReturnValue(null);
    useExecutionStore.getState().startExecution('job-DOT', 'sess-O');
    // Tokens stream in; the bubble's content is a prefix of the final answer.
    useExecutionStore.getState().appendStreamChunk('job-DOT', answer.slice(0, 60));
    sessionState().messages = [
      { id: 'stream-job-DOT-1', role: 'assistant', content: answer.slice(0, 60) },
    ];

    useExecutionStore.getState().completeExecution(answer, 'job-DOT');

    const calls = sessionState().updateMessageInTargetSession.mock.calls;
    const cleared = calls.some(
      (c: unknown[]) => (c[2] as { isStreaming?: boolean })?.isStreaming === false,
    );
    expect(cleared).toBe(true);
  });

  it('clears a bubble left open by an earlier task when folding elsewhere', () => {
    const answer = '# Report\n\n' + 'k'.repeat(3000);

    setCurrentSessionId('sess-O');
    mockedParse.mockReturnValue(null);
    useExecutionStore.getState().startExecution('job-DOT2', 'sess-O');
    useExecutionStore.getState().appendStreamChunk('job-DOT2', 'an earlier task wrote this');
    sessionState().messages = [];
    rememberTaskOutputMessage('job-DOT2', 'task-msg-99');

    useExecutionStore.getState().completeExecution(answer, 'job-DOT2');

    const calls = sessionState().updateMessageInTargetSession.mock.calls;
    // The answer landed on the registered message…
    expect(
      calls.some((c: unknown[]) => c[1] === 'task-msg-99'),
    ).toBe(true);
    // …and the orphaned bubble stopped claiming to be typing.
    expect(
      calls.some(
        (c: unknown[]) =>
          String(c[1]).startsWith('stream-job-DOT2') &&
          (c[2] as { isStreaming?: boolean })?.isStreaming === false,
      ),
    ).toBe(true);
  });

  it('viewing owner with preview but no active execution -> activeExecution stays null', () => {
    setCurrentSessionId('sess-O');
    mockedParse.mockReturnValue(preview);
    useExecutionStore.setState({
      executionOwnerSessionId: 'sess-O',
      activeExecution: null,
    });
    useExecutionStore.getState().completeExecution('result');
    expect(useExecutionStore.getState().activeExecution).toBeNull();
  });

  it('not viewing owner with preview: does not surface but persists and snapshots', () => {
    setCurrentSessionId('sess-VIEW');
    mockedParse.mockReturnValue(preview);
    useExecutionStore.setState({
      executionOwnerSessionId: 'sess-O',
    });
    useExecutionStore.getState().completeExecution('result');
    const s = useExecutionStore.getState();
    // preview NOT surfaced to current view
    expect(s.previewContent).toBeNull();
    expect(mockedSave).toHaveBeenCalledWith('sess-O', preview);
    expect(s.executionOwnerSessionId).toBeNull();
    // snapshot persisted with preview -> restore picks it up
    useExecutionStore.getState().restoreSessionState('sess-O');
    expect(useExecutionStore.getState().previewContent).toEqual(preview);
  });

  it('viewing owner, no preview, with ownerSession: routes text to target session', () => {
    setCurrentSessionId('sess-O');
    mockedParse.mockReturnValue(null);
    useExecutionStore.setState({ executionOwnerSessionId: 'sess-O' });
    useExecutionStore.getState().completeExecution('plain text');
    expect(sessionState().addMessageToTargetSession).toHaveBeenCalledWith(
      'sess-O',
      'assistant',
      'plain text',
      undefined, // no jobId in this legacy path -> no executionId extra
    );
  });

  it('no preview, no ownerSession: uses addMessage and does not save snapshot', () => {
    setCurrentSessionId(null);
    mockedParse.mockReturnValue(null);
    useExecutionStore.setState({ executionOwnerSessionId: null });
    useExecutionStore.getState().completeExecution('plain text');
    expect(sessionState().addMessage).toHaveBeenCalledWith('assistant', 'plain text', undefined);
    // isViewingOwner true (null === null), no snapshot delete attempted
    expect(useExecutionStore.getState().executionOwnerSessionId).toBeNull();
  });

  it('empty resultText posts NOTHING — the notice was not an answer', () => {
    // "Execution completed." was a status line dressed as a reply. The
    // run-activity row already says the run finished, so it added nothing and
    // read as the answer — particularly on a flow, which lands here routinely
    // because its early announcement carries unreadable raw output.
    setCurrentSessionId('sess-O');
    useExecutionStore.setState({ executionOwnerSessionId: 'sess-O' });
    useExecutionStore.getState().completeExecution('');
    expect(sessionState().addMessageToTargetSession).not.toHaveBeenCalled();
    // parsePreviewContent not invoked because resultText falsy
    expect(mockedParse).not.toHaveBeenCalled();
  });

  it('empty resultText without an owner session posts nothing either', () => {
    setCurrentSessionId(null);
    useExecutionStore.setState({ executionOwnerSessionId: null });
    useExecutionStore.getState().completeExecution('');
    expect(sessionState().addMessage).not.toHaveBeenCalled();
  });

  it('preview parsed but ownerSession falsy: surfaces preview without persisting (line 189 false branch)', () => {
    setCurrentSessionId(null);
    mockedParse.mockReturnValue(preview);
    useExecutionStore.setState({ executionOwnerSessionId: null });
    useExecutionStore.getState().completeExecution('result');
    // isViewingOwner true (null===null) so preview surfaced...
    expect(useExecutionStore.getState().previewContent).toEqual(preview);
    // ...but ownerSession falsy so no persistence
    expect(mockedSave).not.toHaveBeenCalled();
  });

  it('not viewing owner and ownerSession falsy: no snapshot, no finalize (line 227 else-if false)', () => {
    setCurrentSessionId('sess-VIEW');
    mockedParse.mockReturnValue(null);
    useExecutionStore.setState({
      executionOwnerSessionId: null,
      isExecuting: true,
    });
    useExecutionStore.getState().completeExecution('plain');
    // ownerSession null -> addMessage path
    expect(sessionState().addMessage).toHaveBeenCalledWith('assistant', 'plain', undefined);
    // neither isViewingOwner (VIEW !== null) nor else-if (ownerSession null) -> state untouched
    expect(useExecutionStore.getState().isExecuting).toBe(true);
    expect(useExecutionStore.getState().hasActiveExecution('sess-VIEW')).toBe(false);
  });

  it('not viewing owner, no preview text: snapshots with null preview', () => {
    setCurrentSessionId('sess-VIEW');
    mockedParse.mockReturnValue(null);
    useExecutionStore.setState({ executionOwnerSessionId: 'sess-O' });
    useExecutionStore.getState().completeExecution('plain');
    expect(sessionState().addMessageToTargetSession).toHaveBeenCalledWith(
      'sess-O',
      'assistant',
      'plain',
      undefined,
    );
    expect(useExecutionStore.getState().executionOwnerSessionId).toBeNull();
    // snapshot has null preview, restore yields null
    useExecutionStore.getState().restoreSessionState('sess-O');
    expect(useExecutionStore.getState().previewContent).toBeNull();
  });
});

describe('executionStore - failExecution', () => {
  it('with ownerSession routes failure message to target session', () => {
    setCurrentSessionId('sess-O');
    useExecutionStore.setState({
      executionOwnerSessionId: 'sess-O',
      activeExecution: { jobId: 'j', status: 'running' },
      isExecuting: true,
      isLoading: true,
    });
    useExecutionStore.getState().failExecution('boom');
    expect(sessionState().addMessageToTargetSession).toHaveBeenCalledWith(
      'sess-O',
      'assistant',
      'Execution failed: boom',
    );
    const s = useExecutionStore.getState();
    expect(s.activeExecution).toEqual({ jobId: 'j', status: 'failed' });
    expect(s.isExecuting).toBe(false);
    expect(s.executionOwnerSessionId).toBeNull();
  });

  it('viewing owner but no active execution -> activeExecution null', () => {
    setCurrentSessionId('sess-O');
    useExecutionStore.setState({
      executionOwnerSessionId: 'sess-O',
      activeExecution: null,
    });
    useExecutionStore.getState().failExecution('err');
    expect(useExecutionStore.getState().activeExecution).toBeNull();
  });

  it('without ownerSession uses addMessage', () => {
    setCurrentSessionId(null);
    useExecutionStore.setState({ executionOwnerSessionId: null });
    useExecutionStore.getState().failExecution('oops');
    expect(sessionState().addMessage).toHaveBeenCalledWith(
      'assistant',
      'Execution failed: oops',
    );
  });

  it('not viewing owner snapshots and clears owner', () => {
    setCurrentSessionId('sess-VIEW');
    useExecutionStore.setState({ executionOwnerSessionId: 'sess-O' });
    useExecutionStore.getState().failExecution('bad');
    expect(useExecutionStore.getState().executionOwnerSessionId).toBeNull();
    // snapshot exists for sess-O with running flags false
    expect(useExecutionStore.getState().hasActiveExecution('sess-O')).toBe(false);
  });

  it('not viewing owner and ownerSession falsy: no snapshot/finalize (line 269 else-if false)', () => {
    setCurrentSessionId('sess-VIEW');
    useExecutionStore.setState({ executionOwnerSessionId: null, isExecuting: true });
    useExecutionStore.getState().failExecution('bad');
    expect(sessionState().addMessage).toHaveBeenCalledWith(
      'assistant',
      'Execution failed: bad',
    );
    expect(useExecutionStore.getState().isExecuting).toBe(true);
  });
});

describe('executionStore - abandonExecution (gone job: deleted / different workspace)', () => {
  it('clears the live slot, the reconnect marker, and the owner mapping — with NO chat message', () => {
    setCurrentSessionId('sess-AB');
    useExecutionStore.getState().startExecution('job-AB', 'sess-AB');
    // sanity: the run is live + tracked + persisted
    expect(useExecutionStore.getState().isExecuting).toBe(true);
    expect(useExecutionStore.getState().runningJobBySession['sess-AB']).toBe('job-AB');
    expect(useExecutionStore.getState().jobOwnerOf('job-AB')).toBe('sess-AB');

    mockedClearMarker.mockClear();
    useExecutionStore.getState().abandonExecution('job-AB');

    const s = useExecutionStore.getState();
    expect(s.activeExecution).toBeNull();
    expect(s.isExecuting).toBe(false);
    expect(s.isLoading).toBe(false);
    expect(s.executionOwnerSessionId).toBeNull();
    expect(s.runningJobBySession['sess-AB']).toBeUndefined();
    expect(s.jobOwnerOf('job-AB')).toBeNull();
    // Durable IndexedDB reconnect marker dropped so a refresh can't resurrect it.
    expect(mockedClearMarker).toHaveBeenCalledWith('sess-AB');
    // A gone run is NOT a failure — no message is posted to the chat.
    expect(sessionState().addMessageToTargetSession).not.toHaveBeenCalled();
    expect(sessionState().addMessage).not.toHaveBeenCalled();
  });

  it('is idempotent — a second call (e.g. a late poller jobNotFound) is a no-op', () => {
    setCurrentSessionId('sess-AB2');
    useExecutionStore.getState().startExecution('job-AB2', 'sess-AB2');
    useExecutionStore.getState().abandonExecution('job-AB2');
    mockedClearMarker.mockClear();
    useExecutionStore.getState().abandonExecution('job-AB2');
    expect(mockedClearMarker).not.toHaveBeenCalled();
  });

  it('ignores an untracked job id', () => {
    mockedClearMarker.mockClear();
    useExecutionStore.getState().abandonExecution('never-started');
    expect(mockedClearMarker).not.toHaveBeenCalled();
  });

  it('scrubs a BACKGROUNDED session snapshot without touching the live slot', () => {
    // sess-bg's run is backgrounded (we are viewing sess-view); startExecution
    // parks a snapshot with running flags for it.
    setCurrentSessionId('sess-view');
    useExecutionStore.setState({ executionOwnerSessionId: 'sess-view' });
    useExecutionStore.getState().startExecution('job-bg', 'sess-bg');
    expect(useExecutionStore.getState().hasActiveExecution('sess-bg')).toBe(true);

    mockedClearMarker.mockClear();
    useExecutionStore.getState().abandonExecution('job-bg');

    const s = useExecutionStore.getState();
    // Backgrounded session no longer reports a running execution...
    expect(s.hasActiveExecution('sess-bg')).toBe(false);
    expect(s.runningJobBySession['sess-bg']).toBeUndefined();
    expect(s.jobOwnerOf('job-bg')).toBeNull();
    expect(mockedClearMarker).toHaveBeenCalledWith('sess-bg');
    // ...and the live slot (sess-view) is untouched.
    expect(s.executionOwnerSessionId).toBe('sess-view');
  });
});

describe('executionStore - generation lifecycle', () => {
  it('startGeneration with provided sessionId', () => {
    useExecutionStore.getState().startGeneration('sess-G');
    const s = useExecutionStore.getState();
    expect(s.executionOwnerSessionId).toBe('sess-G');
    expect(s.isGenerating).toBe(true);
    expect(s.isLoading).toBe(true);
  });

  it('startGeneration falls back to current session', () => {
    setCurrentSessionId('sess-cur');
    useExecutionStore.getState().startGeneration();
    expect(useExecutionStore.getState().executionOwnerSessionId).toBe('sess-cur');
  });

  it('completeGeneration viewing owner finalizes and deletes snapshot', () => {
    setCurrentSessionId('sess-O');
    useExecutionStore.setState({
      executionOwnerSessionId: 'sess-O',
      isGenerating: true,
      isLoading: true,
    });
    useExecutionStore.getState().completeGeneration();
    const s = useExecutionStore.getState();
    expect(s.isGenerating).toBe(false);
    expect(s.isLoading).toBe(false);
    expect(s.executionOwnerSessionId).toBeNull();
  });

  it('completeGeneration viewing owner with null ownerSession (no delete)', () => {
    setCurrentSessionId(null);
    useExecutionStore.setState({ executionOwnerSessionId: null, isGenerating: true });
    useExecutionStore.getState().completeGeneration();
    expect(useExecutionStore.getState().isGenerating).toBe(false);
  });

  it('completeGeneration not viewing owner snapshots and clears owner', () => {
    setCurrentSessionId('sess-VIEW');
    useExecutionStore.setState({ executionOwnerSessionId: 'sess-O' });
    useExecutionStore.getState().completeGeneration();
    expect(useExecutionStore.getState().executionOwnerSessionId).toBeNull();
    expect(useExecutionStore.getState().hasActiveExecution('sess-O')).toBe(false);
  });

  it('completeGeneration not viewing owner & ownerSession falsy: no-op (line 305 else-if false)', () => {
    setCurrentSessionId('sess-VIEW');
    useExecutionStore.setState({ executionOwnerSessionId: null, isGenerating: true });
    useExecutionStore.getState().completeGeneration();
    // not viewing owner (VIEW !== null) and ownerSession null -> untouched
    expect(useExecutionStore.getState().isGenerating).toBe(true);
  });

  it('failGeneration with ownerSession routes message and finalizes when viewing', () => {
    setCurrentSessionId('sess-O');
    useExecutionStore.setState({
      executionOwnerSessionId: 'sess-O',
      isGenerating: true,
      isLoading: true,
    });
    useExecutionStore.getState().failGeneration('gen err');
    expect(sessionState().addMessageToTargetSession).toHaveBeenCalledWith(
      'sess-O',
      'assistant',
      'Generation failed: gen err',
    );
    const s = useExecutionStore.getState();
    expect(s.isGenerating).toBe(false);
    expect(s.executionOwnerSessionId).toBeNull();
  });

  it('failGeneration without ownerSession uses addMessage', () => {
    setCurrentSessionId(null);
    useExecutionStore.setState({ executionOwnerSessionId: null });
    useExecutionStore.getState().failGeneration('x');
    expect(sessionState().addMessage).toHaveBeenCalledWith(
      'assistant',
      'Generation failed: x',
    );
  });

  it('failGeneration not viewing owner snapshots and clears owner', () => {
    setCurrentSessionId('sess-VIEW');
    useExecutionStore.setState({ executionOwnerSessionId: 'sess-O' });
    useExecutionStore.getState().failGeneration('err');
    expect(useExecutionStore.getState().executionOwnerSessionId).toBeNull();
    expect(useExecutionStore.getState().hasActiveExecution('sess-O')).toBe(false);
  });

  it('failGeneration not viewing owner & ownerSession falsy: no-op (line 342 else-if false)', () => {
    setCurrentSessionId('sess-VIEW');
    useExecutionStore.setState({ executionOwnerSessionId: null, isGenerating: true });
    useExecutionStore.getState().failGeneration('err');
    expect(sessionState().addMessage).toHaveBeenCalledWith(
      'assistant',
      'Generation failed: err',
    );
    expect(useExecutionStore.getState().isGenerating).toBe(true);
  });
});

describe('executionStore - saveSessionState / restoreSessionState / hasActiveExecution', () => {
  it('saveSessionState stores snapshot when executing', () => {
    // The session being saved must OWN the run for it to be snapshotted (a run
    // owned by another session must not leak into this session's snapshot).
    useExecutionStore.setState({
      isExecuting: true,
      executionOwnerSessionId: 'sess-S',
      activeExecution: { jobId: 'j', status: 'running' },
    });
    useExecutionStore.getState().saveSessionState('sess-S');
    expect(useExecutionStore.getState().hasActiveExecution('sess-S')).toBe(true);
  });

  it('saveSessionState stores snapshot when generating', () => {
    useExecutionStore.setState({ isGenerating: true, executionOwnerSessionId: 'sess-S' });
    useExecutionStore.getState().saveSessionState('sess-S');
    expect(useExecutionStore.getState().hasActiveExecution('sess-S')).toBe(true);
  });

  it('saveSessionState stores snapshot when previewContent present', () => {
    useExecutionStore.setState({ previewContent: preview as any, previewOwnerSessionId: 'sess-S' });
    useExecutionStore.getState().saveSessionState('sess-S');
    // not running but snapshot exists with preview -> restore brings it back
    useExecutionStore.getState().restoreSessionState('sess-S');
    expect(useExecutionStore.getState().previewContent).toEqual(preview);
    expect(useExecutionStore.getState().previewOwnerSessionId).toBe('sess-S');
  });

  it('restoreSessionState is a no-op while a run is live (owner set)', () => {
    // a snapshot with a preview exists for the target session...
    useExecutionStore.setState({ previewContent: preview as any });
    useExecutionStore.getState().saveSessionState('sess-S');
    // ...but a run owned by ANOTHER session is live, so restore must not clobber it
    useExecutionStore.setState({
      executionOwnerSessionId: 'sess-OTHER',
      previewContent: null,
      isExecuting: true,
    });
    useExecutionStore.getState().restoreSessionState('sess-S');
    // early return: the snapshot was NOT applied, live state is preserved
    expect(useExecutionStore.getState().previewContent).toBeNull();
    expect(useExecutionStore.getState().isExecuting).toBe(true);
    expect(useExecutionStore.getState().executionOwnerSessionId).toBe('sess-OTHER');
  });

  it('saveSessionState deletes snapshot when nothing active', () => {
    // first create a snapshot (session owns the run)
    useExecutionStore.setState({ isExecuting: true, executionOwnerSessionId: 'sess-S' });
    useExecutionStore.getState().saveSessionState('sess-S');
    expect(useExecutionStore.getState().hasActiveExecution('sess-S')).toBe(true);
    // now save with nothing active (run finished, owner cleared) -> deletes
    useExecutionStore.setState({
      isExecuting: false,
      isGenerating: false,
      previewContent: null,
      executionOwnerSessionId: null,
    });
    useExecutionStore.getState().saveSessionState('sess-S');
    expect(useExecutionStore.getState().hasActiveExecution('sess-S')).toBe(false);
  });

  it('saveSessionState does NOT snapshot a run/preview owned by another session', () => {
    // A run + preview owned by sess-OTHER is live in the single global slot.
    // Saving sess-S must NOT capture them, or switching back to sess-S would
    // surface a stale Stop / another chat's preview in the wrong UI.
    useExecutionStore.setState({
      isExecuting: true,
      executionOwnerSessionId: 'sess-OTHER',
      activeExecution: { jobId: 'j', status: 'running' },
      previewContent: preview as any,
      previewOwnerSessionId: 'sess-OTHER',
    });
    useExecutionStore.getState().saveSessionState('sess-S');
    expect(useExecutionStore.getState().hasActiveExecution('sess-S')).toBe(false);
    // Owner clears; restoring sess-S must come up clean (no leaked run/preview).
    useExecutionStore.setState({ executionOwnerSessionId: null, isExecuting: false, previewContent: null });
    useExecutionStore.getState().restoreSessionState('sess-S');
    expect(useExecutionStore.getState().isExecuting).toBe(false);
    expect(useExecutionStore.getState().previewContent).toBeNull();
  });

  it('restoreSessionState restores running snapshot with no preview (owner null)', () => {
    useExecutionStore.setState({
      isExecuting: true,
      executionOwnerSessionId: 'sess-S',
      activeExecution: { jobId: 'j', status: 'running' },
      previewContent: null,
    });
    useExecutionStore.getState().saveSessionState('sess-S');
    // mutate live state (run no longer owned/live) then restore the snapshot
    useExecutionStore.setState({ isExecuting: false, activeExecution: null, executionOwnerSessionId: null });
    useExecutionStore.getState().restoreSessionState('sess-S');
    const s = useExecutionStore.getState();
    expect(s.isExecuting).toBe(true);
    expect(s.activeExecution).toEqual({ jobId: 'j', status: 'running' });
    expect(s.previewOwnerSessionId).toBeNull();
  });

  it('restoreSessionState defaults history when snapshot predates the field', () => {
    // failExecution snapshots don't carry previewHistory/previewIndex.
    setCurrentSessionId('sess-VIEW');
    useExecutionStore.setState({ executionOwnerSessionId: 'sess-O' });
    useExecutionStore.getState().failExecution('bad');
    useExecutionStore.getState().restoreSessionState('sess-O');
    const s = useExecutionStore.getState();
    expect(s.previewHistory).toEqual([]);
    expect(s.previewIndex).toBe(0);
  });

  it('restoreSessionState with no snapshot resets and loads persisted preview', async () => {
    setCurrentSessionId('sess-NONE');
    mockedGet.mockResolvedValue({ type: 'ui', data: '# hi', title: 'MD' });
    useExecutionStore.getState().restoreSessionState('sess-NONE');
    // synchronous reset first
    expect(useExecutionStore.getState().previewContent).toBeNull();
    await vi.waitFor(() => {
      expect(useExecutionStore.getState().previewContent).toEqual({
        type: 'ui',
        data: '# hi',
        title: 'MD',
      });
    });
    expect(useExecutionStore.getState().previewOwnerSessionId).toBe('sess-NONE');
  });

  it('restoreSessionState with no snapshot derives the deliverable from run results', async () => {
    // The single-source path: when a session has no in-memory snapshot, its
    // preview is derived from each run's stored execution.result — so a run that
    // finished while the session was backgrounded still shows on switch-back.
    setCurrentSessionId('sess-DERIVE');
    const derived = { type: 'ui' as const, data: '{"messages":[]}' };
    mockedDerive.mockResolvedValueOnce({ history: [derived], current: derived });

    useExecutionStore.getState().restoreSessionState('sess-DERIVE');

    await vi.waitFor(() => {
      expect(useExecutionStore.getState().previewContent).toEqual(derived);
    });
    expect(useExecutionStore.getState().previewHistory).toEqual([derived]);
    expect(useExecutionStore.getState().previewOwnerSessionId).toBe('sess-DERIVE');
    // Derived deliverable wins — the legacy persisted-preview fallback is skipped.
    expect(mockedGet).not.toHaveBeenCalled();
  });

  it('restoreSessionState with no snapshot ignores persisted preview if session switched', async () => {
    setCurrentSessionId('sess-NONE');
    mockedGet.mockImplementation(() => {
      setCurrentSessionId('sess-OTHER');
      return Promise.resolve({ type: 'ui', data: 'x', title: 't' });
    });
    useExecutionStore.getState().restoreSessionState('sess-NONE');
    await Promise.resolve();
    await Promise.resolve();
    expect(useExecutionStore.getState().previewContent).toBeNull();
  });

  it('restoreSessionState with no snapshot and no persisted preview stays reset', async () => {
    setCurrentSessionId('sess-NONE');
    mockedGet.mockResolvedValue(undefined);
    useExecutionStore.getState().restoreSessionState('sess-NONE');
    await Promise.resolve();
    await Promise.resolve();
    expect(useExecutionStore.getState().previewContent).toBeNull();
  });

  it('hasActiveExecution true when executionOwnerSessionId matches', () => {
    useExecutionStore.setState({ executionOwnerSessionId: 'sess-OWN' });
    expect(useExecutionStore.getState().hasActiveExecution('sess-OWN')).toBe(true);
  });

  it('hasActiveExecution false when no snapshot and not owner', () => {
    useExecutionStore.setState({ executionOwnerSessionId: null });
    expect(useExecutionStore.getState().hasActiveExecution('sess-unknown')).toBe(false);
  });
});

describe('executionStore - resetForSession', () => {
  it('resets all transient state', () => {
    useExecutionStore.setState({
      activeExecution: { jobId: 'j', status: 'running' },
      isExecuting: true,
      isGenerating: true,
      isLoading: true,
      executionContext: { foo: 1 } as any,
      previewContent: preview as any,
      previewOwnerSessionId: 'sess-A',
    });
    useExecutionStore.getState().resetForSession();
    const s = useExecutionStore.getState();
    expect(s.activeExecution).toBeNull();
    expect(s.isExecuting).toBe(false);
    expect(s.isGenerating).toBe(false);
    expect(s.isLoading).toBe(false);
    expect(s.executionContext).toBeNull();
    expect(s.previewContent).toBeNull();
    expect(s.previewOwnerSessionId).toBeNull();
  });
});

// Ensure initial state export exists (touches module-level state object)
describe('executionStore - initial state', () => {
  it('exposes initial defaults', () => {
    expect(initialState.activeExecution).toBeNull();
    expect(initialState.chatCollapsed).toBe(false);
    // The side preview pane is opt-in — closed until the user opens it.
    expect(initialState.previewPaneOpen).toBe(false);
    // Agent Bricks endpoints start empty until the user picks one in the "+" menu.
    expect(initialState.selectedAgentBricksEndpoints).toEqual([]);
    // Run activity defaults to the chat (so the pane stays closed by default).
    expect(initialState.activityPlacement).toBe('chat');
  });

  it('setActivityPlacement switches where the run activity is shown', () => {
    useExecutionStore.getState().setActivityPlacement('chat');
    expect(useExecutionStore.getState().activityPlacement).toBe('chat');
    useExecutionStore.getState().setActivityPlacement('preview');
    expect(useExecutionStore.getState().activityPlacement).toBe('preview');
  });
});

// ===========================================================================
// Parallel-session routing — jobId ownership, preview parking, switch-back.
// Two sessions can have runs in flight at once; the single live slot must
// route each job's completion/preview to ITS session, not whatever is on
// screen now (the bug: a backgrounded run's tracker + preview were lost).
// ===========================================================================
describe('executionStore - parallel sessions (jobId routing)', () => {
  const pvA = { type: 'ui' as const, data: '<p>A</p>', title: 'A' };
  const pvB = { type: 'ui' as const, data: '<p>B</p>', title: 'B' };

  it('jobOwnerOf reports the owner while tracked and null after finalize', () => {
    useExecutionStore.getState().startExecution('job-1', 'sess-A');
    expect(useExecutionStore.getState().jobOwnerOf('job-1')).toBe('sess-A');
    expect(useExecutionStore.getState().jobOwnerOf('missing')).toBeNull();
    setCurrentSessionId('sess-A');
    mockedParse.mockReturnValue(null);
    useExecutionStore.getState().completeExecution('done', 'job-1');
    expect(useExecutionStore.getState().jobOwnerOf('job-1')).toBeNull();
  });

  it('clearJobOwner drops a mapping so a late event is ignored', () => {
    useExecutionStore.getState().startExecution('job-2', 'sess-A');
    useExecutionStore.getState().clearJobOwner('job-2');
    expect(useExecutionStore.getState().jobOwnerOf('job-2')).toBeNull();
    // A completion for the now-untracked job is a no-op (idempotency guard).
    setCurrentSessionId('sess-A');
    mockedParse.mockReturnValue(null);
    useExecutionStore.getState().completeExecution('late', 'job-2');
    expect(sessionState().addMessageToTargetSession).not.toHaveBeenCalled();
  });

  it('completeExecution(jobId) routes to the job OWNER, not the viewed session', () => {
    mockedParse.mockReturnValue(null);
    useExecutionStore.getState().startExecution('job-A', 'sess-A');
    // sess-B's run takes the live slot; we are viewing B.
    useExecutionStore.setState({ executionOwnerSessionId: 'sess-B' });
    setCurrentSessionId('sess-B');
    useExecutionStore.getState().completeExecution('A result', 'job-A');
    // Message lands in sess-A (the owner), and B's live owner is untouched.
    // The message is stamped with the run's executionId so the preview pane can
    // later derive the deliverable from execution.result on demand.
    expect(sessionState().addMessageToTargetSession).toHaveBeenCalledWith(
      'sess-A',
      'assistant',
      'A result',
      { executionId: 'job-A' },
    );
    expect(useExecutionStore.getState().executionOwnerSessionId).toBe('sess-B');
  });

  it('completeExecution(jobId) is idempotent — a duplicate event is a no-op', () => {
    mockedParse.mockReturnValue(null);
    useExecutionStore.getState().startExecution('job-D', 'sess-A');
    setCurrentSessionId('sess-A');
    useExecutionStore.getState().completeExecution('one', 'job-D');
    const after1 = (sessionState().addMessage as any).mock.calls.length;
    useExecutionStore.getState().completeExecution('two', 'job-D');
    expect((sessionState().addMessage as any).mock.calls.length).toBe(after1);
  });

  it('failExecution(jobId) routes to the owner and leaves the viewed run alone', () => {
    useExecutionStore.getState().startExecution('job-F', 'sess-A');
    useExecutionStore.setState({ executionOwnerSessionId: 'sess-B' });
    setCurrentSessionId('sess-B');
    useExecutionStore.getState().failExecution('boom', 'job-F');
    expect(sessionState().addMessageToTargetSession).toHaveBeenCalledWith('sess-A', 'assistant', 'Execution failed: boom');
    expect(useExecutionStore.getState().executionOwnerSessionId).toBe('sess-B');
    // Idempotent: a second fail for the same job is a no-op.
    (sessionState().addMessageToTargetSession as any).mockClear();
    useExecutionStore.getState().failExecution('again', 'job-F');
    expect(sessionState().addMessageToTargetSession).not.toHaveBeenCalled();
  });

  it('a backgrounded completion releases the slot owner when it still points at that job', () => {
    mockedParse.mockReturnValue(null);
    useExecutionStore.getState().startExecution('job-G', 'sess-A'); // owner=A, slot=A
    // Viewing an idle sess-B, but the live slot owner is still the stale sess-A.
    setCurrentSessionId('sess-B');
    useExecutionStore.getState().completeExecution('done', 'job-G');
    // The finished job owned the slot, so its owner is released.
    expect(useExecutionStore.getState().executionOwnerSessionId).toBeNull();
  });

  it('a backgrounded completion preserves a preview parked by task output', () => {
    // sess-A starts, is parked running, then its task output stashes a preview.
    useExecutionStore.getState().startExecution('job-P', 'sess-A');
    useExecutionStore.getState().saveSessionState('sess-A');
    useExecutionStore.getState().stashSessionPreview('sess-A', pvA);
    // We are viewing sess-B; sess-A's run finishes with NO preview in the result.
    useExecutionStore.setState({ executionOwnerSessionId: 'sess-B' });
    setCurrentSessionId('sess-B');
    mockedParse.mockReturnValue(null);
    useExecutionStore.getState().completeExecution('', 'job-P');
    // Switch back to sess-A: the parked preview is restored, not blanked.
    useExecutionStore.setState({ executionOwnerSessionId: null });
    setCurrentSessionId('sess-A');
    useExecutionStore.getState().restoreSessionState('sess-A');
    const s = useExecutionStore.getState();
    expect(s.previewContent).toEqual(pvA);
    expect(s.previewOwnerSessionId).toBe('sess-A');
    expect(s.isExecuting).toBe(false);
  });

  it('a backgrounded completion appends the final preview after the parked one', () => {
    useExecutionStore.getState().startExecution('job-Q', 'sess-A');
    useExecutionStore.getState().saveSessionState('sess-A');
    useExecutionStore.getState().stashSessionPreview('sess-A', pvA);
    useExecutionStore.setState({ executionOwnerSessionId: 'sess-B' });
    setCurrentSessionId('sess-B');
    mockedParse.mockReturnValue(pvB); // final result carries a NEW preview
    useExecutionStore.getState().completeExecution('final', 'job-Q');
    useExecutionStore.setState({ executionOwnerSessionId: null });
    setCurrentSessionId('sess-A');
    useExecutionStore.getState().restoreSessionState('sess-A');
    const s = useExecutionStore.getState();
    expect(s.previewContent).toEqual(pvB);
    expect(s.previewHistory).toEqual([pvA, pvB]);
  });

  it('a backgrounded completion dedupes when the final preview repeats the parked one', () => {
    useExecutionStore.getState().startExecution('job-DD', 'sess-A');
    useExecutionStore.getState().saveSessionState('sess-A');
    useExecutionStore.getState().stashSessionPreview('sess-A', pvA);
    useExecutionStore.setState({ executionOwnerSessionId: 'sess-B' });
    setCurrentSessionId('sess-B');
    mockedParse.mockReturnValue(pvA); // final result repeats the already-parked preview
    useExecutionStore.getState().completeExecution('final', 'job-DD');
    useExecutionStore.setState({ executionOwnerSessionId: null });
    setCurrentSessionId('sess-A');
    useExecutionStore.getState().restoreSessionState('sess-A');
    expect(useExecutionStore.getState().previewHistory).toEqual([pvA]); // not duplicated
  });

  it('a backgrounded failure keeps the partial preview it produced', () => {
    useExecutionStore.getState().startExecution('job-R', 'sess-A');
    useExecutionStore.getState().saveSessionState('sess-A');
    useExecutionStore.getState().stashSessionPreview('sess-A', pvA);
    useExecutionStore.setState({ executionOwnerSessionId: 'sess-B' });
    setCurrentSessionId('sess-B');
    useExecutionStore.getState().failExecution('died', 'job-R');
    useExecutionStore.setState({ executionOwnerSessionId: null });
    setCurrentSessionId('sess-A');
    useExecutionStore.getState().restoreSessionState('sess-A');
    expect(useExecutionStore.getState().previewContent).toEqual(pvA);
  });
});

describe('executionStore - stashSessionPreview', () => {
  const pv1 = { type: 'ui' as const, data: '<p>1</p>', title: '1' };
  const pv2 = { type: 'ui' as const, data: '<p>2</p>', title: '2' };

  it('creates a snapshot for a backgrounded session (no prior snapshot)', () => {
    useExecutionStore.getState().stashSessionPreview('sess-X', pv1);
    // Not running, so hasActiveExecution is false, but the preview restores.
    expect(useExecutionStore.getState().hasActiveExecution('sess-X')).toBe(false);
    setCurrentSessionId('sess-X');
    useExecutionStore.getState().restoreSessionState('sess-X');
    expect(useExecutionStore.getState().previewContent).toEqual(pv1);
  });

  it('appends to history and preserves in-flight run flags', () => {
    setCurrentSessionId('sess-Y'); // viewing the owner → run drives the live slot
    useExecutionStore.getState().startExecution('job-S', 'sess-Y');
    useExecutionStore.getState().saveSessionState('sess-Y'); // running snapshot
    useExecutionStore.getState().stashSessionPreview('sess-Y', pv1);
    useExecutionStore.getState().stashSessionPreview('sess-Y', pv2);
    // Run flags survived the stashes (still considered active for the spinner).
    expect(useExecutionStore.getState().hasActiveExecution('sess-Y')).toBe(true);
    useExecutionStore.setState({ executionOwnerSessionId: null });
    setCurrentSessionId('sess-Y');
    useExecutionStore.getState().restoreSessionState('sess-Y');
    const s = useExecutionStore.getState();
    expect(s.previewHistory).toEqual([pv1, pv2]);
    expect(s.isExecuting).toBe(true);
  });

  it('does not duplicate when the same preview is stashed twice', () => {
    useExecutionStore.getState().stashSessionPreview('sess-Z', pv1);
    useExecutionStore.getState().stashSessionPreview('sess-Z', pv1);
    setCurrentSessionId('sess-Z');
    useExecutionStore.getState().restoreSessionState('sess-Z');
    expect(useExecutionStore.getState().previewHistory).toEqual([pv1]);
  });
});

describe('executionStore - restoreSessionState concurrency', () => {
  it('returns early when restoring the session that already owns the live slot', () => {
    useExecutionStore.setState({
      executionOwnerSessionId: 'sess-A',
      isExecuting: true,
      activeExecution: { jobId: 'j', status: 'running' },
    });
    // Restoring the live owner is a no-op; live state is untouched.
    useExecutionStore.getState().restoreSessionState('sess-A');
    expect(useExecutionStore.getState().isExecuting).toBe(true);
    expect(useExecutionStore.getState().executionOwnerSessionId).toBe('sess-A');
  });

  it('clears a foreign session preview when switching to a session with no run of its own', () => {
    // A previous run (sess-french) left its result in the live preview slot.
    useExecutionStore.setState({
      previewContent: { type: 'ui', data: '<p>french</p>' } as any,
      previewOwnerSessionId: 'sess-french',
      previewHistory: [{ type: 'ui', data: '<p>french</p>' }] as any,
      previewIndex: 0,
      executionOwnerSessionId: null,
    });
    // Switch into a brand-new session (sess-korean) that has no snapshot/result:
    // its preview pane must NOT inherit the other session's result.
    setCurrentSessionId('sess-korean');
    useExecutionStore.getState().restoreSessionState('sess-korean');
    const s = useExecutionStore.getState();
    expect(s.previewContent).toBeNull();
    expect(s.previewOwnerSessionId).toBeNull();
  });

  it('restores a backgrounded running snapshot and re-takes slot ownership', () => {
    // sess-A is running and gets parked; sess-B then owns the live slot. Each
    // run starts while viewing its own session (owner-aware startExecution).
    setCurrentSessionId('sess-A');
    useExecutionStore.getState().startExecution('job-A', 'sess-A');
    useExecutionStore.getState().saveSessionState('sess-A');
    setCurrentSessionId('sess-B');
    useExecutionStore.getState().startExecution('job-B', 'sess-B'); // B now owns slot
    useExecutionStore.getState().saveSessionState('sess-B');
    // Switch back to A: its running snapshot restores and A re-owns the slot.
    setCurrentSessionId('sess-A');
    useExecutionStore.getState().restoreSessionState('sess-A');
    const s = useExecutionStore.getState();
    expect(s.isExecuting).toBe(true);
    expect(s.activeExecution).toEqual({ jobId: 'job-A', status: 'running' });
    expect(s.executionOwnerSessionId).toBe('sess-A');
  });
});

describe('executionStore - generation owner routing', () => {
  it('completeGeneration routes to the passed origin, not the live owner', () => {
    // A generation started in sess-A, but sess-B now owns the live slot.
    useExecutionStore.setState({ isGenerating: true, executionOwnerSessionId: 'sess-B' });
    setCurrentSessionId('sess-B');
    useExecutionStore.getState().completeGeneration('sess-A');
    // sess-B's ownership of the slot is preserved (not blanked by A finishing).
    expect(useExecutionStore.getState().executionOwnerSessionId).toBe('sess-B');
  });

  it('failGeneration routes to the passed origin and posts to it', () => {
    useExecutionStore.setState({ isGenerating: true, executionOwnerSessionId: 'sess-B' });
    setCurrentSessionId('sess-B');
    useExecutionStore.getState().failGeneration('nope', 'sess-A');
    expect(sessionState().addMessageToTargetSession).toHaveBeenCalledWith('sess-A', 'assistant', 'Generation failed: nope');
    expect(useExecutionStore.getState().executionOwnerSessionId).toBe('sess-B');
  });

  it('completeGeneration falls back to the live owner when no origin passed', () => {
    setCurrentSessionId('sess-A');
    useExecutionStore.setState({ isGenerating: true, executionOwnerSessionId: 'sess-A' });
    useExecutionStore.getState().completeGeneration();
    expect(useExecutionStore.getState().isGenerating).toBe(false);
    expect(useExecutionStore.getState().executionOwnerSessionId).toBeNull();
  });
});

describe('executionStore - switch-back run detection', () => {
  it('records the running job per session on start and clears it on completion', () => {
    useExecutionStore.setState({ runningJobBySession: {} });
    setCurrentSessionId('sA');
    useExecutionStore.getState().startExecution('job-1', 'sA');
    // The Zustand map is the source of truth for "does this session have a run?"
    // when you switch back to it.
    expect(useExecutionStore.getState().runningJobBySession.sA).toBe('job-1');
    expect(useExecutionStore.getState().runStartedAt).toBeTypeOf('number');

    useExecutionStore.getState().completeExecution('done', 'job-1');
    expect(useExecutionStore.getState().runningJobBySession.sA).toBeUndefined();
    expect(useExecutionStore.getState().runStartedAt).toBeNull();
  });

});

describe('a surface that arrives after the run has finalized', () => {
  // The crew subprocess announces a run TWICE by design: the plain answer the
  // moment the crew has it, then a second one carrying the composed A2UI surface,
  // which the parent can only build once the subprocess has exited. Measured on
  // one run: crew_completed 20:21:31, the UI finalized at 20:21:34, and the
  // surface finished composing at 20:22:24 — 50s later. Finalization is
  // once-only (a double completion double-posts, to the wrong session), so the
  // second announcement was dropped and a 50-component deck rendered as raw markdown.
  const surface = {
    surfaceKind: 'presentation',
    root: 'r',
    components: [],
    dataModel: {},
  } as never;

  beforeEach(() => {
    sessionState().addMessageToTargetSession.mockReturnValue('msg-1');
  });

  it('attaches the late surface to the finalized message', () => {
    setCurrentSessionId('sess-U');
    mockedParse.mockReturnValue(null);
    useExecutionStore.getState().startExecution('job-U', 'sess-U');
    // First announcement: plain text, no surface composed yet.
    useExecutionStore.getState().completeExecution('# Deck\n## Slide 1', 'job-U');

    const attached = useExecutionStore.getState().attachSurface('job-U', surface);

    expect(attached).toBe(true);
    expect(sessionState().updateMessageInTargetSession).toHaveBeenCalledWith(
      'sess-U',
      'msg-1',
      // The answer text STAYS. The reader has been looking at it for the 25-45s
      // composition took; blanking it mid-read reads as the answer being
      // retracted. (The completion path DOES drop it when the surface arrives
      // in time — there the text never rendered, so nothing is taken away.)
      { resultType: 'a2ui', resultData: surface },
    );
  });

  it('does not post a second message', () => {
    setCurrentSessionId('sess-V');
    mockedParse.mockReturnValue(null);
    useExecutionStore.getState().startExecution('job-V', 'sess-V');
    useExecutionStore.getState().completeExecution('# Deck', 'job-V');
    sessionState().addMessageToTargetSession.mockClear();

    useExecutionStore.getState().attachSurface('job-V', surface);

    expect(sessionState().addMessageToTargetSession).not.toHaveBeenCalled();
  });

  it('accepts exactly one late surface per run', () => {
    setCurrentSessionId('sess-W');
    mockedParse.mockReturnValue(null);
    useExecutionStore.getState().startExecution('job-W', 'sess-W');
    useExecutionStore.getState().completeExecution('# Deck', 'job-W');

    expect(useExecutionStore.getState().attachSurface('job-W', surface)).toBe(true);
    // A third announcement (SSE + poller both re-delivering) must be inert.
    expect(useExecutionStore.getState().attachSurface('job-W', surface)).toBe(false);
  });

  it('is a no-op for a run it never finalized', () => {
    expect(useExecutionStore.getState().attachSurface('job-unknown', surface)).toBe(
      false,
    );
    expect(useExecutionStore.getState().attachSurface(undefined, surface)).toBe(false);
  });

  it('a run that already had its surface has nothing left to wait for', () => {
    // The light-agent path composes in-process, so its single announcement
    // already carries the surface. Nothing is left waiting.
    setCurrentSessionId('sess-X');
    mockedParse.mockReturnValue(null);
    useExecutionStore.getState().startExecution('job-X', 'sess-X');
    useExecutionStore.getState().completeExecution('# Deck', 'job-X', surface);
    sessionState().updateMessageInTargetSession.mockClear();

    // It still resolves (the message exists), but re-applying the same surface
    // changes nothing the user can see.
    useExecutionStore.getState().attachSurface('job-X', surface);
    expect(sessionState().updateMessageInTargetSession).toHaveBeenCalledWith(
      'sess-X',
      'msg-1',
      { resultType: 'a2ui', resultData: surface },
    );
  });
});

describe('a late surface never blanks what the reader is looking at', () => {
  // The completion path drops the text when the surface arrives WITH it, so the
  // same deck is not printed twice. A late surface must not apply that rule
  // retroactively: by then the answer has been on screen for the 25-45s that
  // composition took, and clearing it reads as the answer being withdrawn.
  it('leaves the message content alone', () => {
    const surface = {
      surfaceKind: 'presentation',
      root: 'r',
      components: [],
      dataModel: {},
    } as never;
    sessionState().addMessageToTargetSession.mockReturnValue('msg-late');
    setCurrentSessionId('sess-Y');
    mockedParse.mockReturnValue(null);
    useExecutionStore.getState().startExecution('job-Y', 'sess-Y');
    useExecutionStore.getState().completeExecution('# The answer', 'job-Y');

    useExecutionStore.getState().attachSurface('job-Y', surface);

    const update = sessionState().updateMessageInTargetSession.mock.calls.at(-1)[2];
    expect(update).not.toHaveProperty('content');
  });
});

describe('a flow finishes with no readable text, then its surface arrives', () => {
  // The path that dropped a composed mindmap on a real run. A flow's early
  // announcement carries the last crew's raw output, which is often not
  // readable text, so the run finalizes on the bare "Execution completed."
  // line — and the surface, composed 14s later, needs somewhere to land. The
  // message id was only registered on the branches that HAD text, so
  // attachSurface found nothing and the deck was discarded after being built,
  // stored and paid for.
  const surface = {
    surfaceKind: 'mindmap',
    root: 'r',
    components: [],
    dataModel: {},
  } as never;

  it('registers the run so a later surface can still land', () => {
    setCurrentSessionId('sess-F');
    mockedParse.mockReturnValue(null);
    useExecutionStore.getState().startExecution('job-F', 'sess-F');
    // No result text at all — the flow case.
    useExecutionStore.getState().completeExecution('', 'job-F');

    expect(useExecutionStore.getState().attachSurface('job-F', surface)).toBe(true);
  });

  it('POSTS the surface as its own message, since none was posted', () => {
    // No notice was written, so there is nothing to fill — the surface is the
    // whole answer and arrives as its own message.
    setCurrentSessionId('sess-G');
    mockedParse.mockReturnValue(null);
    useExecutionStore.getState().startExecution('job-G', 'sess-G');
    useExecutionStore.getState().completeExecution('', 'job-G');
    sessionState().addMessageToTargetSession.mockClear();

    useExecutionStore.getState().attachSurface('job-G', surface);

    expect(sessionState().addMessageToTargetSession).toHaveBeenCalledWith(
      'sess-G',
      'assistant',
      '',
      { executionId: 'job-G', resultType: 'a2ui', resultData: surface },
    );
  });

  it('carries the answer TEXT alongside the surface', () => {
    // A flow's second announcement brings both, and they are different things:
    // the surface is a mindmap, the text is the news it was drawn from. Posting
    // the surface alone dropped half of what the run produced.
    setCurrentSessionId('sess-I');
    mockedParse.mockReturnValue(null);
    useExecutionStore.getState().startExecution('job-I', 'sess-I');
    useExecutionStore.getState().completeExecution('', 'job-I');
    sessionState().addMessageToTargetSession.mockClear();

    useExecutionStore.getState().attachSurface('job-I', surface, '# Lebanese News');

    expect(sessionState().addMessageToTargetSession).toHaveBeenCalledWith(
      'sess-I',
      'assistant',
      '# Lebanese News',
      { executionId: 'job-I', resultType: 'a2ui', resultData: surface },
    );
  });

  it('a REAL answer is still kept when the surface lands', () => {
    sessionState().addMessageToTargetSession.mockReturnValue('msg-real');
    sessionState().messages = [{ id: 'msg-real', content: '# The answer' }];
    setCurrentSessionId('sess-H');
    mockedParse.mockReturnValue(null);
    useExecutionStore.getState().startExecution('job-H', 'sess-H');
    useExecutionStore.getState().completeExecution('# The answer', 'job-H');

    useExecutionStore.getState().attachSurface('job-H', surface);

    const update = sessionState().updateMessageInTargetSession.mock.calls.at(-1)[2];
    expect(update).not.toHaveProperty('content');
  });
});

describe('the final answer does not print twice', () => {
  // A run posts each task's output as it completes, and the LAST task's output
  // IS the run's result — so the same content arrived twice: once as the task
  // summary (capped at 300 chars by summarizeTaskOutput) and again in full at
  // completion.
  const FULL =
    '{ "query": "Lebanese News Collection", "results_found": 6, ' +
    '"has_results": true, "top_result_title": "Parliament Approves Reforms" }';

  beforeEach(() => {
    sessionState().messages = [];
    sessionState().updateMessageInTargetSession.mockClear();
    sessionState().addMessageToTargetSession.mockClear();
  });

  it('folds the full text into the capped task message', () => {
    sessionState().messages = [
      { id: 'task-1', role: 'assistant', content: `${FULL.slice(0, 60)}…` },
    ];
    setCurrentSessionId('sess-D1');
    mockedParse.mockReturnValue(null);
    useExecutionStore.getState().startExecution('job-D1', 'sess-D1');

    useExecutionStore.getState().completeExecution(FULL, 'job-D1');

    expect(sessionState().updateMessageInTargetSession).toHaveBeenCalledWith(
      'sess-D1',
      'task-1',
      expect.objectContaining({ content: FULL, isStreaming: false }),
    );
    // and NOT posted a second time
    expect(sessionState().addMessageToTargetSession).not.toHaveBeenCalled();
  });

  it('folds it when the task message carries a "**Task** — " header', () => {
    sessionState().messages = [
      {
        id: 'task-2',
        role: 'assistant',
        content: `**Create Presentation** — ${FULL.slice(0, 60)}…`,
      },
    ];
    setCurrentSessionId('sess-D2');
    mockedParse.mockReturnValue(null);
    useExecutionStore.getState().startExecution('job-D2', 'sess-D2');

    useExecutionStore.getState().completeExecution(FULL, 'job-D2');

    expect(sessionState().addMessageToTargetSession).not.toHaveBeenCalled();
  });

  it('still posts when nothing above it matches', () => {
    sessionState().messages = [
      { id: 'other', role: 'assistant', content: 'A completely different answer' },
    ];
    setCurrentSessionId('sess-D3');
    mockedParse.mockReturnValue(null);
    useExecutionStore.getState().startExecution('job-D3', 'sess-D3');

    useExecutionStore.getState().completeExecution(FULL, 'job-D3');

    expect(sessionState().addMessageToTargetSession).toHaveBeenCalled();
  });

  it('does not rewrite a rich card that happens to share an opening', () => {
    // A card carries its own meaning; only a plain task line is a candidate.
    sessionState().messages = [
      {
        id: 'card',
        role: 'assistant',
        content: FULL.slice(0, 60),
        resultType: 'a2ui',
      },
    ];
    setCurrentSessionId('sess-D4');
    mockedParse.mockReturnValue(null);
    useExecutionStore.getState().startExecution('job-D4', 'sess-D4');

    useExecutionStore.getState().completeExecution(FULL, 'job-D4');

    expect(sessionState().updateMessageInTargetSession).not.toHaveBeenCalledWith(
      'sess-D4',
      'card',
      expect.anything(),
    );
    expect(sessionState().addMessageToTargetSession).toHaveBeenCalled();
  });

  it('ignores a short line that shares a prefix by accident', () => {
    sessionState().messages = [
      { id: 'noise', role: 'assistant', content: '{ "query"…' },
    ];
    setCurrentSessionId('sess-D5');
    mockedParse.mockReturnValue(null);
    useExecutionStore.getState().startExecution('job-D5', 'sess-D5');

    useExecutionStore.getState().completeExecution(FULL, 'job-D5');

    expect(sessionState().addMessageToTargetSession).toHaveBeenCalled();
  });
});

describe('a late surface does not reprint an answer already shown', () => {
  // The second duplication: the run's text appeared immediately, then again a
  // few seconds later beneath the mindmap. The completion had folded the text
  // into the task message but registered NO message id, so attachSurface
  // treated the run as having posted nothing and posted the answer again.
  const FULL =
    '{ "query": "Lebanese News Collection", "results_found": 6, ' +
    '"has_results": true, "top_result_title": "Parliament Approves Reforms" }';
  const surface = {
    surfaceKind: 'mindmap',
    root: 'r',
    components: [],
    dataModel: {},
  } as never;

  it('attaches to the folded message instead of posting again', () => {
    sessionState().messages = [
      { id: 'task-x', role: 'assistant', content: `${FULL.slice(0, 60)}…` },
    ];
    sessionState().addMessageToTargetSession.mockClear();
    setCurrentSessionId('sess-L');
    mockedParse.mockReturnValue(null);
    useExecutionStore.getState().startExecution('job-L', 'sess-L');
    useExecutionStore.getState().completeExecution(FULL, 'job-L');

    useExecutionStore.getState().attachSurface('job-L', surface, FULL);

    expect(sessionState().updateMessageInTargetSession).toHaveBeenLastCalledWith(
      'sess-L',
      'task-x',
      { resultType: 'a2ui', resultData: surface },
    );
    // The answer is never posted as a new message — not once, not twice.
    expect(sessionState().addMessageToTargetSession).not.toHaveBeenCalled();
  });
});
