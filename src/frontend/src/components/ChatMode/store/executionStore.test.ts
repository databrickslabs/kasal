import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import { useExecutionStore } from './executionStore';
import { useSessionStore } from './sessionStore';
import { saveSessionPreview, getSessionPreview } from '../db/sessionApi';
import { parsePreviewContent } from '../components/Preview/PreviewPanel';

// --- Mocks for sibling modules ---
vi.mock('./sessionStore', () => {
  const state = {
    currentSessionId: null as string | null,
    addMessage: vi.fn(),
    addMessageToTargetSession: vi.fn(),
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
  setSessionRunningJob: vi.fn(() => Promise.resolve()),
  getSessionRunningJob: vi.fn(() => Promise.resolve(null)),
  clearSessionRunningJob: vi.fn(() => Promise.resolve()),
}));

vi.mock('../components/Preview/PreviewPanel', () => ({
  parsePreviewContent: vi.fn(),
}));

// Typed helpers to access the mocked sessionStore state
const sessionState = () => (useSessionStore as unknown as { getState: () => any }).getState();
const setCurrentSessionId = (id: string | null) => {
  sessionState().currentSessionId = id;
};

const mockedSave = saveSessionPreview as unknown as ReturnType<typeof vi.fn>;
const mockedGet = getSessionPreview as unknown as ReturnType<typeof vi.fn>;
const mockedParse = parsePreviewContent as unknown as ReturnType<typeof vi.fn>;

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
    executionLog: [],
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
  it('appendLog adds entry with generated id/timestamp and clearLog empties it', () => {
    const store = useExecutionStore.getState();
    store.appendLog({ kind: 'trace', label: 'L1' });
    store.appendLog({ kind: 'status', label: 'L2', detail: 'd' });
    const log = useExecutionStore.getState().executionLog;
    expect(log).toHaveLength(2);
    expect(log[0].id).toBeTruthy();
    expect(typeof log[0].timestamp).toBe('number');
    expect(log[1].label).toBe('L2');

    useExecutionStore.getState().clearLog();
    expect(useExecutionStore.getState().executionLog).toHaveLength(0);
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
    // Defaults to workspace-wide so recall spans the whole workspace.
    expect(useExecutionStore.getState().workspaceMemory).toBe(true);
    useExecutionStore.getState().setWorkspaceMemory(false);
    expect(useExecutionStore.getState().workspaceMemory).toBe(false);
    useExecutionStore.getState().setWorkspaceMemory(true);
    expect(useExecutionStore.getState().workspaceMemory).toBe(true);
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

  it('clearPreview hides preview and uncollapses chat but keeps data in db', () => {
    useExecutionStore.setState({
      previewContent: preview as any,
      previewOwnerSessionId: 'sess-A',
      chatCollapsed: true,
    });
    useExecutionStore.getState().clearPreview();
    const s = useExecutionStore.getState();
    expect(s.previewContent).toBeNull();
    expect(s.previewOwnerSessionId).toBeNull();
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

  it('startExecution clears preview history', () => {
    useExecutionStore.setState({ previewHistory: [a, b] as any, previewIndex: 1 });
    useExecutionStore.getState().startExecution('job-1', 'sess-X');
    const s = useExecutionStore.getState();
    expect(s.previewHistory).toEqual([]);
    expect(s.previewIndex).toBe(0);
  });

  it('startExecution with preservePreview keeps the existing preview + history (refine continuation)', () => {
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

  it('keeps existing preview history when reopening', async () => {
    setCurrentSessionId('sess-A');
    const existing = [
      { type: 'ui', data: '# x', title: 'X' },
      { type: 'ui', data: '<p>y</p>', title: 'Y' },
    ];
    useExecutionStore.setState({ previewHistory: existing as any, previewIndex: 1 });
    mockedGet.mockResolvedValue({ type: 'ui', data: '{}', title: 'JT' });
    useExecutionStore.getState().reopenPreview();
    await vi.waitFor(() => {
      expect(useExecutionStore.getState().previewContent).toEqual({ type: 'ui', data: '{}', title: 'JT' });
    });
    expect(useExecutionStore.getState().previewHistory).toEqual(existing);
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
    useExecutionStore.getState().startExecution('job-1', 'sess-X');
    const s = useExecutionStore.getState();
    expect(s.executionOwnerSessionId).toBe('sess-X');
    expect(s.isExecuting).toBe(true);
    expect(s.isLoading).toBe(true);
    expect(s.activeExecution).toEqual({ jobId: 'job-1', status: 'running' });
    expect(s.previewContent).toBeNull();
    expect(s.previewOwnerSessionId).toBeNull();
    expect(s.executionLog).toEqual([]);
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
    );
  });

  it('no preview, no ownerSession: uses addMessage and does not save snapshot', () => {
    setCurrentSessionId(null);
    mockedParse.mockReturnValue(null);
    useExecutionStore.setState({ executionOwnerSessionId: null });
    useExecutionStore.getState().completeExecution('plain text');
    expect(sessionState().addMessage).toHaveBeenCalledWith('assistant', 'plain text');
    // isViewingOwner true (null === null), no snapshot delete attempted
    expect(useExecutionStore.getState().executionOwnerSessionId).toBeNull();
  });

  it('empty resultText with ownerSession: posts "Execution completed."', () => {
    setCurrentSessionId('sess-O');
    useExecutionStore.setState({ executionOwnerSessionId: 'sess-O' });
    useExecutionStore.getState().completeExecution('');
    expect(sessionState().addMessageToTargetSession).toHaveBeenCalledWith(
      'sess-O',
      'assistant',
      'Execution completed.',
    );
    // parsePreviewContent not invoked because resultText falsy
    expect(mockedParse).not.toHaveBeenCalled();
  });

  it('empty resultText without ownerSession: addMessage "Execution completed."', () => {
    setCurrentSessionId(null);
    useExecutionStore.setState({ executionOwnerSessionId: null });
    useExecutionStore.getState().completeExecution('');
    expect(sessionState().addMessage).toHaveBeenCalledWith(
      'assistant',
      'Execution completed.',
    );
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
    expect(sessionState().addMessage).toHaveBeenCalledWith('assistant', 'plain');
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
    expect(initialState.executionLog).toEqual([]);
    expect(initialState.chatCollapsed).toBe(false);
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
    expect(sessionState().addMessageToTargetSession).toHaveBeenCalledWith('sess-A', 'assistant', 'A result');
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

  it('restores a backgrounded running snapshot and re-takes slot ownership', () => {
    // sess-A is running and gets parked; sess-B then owns the live slot.
    useExecutionStore.getState().startExecution('job-A', 'sess-A');
    useExecutionStore.getState().saveSessionState('sess-A');
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
