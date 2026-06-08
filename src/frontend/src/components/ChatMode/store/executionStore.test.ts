import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import { useExecutionStore } from './executionStore';
import { useSessionStore } from './sessionStore';
import { saveSessionPreview, getSessionPreview } from '../db/sessionDb';
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

vi.mock('../db/sessionDb', () => ({
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

const preview = { type: 'html' as const, data: '<p>hi</p>', title: 'T' };

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
  const a = { type: 'markdown' as const, data: '# A', title: 'A' };
  const b = { type: 'html' as const, data: '<p>B</p>', title: 'B' };

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
    mockedGet.mockResolvedValue({ type: 'json', data: '{}', title: 'JT' });
    useExecutionStore.getState().reopenPreview();
    await vi.waitFor(() => {
      expect(useExecutionStore.getState().previewContent).toEqual({
        type: 'json',
        data: '{}',
        title: 'JT',
      });
    });
    expect(useExecutionStore.getState().previewOwnerSessionId).toBe('sess-A');
  });

  it('seeds preview history when empty', async () => {
    setCurrentSessionId('sess-A');
    mockedGet.mockResolvedValue({ type: 'json', data: '{}', title: 'JT' });
    useExecutionStore.getState().reopenPreview();
    await vi.waitFor(() => {
      expect(useExecutionStore.getState().previewHistory).toHaveLength(1);
    });
    const s = useExecutionStore.getState();
    expect(s.previewHistory[0]).toEqual({ type: 'json', data: '{}', title: 'JT' });
    expect(s.previewIndex).toBe(0);
  });

  it('keeps existing preview history when reopening', async () => {
    setCurrentSessionId('sess-A');
    const existing = [
      { type: 'markdown', data: '# x', title: 'X' },
      { type: 'html', data: '<p>y</p>', title: 'Y' },
    ];
    useExecutionStore.setState({ previewHistory: existing as any, previewIndex: 1 });
    mockedGet.mockResolvedValue({ type: 'json', data: '{}', title: 'JT' });
    useExecutionStore.getState().reopenPreview();
    await vi.waitFor(() => {
      expect(useExecutionStore.getState().previewContent).toEqual({ type: 'json', data: '{}', title: 'JT' });
    });
    expect(useExecutionStore.getState().previewHistory).toEqual(existing);
  });

  it('ignores stored preview if session switched away', async () => {
    setCurrentSessionId('sess-A');
    mockedGet.mockImplementation(() => {
      // switch session before promise resolves
      setCurrentSessionId('sess-B');
      return Promise.resolve({ type: 'html', data: 'x', title: 't' });
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
    mockedGet.mockResolvedValue({ type: 'markdown', data: '# hi', title: 'MD' });
    useExecutionStore.getState().restoreSessionState('sess-NONE');
    // synchronous reset first
    expect(useExecutionStore.getState().previewContent).toBeNull();
    await vi.waitFor(() => {
      expect(useExecutionStore.getState().previewContent).toEqual({
        type: 'markdown',
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
      return Promise.resolve({ type: 'html', data: 'x', title: 't' });
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
