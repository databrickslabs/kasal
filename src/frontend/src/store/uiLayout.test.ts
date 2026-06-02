import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';

const KEY = 'ui-layout-storage';

/**
 * Import a fresh copy of the store module with localStorage pre-seeded so the
 * module-level init branches (persisted-value ternaries, load catch) can be
 * exercised. Optionally evaluate with `window` undefined to hit the SSR guards.
 */
async function freshModule(
  persisted?: unknown,
  opts?: { rawValue?: string; noWindow?: boolean },
) {
  vi.resetModules();
  localStorage.clear();
  if (opts?.rawValue !== undefined) {
    localStorage.setItem(KEY, opts.rawValue);
  } else if (persisted !== undefined) {
    localStorage.setItem(KEY, JSON.stringify(persisted));
  }
  if (opts?.noWindow) vi.stubGlobal('window', undefined);
  const mod = await import('./uiLayout');
  if (opts?.noWindow) vi.unstubAllGlobals();
  return mod;
}

function readPersisted(): Record<string, unknown> {
  const raw = localStorage.getItem(KEY);
  return raw ? JSON.parse(raw) : {};
}

describe('uiLayout store — initial state', () => {
  beforeEach(() => {
    localStorage.clear();
    vi.restoreAllMocks();
  });
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('uses defaults when nothing is persisted', async () => {
    const { useUILayoutStore } = await freshModule();
    const s = useUILayoutStore.getState();
    expect(s.appMode).toBe('crew');
    expect(s.areFlowsVisible).toBe(false);
    expect(s.chatPanelVisible).toBe(true);
    expect(s.chatPanelCollapsed).toBe(false);
    expect(s.chatPanelWidth).toBe(450);
    expect(s.chatPanelSide).toBe('right');
    expect(s.executionHistoryVisible).toBe(false);
    expect(s.executionHistoryHeight).toBe(60);
    expect(s.panelPosition).toBe(50);
    expect(s.layoutOrientation).toBe('horizontal');
    expect(s.tabBarHeight).toBe(48);
  });

  it('hydrates from persisted values (truthy/defined branches)', async () => {
    const { useUILayoutStore } = await freshModule({
      chatPanelVisible: false,
      chatPanelCollapsed: true,
      chatPanelWidth: 600,
      chatPanelSide: 'left',
      executionHistoryVisible: true,
      executionHistoryHeight: 200,
      panelPosition: 70,
      areFlowsVisible: true,
      layoutOrientation: 'vertical',
      appMode: 'chat',
    });
    const s = useUILayoutStore.getState();
    expect(s.chatPanelVisible).toBe(false);
    expect(s.chatPanelCollapsed).toBe(true);
    expect(s.chatPanelWidth).toBe(600);
    expect(s.chatPanelSide).toBe('left');
    expect(s.executionHistoryVisible).toBe(true);
    expect(s.executionHistoryHeight).toBe(200);
    expect(s.panelPosition).toBe(70);
    expect(s.areFlowsVisible).toBe(true);
    expect(s.layoutOrientation).toBe('vertical');
    expect(s.appMode).toBe('chat');
  });

  it('falls back to {} when persisted JSON is invalid (load catch)', async () => {
    const errSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    const { useUILayoutStore } = await freshModule(undefined, { rawValue: '{not-json' });
    const s = useUILayoutStore.getState();
    expect(s.appMode).toBe('crew');
    expect(errSpy).toHaveBeenCalledWith('Failed to load persisted UI state:', expect.anything());
  });

  it('uses SSR fallback dimensions when window is undefined at init', async () => {
    const { useUILayoutStore } = await freshModule(undefined, { noWindow: true });
    const s = useUILayoutStore.getState();
    expect(s.screenWidth).toBe(1200);
    expect(s.screenHeight).toBe(800);
  });

  it('exposes the store on window when window is defined', async () => {
    await freshModule();
    expect((window as unknown as Record<string, unknown>).useUILayoutStore).toBeDefined();
  });
});

describe('uiLayout store — appMode / areFlowsVisible sync', () => {
  beforeEach(() => localStorage.clear());

  it('setAppMode("flow") shows flows and persists', async () => {
    const { useUILayoutStore } = await freshModule();
    useUILayoutStore.getState().setAppMode('flow');
    const s = useUILayoutStore.getState();
    expect(s.appMode).toBe('flow');
    expect(s.areFlowsVisible).toBe(true);
    expect(readPersisted()).toMatchObject({ appMode: 'flow', areFlowsVisible: true });
  });

  it('setAppMode("crew") hides flows', async () => {
    const { useUILayoutStore } = await freshModule();
    useUILayoutStore.getState().setAppMode('flow');
    useUILayoutStore.getState().setAppMode('crew');
    const s = useUILayoutStore.getState();
    expect(s.appMode).toBe('crew');
    expect(s.areFlowsVisible).toBe(false);
  });

  it('setAppMode("chat") hides flows', async () => {
    const { useUILayoutStore } = await freshModule();
    useUILayoutStore.getState().setAppMode('chat');
    const s = useUILayoutStore.getState();
    expect(s.appMode).toBe('chat');
    expect(s.areFlowsVisible).toBe(false);
  });

  it('setAreFlowsVisible(true) moves to flow mode', async () => {
    const { useUILayoutStore } = await freshModule();
    useUILayoutStore.getState().setAreFlowsVisible(true);
    const s = useUILayoutStore.getState();
    expect(s.areFlowsVisible).toBe(true);
    expect(s.appMode).toBe('flow');
  });

  it('setAreFlowsVisible(false) from flow returns to crew', async () => {
    const { useUILayoutStore } = await freshModule();
    useUILayoutStore.getState().setAppMode('flow');
    useUILayoutStore.getState().setAreFlowsVisible(false);
    const s = useUILayoutStore.getState();
    expect(s.areFlowsVisible).toBe(false);
    expect(s.appMode).toBe('crew');
  });

  it('setAreFlowsVisible(false) does NOT disturb chat mode', async () => {
    const { useUILayoutStore } = await freshModule();
    useUILayoutStore.getState().setAppMode('chat');
    useUILayoutStore.getState().setAreFlowsVisible(false);
    const s = useUILayoutStore.getState();
    expect(s.appMode).toBe('chat');
    expect(s.areFlowsVisible).toBe(false);
  });
});

describe('uiLayout store — setters persist to localStorage', () => {
  beforeEach(() => localStorage.clear());

  it('updateScreenDimensions does not persist', async () => {
    const { useUILayoutStore } = await freshModule();
    useUILayoutStore.getState().updateScreenDimensions(111, 222);
    const s = useUILayoutStore.getState();
    expect(s.screenWidth).toBe(111);
    expect(s.screenHeight).toBe(222);
  });

  it('setChatPanelWidth / Collapsed / Visible persist', async () => {
    const { useUILayoutStore } = await freshModule();
    useUILayoutStore.getState().setChatPanelWidth(500);
    useUILayoutStore.getState().setChatPanelCollapsed(true);
    useUILayoutStore.getState().setChatPanelVisible(false);
    expect(readPersisted()).toMatchObject({
      chatPanelWidth: 500,
      chatPanelCollapsed: true,
      chatPanelVisible: false,
    });
  });

  it('setExecutionHistoryHeight / Visible persist', async () => {
    const { useUILayoutStore } = await freshModule();
    useUILayoutStore.getState().setExecutionHistoryHeight(300);
    useUILayoutStore.getState().setExecutionHistoryVisible(true);
    expect(readPersisted()).toMatchObject({ executionHistoryHeight: 300, executionHistoryVisible: true });
  });

  it('setLeftSidebarExpanded / Visible (no persist)', async () => {
    const { useUILayoutStore } = await freshModule();
    useUILayoutStore.getState().setLeftSidebarExpanded(true);
    useUILayoutStore.getState().setLeftSidebarVisible(false);
    const s = useUILayoutStore.getState();
    expect(s.leftSidebarExpanded).toBe(true);
    expect(s.leftSidebarVisible).toBe(false);
  });

  it('setPanelPosition / ChatPanelSide / LayoutOrientation persist', async () => {
    const { useUILayoutStore } = await freshModule();
    useUILayoutStore.getState().setPanelPosition(33);
    useUILayoutStore.getState().setChatPanelSide('left');
    useUILayoutStore.getState().setLayoutOrientation('vertical');
    expect(readPersisted()).toMatchObject({
      panelPosition: 33,
      chatPanelSide: 'left',
      layoutOrientation: 'vertical',
    });
  });

  it('save catch path logs when existing storage is invalid JSON', async () => {
    const { useUILayoutStore } = await freshModule();
    const errSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    localStorage.setItem(KEY, '{corrupt'); // makes JSON.parse inside save throw
    useUILayoutStore.getState().setPanelPosition(42);
    expect(errSpy).toHaveBeenCalledWith('Failed to save UI state:', expect.anything());
  });
});

describe('uiLayout store — getUILayoutState / useUILayoutState', () => {
  beforeEach(() => localStorage.clear());

  it('getUILayoutState returns a full snapshot', async () => {
    const { useUILayoutStore } = await freshModule();
    const snap = useUILayoutStore.getState().getUILayoutState();
    expect(snap).toMatchObject({
      tabBarHeight: 48,
      leftSidebarBaseWidth: 48,
      rightSidebarWidth: 48,
      areFlowsVisible: false,
      layoutOrientation: 'horizontal',
    });
    expect(Object.keys(snap)).toContain('chatPanelCollapsedWidth');
  });

  it('useUILayoutState hook returns the computed snapshot', async () => {
    const mod = await freshModule();
    const { renderHook } = await import('@testing-library/react');
    const { result } = renderHook(() => mod.useUILayoutState());
    expect(result.current.tabBarHeight).toBe(48);
  });
});
