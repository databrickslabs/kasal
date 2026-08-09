/**
 * createTab view-mode inheritance.
 *
 * Adding a new tab while looking at the flow canvas must keep the flow canvas —
 * previously every new tab was hardcoded to viewMode 'crew', which the tab-switch
 * reconciliation effect then used to snap the user back to the crew canvas.
 */
import { describe, it, expect, beforeEach } from 'vitest';
import { useTabManagerStore } from './tabManager';
import { useUILayoutStore } from './uiLayout';

describe('tabManager - createTab view mode', () => {
  beforeEach(() => {
    useTabManagerStore.setState({ tabs: [], activeTabId: null });
  });

  it('inherits the flow canvas when flows are visible', () => {
    useUILayoutStore.setState({ areFlowsVisible: true });
    const tabId = useTabManagerStore.getState().createTab('New Tab');
    expect(useTabManagerStore.getState().getTab(tabId)?.viewMode).toBe('flow');
  });

  it('inherits the crew canvas when flows are not visible', () => {
    useUILayoutStore.setState({ areFlowsVisible: false });
    const tabId = useTabManagerStore.getState().createTab('New Tab');
    expect(useTabManagerStore.getState().getTab(tabId)?.viewMode).toBe('crew');
  });

  it('honors an explicit viewMode override regardless of current canvas', () => {
    // A crew load forces 'crew' even when the user was on the flow canvas.
    useUILayoutStore.setState({ areFlowsVisible: true });
    const tabId = useTabManagerStore.getState().createTab('Loaded Crew', 'crew');
    expect(useTabManagerStore.getState().getTab(tabId)?.viewMode).toBe('crew');
  });
});

describe('crew load forces crew view (wiring)', () => {
  it('passes an explicit "crew" view mode when loading a crew into a new tab', async () => {
    const { readFileSync } = await import('fs');
    const { resolve } = await import('path');
    const src = readFileSync(
      resolve(__dirname, '../components/WorkflowDesigner/WorkflowEventHandlers.ts'),
      'utf-8',
    );
    // Loading a crew must override the inherited canvas so it always lands on crew.
    expect(src).toContain("createTab(actualCrewName, 'crew')");
  });
});

describe('tabManager - updateTabFlowInfo renames the tab', () => {
  beforeEach(() => {
    useTabManagerStore.setState({ tabs: [], activeTabId: null });
  });

  it('adapts the canvas/tab name to the saved flow name', () => {
    const tabId = useTabManagerStore.getState().createTab('Canvas 1', 'flow');
    useTabManagerStore.getState().updateTabFlowInfo(tabId, 'flow-42', 'My Saved Flow');

    const tab = useTabManagerStore.getState().getTab(tabId);
    expect(tab?.name).toBe('My Saved Flow');
    expect(tab?.savedFlowId).toBe('flow-42');
    expect(tab?.savedFlowName).toBe('My Saved Flow');
    expect(tab?.isDirty).toBe(false);
  });
});

describe('tabManager - activateTabForViewMode', () => {
  const GROUP = 'group-1';

  beforeEach(() => {
    localStorage.setItem('selectedGroupId', GROUP);
    useTabManagerStore.setState({ tabs: [], activeTabId: null });
  });

  const seed = (
    tabs: Array<{ id: string; viewMode: 'crew' | 'flow'; lastModified: Date; group_id?: string }>,
    activeTabId: string,
  ) => {
    useTabManagerStore.setState({
      tabs: tabs.map(t => ({
        id: t.id,
        name: t.id,
        nodes: [],
        edges: [],
        flowNodes: [],
        flowEdges: [],
        viewMode: t.viewMode,
        isActive: t.id === activeTabId,
        isDirty: false,
        createdAt: new Date(0),
        lastModified: t.lastModified,
        group_id: t.group_id ?? GROUP,
      })),
      activeTabId,
    });
  };

  it('goes back to the flow tab instead of emptying the crew tab', () => {
    // Opening a crew from a flow node lands you on a new crew tab. Switching
    // back to Flow Builder used to relabel THAT tab, showing its empty flow
    // canvas while the real flow sat on the tab behind it.
    seed(
      [
        { id: 'flow-tab', viewMode: 'flow', lastModified: new Date(1000) },
        { id: 'crew-tab', viewMode: 'crew', lastModified: new Date(2000) },
      ],
      'crew-tab',
    );

    expect(useTabManagerStore.getState().activateTabForViewMode('flow')).toBe(true);
    expect(useTabManagerStore.getState().activeTabId).toBe('flow-tab');
  });

  it('stays put when the active tab is already that mode', () => {
    seed(
      [
        { id: 'flow-a', viewMode: 'flow', lastModified: new Date(1000) },
        { id: 'flow-b', viewMode: 'flow', lastModified: new Date(5000) },
      ],
      'flow-a',
    );

    expect(useTabManagerStore.getState().activateTabForViewMode('flow')).toBe(true);
    expect(useTabManagerStore.getState().activeTabId).toBe('flow-a');
  });

  it('picks the most recently touched matching tab', () => {
    seed(
      [
        { id: 'flow-old', viewMode: 'flow', lastModified: new Date(1000) },
        { id: 'flow-new', viewMode: 'flow', lastModified: new Date(9000) },
        { id: 'crew-tab', viewMode: 'crew', lastModified: new Date(2000) },
      ],
      'crew-tab',
    );

    useTabManagerStore.getState().activateTabForViewMode('flow');
    expect(useTabManagerStore.getState().activeTabId).toBe('flow-new');
  });

  it('reports false with no matching tab, so the caller converts in place', () => {
    // "Switch to Flow Builder to start a flow" has to keep working.
    seed([{ id: 'crew-tab', viewMode: 'crew', lastModified: new Date(1000) }], 'crew-tab');

    expect(useTabManagerStore.getState().activateTabForViewMode('flow')).toBe(false);
    expect(useTabManagerStore.getState().activeTabId).toBe('crew-tab');
  });

  it('ignores tabs belonging to another workspace', () => {
    seed(
      [
        { id: 'crew-tab', viewMode: 'crew', lastModified: new Date(1000) },
        { id: 'other-flow', viewMode: 'flow', lastModified: new Date(9000), group_id: 'group-2' },
      ],
      'crew-tab',
    );

    expect(useTabManagerStore.getState().activateTabForViewMode('flow')).toBe(false);
    expect(useTabManagerStore.getState().activeTabId).toBe('crew-tab');
  });
});
