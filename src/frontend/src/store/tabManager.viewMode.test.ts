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
