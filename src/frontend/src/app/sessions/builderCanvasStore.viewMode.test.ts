/**
 * createCanvas view-mode inheritance.
 *
 * Adding a new tab while looking at the flow canvas must keep the flow canvas —
 * previously every new tab was hardcoded to viewMode 'crew', which the tab-switch
 * reconciliation effect then used to snap the user back to the crew canvas.
 */
import { describe, it, expect, beforeEach } from 'vitest';
import { useBuilderCanvasStore } from './builderCanvasStore';
import { useUILayoutStore } from '../../store/uiLayout';

describe('builderCanvasStore - createCanvas view mode', () => {
  beforeEach(() => {
    useBuilderCanvasStore.setState({ canvases: [], activeCanvasId: null });
  });

  it('inherits the flow canvas when flows are visible', () => {
    useUILayoutStore.setState({ areFlowsVisible: true });
    const tabId = useBuilderCanvasStore.getState().createCanvas('New Tab');
    expect(useBuilderCanvasStore.getState().getCanvas(tabId)?.viewMode).toBe('flow');
  });

  it('inherits the crew canvas when flows are not visible', () => {
    useUILayoutStore.setState({ areFlowsVisible: false });
    const tabId = useBuilderCanvasStore.getState().createCanvas('New Tab');
    expect(useBuilderCanvasStore.getState().getCanvas(tabId)?.viewMode).toBe('crew');
  });

  it('honors an explicit viewMode override regardless of current canvas', () => {
    // A crew load forces 'crew' even when the user was on the flow canvas.
    useUILayoutStore.setState({ areFlowsVisible: true });
    const tabId = useBuilderCanvasStore.getState().createCanvas('Loaded Crew', 'crew');
    expect(useBuilderCanvasStore.getState().getCanvas(tabId)?.viewMode).toBe('crew');
  });
});

describe('crew load forces crew view (wiring)', () => {
  it('passes an explicit "crew" view mode when loading a crew into a new tab', async () => {
    const { readFileSync } = await import('fs');
    const { resolve } = await import('path');
    const src = readFileSync(
      resolve(__dirname, '../workspace/WorkflowEventHandlers.ts'),
      'utf-8',
    );
    // Loading a crew must override the inherited canvas so it always lands on crew.
    expect(src).toContain("createCanvas(actualCrewName, 'crew')");
  });
});

describe('builderCanvasStore - updateCanvasFlowInfo renames the tab', () => {
  beforeEach(() => {
    useBuilderCanvasStore.setState({ canvases: [], activeCanvasId: null });
  });

  it('adapts the canvas/tab name to the saved flow name', () => {
    const tabId = useBuilderCanvasStore.getState().createCanvas('Canvas 1', 'flow');
    useBuilderCanvasStore.getState().updateCanvasFlowInfo(tabId, 'flow-42', 'My Saved Flow');

    const tab = useBuilderCanvasStore.getState().getCanvas(tabId);
    expect(tab?.name).toBe('My Saved Flow');
    expect(tab?.savedFlowId).toBe('flow-42');
    expect(tab?.savedFlowName).toBe('My Saved Flow');
    expect(tab?.isDirty).toBe(false);
  });
});

