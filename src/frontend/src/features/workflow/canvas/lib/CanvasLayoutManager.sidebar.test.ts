import { describe, it, expect } from 'vitest';
import { CanvasLayoutManager } from './CanvasLayoutManager';
describe('Builder panel space', () => {
  it('reserves history width and prompt height, preserving canvas width for nodes', () => {
    const manager = new CanvasLayoutManager();
    manager.updateUIState({ areFlowsVisible: false, screenWidth: 1512, screenHeight: 982, chatPanelVisible: false, executionHistoryVisible: false });
    const full = manager.getAvailableCanvasArea();
    manager.updateUIState({ executionHistoryVisible: true });
    const history = manager.getAvailableCanvasArea();
    expect(history.width).toBe(full.width - 268);
    expect(history.height).toBe(full.height);
    manager.updateUIState({ chatPanelVisible: true, assistantDockHeight: 128 });
    const both = manager.getAvailableCanvasArea();
    expect(both.width).toBe(history.width);
    expect(both.height).toBe(history.height - 128);
  });
  it('does not reserve horizontal space for a temporary mobile history drawer', () => {
    const manager = new CanvasLayoutManager();
    manager.updateUIState({ screenWidth: 390, screenHeight: 844, chatPanelVisible: false, executionHistoryVisible: false });
    const full = manager.getAvailableCanvasArea();
    manager.updateUIState({ executionHistoryVisible: true });
    expect(manager.getAvailableCanvasArea().width).toBe(full.width);
  });
});


it('reserves the main response area and gives the right canvas its full height', () => {
  const manager = new CanvasLayoutManager();
  manager.updateUIState({ screenWidth: 1500, screenHeight: 1000, areFlowsVisible: false, leftSidebarExpanded: false, assistantPanelVisible: true, assistantResponseFocused: true, executionHistoryVisible: true, chatPanelVisible: true, assistantDockHeight: 180 });
  const area = manager.getAvailableCanvasArea('crew');
  expect(area.x).toBeCloseTo(48 + (1500 - 96) * 0.62 + 20);
  expect(area.width).toBeCloseTo((1500 - 96) * 0.38 - 40);
  expect(area.height).toBe(1000 - 48 - 40);
});
