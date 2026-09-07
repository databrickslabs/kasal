import React from 'react';
import { act, fireEvent, render } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { ConceptForceGraph } from './ConceptForceGraph';

const fake = vi.hoisted(() => {
  const methods: Record<string, ReturnType<typeof vi.fn>> = {};
  let data: { nodes: object[]; links: object[] } = { nodes: [], links: [] };
  const graph: object = new Proxy({}, {
    get: (_, key: string) => {
      if (!methods[key]) methods[key] = vi.fn((...args: unknown[]) => {
        if (key === 'graphData') {
          if (!args.length) return data;
          data = args[0] as typeof data;
          data.nodes.forEach((node, index) => Object.assign(node, { x: index * 250, y: index * 100 }));
        }
        if (key === 'd3Force') return undefined;
        if (key === 'zoom' && !args.length) return 1;
        return graph;
      });
      return methods[key];
    },
  });
  return { graph, methods, clearData: () => { data = { nodes: [], links: [] }; } };
});
vi.mock('force-graph', () => ({ default: () => () => fake.graph }));

let resize: () => void;
let width: number;
beforeEach(() => {
  vi.clearAllMocks(); fake.clearData(); width = 600;
  vi.stubGlobal('ResizeObserver', class {
    constructor(callback: () => void) { resize = callback; }
    observe() {} disconnect() {}
  });
  vi.stubGlobal('requestAnimationFrame', vi.fn(() => 1));
  vi.stubGlobal('cancelAnimationFrame', vi.fn());
  vi.spyOn(HTMLElement.prototype, 'getBoundingClientRect').mockImplementation(() => ({
    x: 0, y: 0, left: 0, top: 0, right: width, bottom: 380, width, height: 380, toJSON: () => ({}),
  }));
});
afterEach(() => { vi.restoreAllMocks(); vi.unstubAllGlobals(); });
const nodes = [{ id: 'one', label: 'One', count: 3, avgImportance: 0.8 }, { id: 'two', label: 'Two', count: 1, avgImportance: 0.5 }];
const props = { nodes, edges: [], activeIds: new Set<string>(), onToggleNode: vi.fn(), importanceColor: () => '#658373' };
const engine = (name: string) => act(() => fake.methods[name].mock.calls.at(-1)![0]());

describe('memory graph viewport', () => {
  it('fits records that arrive after mount, then fits their settled layout', () => {
    const view = render(<ConceptForceGraph {...props} nodes={[]} />);
    engine('onEngineTick');
    expect(fake.methods.zoomToFit).toBeUndefined();
    view.rerender(<ConceptForceGraph {...props} />);
    engine('onEngineTick');
    expect(fake.methods.zoomToFit).toHaveBeenLastCalledWith(0, 56);
    engine('onEngineStop');
    expect(fake.methods.zoomToFit).toHaveBeenLastCalledWith(250, 56);
  });

  it('does not snap back after the user zooms while physics is running', () => {
    const view = render(<ConceptForceGraph {...props} />);
    engine('onEngineTick');
    fake.methods.zoomToFit.mockClear();
    const canvasHost = view.container.firstElementChild!.firstElementChild!;
    fireEvent.wheel(canvasHost);
    engine('onEngineStop');
    expect(fake.methods.zoomToFit).not.toHaveBeenCalled();
  });

  it('refits after a pane resize but ignores identical size notifications', () => {
    render(<ConceptForceGraph {...props} />);
    vi.mocked(requestAnimationFrame).mockClear();
    act(() => resize());
    expect(requestAnimationFrame).not.toHaveBeenCalled();
    width = 320;
    act(() => resize());
    expect(fake.methods.width).toHaveBeenLastCalledWith(320);
    act(() => vi.mocked(requestAnimationFrame).mock.calls.at(-1)![0](0));
    expect(fake.methods.zoomToFit).toHaveBeenLastCalledWith(0, 56);
  });
});
