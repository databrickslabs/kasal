import { useState } from 'react';
import { act, renderHook } from '@testing-library/react';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import type { Node, Edge } from 'reactflow';
import { useBuilderCanvasStore } from '../../app/sessions/builderCanvasStore';
import { useBuilderCanvasSync } from './useBuilderCanvasSync';

beforeEach(() => {
  vi.useFakeTimers();
  useBuilderCanvasStore.setState({ canvases: [], activeCanvasId: null });
});
afterEach(() => vi.useRealTimers());

it('continues syncing edits when a crew loads during an unfinished session switch', () => {
  const tabs = useBuilderCanvasStore.getState();
  tabs.createCanvas('New session', 'crew');
  const { result } = renderHook(() => {
    const [nodes, setNodes] = useState<Node[]>([]);
    const [edges, setEdges] = useState<Edge[]>([]);
    useBuilderCanvasSync({ nodes, edges, setNodes, setEdges });
    return { setNodes };
  });
  const original: Node[] = [{ id: 'agent', type: 'agentNode', position: { x: 0, y: 0 }, data: { goal: 'Original' } }];
  let loadedId = '';
  act(() => {
    window.dispatchEvent(new CustomEvent('crewLoadStarted'));
    loadedId = tabs.createCanvas('Loaded crew', 'crew');
    tabs.updateCanvasNodes(loadedId, original);
    tabs.updateCanvasCrewInfo(loadedId, 'catalog-42', 'Loaded crew');
    result.current.setNodes(original);
  });
  act(() => window.dispatchEvent(new CustomEvent('crewLoadCompleted')));
  act(() => vi.advanceTimersByTime(1000));
  act(() => result.current.setNodes([{ ...original[0], data: { goal: 'Edited instructions' } }]));
  act(() => vi.advanceTimersByTime(300));
  expect(tabs.getCanvas(loadedId)?.nodes[0].data.goal).toBe('Edited instructions');
});
