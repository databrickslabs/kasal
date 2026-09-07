import { act, cleanup, renderHook } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { Node, ReactFlowInstance } from 'reactflow';
import { useUIFitView } from './useUIFitView';
import { useUILayoutStore } from '../../store/uiLayout';

beforeEach(() => {
  vi.useFakeTimers();
  vi.stubGlobal('requestAnimationFrame', (cb: FrameRequestCallback) => setTimeout(() => cb(0), 16));
  vi.stubGlobal('cancelAnimationFrame', (id: number) => clearTimeout(id));
  useUILayoutStore.setState({ areFlowsVisible: false, chatPanelVisible: true, assistantDockHeight: 180, executionHistoryVisible: true, assistantPanelSide: 'right' });
});
afterEach(() => { cleanup(); vi.useRealTimers(); vi.unstubAllGlobals(); });

const makeNode = (id: string, y: number): Node => ({ id, type: 'taskNode', position: { x: 400, y }, data: {}, width: 270, height: 230 });

it('waits for new node measurements, fits all nodes, and leaves later dragging alone', () => {
  let currentNodes = [makeNode('first', 0)];
  const setViewport = vi.fn();
  const crewFlowInstanceRef = { current: { getNodes: () => currentNodes, setViewport } as unknown as ReactFlowInstance };
  const flowFlowInstanceRef = { current: null };
  const { rerender } = renderHook(({ nodes }) => useUIFitView({ nodes, crewFlowInstanceRef, flowFlowInstanceRef }), { initialProps: { nodes: currentNodes } });
  act(() => vi.advanceTimersByTime(32));
  const initialZoom = setViewport.mock.calls.at(-1)?.[0].zoom;
  setViewport.mockClear();
  currentNodes = [...currentNodes, { ...makeNode('second', 900), width: undefined, height: undefined }];
  rerender({ nodes: currentNodes });
  act(() => vi.advanceTimersByTime(32));
  expect(setViewport).not.toHaveBeenCalled();
  currentNodes = currentNodes.map(node => ({ ...node, width: 270, height: 230 }));
  act(() => vi.advanceTimersByTime(32));
  expect(setViewport).toHaveBeenCalledTimes(1);
  expect(setViewport.mock.calls[0][0].zoom).toBeLessThan(initialZoom);
  setViewport.mockClear();
  currentNodes = currentNodes.map(node => ({ ...node, position: { ...node.position, x: 500 } }));
  rerender({ nodes: currentNodes });
  act(() => vi.advanceTimersByTime(100));
  expect(setViewport).not.toHaveBeenCalled();
});

it('centers nodes inside the measured canvas when the session sidebar changes width', () => {
  const host = document.createElement('div');
  host.setAttribute('data-crew-container', '');
  const canvas = document.createElement('div'); canvas.className = 'react-flow'; host.appendChild(canvas); document.body.appendChild(host);
  let width = 700;
  vi.spyOn(canvas, 'getBoundingClientRect').mockImplementation(() => ({ left: 700, top: 48, width, height: 700 } as DOMRect));
  const currentNodes = [makeNode('task', 0)];
  const setViewport = vi.fn();
  const crewFlowInstanceRef = { current: { getNodes: () => currentNodes, setViewport } as unknown as ReactFlowInstance };
  const { result } = renderHook(() => useUIFitView({ nodes: currentNodes, crewFlowInstanceRef, flowFlowInstanceRef: { current: null } }));
  act(() => result.current.handleUIAwareFitView());
  let viewport = setViewport.mock.calls.at(-1)![0];
  expect(viewport.x + (400 + 270 / 2) * viewport.zoom).toBeCloseTo(width / 2);
  width = 480;
  act(() => result.current.handleUIAwareFitView());
  viewport = setViewport.mock.calls.at(-1)![0];
  expect(viewport.x + (400 + 270 / 2) * viewport.zoom).toBeCloseTo(width / 2);
  expect(currentNodes[0].position).toEqual({ x: 400, y: 0 });
  host.remove();
});
