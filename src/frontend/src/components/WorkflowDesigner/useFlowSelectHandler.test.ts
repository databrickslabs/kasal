/**
 * useFlowSelectHandler — loading a flow onto the FlowCanvas.
 *
 * Covers the layout decision: a flow with a real saved layout keeps its branch
 * placement; a degenerate load (nodes stacked at one point) is auto-arranged into
 * a centered horizontal row.
 */
import { renderHook } from '@testing-library/react';
import { describe, it, expect, vi } from 'vitest';
import { Node, Edge } from 'reactflow';
import { useFlowSelectHandler } from './WorkflowEventHandlers';

const node = (id: string, x: number, y: number): Node => ({
  id,
  type: 'crewNode',
  position: { x, y },
  data: { label: id },
});

const edge = (source: string, target: string): Edge => ({
  id: `${source}-${target}`,
  source,
  target,
});

const captureNodes = () => {
  const setFlowNodes = vi.fn();
  const setFlowEdges = vi.fn();
  const { result } = renderHook(() => useFlowSelectHandler(setFlowNodes, setFlowEdges));
  return { run: result.current, setFlowNodes, setFlowEdges };
};

describe('useFlowSelectHandler', () => {
  it('auto-arranges a degenerate load into a horizontal row (same height)', () => {
    const { run, setFlowNodes } = captureNodes();
    // Two nodes piled at the same point -> no meaningful saved layout.
    run([node('a', 250, 150), node('b', 250, 150)], [edge('a', 'b')]);

    const laidOut = setFlowNodes.mock.calls[0][0] as Node[];
    expect(new Set(laidOut.map(n => n.position.y)).size).toBe(1); // same height
    expect(new Set(laidOut.map(n => n.position.x)).size).toBe(2); // spread out
  });

  it('retains the saved branch placement when the flow has a real layout', () => {
    const { run, setFlowNodes } = captureNodes();
    const saved = [node('a', 0, 0), node('b', 300, -120), node('c', 300, 120)];
    run(saved, [edge('a', 'b'), edge('a', 'c')]);

    const laidOut = setFlowNodes.mock.calls[0][0] as Node[];
    const ys = laidOut.map(n => n.position.y).sort((p, q) => p - q);
    // Distinct branch heights preserved (not flattened to a single row).
    expect(new Set(ys).size).toBeGreaterThan(1);
  });
});
