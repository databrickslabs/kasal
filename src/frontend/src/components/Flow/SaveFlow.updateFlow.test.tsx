/**
 * SaveFlow "update existing flow" data source.
 *
 * Regression for: saving a flow landed crew-canvas content (or nothing) in the
 * flow catalog because the update path read tab.nodes/tab.edges (crew canvas)
 * instead of tab.flowNodes/tab.flowEdges (flow canvas).
 */
import React from 'react';
import { render } from '@testing-library/react';
import { describe, it, expect, beforeEach, vi } from 'vitest';
import { Node, Edge } from 'reactflow';
import SaveFlow from './SaveFlow';
import { useTabManagerStore } from '../../store/tabManager';
import { FlowService } from '../../api/FlowService';

vi.mock('../../api/FlowService', () => ({
  FlowService: {
    updateFlow: vi.fn().mockResolvedValue({ id: 'flow-1', name: 'My Flow' }),
  },
}));

const crewNode = (id: string): Node => ({
  id,
  type: 'crewNode',
  position: { x: 0, y: 0 },
  data: { crewId: 'crew-1', label: id },
});

const flowEdge = (source: string, target: string): Edge => ({
  id: `${source}-${target}`,
  source,
  target,
});

describe('SaveFlow - updateExistingFlow', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    useTabManagerStore.setState({ tabs: [], activeTabId: null });
  });

  it('persists the flow canvas nodes/edges, not the crew canvas ones', async () => {
    const tabId = useTabManagerStore.getState().createTab('My Flow', 'flow');
    // Crew canvas content that must NOT be saved as the flow.
    useTabManagerStore.getState().updateTabNodes(tabId, [crewNode('crew-canvas-node')]);
    useTabManagerStore.getState().updateTabEdges(tabId, []);
    // Flow canvas content that SHOULD be saved.
    useTabManagerStore.getState().updateTabFlowNodes(tabId, [
      crewNode('flow-node-a'),
      crewNode('flow-node-b'),
    ]);
    useTabManagerStore.getState().updateTabFlowEdges(tabId, [flowEdge('flow-node-a', 'flow-node-b')]);

    render(<SaveFlow nodes={[]} edges={[]} trigger={<button>save</button>} />);

    window.dispatchEvent(
      new CustomEvent('updateExistingFlow', { detail: { tabId, flowId: 'flow-1' } }),
    );

    // Let the async handler run.
    await vi.waitFor(() => expect(FlowService.updateFlow).toHaveBeenCalled());

    const [, payload] = (FlowService.updateFlow as ReturnType<typeof vi.fn>).mock.calls[0];
    const savedNodeIds = (payload.nodes as Node[]).map(n => n.id).sort();
    expect(savedNodeIds).toEqual(['flow-node-a', 'flow-node-b']);
    // The crew-canvas node must never reach the flow catalog.
    expect(savedNodeIds).not.toContain('crew-canvas-node');
    expect((payload.edges as Edge[]).map(e => e.id)).toEqual(['flow-node-a-flow-node-b']);
  });
});
