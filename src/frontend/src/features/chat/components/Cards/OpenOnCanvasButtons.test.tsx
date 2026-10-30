import React from 'react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import OpenOnCanvasButtons from './OpenOnCanvasButtons';
import { useExecutionStore } from '../../store/executionStore';

vi.mock('../../../../store/uiLayout', () => ({
  useUILayoutStore: (sel: (s: { setAppMode: () => void }) => unknown) =>
    sel({ setAppMode: vi.fn() }),
}));

// A generated crew with a plain (non-Genie) agent + task — no tool_configs of
// its own, so anything on the dispatched nodes must come from the chat picker.
const DATA = {
  agents: [{ id: 'a1', name: 'A', role: 'r', tools: ['SomeTool'] }],
  tasks: [{ id: 't1', name: 'T', tools: [], agent_id: 'a1' }],
} as never;

type CanvasEvent = CustomEvent<{
  nodes: Array<{ type: string; data: Record<string, unknown> }>;
}>;

beforeEach(() => {
  // Simulate the chat "+" picker having selected an MCP server + Agent Bricks endpoint.
  useExecutionStore.setState({
    memoryEnabled: false,
    selectedMcpServers: ['example-mcp'],
    selectedAgentBricksEndpoints: [],
  });
});

afterEach(() => {
  vi.restoreAllMocks();
});

describe('OpenOnCanvasButtons — chat-only users', () => {
  it('renders nothing when both builder capabilities are off', async () => {
    const { usePermissionStore } = await import('../../../../store/permissions');
    usePermissionStore.setState({ allowAgentBuilder: false, allowFlowBuilder: false });
    const { container } = render(
      <OpenOnCanvasButtons crewId="c1" flowId={undefined} />,
    );
    expect(container.firstChild).toBeNull();
    usePermissionStore.setState({ allowAgentBuilder: true, allowFlowBuilder: true });
  });
});

describe('OpenOnCanvasButtons — Open in Agent Builder', () => {
  it('retains the catalog identity so later canvas edits update the same crew', () => {
    const spy = vi.spyOn(window, 'dispatchEvent');
    render(<OpenOnCanvasButtons data={DATA} savedCrewId="catalog-42" savedName="Saved crew" />);
    fireEvent.click(screen.getByLabelText('Open in Agent Builder'));
    const event = spy.mock.calls.map(([event]) => event).find(event => event.type === 'catalogLoadCrew') as CustomEvent;
    expect(event.detail).toMatchObject({ id: 'catalog-42', name: 'Saved crew' });
  });
  it('carries the chat-selected MCP servers onto the canvas crew (tasks + agents)', () => {
    const events: CanvasEvent[] = [];
    const spy = vi
      .spyOn(window, 'dispatchEvent')
      .mockImplementation((e: Event) => {
        if (e.type === 'catalogLoadCrew') events.push(e as CanvasEvent);
        return true;
      });

    render(<OpenOnCanvasButtons data={DATA} />);
    fireEvent.click(screen.getByLabelText('Open in Agent Builder'));

    expect(spy).toHaveBeenCalled();
    const { nodes } = events[0].detail;
    const expected = { MCP_SERVERS: { servers: ['example-mcp'] } };
    expect(nodes.find((n) => n.type === 'agentNode')!.data.tool_configs).toEqual(expected);
    expect(nodes.find((n) => n.type === 'taskNode')!.data.tool_configs).toEqual(expected);
  });

  it('adds no MCP tool_configs when the picker has nothing selected', () => {
    useExecutionStore.setState({ selectedMcpServers: [], selectedAgentBricksEndpoints: [] });
    const events: CanvasEvent[] = [];
    vi.spyOn(window, 'dispatchEvent').mockImplementation((e: Event) => {
      if (e.type === 'catalogLoadCrew') events.push(e as CanvasEvent);
      return true;
    });

    render(<OpenOnCanvasButtons data={DATA} />);
    fireEvent.click(screen.getByLabelText('Open in Agent Builder'));

    const { nodes } = events[0].detail;
    expect(nodes.find((n) => n.type === 'taskNode')!.data.tool_configs).toBeUndefined();
  });
});

describe('OpenOnCanvasButtons — Open in Flow Builder', () => {
  const MULTI = {
    agents: [{ id: 'a1', name: 'A', role: 'r', tools: [] }],
    tasks: [
      { id: 't1', name: 'Fetch News', description: 'search', agent_id: 'a1' },
      { id: 't2', name: 'Summarize', description: 'synthesize', agent_id: 'a1' },
    ],
  } as never;

  it("populates the crew node's allTasks so the flow has starting points", async () => {
    // Regression: a bare flow node (no allTasks) makes buildFlowConfiguration emit
    // ZERO startingPoints — the flow loads but runs nothing. The node must mirror
    // the Flow canvas "add crew" shape: crew-<id>-<ts> id + allTasks from the crew.
    const events: CanvasEvent[] = [];
    vi.spyOn(window, 'dispatchEvent').mockImplementation((e: Event) => {
      if (e.type === 'catalogLoadFlow') events.push(e as CanvasEvent);
      return true;
    });

    render(<OpenOnCanvasButtons data={MULTI} savedCrewId="crew-123" savedName="News Crew" />);
    fireEvent.click(screen.getByLabelText('Open in Flow Builder'));

    await vi.waitFor(() => expect(events.length).toBe(1));
    const node = events[0].detail.nodes[0];
    expect(node.type).toBe('crewNode');
    expect(String(node.id)).toMatch(/^crew-crew-123-\d+$/);
    expect(node.data.id).toBe('crew-crew-123');
    expect(node.data.crewId).toBe('crew-123');
    const allTasks = node.data.allTasks as Array<{ id: string; name: string }>;
    expect(allTasks).toEqual([
      { id: 't1', name: 'Fetch News', description: 'search' },
      { id: 't2', name: 'Summarize', description: 'synthesize' },
    ]);
  });
});
