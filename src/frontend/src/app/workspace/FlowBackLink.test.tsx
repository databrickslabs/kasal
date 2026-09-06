import React from 'react';
import { describe, it, expect, beforeEach, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { ThemeProvider, createTheme } from '@mui/material';
import FlowBackLink from './FlowBackLink';
import { useTabManagerStore } from '../../store/tabManager';
import { useUILayoutStore } from '../../store/uiLayout';

const theme = createTheme();
const renderLink = () =>
  render(
    <ThemeProvider theme={theme}>
      <FlowBackLink />
    </ThemeProvider>
  );

const base = {
  name: 'tab',
  nodes: [],
  edges: [],
  flowNodes: [],
  flowEdges: [],
  viewMode: 'crew' as const,
  isActive: false,
  isDirty: false,
  createdAt: new Date(0),
  lastModified: new Date(0),
  group_id: 'g1',
};

const crewNode = (crewId: string) => ({
  id: `crew-${crewId}`,
  type: 'crewNode',
  position: { x: 0, y: 0 },
  data: { crewId, label: 'A Crew' },
});

describe('FlowBackLink', () => {
  beforeEach(() => {
    vi.restoreAllMocks();
    useTabManagerStore.setState({ tabs: [], activeTabId: null });
  });

  it('links back to the open flow that uses this crew', () => {
    useTabManagerStore.setState({
      tabs: [
        { ...base, id: 'crew-tab', savedCrewId: '7', isActive: true },
        {
          ...base,
          id: 'flow-tab',
          viewMode: 'flow',
          flowNodes: [crewNode('7')],
          savedFlowName: 'Top News Flow',
        },
      ],
      activeTabId: 'crew-tab',
    });

    renderLink();
    fireEvent.click(screen.getByText('In flow: Top News Flow'));

    expect(useTabManagerStore.getState().activeTabId).toBe('flow-tab');
    expect(useUILayoutStore.getState().appMode).toBe('flow');
  });

  it('points the target tab at its flow canvas, not whichever it last showed', () => {
    // A flow tab left on its crew side would otherwise be restored to that side
    // by the tab-switch effect, right after this asked for the flow.
    useTabManagerStore.setState({
      tabs: [
        { ...base, id: 'crew-tab', savedCrewId: '7', isActive: true },
        { ...base, id: 'flow-tab', viewMode: 'crew', flowNodes: [crewNode('7')] },
      ],
      activeTabId: 'crew-tab',
    });

    renderLink();
    fireEvent.click(screen.getByText('In flow: tab'));

    expect(
      useTabManagerStore.getState().tabs.find((t) => t.id === 'flow-tab')?.viewMode
    ).toBe('flow');
  });

  it('shows nothing for a crew that was never saved', () => {
    // Nothing on the flow canvas could be pointing at it.
    useTabManagerStore.setState({
      tabs: [
        { ...base, id: 'crew-tab', isActive: true },
        { ...base, id: 'flow-tab', flowNodes: [crewNode('7')] },
      ],
      activeTabId: 'crew-tab',
    });

    const { container } = renderLink();
    expect(container).toBeEmptyDOMElement();
  });

  it('shows nothing when no open flow uses the crew', () => {
    useTabManagerStore.setState({
      tabs: [
        { ...base, id: 'crew-tab', savedCrewId: '7', isActive: true },
        { ...base, id: 'flow-tab', flowNodes: [crewNode('8')] },
      ],
      activeTabId: 'crew-tab',
    });

    expect(renderLink().container).toBeEmptyDOMElement();
  });

  it('ignores a flow belonging to another workspace', () => {
    useTabManagerStore.setState({
      tabs: [
        { ...base, id: 'crew-tab', savedCrewId: '7', isActive: true },
        { ...base, id: 'other', group_id: 'g2', flowNodes: [crewNode('7')] },
      ],
      activeTabId: 'crew-tab',
    });

    expect(renderLink().container).toBeEmptyDOMElement();
  });

  it('offers the choice when several open flows use the crew', () => {
    useTabManagerStore.setState({
      tabs: [
        { ...base, id: 'crew-tab', savedCrewId: '7', isActive: true },
        {
          ...base,
          id: 'flow-a',
          flowNodes: [crewNode('7')],
          savedFlowName: 'Morning Digest',
          lastModified: new Date(9000),
        },
        {
          ...base,
          id: 'flow-b',
          flowNodes: [crewNode('7')],
          savedFlowName: 'Evening Digest',
          lastModified: new Date(1000),
        },
      ],
      activeTabId: 'crew-tab',
    });

    renderLink();
    fireEvent.click(screen.getByText('In 2 flows'));

    expect(screen.getByText('Morning Digest')).toBeInTheDocument();
    fireEvent.click(screen.getByText('Evening Digest'));

    expect(useTabManagerStore.getState().activeTabId).toBe('flow-b');
  });
});
