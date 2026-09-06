import React from 'react';
import { describe, it, expect, beforeEach } from 'vitest';
import { render, screen, fireEvent, act } from '@testing-library/react';
import TrifectaNotice from './TrifectaNotice';
import { useExecutionStore } from '../../store/executionStore';

const setSelection = (mcp: string[], agentBricks: string[] = []) =>
  useExecutionStore.setState({
    selectedMcpServers: mcp,
    selectedAgentBricksEndpoints: agentBricks,
  });

beforeEach(() => {
  setSelection([], []);
});

describe('TrifectaNotice', () => {
  it('renders nothing when nothing is selected', () => {
    render(<TrifectaNotice />);
    expect(screen.queryByTestId('trifecta-notice')).toBeNull();
  });

  it('renders nothing for a single internal source', () => {
    setSelection(['Genie']);
    render(<TrifectaNotice />);
    expect(screen.queryByTestId('trifecta-notice')).toBeNull();
  });

  it('renders nothing for two internal sources with no egress channel', () => {
    setSelection(['Genie', 'Databricks SQL']);
    render(<TrifectaNotice />);
    expect(screen.queryByTestId('trifecta-notice')).toBeNull();
  });

  it('shows the notice when an internal source is combined with an unknown MCP server', () => {
    setSelection(['Genie', 'Some Custom Slack MCP']);
    render(<TrifectaNotice />);
    expect(screen.getByTestId('trifecta-notice')).toBeInTheDocument();
    expect(screen.getByText(/data-exfiltration risk/i)).toBeInTheDocument();
  });

  it('shows the notice for a selected Agent Bricks endpoint on its own', () => {
    setSelection([], ['my-agent']);
    render(<TrifectaNotice />);
    expect(screen.getByTestId('trifecta-notice')).toBeInTheDocument();
  });

  it('can be dismissed and stays dismissed for the same selection', () => {
    setSelection(['Genie', 'Some Custom Slack MCP']);
    render(<TrifectaNotice />);
    fireEvent.click(screen.getByLabelText('Dismiss security notice'));
    expect(screen.queryByTestId('trifecta-notice')).toBeNull();
  });

  it('reappears after dismissal when the selection changes', () => {
    setSelection(['Genie', 'Some Custom Slack MCP']);
    render(<TrifectaNotice />);
    fireEvent.click(screen.getByLabelText('Dismiss security notice'));
    expect(screen.queryByTestId('trifecta-notice')).toBeNull();

    // Adding another egress channel changes the signature → notice returns.
    act(() => setSelection(['Genie', 'Some Custom Slack MCP', 'Another Custom MCP']));
    expect(screen.getByTestId('trifecta-notice')).toBeInTheDocument();
  });

  it('names the internal source and the egress channel', () => {
    setSelection(['Genie', 'Some Custom Slack MCP']);
    render(<TrifectaNotice />);
    const notice = screen.getByTestId('trifecta-notice');
    expect(notice).toHaveTextContent('Genie');
    expect(notice).toHaveTextContent('Some Custom Slack MCP');
  });
});
