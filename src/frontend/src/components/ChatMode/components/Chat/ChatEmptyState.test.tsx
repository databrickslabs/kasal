import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import ChatEmptyState from './ChatEmptyState';
import { useExecutionStore } from '../../store/executionStore';
import { useUILayoutStore } from '../../../../store/uiLayout';
import { useFlowConfigStore } from '../../../../store/flowConfig';

const setAppMode = vi.fn();

beforeEach(() => {
  vi.clearAllMocks();
  useUILayoutStore.setState({ setAppMode });
  useFlowConfigStore.setState({ crewAIFlowEnabled: true });
  useExecutionStore.setState({ chatModeType: 'chat' });
});

describe('ChatEmptyState', () => {
  it('renders the three answer-mode chips', () => {
    render(<ChatEmptyState onPrefill={vi.fn()} />);
    expect(screen.getByText('Chat')).toBeInTheDocument();
    expect(screen.getByText('Research')).toBeInTheDocument();
    expect(screen.getByText('Deep Research')).toBeInTheDocument();
  });

  it('selects the answer mode AND seeds a starter prompt when a chip is picked', () => {
    const onPrefill = vi.fn();
    render(<ChatEmptyState onPrefill={onPrefill} />);

    fireEvent.click(screen.getByText('Research'));
    expect(useExecutionStore.getState().chatModeType).toBe('research');
    expect(onPrefill).toHaveBeenCalledTimes(1);
    expect(onPrefill.mock.calls[0][0]).toMatch(/Research \[topic\]/i);

    fireEvent.click(screen.getByText('Deep Research'));
    expect(useExecutionStore.getState().chatModeType).toBe('deep');
    expect(onPrefill.mock.calls[1][0]).toMatch(/deep-dive analysis/i);
  });

  it('marks the active answer-mode chip as pressed', () => {
    useExecutionStore.setState({ chatModeType: 'deep' });
    render(<ChatEmptyState onPrefill={vi.fn()} />);
    expect(screen.getByText('Deep Research').closest('button')!).toHaveAttribute('aria-pressed', 'true');
    expect(screen.getByText('Chat').closest('button')!).toHaveAttribute('aria-pressed', 'false');
  });

  it('switches to Agent Builder from the builder bridge', () => {
    render(<ChatEmptyState onPrefill={vi.fn()} />);
    fireEvent.click(screen.getByRole('button', { name: 'Agent Builder' }));
    expect(setAppMode).toHaveBeenCalledWith('crew');
  });

  it('offers Flow Builder only when the flow feature is enabled', () => {
    useFlowConfigStore.setState({ crewAIFlowEnabled: false });
    const { rerender } = render(<ChatEmptyState onPrefill={vi.fn()} />);
    expect(screen.queryByRole('button', { name: 'Flow Builder' })).toBeNull();

    useFlowConfigStore.setState({ crewAIFlowEnabled: true });
    rerender(<ChatEmptyState onPrefill={vi.fn()} />);
    fireEvent.click(screen.getByRole('button', { name: 'Flow Builder' }));
    expect(setAppMode).toHaveBeenCalledWith('flow');
  });

  it('links to the docs (new tab) and opens an absolute URL imperatively', () => {
    // The Databricks Apps iframe defeats bare target="_blank"; clicking must open
    // an absolute /docs URL via window.open so it escapes into a real new tab.
    const openSpy = vi.spyOn(window, 'open').mockReturnValue(null);
    render(<ChatEmptyState onPrefill={vi.fn()} />);
    const docs = screen.getByRole('link', { name: 'Check the docs' });
    expect(docs).toHaveAttribute('href', '/docs'); // middle-click / keyboard fallback
    expect(docs).toHaveAttribute('target', '_blank');
    fireEvent.click(docs);
    expect(openSpy).toHaveBeenCalledWith(
      `${window.location.origin}/docs`,
      '_blank',
      'noopener,noreferrer',
    );
    openSpy.mockRestore();
  });
});
