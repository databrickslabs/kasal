import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import ChatEmptyState from './ChatEmptyState';
import { useExecutionStore } from '../../store/executionStore';
import { useUILayoutStore } from '../../../../store/uiLayout';
import { useFlowConfigStore } from '../../../../store/flowConfig';
import { useAppStore } from '../../store/appStore';

const setAppMode = vi.fn();

beforeEach(() => {
  vi.clearAllMocks();
  useUILayoutStore.setState({ setAppMode });
  useFlowConfigStore.setState({ kasalFlowEnabled: true });
  useExecutionStore.setState({ chatModeType: 'chat' });
  useAppStore.setState({ models: [], selectedModel: '' });
});

const asModel = (key: string, supports_reasoning_effort: boolean) =>
  ({
    id: 1,
    key,
    name: key,
    provider: 'openai',
    temperature: 1,
    context_window: 128000,
    max_output_tokens: 32000,
    extended_thinking: false,
    enabled: true,
    supports_reasoning_effort,
    created_at: '',
    updated_at: '',
  }) as never;

describe('ChatEmptyState', () => {
        it('switches to Agent Builder from the builder bridge', () => {
    render(<ChatEmptyState onPrefill={vi.fn()} />);
    fireEvent.click(screen.getByRole('button', { name: 'Agent Builder' }));
    expect(setAppMode).toHaveBeenCalledWith('crew');
  });

  it('offers Flow Builder only when the flow feature is enabled', () => {
    useFlowConfigStore.setState({ kasalFlowEnabled: false });
    const { rerender } = render(<ChatEmptyState onPrefill={vi.fn()} />);
    expect(screen.queryByRole('button', { name: 'Flow Builder' })).toBeNull();

    useFlowConfigStore.setState({ kasalFlowEnabled: true });
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

// A model with no reasoning budget makes Deep Research identical to Research —
// same crew, same tools, both efforts dropped by the engine. Offering it would
// promise a difference that cannot happen.
