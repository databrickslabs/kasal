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
describe('ChatEmptyState answer modes vs model capability', () => {
  const pickDeepCard = () =>
    screen.getByText('Deep Research').closest('button') as HTMLButtonElement;

  it('disables Deep Research for a model with no reasoning budget', () => {
    useAppStore.setState({
      models: [asModel('Qwen3-Coder-30B-A3B-Instruct', false)],
      selectedModel: 'Qwen3-Coder-30B-A3B-Instruct',
    });
    render(<ChatEmptyState onPrefill={vi.fn()} />);

    expect(pickDeepCard()).toBeDisabled();
    expect(pickDeepCard().title).toContain('Qwen3-Coder-30B-A3B-Instruct');
    expect(screen.getByText('Needs a model with a reasoning budget')).toBeInTheDocument();
  });

  it('keeps Research enabled there — a crew is a real difference on any model', () => {
    useAppStore.setState({
      models: [asModel('Qwen3-Coder-30B-A3B-Instruct', false)],
      selectedModel: 'Qwen3-Coder-30B-A3B-Instruct',
    });
    const onPrefill = vi.fn();
    render(<ChatEmptyState onPrefill={onPrefill} />);

    const research = screen.getByText('Research').closest('button') as HTMLButtonElement;
    expect(research).not.toBeDisabled();
    fireEvent.click(research);
    expect(onPrefill).toHaveBeenCalled();
    // ...but it no longer claims reasoning it cannot do.
    expect(screen.getByText('Full multi-agent crew')).toBeInTheDocument();
  });

  it('offers both, with the reasoning wording, for a reasoning-capable model', () => {
    useAppStore.setState({
      models: [asModel('gpt-5.6-terra', true)],
      selectedModel: 'gpt-5.6-terra',
    });
    render(<ChatEmptyState onPrefill={vi.fn()} />);

    expect(pickDeepCard()).not.toBeDisabled();
    expect(screen.getByText('Deep tools with maximum reasoning')).toBeInTheDocument();
    expect(screen.getByText('Full crew with reasoning')).toBeInTheDocument();
  });

  it('does not disable anything while the model list is still loading', () => {
    render(<ChatEmptyState onPrefill={vi.fn()} />);
    expect(pickDeepCard()).not.toBeDisabled();
  });
});
