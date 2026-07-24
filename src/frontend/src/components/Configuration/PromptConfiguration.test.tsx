/**
 * Tests for PromptConfiguration.
 *
 * Covers the PR-introduced behavior: the per-template Optimize (✨) action
 * appears ONLY for templates wired in OPTIMIZABLE_TEMPLATES and only when an
 * onOptimize handler is provided, and clicking it hands the template name to
 * the parent. Also covers the base list rendering and the edit dialog opening.
 */

import { describe, it, expect, vi, beforeEach, Mock } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import PromptConfiguration from './PromptConfiguration';
import { PromptService } from '../../api/PromptService';

vi.mock('../../api/PromptService', () => {
  const instance = {
    getAllPrompts: vi.fn(),
    updatePrompt: vi.fn(),
    resetPromptTemplates: vi.fn(),
  };
  return {
    PromptService: {
      getInstance: () => instance,
    },
  };
});

const service = PromptService.getInstance() as unknown as {
  getAllPrompts: Mock;
  updatePrompt: Mock;
  resetPromptTemplates: Mock;
};

const PROMPTS = [
  {
    id: 1,
    name: 'detect_intent',
    description: 'Routes chat intents',
    template: 'You are an intent detector.',
  },
  {
    id: 2,
    name: 'improve_prompt',
    description: 'Not optimizable',
    template: 'You improve prompts.',
  },
];

beforeEach(() => {
  vi.clearAllMocks();
  service.getAllPrompts.mockResolvedValue(PROMPTS);
});

describe('PromptConfiguration', () => {
  it('renders the fetched prompt list', async () => {
    render(<PromptConfiguration />);
    expect(await screen.findByText('detect_intent')).toBeInTheDocument();
    expect(screen.getByText('improve_prompt')).toBeInTheDocument();
  });

  it('shows the Optimize action only for optimizable templates', async () => {
    render(<PromptConfiguration onOptimize={vi.fn()} />);
    await screen.findByText('detect_intent');
    // One optimizable row (detect_intent) → exactly one optimize button.
    const optimizeButtons = screen.getAllByRole('button', {
      name: /optimize/i,
    });
    expect(optimizeButtons).toHaveLength(1);
  });

  it('hides all Optimize actions when no handler is provided', async () => {
    render(<PromptConfiguration />);
    await screen.findByText('detect_intent');
    expect(
      screen.queryByRole('button', { name: /optimize/i }),
    ).not.toBeInTheDocument();
  });

  it('hands the template name to onOptimize on click', async () => {
    const onOptimize = vi.fn();
    render(<PromptConfiguration onOptimize={onOptimize} />);
    await screen.findByText('detect_intent');
    await userEvent.click(screen.getByRole('button', { name: /optimize/i }));
    expect(onOptimize).toHaveBeenCalledWith('detect_intent');
  });

  it('opens the edit dialog with the template content', async () => {
    render(<PromptConfiguration />);
    await screen.findByText('detect_intent');
    const editButtons = screen.getAllByTestId('EditIcon');
    await userEvent.click(editButtons[0].closest('button') as HTMLElement);
    await waitFor(() =>
      expect(
        screen.getByDisplayValue('You are an intent detector.'),
      ).toBeInTheDocument(),
    );
  });
});
