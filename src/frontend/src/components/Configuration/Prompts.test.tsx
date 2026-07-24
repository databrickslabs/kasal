/**
 * Tests for the Prompts surface: the template list is the single entry point,
 * and a row's Optimize action opens the optimization dialog scoped to that
 * template (closable via the dialog's close button).
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import Prompts from './Prompts';

vi.mock('./PromptConfiguration', () => ({
  default: ({ onOptimize }: { onOptimize?: (name: string) => void }) => (
    <button onClick={() => onOptimize && onOptimize('detect_intent')}>
      fake-optimize-detect_intent
    </button>
  ),
}));

vi.mock('./PromptOptimization', () => ({
  default: ({ fixedTemplate }: { fixedTemplate?: string }) => (
    <div>fake-optimization-panel:{fixedTemplate}</div>
  ),
}));

beforeEach(() => {
  vi.clearAllMocks();
});

describe('Prompts', () => {
  it('renders the template list without the optimization dialog', () => {
    render(<Prompts />);
    expect(screen.getByText('fake-optimize-detect_intent')).toBeInTheDocument();
    expect(screen.queryByText(/fake-optimization-panel/)).not.toBeInTheDocument();
  });

  it('opens the optimization dialog scoped to the chosen template', async () => {
    render(<Prompts />);
    await userEvent.click(screen.getByText('fake-optimize-detect_intent'));
    expect(
      await screen.findByText('fake-optimization-panel:detect_intent'),
    ).toBeInTheDocument();
  });

  it('closes the dialog via the close button', async () => {
    render(<Prompts />);
    await userEvent.click(screen.getByText('fake-optimize-detect_intent'));
    await screen.findByText('fake-optimization-panel:detect_intent');
    await userEvent.click(screen.getByTestId('CloseIcon').closest('button') as HTMLElement);
    await waitFor(() =>
      expect(
        screen.queryByText('fake-optimization-panel:detect_intent'),
      ).not.toBeInTheDocument(),
    );
  });
});
