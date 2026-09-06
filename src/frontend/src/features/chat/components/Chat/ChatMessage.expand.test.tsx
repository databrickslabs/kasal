import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import ChatMessageComponent from './ChatMessage';

vi.mock('../../store/executionStore', () => ({
  useExecutionStore: Object.assign(vi.fn(() => false), { getState: () => ({}) }),
}));
vi.mock('../../store/sessionStore', () => ({
  useSessionStore: Object.assign(vi.fn(() => undefined), { getState: () => ({}) }),
}));

/**
 * A run posts each crew's output as a capped preview, carrying the uncapped
 * text alongside it as `fullContent`.
 *
 * That used to sit behind a "Show the full output" button, so a verbose step
 * read as a truncated stub until someone thought to click. The full text now
 * renders directly — `content` is only the fallback for messages that were
 * never capped.
 */
const base = {
  id: 'm1',
  role: 'assistant' as const,
  content: '# Catalog of Agentic AI Frameworks\n\nfirst part…',
  timestamp: new Date(),
};

describe('ChatMessage — a capped step preview', () => {
  it('renders the uncapped text with no interaction', () => {
    render(
      <ChatMessageComponent
        message={{ ...base, fullContent: 'THE WHOLE OUTPUT of the step' }}
      />,
    );

    expect(screen.getByText(/THE WHOLE OUTPUT/)).toBeInTheDocument();
  });

  it('never asks the reader to click for the rest', () => {
    render(
      <ChatMessageComponent
        message={{ ...base, fullContent: 'THE WHOLE OUTPUT of the step' }}
      />,
    );

    expect(
      screen.queryByRole('button', { name: /full output/i }),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole('button', { name: /show less/i }),
    ).not.toBeInTheDocument();
  });

  it('shows the capped preview only when nothing longer exists', () => {
    // The final answer is swapped in whole and carries no fullContent.
    render(<ChatMessageComponent message={base} />);

    expect(screen.getByText(/Catalog of Agentic AI Frameworks/)).toBeInTheDocument();
    expect(
      screen.queryByRole('button', { name: /full output/i }),
    ).not.toBeInTheDocument();
  });
});
