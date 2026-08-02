import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import ChatMessageComponent from './ChatMessage';

vi.mock('../../store/executionStore', () => ({
  useExecutionStore: Object.assign(vi.fn(() => false), { getState: () => ({}) }),
}));
vi.mock('../../store/sessionStore', () => ({
  useSessionStore: Object.assign(vi.fn(() => undefined), { getState: () => ({}) }),
}));

/**
 * A run posts each crew's output as a capped preview, and exactly ONE message
 * per run is ever expanded — the final answer, swapped in at completion. So an
 * intermediate crew's work stayed at 2000 characters permanently, unreadable,
 * with nothing offering to open it.
 */
const base = {
  id: 'm1',
  role: 'assistant' as const,
  content: '# Catalog of Agentic AI Frameworks\n\nfirst part…',
  timestamp: new Date(),
};

describe('ChatMessage — opening a capped step preview', () => {
  it('offers to open a preview and shows the uncapped text', () => {
    render(
      <ChatMessageComponent
        message={{ ...base, fullContent: 'THE WHOLE OUTPUT of the step' }}
      />,
    );

    const toggle = screen.getByRole('button', { name: /full output/i });
    expect(screen.queryByText(/THE WHOLE OUTPUT/)).not.toBeInTheDocument();

    fireEvent.click(toggle);

    expect(screen.getByText(/THE WHOLE OUTPUT/)).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /show less/i })).toBeInTheDocument();
  });

  it('offers nothing when the message is already complete', () => {
    // The final answer is swapped in whole; it must not grow a pointless toggle.
    render(<ChatMessageComponent message={base} />);

    expect(screen.queryByRole('button', { name: /full output/i })).not.toBeInTheDocument();
  });
});
