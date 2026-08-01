/**
 * The pill exists so stickiness is visible and refusable.
 *
 * A capability that holds a conversation keeps the next turn even when the
 * message is a fragment the router would not have matched on its own words.
 * Without a sign of where follow-ups are going, and a way to stop it, that is
 * indistinguishable from a bug.
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import HeldConversationPill from './HeldConversationPill';

describe('HeldConversationPill', () => {
  it('names the capability that will get the next turn', () => {
    render(<HeldConversationPill capability="swiss_news_flow" onLeave={vi.fn()} />);

    expect(screen.getByText(/Continuing/)).toBeInTheDocument();
    expect(screen.getByText('swiss_news_flow')).toBeInTheDocument();
  });

  it('renders nothing when no conversation is held', () => {
    // The common case by far: one-shot capabilities and ordinary chat turns
    // must not gain a control that says nothing.
    const { container } = render(
      <HeldConversationPill capability={null} onLeave={vi.fn()} />,
    );

    expect(container).toBeEmptyDOMElement();
  });

  it('offers a way out', async () => {
    const onLeave = vi.fn();
    render(<HeldConversationPill capability="swiss_news_flow" onLeave={onLeave} />);

    await userEvent.click(
      screen.getByRole('button', { name: /stop continuing swiss_news_flow/i }),
    );

    expect(onLeave).toHaveBeenCalledTimes(1);
  });

  it('labels the exit for a screen reader', () => {
    // "×" alone says nothing out of context, and this control changes where a
    // user's next message goes.
    render(<HeldConversationPill capability="risk_review" onLeave={vi.fn()} />);

    expect(
      screen.getByRole('button', { name: 'Stop continuing risk_review' }),
    ).toBeInTheDocument();
  });
});
