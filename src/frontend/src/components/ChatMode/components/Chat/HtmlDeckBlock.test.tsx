import { describe, expect, it } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
import HtmlDeckBlock from './HtmlDeckBlock';

const DECK =
  '<section class="slide"><h1>One</h1></section>' +
  '<section class="slide"><h1>Two</h1></section>' +
  '<section class="slide"><h1>Three</h1></section>';

describe('HtmlDeckBlock keyboard navigation', () => {
  it('pages with arrow keys when the deck has focus', () => {
    render(<HtmlDeckBlock code={DECK} />);
    const deck = screen.getByRole('group');
    deck.focus();
    expect(screen.getByText('Slide 1 / 3')).toBeInTheDocument();
    fireEvent.keyDown(deck, { key: 'ArrowRight' });
    expect(screen.getByText('Slide 2 / 3')).toBeInTheDocument();
    fireEvent.keyDown(deck, { key: 'ArrowLeft' });
    expect(screen.getByText('Slide 1 / 3')).toBeInTheDocument();
  });

  it('End and Home jump to the last and first slide', () => {
    render(<HtmlDeckBlock code={DECK} />);
    const deck = screen.getByRole('group');
    fireEvent.keyDown(deck, { key: 'End' });
    expect(screen.getByText('Slide 3 / 3')).toBeInTheDocument();
    fireEvent.keyDown(deck, { key: 'Home' });
    expect(screen.getByText('Slide 1 / 3')).toBeInTheDocument();
  });

  it('pages window-wide while presenting', () => {
    render(<HtmlDeckBlock code={DECK} />);
    fireEvent.click(screen.getByTitle('Present'));
    fireEvent.keyDown(window, { key: 'ArrowRight' });
    // Both the inline header and the presentation counter show the new position.
    expect(screen.getAllByText('Slide 2 / 3').length).toBeGreaterThan(0);
  });

  it('does not page past the ends', () => {
    render(<HtmlDeckBlock code={DECK} />);
    const deck = screen.getByRole('group');
    fireEvent.keyDown(deck, { key: 'ArrowLeft' });
    expect(screen.getByText('Slide 1 / 3')).toBeInTheDocument();
    fireEvent.keyDown(deck, { key: 'End' });
    fireEvent.keyDown(deck, { key: 'ArrowRight' });
    expect(screen.getByText('Slide 3 / 3')).toBeInTheDocument();
  });
});

describe('HtmlDeckBlock slide refine', () => {
  it('opens on the slide a refine marked as changed', () => {
    const refined =
      '<section class="slide"><h1>One</h1></section>' +
      '<section data-refined="1" class="slide"><h1>Two</h1></section>' +
      '<section class="slide"><h1>Three</h1></section>';
    render(<HtmlDeckBlock code={refined} />);
    expect(screen.getByText('Slide 2 / 3')).toBeInTheDocument();
  });

  it('"Edit deck" opens the studio on the slide on screen', () => {
    render(<HtmlDeckBlock code={DECK} messageId="m1" />);
    fireEvent.keyDown(screen.getByRole('group'), { key: 'ArrowRight' });
    fireEvent.click(screen.getByTitle('Edit deck'));
    const studio = screen.getByRole('dialog', { name: 'Deck studio' });
    expect(studio).toBeInTheDocument();
    expect(screen.getByText('Slide 2')).toBeInTheDocument(); // the instruction bar's target
    fireEvent.click(screen.getByTitle('Done (Esc)'));
    expect(screen.queryByRole('dialog', { name: 'Deck studio' })).toBeNull();
  });

  it('typing in the studio keeps focus — the deck card must not grab it back', () => {
    render(<HtmlDeckBlock code={DECK} messageId="m1" />);
    fireEvent.click(screen.getByTitle('Edit deck'));
    const box = screen.getByLabelText('Slide instruction') as HTMLTextAreaElement;
    box.focus();
    // The card's onClick focuses the card on any click inside it; a click in the
    // (portaled) studio bubbles there through React unless the studio stops it.
    fireEvent.click(box);
    expect(document.activeElement).toBe(box);
    fireEvent.change(box, { target: { value: 'bigger title' } });
    expect(box.value).toBe('bigger title');
  });
});
