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
