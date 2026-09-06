import { describe, expect, it, vi } from 'vitest';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
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

describe('HtmlDeckBlock attached images', () => {
  it('renders frames with asset references resolved, and hands the studio the raw deck', async () => {
    const { AssetService } = await import('../../../../api/chat/AssetService');
    const spy = vi.spyOn(AssetService, 'dataUrl').mockResolvedValue('data:image/png;base64,QQ==');
    const deck = '<section class="slide"><img src="asset:abc123def"></section>';
    render(<HtmlDeckBlock code={deck} messageId="m1" />);
    // The frame is REMOUNTED once the bytes are in — a fresh element, not the
    // first one re-navigated (Chrome drops a srcdoc change that lands before a
    // new frame's first navigation finishes; the studio showed blank slides).
    const pending = document.querySelector('iframe[title="Slide deck"]') as HTMLIFrameElement;
    expect(pending.srcdoc).not.toContain('data:image/png;base64,QQ==');
    await waitFor(() => {
      const frame = document.querySelector('iframe[title="Slide deck"]') as HTMLIFrameElement;
      expect(frame.srcdoc).toContain('data:image/png;base64,QQ==');
      expect(frame).not.toBe(pending);
    });
    fireEvent.click(screen.getByTitle('Edit deck'));
    // The studio shows resolved thumbnails but edits the raw deck (references kept).
    await waitFor(() => {
      const thumb = document.querySelector('iframe[title="Slide 1 thumbnail"]') as HTMLIFrameElement;
      expect(thumb.srcdoc).toContain('data:image/png;base64,QQ==');
    });
    spy.mockRestore();
  });
});
