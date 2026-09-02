import React from 'react';
import { describe, expect, it, vi } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
import DeckPresentation from './DeckPresentation';

function mount() {
  const h = { onPrev: vi.fn(), onNext: vi.fn(), onFirst: vi.fn(), onLast: vi.fn(), onClose: vi.fn() };
  render(<DeckPresentation stage="<section class='slide'>A</section>" index={1} count={3} {...h} />);
  return h;
}

describe('DeckPresentation', () => {
  it('fills the screen on black with nothing but the slide', () => {
    mount();
    const dialog = screen.getByRole('dialog', { name: /Presentation, slide 2 of 3/ });
    expect(dialog.style.background).toBe('rgb(0, 0, 0)');
    // No counter, no header: the position lives in the aria-label only.
    expect(screen.queryByText('Slide 2 / 3')).toBeNull();
    // The exit control is hidden until the mouse moves.
    expect(screen.getByLabelText('Exit presentation').style.opacity).toBe('0');
    fireEvent.mouseMove(dialog);
    expect(screen.getByLabelText('Exit presentation').style.opacity).toBe('1');
  });

  it('pages with the keyboard window-wide (focus never enters the iframe)', () => {
    const h = mount();
    fireEvent.keyDown(window, { key: 'ArrowRight' });
    fireEvent.keyDown(window, { key: ' ' });
    fireEvent.keyDown(window, { key: 'PageDown' });
    expect(h.onNext).toHaveBeenCalledTimes(3);
    fireEvent.keyDown(window, { key: 'ArrowLeft' });
    fireEvent.keyDown(window, { key: 'PageUp' });
    expect(h.onPrev).toHaveBeenCalledTimes(2);
    fireEvent.keyDown(window, { key: 'Home' });
    fireEvent.keyDown(window, { key: 'End' });
    expect(h.onFirst).toHaveBeenCalledTimes(1);
    expect(h.onLast).toHaveBeenCalledTimes(1);
  });

  it('Escape and the close button leave presentation mode', () => {
    const h = mount();
    fireEvent.keyDown(window, { key: 'Escape' });
    fireEvent.click(screen.getByLabelText('Exit presentation'));
    expect(h.onClose).toHaveBeenCalledTimes(2);
  });

  it('clicking the right half advances, the left half goes back', () => {
    const h = mount();
    fireEvent.click(screen.getByTestId('deck-next-zone'));
    fireEvent.click(screen.getByTestId('deck-prev-zone'));
    expect(h.onNext).toHaveBeenCalledTimes(1);
    expect(h.onPrev).toHaveBeenCalledTimes(1);
  });

  it('survives a browser without the Fullscreen API (jsdom)', () => {
    expect(() => mount()).not.toThrow();
  });
});
