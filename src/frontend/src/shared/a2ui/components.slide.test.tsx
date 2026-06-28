import { describe, it, expect } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { A2UIRenderer } from './A2UIRenderer';
import type { Surface } from './types';

// A deck with a title-only `content` slide (no body) and a normal content slide
// with a real bullet. The title-only one must NOT render as a title stranded
// over a void — it falls back to the centered SECTION layout so it reads as a
// deliberate divider instead of a broken near-empty slide.
const deck: Surface = {
  surfaceKind: 'presentation',
  root: 'deck',
  components: [
    { id: 'deck', component: 'SlideDeck', children: ['s1', 's2'] },
    { id: 's1', component: 'Slide', variant: 'content', kicker: 'SECTION', title: 'Lonely Title' },
    { id: 's2', component: 'Slide', variant: 'content', title: 'Filled Title', children: ['t1'] },
    { id: 't1', component: 'Text', text: 'A real bullet point' },
  ],
};

describe('SlideDeck — empty content slides degrade gracefully', () => {
  it('renders a title-only content slide as a centered section, not a top-aligned void', () => {
    render(<A2UIRenderer payload={deck} />);
    const slide = screen.getByText('Lonely Title').closest('.a2-slide') as HTMLElement;
    expect(slide).toBeTruthy();
    // Centered (section) layout — title vertically centered, not stranded at top.
    expect(slide.className).toContain('justify-center');
    expect(slide.className).toContain('text-center');
  });

  it('keeps the top-aligned content layout for a slide that actually has a body', () => {
    render(<A2UIRenderer payload={deck} />);
    // Advance to the second slide (the deck shows one slide at a time).
    fireEvent.click(screen.getByText(/Next/));
    const slide = screen.getByText('Filled Title').closest('.a2-slide') as HTMLElement;
    expect(slide.className).not.toContain('text-center');
    expect(screen.getByText('A real bullet point')).toBeInTheDocument();
  });

  it('treats a content slide with only BLANK children as title-only (section layout)', () => {
    // children exist but render to nothing (empty Text + whitespace Markdown) —
    // the naive children.length check would miss this; nodeHasContent catches it.
    const blankDeck: Surface = {
      surfaceKind: 'presentation',
      root: 'deck',
      components: [
        { id: 'deck', component: 'SlideDeck', children: ['s1'] },
        { id: 's1', component: 'Slide', variant: 'content', kicker: 'APPLICATIONS', title: 'Real-World Uses', children: ['t0', 'm0'] },
        { id: 't0', component: 'Text', text: '' },
        { id: 'm0', component: 'Markdown', content: '   ' },
      ],
    };
    render(<A2UIRenderer payload={blankDeck} />);
    const slide = screen.getByText('Real-World Uses').closest('.a2-slide') as HTMLElement;
    expect(slide.className).toContain('justify-center');
    expect(slide.className).toContain('text-center');
  });
});
