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

  it('themes Markdown body prose from the deck theme so bullets are not dark-on-dark', () => {
    // Regression: a Markdown bullet list inside a slide kept Tailwind's default
    // near-black prose colors, vanishing on a dark deck stage. The prose wrapper
    // must drive `--tw-prose-*` from the deck theme (default Midnight → light fg).
    const mdDeck: Surface = {
      surfaceKind: 'presentation',
      root: 'deck',
      components: [
        { id: 'deck', component: 'SlideDeck', children: ['s1'] },
        { id: 's1', component: 'Slide', variant: 'content', kicker: 'AGENDA', title: "What We'll Cover", children: ['m1'] },
        { id: 'm1', component: 'Markdown', content: '- Why the Swiss Data & AI scene matters\n- The 2026 event landscape' },
      ],
    };
    const { container } = render(<A2UIRenderer payload={mdDeck} />);
    const prose = container.querySelector('.prose') as HTMLElement;
    expect(prose).toBeTruthy();
    // Midnight theme body color (#e6ecff) drives prose body text — not the
    // default dark gray. (style.getPropertyValue reads the CSS custom property.)
    expect(prose.style.getPropertyValue('--tw-prose-body')).toBe('#e6ecff');
    expect(screen.getByText('Why the Swiss Data & AI scene matters')).toBeInTheDocument();
  });

  it('renders a two-column slide with text left and the visual right', () => {
    const twoCol: Surface = {
      surfaceKind: 'presentation',
      root: 'deck',
      dataModel: { items: [{ label: 'Plan' }, { label: 'Build' }] },
      components: [
        { id: 'deck', component: 'SlideDeck', children: ['s1'] },
        { id: 's1', component: 'Slide', variant: 'two-column', title: 'Split', children: ['t1', 'd1'] },
        { id: 't1', component: 'Text', text: 'A supporting bullet' },
        { id: 'd1', component: 'Diagram', archetype: 'process', items: { path: '/items' } },
      ],
    };
    render(<A2UIRenderer payload={twoCol} />);
    const slide = screen.getByText('Split').closest('.a2-slide') as HTMLElement;
    expect(slide.querySelector('.grid-cols-2')).toBeTruthy();
    expect(screen.getByText('A supporting bullet')).toBeInTheDocument();
    expect(screen.getByText('Plan')).toBeInTheDocument();
  });

  it('renders an agenda slide with numbered rows', () => {
    const agenda: Surface = {
      surfaceKind: 'presentation',
      root: 'deck',
      components: [
        { id: 'deck', component: 'SlideDeck', children: ['s1'] },
        { id: 's1', component: 'Slide', variant: 'agenda', title: 'Agenda', children: ['t1', 't2'] },
        { id: 't1', component: 'Text', text: 'Where we are' },
        { id: 't2', component: 'Text', text: 'Where we go' },
      ],
    };
    render(<A2UIRenderer payload={agenda} />);
    expect(screen.getByText('1')).toBeInTheDocument();
    expect(screen.getByText('2')).toBeInTheDocument();
    expect(screen.getByText('Where we are')).toBeInTheDocument();
    expect(screen.getByText('Where we go')).toBeInTheDocument();
  });

  it('treats a bodyless two-column slide as title-only (section layout)', () => {
    const empty: Surface = {
      surfaceKind: 'presentation',
      root: 'deck',
      components: [
        { id: 'deck', component: 'SlideDeck', children: ['s1'] },
        { id: 's1', component: 'Slide', variant: 'two-column', title: 'Nothing Here' },
      ],
    };
    render(<A2UIRenderer payload={empty} />);
    const slide = screen.getByText('Nothing Here').closest('.a2-slide') as HTMLElement;
    expect(slide.className).toContain('justify-center');
    expect(slide.className).toContain('text-center');
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
