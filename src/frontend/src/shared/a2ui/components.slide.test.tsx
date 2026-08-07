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
    // The column count is an inline style now, not a `grid-cols-2` class: the body
    // collapses to a single column when there is no visual, so the track list is
    // computed rather than fixed. Two children (text + Diagram) → two columns.
    const bodyGrid = slide.querySelector<HTMLElement>('div[style*="grid-template-columns"]');
    expect(bodyGrid?.style.gridTemplateColumns).toBe('1fr 1fr');
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
    // Badges are zero-padded, as the printed contents page draws them.
    expect(screen.getByText('01')).toBeInTheDocument();
    expect(screen.getByText('02')).toBeInTheDocument();
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

// --- Research capabilities: citations + speaker notes ----------------------

const sourcedDeck: Surface = {
  surfaceKind: 'presentation',
  root: 'deck',
  dataModel: { shared: ['https://example.com/report'] },
  components: [
    { id: 'deck', component: 'SlideDeck', children: ['s1', 's2', 's3'] },
    {
      id: 's1',
      component: 'Slide',
      variant: 'content',
      title: 'Sourced',
      children: ['t1'],
      notes: 'Open with the market framing, then hand to the chart.',
      sources: [
        { label: 'IEA Global EV Outlook', url: 'https://example.com/iea' },
        'BNEF 2025',
      ],
    },
    { id: 't1', component: 'Text', text: 'EV sales grew 34%' },
    // sources arriving as a BINDING into the dataModel, and a bare URL entry.
    { id: 's2', component: 'Slide', variant: 'content', title: 'Bound', children: ['t2'], sources: { path: '/shared' } },
    { id: 't2', component: 'Text', text: 'Second point' },
    // No sources, no notes — must render clean.
    { id: 's3', component: 'Slide', variant: 'content', title: 'Bare', children: ['t3'] },
    { id: 't3', component: 'Text', text: 'Third point' },
  ],
};

// The slide canvas lives inside SlideStage, which stays `visibility: hidden`
// until it has measured itself — and jsdom performs no layout, so the measure
// never yields a scale. Nodes inside a slide are therefore outside the
// accessibility tree, and neither role queries nor accessible-name computation
// work on them; assert against the DOM instead. (Deck chrome such as the
// Next/Notes buttons sits OUTSIDE the stage and queries by role normally.)
const sourceLinks = (c: HTMLElement) =>
  Array.from(c.querySelectorAll('.a2-slide-sources a')) as HTMLAnchorElement[];
describe('Slide — citations', () => {
  it('renders a sources footer, numbering entries and linking the ones with URLs', () => {
    const { container } = render(<A2UIRenderer payload={sourcedDeck} />);
    expect(screen.getByText('Sources')).toBeInTheDocument();
    const links = sourceLinks(container);
    expect(links).toHaveLength(1);
    expect(links[0]).toHaveTextContent('IEA Global EV Outlook');
    expect(links[0]).toHaveAttribute('href', 'https://example.com/iea');
    expect(links[0]).toHaveAttribute('rel', 'noopener noreferrer');
    expect(links[0]).toHaveAttribute('target', '_blank');
    // A plain-string source still renders, just without a link.
    expect(screen.getByText('BNEF 2025')).toBeInTheDocument();
    // Entries are numbered in order.
    expect(container.querySelector('.a2-slide-sources')).toHaveTextContent('1.');
    expect(container.querySelector('.a2-slide-sources')).toHaveTextContent('2.');
  });

  it('resolves a bound sources list and uses a bare URL as its own label', () => {
    const { container } = render(<A2UIRenderer payload={sourcedDeck} />);
    fireEvent.click(screen.getByText(/Next/));
    const links = sourceLinks(container);
    expect(links).toHaveLength(1);
    expect(links[0]).toHaveAttribute('href', 'https://example.com/report');
    expect(links[0]).toHaveTextContent('https://example.com/report');
  });

  it('renders no footer at all for a slide without sources', () => {
    const { container } = render(<A2UIRenderer payload={sourcedDeck} />);
    fireEvent.click(screen.getByText(/Next/));
    fireEvent.click(screen.getByText(/Next/));
    expect(screen.getByText('Third point')).toBeInTheDocument();
    expect(container.querySelector('.a2-slide-sources')).toBeNull();
  });
});

describe('SlideDeck — speaker notes', () => {
  it('keeps notes off the slide canvas and reveals them behind a toggle', () => {
    render(<A2UIRenderer payload={sourcedDeck} />);
    const script = 'Open with the market framing, then hand to the chart.';
    // Never on the slide itself — notes are the presenter's script, not content.
    expect(screen.queryByText(script)).toBeNull();
    fireEvent.click(screen.getByRole('button', { name: /Notes/ }));
    expect(screen.getByText(script)).toBeInTheDocument();
    expect(screen.getByText(/Speaker notes · slide 1/)).toBeInTheDocument();
  });

  it('reports slides that have no notes instead of showing the previous slide’s', () => {
    render(<A2UIRenderer payload={sourcedDeck} />);
    fireEvent.click(screen.getByRole('button', { name: /Notes/ }));
    fireEvent.click(screen.getByText(/Next/));
    expect(screen.getByText('No notes for this slide.')).toBeInTheDocument();
  });

  it('offers no notes control for a deck where no slide has notes', () => {
    const noNotes: Surface = {
      surfaceKind: 'presentation',
      root: 'deck',
      components: [
        { id: 'deck', component: 'SlideDeck', children: ['s1'] },
        { id: 's1', component: 'Slide', variant: 'content', title: 'X', children: ['t1'] },
        { id: 't1', component: 'Text', text: 'Point' },
      ],
    };
    render(<A2UIRenderer payload={noNotes} />);
    expect(screen.queryByRole('button', { name: /Notes/ })).toBeNull();
  });
});

describe('Slide — comparison and image-full variants', () => {
  it('splits comparison children into two labelled peer panels', () => {
    const cmp: Surface = {
      surfaceKind: 'presentation',
      root: 'deck',
      components: [
        { id: 'deck', component: 'SlideDeck', children: ['s1'] },
        {
          id: 's1', component: 'Slide', variant: 'comparison', title: 'Build vs Buy',
          leftLabel: 'Build', rightLabel: 'Buy', children: ['a1', 'a2', 'b1', 'b2'],
        },
        { id: 'a1', component: 'Text', text: 'Full control' },
        { id: 'a2', component: 'Text', text: 'Higher upfront cost' },
        { id: 'b1', component: 'Text', text: 'Faster to launch' },
        { id: 'b2', component: 'Text', text: 'Vendor lock-in' },
      ],
    };
    render(<A2UIRenderer payload={cmp} />);
    expect(screen.getByText('Build')).toBeInTheDocument();
    expect(screen.getByText('Buy')).toBeInTheDocument();
    // The halves land in different panels, in order.
    const left = screen.getByText('Full control').closest('div.rounded-2xl') as HTMLElement;
    const right = screen.getByText('Faster to launch').closest('div.rounded-2xl') as HTMLElement;
    expect(left).not.toBe(right);
    expect(left).toContainElement(screen.getByText('Higher upfront cost'));
    expect(right).toContainElement(screen.getByText('Vendor lock-in'));
  });

  it('overlays the title on a full-bleed image slide', () => {
    const img: Surface = {
      surfaceKind: 'presentation',
      root: 'deck',
      components: [
        { id: 'deck', component: 'SlideDeck', children: ['s1'] },
        { id: 's1', component: 'Slide', variant: 'image-full', kicker: 'CHAPTER', title: 'The Shift', children: ['i1'] },
        { id: 'i1', component: 'Image', src: 'https://example.com/a.jpg', alt: 'city' },
      ],
    };
    render(<A2UIRenderer payload={img} />);
    const slide = screen.getByText('The Shift').closest('.a2-slide') as HTMLElement;
    expect(slide).toBeTruthy();
    expect(slide.querySelector('img')).toBeTruthy();
    expect(screen.getByText('CHAPTER')).toBeInTheDocument();
  });

  it('falls back to the section layout when an image-full slide has no media', () => {
    const empty: Surface = {
      surfaceKind: 'presentation',
      root: 'deck',
      components: [
        { id: 'deck', component: 'SlideDeck', children: ['s1'] },
        { id: 's1', component: 'Slide', variant: 'image-full', title: 'No Media' },
      ],
    };
    render(<A2UIRenderer payload={empty} />);
    const slide = screen.getByText('No Media').closest('.a2-slide') as HTMLElement;
    expect(slide.className).toContain('text-center');
  });
});
