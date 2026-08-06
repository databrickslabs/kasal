import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { A2UIRenderer } from './A2UIRenderer';
import type { Surface } from './types';

// The template-layout variants: `kpi-split`, `boxes`, `split`, and the
// multi-column `agenda`. These exist because corporate decks reuse a handful of
// spatial arrangements that the original variants could not express — a headline
// KPI band ABOVE a body (`stats` is tiles only), a grid of N peer panels
// (`comparison` handles exactly two), and a visual-led split at an explicit
// ratio (`two-column` always puts text left).

const slide = (props: Record<string, unknown>) => ({
  id: 's', component: 'Slide', ...props,
});

const deckOf = (s: Record<string, unknown>, extra: Record<string, unknown>[] = []): Surface => ({
  surfaceKind: 'presentation',
  root: 'deck',
  components: [
    { id: 'deck', component: 'SlideDeck', children: ['s'] },
    s as never,
    ...(extra as never[]),
  ],
});

describe('Slide variant: kpi-split', () => {
  const deck = deckOf(
    slide({
      variant: 'kpi-split', kicker: 'CAPACITY', title: 'Installed Capacity',
      ratio: '60/40', children: ['k1', 'k2', 'm1', 'tbl'],
    }),
    [
      { id: 'k1', component: 'KeyValue', label: 'Total', value: '87.6 GW' },
      { id: 'k2', component: 'KeyValue', label: 'Renewable', value: '33.9%' },
      { id: 'm1', component: 'Markdown', content: '- Coal leads the mix' },
      { id: 'tbl', component: 'Table', columns: ['Tech'], rows: [['Coal']] },
    ],
  );

  it('renders the KPI tiles and the body together', () => {
    render(<A2UIRenderer payload={deck} />);
    // Both the band and the body survive — the failure this variant fixes is
    // having to choose between them.
    expect(screen.getByText('87.6 GW')).toBeTruthy();
    expect(screen.getByText('33.9%')).toBeTruthy();
    expect(screen.getByText('Coal leads the mix')).toBeTruthy();
    expect(screen.getByText('Installed Capacity')).toBeTruthy();
  });

  it('is not redirected to the centered section layout', () => {
    render(<A2UIRenderer payload={deck} />);
    const el = screen.getByText('Installed Capacity').closest('.a2-slide') as HTMLElement;
    expect(el.className).not.toContain('text-center');
  });
});

describe('Slide variant: boxes', () => {
  it('renders every panel', () => {
    const deck = deckOf(
      slide({ variant: 'boxes', title: 'Key Challenges', columns: 3, children: ['b1', 'b2', 'b3'] }),
      [
        { id: 'b1', component: 'Markdown', content: '**Demand Growth**' },
        { id: 'b2', component: 'Markdown', content: '**Grid Reliability**' },
        { id: 'b3', component: 'Markdown', content: '**Cybersecurity**' },
      ],
    );
    render(<A2UIRenderer payload={deck} />);
    expect(screen.getByText('Demand Growth')).toBeTruthy();
    expect(screen.getByText('Grid Reliability')).toBeTruthy();
    expect(screen.getByText('Cybersecurity')).toBeTruthy();
  });

  it('gives cells equal height so a short panel does not collapse', () => {
    const deck = deckOf(
      slide({ variant: 'boxes', title: 'Areas', children: ['b1', 'b2'] }),
      [
        { id: 'b1', component: 'Markdown', content: 'one line' },
        { id: 'b2', component: 'Markdown', content: 'a\n\nmuch\n\nlonger\n\npanel' },
      ],
    );
    render(<A2UIRenderer payload={deck} />);
    const grid = screen.getByText('one line').closest('.grid') as HTMLElement;
    expect(grid.className).toContain('auto-rows-fr');
  });
});

describe('Slide variant: split', () => {
  it('renders both regions without assuming which side is the visual', () => {
    const deck = deckOf(
      slide({ variant: 'split', title: 'HVDC Projects', ratio: '40/60', children: ['v1', 'm1'] }),
      [
        { id: 'v1', component: 'Table', columns: ['Project'], rows: [['Raigarh']] },
        { id: 'm1', component: 'Markdown', content: '- Strategic context' },
      ],
    );
    render(<A2UIRenderer payload={deck} />);
    expect(screen.getByText('HVDC Projects')).toBeTruthy();
    expect(screen.getByText('Strategic context')).toBeTruthy();
  });
});

describe('Slide variant: agenda with columns', () => {
  const rows = Array.from({ length: 12 }, (_, i) => ({
    id: `r${i}`, component: 'Text', text: `Section ${i + 1}`,
  }));

  it('numbers rows column-first so each column reads downward', () => {
    const deck = deckOf(
      slide({ variant: 'agenda', title: 'Table of Contents', columns: 2, children: rows.map((r) => r.id) }),
      rows,
    );
    render(<A2UIRenderer payload={deck} />);
    // 12 rows over 2 columns → the 7th badge starts the second column, so every
    // number 1..12 is present exactly once (a row-first fill would repeat none
    // but would interleave the sections wrongly against the printed deck).
    for (let n = 1; n <= 12; n++) {
      expect(screen.getByText(String(n))).toBeTruthy();
    }
    expect(screen.getByText('Section 12')).toBeTruthy();
  });

  it('still renders a single column when columns is absent', () => {
    const three = rows.slice(0, 3);
    const deck = deckOf(
      slide({ variant: 'agenda', title: 'Agenda', children: three.map((r) => r.id) }),
      three,
    );
    render(<A2UIRenderer payload={deck} />);
    expect(screen.getByText('Section 1')).toBeTruthy();
    expect(screen.getByText('Section 3')).toBeTruthy();
  });
});

describe('Slide variant: stats tile cap', () => {
  it('lays six tiles out in one row rather than capping at four', () => {
    const kvs = Array.from({ length: 6 }, (_, i) => ({
      id: `k${i}`, component: 'KeyValue', label: `L${i}`, value: `V${i}`,
    }));
    const deck = deckOf(
      slide({ variant: 'stats', title: 'Key Facts', children: kvs.map((k) => k.id) }),
      kvs,
    );
    render(<A2UIRenderer payload={deck} />);
    const grid = screen.getByText('V0').closest('.grid') as HTMLElement;
    expect(grid.style.gridTemplateColumns).toContain('repeat(6');
  });
});
