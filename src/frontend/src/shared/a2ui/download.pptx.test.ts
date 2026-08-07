import { describe, it, expect, vi, beforeAll } from 'vitest';
import { downloadPptx } from './lib/download';
import { getDeckTheme, DEFAULT_DECK_THEME_ID } from './lib/deckThemes';
import type { Surface } from './types';

// End-to-end guard on the REAL exporter. It is a second renderer (pptxgenjs, no
// DOM), so a variant it does not handle degrades SILENTLY — which is how a
// 'kpi-split' slide carrying bullets AND a chart exported the tiles and the chart
// and dropped every bullet. Asserting the generated XML is the only way to catch
// that; a code-level assertion cannot see it.
let captured: unknown = null;
beforeAll(async () => {
  const mod = await import('pptxgenjs');
  const Pptx = mod.default as unknown as { prototype: Record<string, unknown> };
  // Intercept at writeFile so we get the built deck without touching the DOM.
  vi.spyOn(Pptx.prototype, 'writeFile' as never).mockImplementation(async function (
    this: { write: (o: unknown) => Promise<unknown> },
  ) {
    captured = await this.write({ outputType: 'arraybuffer' });
    return 'stub.pptx';
  } as never);
});

async function slideXml(surface: Surface): Promise<string> {
  captured = null;
  await downloadPptx(surface, getDeckTheme(DEFAULT_DECK_THEME_ID), 'probe.pptx');
  expect(captured).toBeTruthy();
  const { unzipSync } = await import('fflate');
  const files = unzipSync(new Uint8Array(captured as ArrayBuffer));
  return Object.entries(files)
    .filter(([k]) => /^ppt\/slides\/slide\d+\.xml$/.test(k))
    .map(([, v]) => new TextDecoder().decode(v))
    .join('\n');
}

const deck = (slide: Record<string, unknown>, kids: Record<string, unknown>[]): Surface => ({
  surfaceKind: 'presentation',
  root: 'deck',
  components: [
    { id: 'deck', component: 'SlideDeck', children: ['s'] },
    { id: 's', component: 'Slide', ...slide } as never,
    ...(kids as never[]),
  ],
});

describe('PPTX export keeps every part of the slide', () => {
  it('never emits two autofit elements in one bodyPr', async () => {
    // Two <a:normAutofit/> in one <a:bodyPr> violates the schema and PowerPoint
    // refuses the whole FILE: "found a problem with content… attempt to repair".
    const xml = await slideXml(deck({ variant: 'content', title: 'T', children: ['m'] }, [
      { id: 'm', component: 'Markdown', content: '- a\n- b' },
    ]));
    expect(xml).not.toMatch(/<a:normAutofit[^>]*\/><a:normAutofit/);
  });

  it('keeps kpi-split prose when the slide also carries a chart', async () => {
    const xml = await slideXml(
      deck({ variant: 'kpi-split', ratio: '60/40', kicker: 'OVERVIEW', title: 'Volume', children: ['k1', 'k2', 'm1', 'c1'] }, [
        { id: 'k1', component: 'KeyValue', label: 'Total volume for the period', value: '85.5 units' },
        { id: 'k2', component: 'KeyValue', label: 'Leading segment share', value: '34%' },
        { id: 'm1', component: 'Markdown', content: '- **Concentrated mix:** the leading segment is 34% of the total\n- Volume of 85.5 units serves 102.3 million end users' },
        { id: 'c1', component: 'Chart', chartType: 'pie', data: [{ name: 'Segment A', value: 34 }], xKey: 'name', yKeys: ['value'] },
      ]),
    );
    // The bug: only the tiles and the chart survived.
    expect(xml).toContain('Concentrated mix');
    expect(xml).toContain('102.3 million end users');
    // Tiles still there…
    expect(xml).toContain('85.5 units');
    // …and NOT duplicated as body lines ("Label: value"), which printed over the title.
    expect(xml).not.toContain('Total volume for the period: 85.5 units');
    // The tiles are PANELS (rounded shapes), matching the on-screen band. Bare
    // text let a long value sprawl across its neighbour and drag its label out of
    // line with the rest of the row.
    expect((xml.match(/roundRect/g) || []).length).toBeGreaterThanOrEqual(2);
  });

  it('keeps table notes on a table slide', async () => {
    const xml = await slideXml(deck({ variant: 'visual', title: 'Structure', children: ['t1', 'm1'] }, [
      { id: 't1', component: 'Table', columns: ['Stage'], rows: [['State-owned']] },
      { id: 'm1', component: 'Markdown', content: '- Private-sector share is 42%' },
    ]));
    expect(xml).toContain('State-owned');
    expect(xml).toContain('Private-sector share');
  });

  it('draws boxes panels rather than one bullet stream', async () => {
    const xml = await slideXml(deck({ variant: 'boxes', columns: 2, title: 'Panels', children: ['p1', 'p2'] }, [
      { id: 'p1', component: 'Markdown', content: '**Growth**\n- CAGR 6.1%' },
      { id: 'p2', component: 'Markdown', content: '**Volume Profile**\n- Outbound 403bn' },
    ]));
    expect((xml.match(/roundRect/g) || []).length).toBeGreaterThanOrEqual(2);
    expect(xml).toContain('Growth');
    expect(xml).toContain('Volume Profile');
  });

  it('renders agenda rows as tiles with a title and descriptor', async () => {
    const xml = await slideXml(deck({ variant: 'agenda', columns: 2, title: 'Table of Contents', children: ['r1', 'r2'] }, [
      // No '&' in the fixture: it is XML-escaped to '&amp;' in the slide part, so a
      // raw-text assertion would fail for a reason that has nothing to do with layout.
      { id: 'r1', component: 'Text', text: 'Overview and Context — Key facts, background' },
      { id: 'r2', component: 'Text', text: 'Sector Detail — Volume, supply & demand' },
    ]));
    expect(xml).toContain('Overview and Context');
    expect(xml).toContain('Key facts, background');
    // Zero-padded number tiles, as the printed template draws them.
    expect(xml).toContain('01');
  });
});
