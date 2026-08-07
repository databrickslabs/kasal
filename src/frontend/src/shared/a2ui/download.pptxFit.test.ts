import { describe, it, expect } from 'vitest';
import { fitFontSize } from './lib/download';

// The PPTX exporter has no DOM to measure text with, so it estimates the font size
// that fits a body into its box. Every bug this guards produced the SAME visible
// symptom in the downloaded deck: body text printed over the title above or the
// sources footer below.
//
// The estimate must ERR LARGE (over-estimate the height needed). Under-estimating
// overflows the slide; over-estimating just leaves it a little roomier.
describe('fitFontSize', () => {
  // The shape that shipped the bug: six paragraphs, one of them very long, in the
  // body box left after a kicker + title (1.80in) above a sources footer (6.65in).
  // Deliberately generic prose — what matters is the LENGTH and the mix, not the
  // subject; this is a layout test, not a domain fixture.
  const overfull = [
    'Regional breakdown: not available. The stored dataset contains no per-region rows for this query, so no percentage split (north, south, east, west, central) can be shown here — no figures have been estimated in its place.',
    'What the available data does tell us:',
    'Total volume stood at 85.5 units in the latest reporting year, against a rapidly growing base.',
    'Group sales reached 287.9 units last year, with peak throughput exceeding 54 per hour — a proxy for how demand-driven the mix has become.',
    'Intensity of 0.168 units per thousand of spend points to a structure with substantial efficiency headroom.',
    'A segment-by-segment breakdown should be sourced from the upstream system before this slide is finalised.',
  ];
  const BOX_W = 11.9;
  const BOX_H = 6.65 - 1.8;

  it('shrinks an overfull body to a size that actually fits', () => {
    const size = fitFontSize(overfull, BOX_W, BOX_H, 20);
    // Measured limit for this content in this box is 16pt. An earlier version
    // ignored paragraph spacing and returned 19pt, and the text still overflowed.
    expect(size).toBeLessThanOrEqual(16);
    expect(size).toBeGreaterThan(8);
  });

  it('leaves a body that already fits at its desired size', () => {
    // Shrinking a slide that fits would make the deck inconsistent for no reason.
    expect(fitFontSize(['Short one.', 'Short two.', 'Short three.'], BOX_W, BOX_H, 20)).toBe(20);
  });

  it('never returns a size whose estimated height exceeds the box', () => {
    // The invariant, restated independently of the constants: whatever it returns,
    // the text it describes has to fit.
    for (const n of [1, 4, 8, 16, 32]) {
      const lines = Array.from({ length: n }, (_, i) => `Bullet ${i + 1} ${'word '.repeat(14)}`);
      const size = fitFontSize(lines, BOX_W, BOX_H, 20);
      const perLine = Math.floor((BOX_W * 72) / (size * 0.55));
      const wrapped = lines.reduce((t, s) => t + Math.max(Math.ceil(s.length / perLine), 1), 0);
      const height = (wrapped * size * 1.3 * 1.2 + lines.length * 12) / 72;
      // At the floor (8pt) truly excessive content still cannot fit — that is a
      // content problem, not a sizing one — so only assert it above the floor.
      if (size > 8) expect(height).toBeLessThanOrEqual(BOX_H);
    }
  });

  it('honours a tighter paragraph spacing', () => {
    // The `boxes` panels use paraSpaceAfter 4, not 12; charging them 12 would
    // shrink small panels far more than needed.
    const lines = ['One line.', 'Two line.', 'Three line.'];
    const tight = fitFontSize(lines, 2.5, 1.0, 10.5, 6, 4);
    const loose = fitFontSize(lines, 2.5, 1.0, 10.5, 6, 12);
    expect(tight).toBeGreaterThanOrEqual(loose);
  });
});
