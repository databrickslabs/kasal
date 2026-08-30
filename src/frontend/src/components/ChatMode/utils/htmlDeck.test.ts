import { describe, it, expect } from 'vitest';
import type { DiagramSegment } from './mdSandboxDiagram';
import { isDeck, mergeDeckSegments, splitSlides } from './htmlDeck';

describe('isDeck', () => {
  it('detects a slide section', () => {
    expect(isDeck('<section class="slide">a</section>')).toBe(true);
    expect(isDeck('<section class="slide dark">a</section>')).toBe(true);
    expect(isDeck("<section class='intro slide'>a</section>")).toBe(true);
  });

  it('is false for a plain diagram or non-slide html', () => {
    expect(isDeck('<div><svg></svg></div>')).toBe(false);
    expect(isDeck('<section class="hero">a</section>')).toBe(false);
  });
});

describe('splitSlides', () => {
  it('splits multiple slides into their full outer html', () => {
    const deck =
      '<section class="slide"><h1>One</h1></section>' +
      '<section class="slide"><h1>Two</h1></section>';
    expect(splitSlides(deck)).toEqual([
      '<section class="slide"><h1>One</h1></section>',
      '<section class="slide"><h1>Two</h1></section>',
    ]);
  });

  it('handles a nested <section> inside a slide without ending early', () => {
    const deck = '<section class="slide"><section>inner</section>tail</section>';
    expect(splitSlides(deck)).toEqual([
      '<section class="slide"><section>inner</section>tail</section>',
    ]);
  });

  it('takes the rest as the last slide when the final section is unclosed (streaming)', () => {
    const deck = '<section class="slide"><h1>Done</h1></section><section class="slide"><h1>Bui';
    const slides = splitSlides(deck);
    expect(slides).toHaveLength(2);
    expect(slides[0]).toBe('<section class="slide"><h1>Done</h1></section>');
    expect(slides[1]).toBe('<section class="slide"><h1>Bui');
  });

  it('returns [] when there are no slide sections', () => {
    expect(splitSlides('<div>nope</div>')).toEqual([]);
  });
});

describe('mergeDeckSegments', () => {
  const slide = (n: number) => `<section class="slide">S${n}</section>`;
  const deckSeg = (code: string, closed = true) =>
    ({ type: 'diagram', code, lang: 'html', closed }) as DiagramSegment;
  const textSeg = (text: string) => ({ type: 'text', text }) as DiagramSegment;

  it('collapses one-fence-per-slide answers into a single deck', () => {
    // Observed live: "create a presentation" arrived as eight ```html fences
    // separated by --- lines, rendering as eight "Slide 1 / 1" cards.
    const merged = mergeDeckSegments([
      deckSeg(slide(1)),
      textSeg('\n---\n'),
      deckSeg(slide(2)),
      textSeg('\n\n'),
      deckSeg(slide(3)),
    ]);
    expect(merged).toHaveLength(1);
    expect(merged[0].type).toBe('diagram');
    const code = (merged[0] as { code: string }).code;
    expect(splitSlides(code)).toHaveLength(3);
  });

  it('keeps decks apart when real prose sits between them', () => {
    const merged = mergeDeckSegments([
      deckSeg(slide(1)),
      textSeg('Here is a second, unrelated deck:'),
      deckSeg(slide(2)),
    ]);
    expect(merged).toHaveLength(3);
  });

  it('does not merge a non-deck diagram into a deck', () => {
    const merged = mergeDeckSegments([
      deckSeg(slide(1)),
      textSeg('---'),
      deckSeg('<svg viewBox="0 0 10 10"></svg>'),
    ]);
    expect(merged).toHaveLength(3); // separator text survives when no deck follows
  });

  it('a merged deck still streams while its last fence is unclosed', () => {
    const merged = mergeDeckSegments([
      deckSeg(slide(1)),
      textSeg('---'),
      deckSeg('<section class="slide">building…', false),
    ]);
    expect(merged).toHaveLength(1);
    expect((merged[0] as { closed: boolean }).closed).toBe(false);
  });

  it('leaves a single well-formed deck untouched', () => {
    const seg = deckSeg(slide(1) + slide(2));
    expect(mergeDeckSegments([textSeg('intro'), seg])).toEqual([textSeg('intro'), seg]);
  });
});

