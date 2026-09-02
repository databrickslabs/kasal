import { describe, it, expect } from 'vitest';
import type { DiagramSegment } from './mdSandboxDiagram';
import {
  replaceDeckInContent,
  stageFor,
  clearRefined,
  duplicateSlide,
  ensureDeckFence,
  insertSlide,
  isDeck,
  markRefined,
  mergeDeckSegments,
  moveSlide,
  refinedSlideIndex,
  removeSlide,
  replaceSlide,
  sectionFromReply,
  splitSlides,
} from './htmlDeck';

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


describe('slide edits', () => {
  const A = '<section class="slide"><h1>A</h1></section>';
  const B = '<section class="slide"><h1>B</h1></section>';
  const C = '<section class="slide"><h1>C</h1></section>';
  const DECK = `<style>.x{}</style>\n${A}\n${B}\n${C}`;
  const titles = (html: string) => splitSlides(html).map((s) => s.match(/<h1>(.*?)<\/h1>/)?.[1]);

  it('replaceSlide swaps one slide and leaves everything else byte-identical', () => {
    const out = replaceSlide(DECK, 1, '<section class="slide"><h1>B2</h1></section>');
    expect(titles(out)).toEqual(['A', 'B2', 'C']);
    expect(out.startsWith('<style>.x{}</style>\n')).toBe(true);
    expect(replaceSlide(DECK, 7, '<section class="slide"></section>')).toBe(DECK);
  });

  it('insertSlide places a slide at an index, or appends past the end', () => {
    const N = '<section class="slide"><h1>N</h1></section>';
    expect(titles(insertSlide(DECK, 0, N))).toEqual(['N', 'A', 'B', 'C']);
    expect(titles(insertSlide(DECK, 2, N))).toEqual(['A', 'B', 'N', 'C']);
    expect(titles(insertSlide(DECK, 99, N))).toEqual(['A', 'B', 'C', 'N']);
    expect(insertSlide('', 0, N)).toBe(N);
  });

  it('removeSlide drops a slide but never the last one', () => {
    expect(titles(removeSlide(DECK, 1))).toEqual(['A', 'C']);
    expect(removeSlide(A, 0)).toBe(A);
  });

  it('moveSlide lands the slide at its target in both directions', () => {
    expect(titles(moveSlide(DECK, 0, 2))).toEqual(['B', 'C', 'A']);
    expect(titles(moveSlide(DECK, 2, 0))).toEqual(['C', 'A', 'B']);
    expect(moveSlide(DECK, 1, 1)).toBe(DECK);
  });

  it('duplicateSlide copies a slide right after itself', () => {
    expect(titles(duplicateSlide(DECK, 0))).toEqual(['A', 'A', 'B', 'C']);
  });

  it('the refined marker is set on one opening tag, found, and cleared', () => {
    const marked = markRefined(B);
    expect(marked.startsWith('<section data-refined="1" class="slide">')).toBe(true);
    expect(markRefined(marked)).toBe(marked); // idempotent
    const deck = replaceSlide(DECK, 1, marked);
    expect(refinedSlideIndex(deck)).toBe(1);
    expect(isDeck(deck) && splitSlides(deck).length).toBe(3);
    expect(refinedSlideIndex(clearRefined(deck))).toBe(-1);
    expect(refinedSlideIndex(DECK)).toBe(-1);
  });

  it('sectionFromReply takes the one finished slide, fenced or bare', () => {
    expect(sectionFromReply('Here you go:\n```html\n' + B + '\n```\nDone.')).toBe(B);
    expect(sectionFromReply(B)).toBe(B);
    expect(sectionFromReply('```html\n<section class="slide"><h1>cut')).toBeNull();
    expect(sectionFromReply('no slide here')).toBeNull();
  });

  it('ensureDeckFence fences a bare deck and leaves everything else alone', () => {
    expect(ensureDeckFence(DECK)).toBe('```html\n' + DECK + '\n```');
    const fenced = '```html\n' + DECK + '\n```';
    expect(ensureDeckFence(fenced)).toBe(fenced);
    expect(ensureDeckFence('<p>just html</p>')).toBe('<p>just html</p>');
  });
});

describe('replaceDeckInContent', () => {
  it('swaps the deck inside a message and keeps the prose around it', () => {
    const before = 'Here is the deck:\n\n```html\n<section class="slide"><h1>A</h1></section>\n```\n\nEnjoy.';
    const out = replaceDeckInContent(before, '<section class="slide"><h1>B</h1></section>');
    expect(out).toBe('Here is the deck:\n\n```html\n<section class="slide"><h1>B</h1></section>\n```\n\nEnjoy.');
  });

  it('collapses a deck the model split across fences into one, and leaves other content alone', () => {
    const split = '```html\n<section class="slide"><h1>A</h1></section>\n```\n---\n```html\n<section class="slide"><h1>B</h1></section>\n```';
    expect(replaceDeckInContent(split, '<section class="slide"><h1>C</h1></section>')).toBe(
      '```html\n<section class="slide"><h1>C</h1></section>\n```',
    );
    expect(replaceDeckInContent('no deck here', '<section class="slide"></section>')).toBe('no deck here');
    expect(stageFor('<section class="slide">x</section>')).toContain('class="kwrap"');
  });
});
