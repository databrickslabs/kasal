import { describe, it, expect } from 'vitest';
import { isDeck, splitSlides } from './htmlDeck';

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
