import { describe, expect, it } from 'vitest';
import { latestDeck, parseSlideEdit, planSlideEdit } from './slideRefine';
import { refinedSlideIndex, splitSlides } from './htmlDeck';
import type { ChatMessage } from '../types/chat';

const slide = (t: string) => `<section class="slide"><h1>${t}</h1></section>`;
const DECK = ['Cover', 'Two', 'Three', 'Four'].map(slide).join('\n');
const titles = (html: string) => splitSlides(html).map((s) => s.match(/<h1>(.*?)<\/h1>/)?.[1]);

describe('parseSlideEdit', () => {
  it('reads a refine in the ways people write it', () => {
    expect(parseSlideEdit('slide 3: make the chart bigger', 4)).toEqual({ kind: 'refine', index: 2, instruction: 'make the chart bigger' });
    expect(parseSlideEdit('On slide 2, drop the footer', 4)).toEqual({ kind: 'refine', index: 1, instruction: 'drop the footer' });
    expect(parseSlideEdit('make the chart bigger on slide 3', 4)).toEqual({ kind: 'refine', index: 2, instruction: 'make the chart bigger' });
    expect(parseSlideEdit('add a footer to the last slide.', 4)).toEqual({ kind: 'refine', index: 3, instruction: 'add a footer' });
    expect(parseSlideEdit('please refine the 2nd slide so it has less text', 4)).toEqual({ kind: 'refine', index: 1, instruction: 'it has less text' });
    expect(parseSlideEdit('Refine slide 4', 4)).toEqual({ kind: 'refine', index: 3, instruction: 'improve it' });
  });

  it('reads structural edits and additions', () => {
    expect(parseSlideEdit('delete slide 2', 4)).toEqual({ kind: 'remove', index: 1 });
    expect(parseSlideEdit('Remove the third slide.', 4)).toEqual({ kind: 'remove', index: 2 });
    expect(parseSlideEdit('duplicate slide 1', 4)).toEqual({ kind: 'duplicate', index: 0 });
    expect(parseSlideEdit('move slide 1 after slide 3', 4)).toEqual({ kind: 'move', from: 0, to: 2 });
    expect(parseSlideEdit('move slide 4 before slide 2', 4)).toEqual({ kind: 'move', from: 3, to: 1 });
    expect(parseSlideEdit('move slide 2 to the end', 4)).toEqual({ kind: 'move', from: 1, to: 3 });
    expect(parseSlideEdit('add a slide after slide 2 about pricing', 4)).toEqual({ kind: 'add', index: 2, instruction: 'pricing' });
    expect(parseSlideEdit('insert a new slide before slide 1: agenda', 4)).toEqual({ kind: 'add', index: 0, instruction: 'agenda' });
    expect(parseSlideEdit('add a closing slide at the end with next steps', 4)).toEqual({ kind: 'add', index: 4, instruction: 'next steps' });
  });

  it('is not an edit without a slide, with no deck, or out of range', () => {
    expect(parseSlideEdit('make the chart bigger', 4)).toBeNull();
    expect(parseSlideEdit('slide 3: bigger', 0)).toBeNull();
    expect(parseSlideEdit('delete slide 9', 4)).toBeNull();
    expect(parseSlideEdit('compare snowflake with databricks', 4)).toBeNull();
    expect(parseSlideEdit('what is a slide deck?', 4)).toBeNull();
  });
});

describe('latestDeck', () => {
  const msg = (role: ChatMessage['role'], content: string, id: string): ChatMessage =>
    ({ id, role, content, timestamp: 0 }) as unknown as ChatMessage;

  it('finds the newest finished deck among assistant messages', () => {
    const messages = [
      msg('assistant', 'Deck:\n```html\n' + DECK + '\n```', 'a'),
      msg('user', 'thanks', 'b'),
      msg('assistant', 'Another:\n```html\n' + slide('Solo') + '\n```', 'c'),
      msg('assistant', 'still streaming\n```html\n' + slide('Open'), 'd'),
    ];
    expect(latestDeck(messages)?.messageId).toBe('c');
    expect(latestDeck([msg('assistant', 'no deck', 'x')])).toBeNull();
  });
});

describe('planSlideEdit', () => {
  it('structural edits are instant and open on the slide that changed', () => {
    const moved = planSlideEdit({ kind: 'move', from: 0, to: 2 }, DECK);
    expect(moved.kind).toBe('instant');
    if (moved.kind !== 'instant') return;
    expect(titles(moved.deck)).toEqual(['Two', 'Three', 'Cover', 'Four']);
    expect(refinedSlideIndex(moved.deck)).toBe(2);

    const removed = planSlideEdit({ kind: 'remove', index: 3 }, DECK);
    if (removed.kind !== 'instant') throw new Error('expected instant');
    expect(titles(removed.deck)).toEqual(['Cover', 'Two', 'Three']);

    const dup = planSlideEdit({ kind: 'duplicate', index: 1 }, DECK);
    if (dup.kind !== 'instant') throw new Error('expected instant');
    expect(titles(dup.deck)).toEqual(['Cover', 'Two', 'Two', 'Three', 'Four']);
    expect(refinedSlideIndex(dup.deck)).toBe(2);
  });

  it('a refine asks for ONE slide plus the cover, and splices the answer back', () => {
    const plan = planSlideEdit({ kind: 'refine', index: 2, instruction: 'bigger title' }, DECK);
    expect(plan.kind).toBe('call');
    if (plan.kind !== 'call') return;
    expect(plan.request).toEqual({
      mode: 'refine',
      instruction: 'bigger title',
      slide: slide('Three'),
      reference: slide('Cover'),
      position: '3 of 4',
    });
    expect(plan.summary).toBe('Refining slide 3');
    expect(plan.done).toBe('Refined slide 3');
    const out = plan.apply(slide('Three!'));
    expect(titles(out)).toEqual(['Cover', 'Two', 'Three!', 'Four']);
    expect(refinedSlideIndex(out)).toBe(2);
    expect(plan.focus).toBe(2);
  });

  it("a blank is inserted at once in a neighbour's design; fill writes it from its neighbours", () => {
    const blank = planSlideEdit({ kind: 'blank', index: 2 }, DECK);
    if (blank.kind !== 'instant') throw new Error('expected instant');
    expect(titles(blank.deck)).toEqual(['Cover', 'Two', undefined, 'Three', 'Four']);
    expect(splitSlides(blank.deck)[2]).toContain('New slide');
    expect(blank.focus).toBe(2);

    const fill = planSlideEdit({ kind: 'fill', index: 2, instruction: 'pricing' }, blank.deck);
    if (fill.kind !== 'call') throw new Error('expected call');
    expect(fill.request).toMatchObject({ mode: 'add', instruction: 'pricing', before: slide('Two'), after: slide('Three'), position: '3 of 5' });
    expect(titles(fill.apply(slide('Pricing')))).toEqual(['Cover', 'Two', 'Pricing', 'Three', 'Four']);
  });

  it('refining the cover uses the next slide as the design reference', () => {
    const plan = planSlideEdit({ kind: 'refine', index: 0, instruction: 'x' }, DECK);
    if (plan.kind !== 'call') throw new Error('expected call');
    expect(plan.request.reference).toBe(slide('Two'));
  });

  it('an add names both neighbours and inserts the answer between them', () => {
    const plan = planSlideEdit({ kind: 'add', index: 2, instruction: 'pricing' }, DECK);
    if (plan.kind !== 'call') throw new Error('expected call');
    expect(plan.request).toEqual({
      mode: 'add',
      instruction: 'pricing',
      before: slide('Two'),
      after: slide('Three'),
      position: '3 of 5',
    });
    const out = plan.apply(slide('Pricing'));
    expect(titles(out)).toEqual(['Cover', 'Two', 'Pricing', 'Three', 'Four']);
    expect(refinedSlideIndex(out)).toBe(2);
    const atEnd = planSlideEdit({ kind: 'add', index: 4, instruction: '' }, DECK);
    if (atEnd.kind !== 'call') throw new Error('expected call');
    expect(atEnd.request.after).toBeUndefined();
    expect(atEnd.request.before).toBe(slide('Four'));
  });
});
