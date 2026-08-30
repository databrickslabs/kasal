/**
 * HTML slide-deck contract + parsing.
 *
 * A deck is a single ```html block whose slides are delimited by
 * `<section class="slide">…</section>`. Each slide is authored to fill a fixed
 * 1280×720 (16:9) logical stage with inline, self-contained styling. That
 * delimiter is what lets us page the deck on screen AND export it (one PDF page
 * / one PPTX slide per <section>). Freeform HTML/SVG is allowed *inside* a slide.
 */

import type { DiagramSegment } from './mdSandboxDiagram';

/** Logical slide stage — fixed 16:9 so on-screen, PDF and PPTX all agree. */
export const SLIDE_W = 1280;
export const SLIDE_H = 720;

// A slide section opener: <section class="slide"> possibly with more classes /
// attributes. Case-insensitive, tolerant of attribute order and whitespace.
const SLIDE_OPEN = /<section\b[^>]*\bclass\s*=\s*["'][^"']*\bslide\b[^"']*["'][^>]*>/gi;

/** True when the HTML is a slide deck (has at least one `.slide` section). */
export function isDeck(html: string): boolean {
  SLIDE_OPEN.lastIndex = 0;
  return SLIDE_OPEN.test(html || '');
}

/**
 * Split a deck into the FULL outer HTML of each `<section class="slide">…</section>`
 * (so the section's own background/padding/styling is preserved when rendered).
 * Handles nested <section> elements inside a slide by depth-matching.
 * Returns [] when there are no slide sections.
 */
export function splitSlides(html: string): string[] {
  const src = html || '';
  const slides: string[] = [];
  SLIDE_OPEN.lastIndex = 0;
  let open = SLIDE_OPEN.exec(src);
  while (open) {
    const start = open.index;
    const bodyStart = start + open[0].length;
    // Walk forward from bodyStart matching <section>…</section> nesting so a
    // nested section doesn't end the slide early.
    const tag = /<\s*(\/?)section\b[^>]*>/gi;
    tag.lastIndex = bodyStart;
    let depth = 1;
    let closeEnd = -1;
    let m = tag.exec(src);
    while (m) {
      depth += m[1] ? -1 : 1;
      if (depth === 0) {
        closeEnd = tag.lastIndex;
        break;
      }
      m = tag.exec(src);
    }
    if (closeEnd === -1) {
      // Unclosed final slide (still streaming) — take the rest (opener + body).
      slides.push(src.slice(start));
      break;
    }
    slides.push(src.slice(start, closeEnd));
    SLIDE_OPEN.lastIndex = closeEnd;
    open = SLIDE_OPEN.exec(src);
  }
  return slides;
}

/** Text that only separates deck fences: whitespace and/or `---` rules. */
const SEPARATOR_ONLY = /^[\s]*(?:-{3,}[\s]*)*$/;

/**
 * Collapse consecutive deck fences into ONE deck.
 *
 * The directive asks for a single ```html block with every
 * `<section class="slide">` inside it, and the renderer pages that block.
 * Models intermittently emit one fence PER slide, separated by `---` lines —
 * observed live: a "presentation" arrived as eight stacked "Slide 1 / 1"
 * cards with literal `---` rendered between them. Merging at render time
 * makes the contract hold regardless: deck segments whose intervening text
 * is only whitespace/`---` become one deck (the separators are dropped), and
 * the merged deck is still "streaming" while its last fence is unclosed.
 * Real prose between decks keeps them apart — two decks with a paragraph
 * between them are genuinely two decks.
 */
export function mergeDeckSegments(segments: DiagramSegment[]): DiagramSegment[] {
  const out: DiagramSegment[] = [];
  const isDeckSeg = (s: DiagramSegment | undefined): s is Extract<DiagramSegment, { type: 'diagram' }> =>
    !!s && s.type === 'diagram' && s.lang === 'html' && isDeck(s.code);
  let i = 0;
  while (i < segments.length) {
    const seg = segments[i];
    if (!isDeckSeg(seg)) {
      out.push(seg);
      i += 1;
      continue;
    }
    const codes = [seg.code];
    let closed = seg.closed;
    let j = i + 1;
    while (j < segments.length) {
      const next = segments[j];
      if (next.type === 'text' && SEPARATOR_ONLY.test(next.text) && isDeckSeg(segments[j + 1])) {
        j += 1; // drop the separator, continue into the next fence
        continue;
      }
      if (!isDeckSeg(next)) break;
      codes.push(next.code);
      closed = next.closed;
      j += 1;
    }
    out.push(codes.length === 1 ? seg : { type: 'diagram', code: codes.join('\n'), lang: 'html', closed });
    i = j;
  }
  return out;
}

