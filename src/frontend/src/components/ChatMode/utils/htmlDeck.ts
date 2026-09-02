/**
 * HTML slide-deck contract + parsing.
 *
 * A deck is a single ```html block whose slides are delimited by
 * `<section class="slide">…</section>`. Each slide is authored to fill a fixed
 * 1280×720 (16:9) logical stage with inline, self-contained styling. That
 * delimiter is what lets us page the deck on screen AND export it (one PDF page
 * / one PPTX slide per <section>). Freeform HTML/SVG is allowed *inside* a slide.
 */

import { splitDiagramSegments, type DiagramSegment } from './mdSandboxDiagram';

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
  return slideRanges(src).map((r) => src.slice(r.start, r.end));
}

/** Where one slide sits in the deck's HTML: `[start, end)` of its outer HTML. */
export interface SlideRange {
  start: number;
  end: number;
}

/**
 * The position of every slide in the deck, in order — the single parse that
 * `splitSlides` and the slide edits below share. A slide edit splices the
 * deck's own text at these offsets, so everything between the slides (a
 * shared `<style>`, comments, whitespace) survives untouched.
 */
export function slideRanges(html: string): SlideRange[] {
  const src = html || '';
  const ranges: SlideRange[] = [];
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
      ranges.push({ start, end: src.length });
      break;
    }
    ranges.push({ start, end: closeEnd });
    SLIDE_OPEN.lastIndex = closeEnd;
    open = SLIDE_OPEN.exec(src);
  }
  return ranges;
}

// ── Slide edits ──────────────────────────────────────────────────────────────
// A deck is refined one slide at a time (see utils/slideRefine): the model
// writes ONE <section>, and these splice it into the deck the reader already
// has. Every other slide stays byte-identical — the point of the exercise.

/**
 * Marks the slide a refine changed, on its opening tag. The deck opens on it
 * (HtmlDeckBlock) so the reader lands on what changed rather than on the cover;
 * cleared before the next edit so only the latest change is marked.
 */
export const REFINED_ATTR = 'data-refined';
const REFINED_RE = /\s+data-refined\s*=\s*["'][^"']*["']/gi;

/** The deck with slide `index` (0-based) replaced by `section`. */
export function replaceSlide(html: string, index: number, section: string): string {
  const src = html || '';
  const r = slideRanges(src)[index];
  if (!r) return src;
  return src.slice(0, r.start) + section + src.slice(r.end);
}

/**
 * The deck with `section` inserted so that it becomes slide `index` (0-based;
 * `index >= count` appends after the last slide).
 */
export function insertSlide(html: string, index: number, section: string): string {
  const src = html || '';
  const ranges = slideRanges(src);
  if (ranges.length === 0) return src + section;
  const at = Math.max(0, Math.min(index, ranges.length));
  if (at < ranges.length) {
    const pos = ranges[at].start;
    return src.slice(0, pos) + section + '\n' + src.slice(pos);
  }
  const pos = ranges[ranges.length - 1].end;
  return src.slice(0, pos) + '\n' + section + src.slice(pos);
}

/** The deck without slide `index`. A one-slide deck is left alone. */
export function removeSlide(html: string, index: number): string {
  const src = html || '';
  const ranges = slideRanges(src);
  const r = ranges[index];
  if (!r || ranges.length <= 1) return src;
  return src.slice(0, r.start) + src.slice(r.end).replace(/^\s*\n/, '');
}

/** The deck with slide `from` moved so that it becomes slide `to`. */
export function moveSlide(html: string, from: number, to: number): string {
  const src = html || '';
  const slides = splitSlides(src);
  if (!slides[from] || to < 0 || to >= slides.length || from === to) return src;
  // Remove, then insert at the target: after the removal the slides above
  // `from` have shifted down by one, which is exactly what makes `to` the
  // final position in both directions ([A,B,C,D] 0→2 gives [B,C,A,D]).
  return insertSlide(removeSlide(src, from), to, slides[from]);
}

/** The deck with a copy of slide `index` right after it. */
export function duplicateSlide(html: string, index: number): string {
  const src = html || '';
  const slide = splitSlides(src)[index];
  return slide ? insertSlide(src, index + 1, slide) : src;
}

/** `section` with the refined marker on its opening tag. */
export function markRefined(section: string): string {
  return (section || '').replace(REFINED_RE, '').replace(/^(\s*<section\b)/i, `$1 ${REFINED_ATTR}="1"`);
}

/** The deck with every refined marker removed. */
export function clearRefined(html: string): string {
  return (html || '').replace(REFINED_RE, '');
}

/** Index of the slide carrying the refined marker, or -1. */
export function refinedSlideIndex(html: string): number {
  return splitSlides(html).findIndex((s) => {
    const open = s.match(/^\s*<section\b[^>]*>/i);
    return !!open && /\bdata-refined\b/i.test(open[0]);
  });
}

/**
 * The one COMPLETE slide a model reply carries — the first
 * `<section class="slide">…</section>` inside its ```html fence (or bare, when
 * the model skipped the fence) — or null when the reply holds no finished slide.
 */
export function sectionFromReply(reply: string): string | null {
  const text = reply || '';
  const fence = splitDiagramSegments(text).find((seg) => seg.type === 'diagram');
  const code = fence && fence.type === 'diagram' ? fence.code : text;
  const first = splitSlides(code)[0];
  return first && /<\/section>\s*$/i.test(first) ? first : null;
}

/**
 * One slide on the fixed stage: the section is forced to the stage size even
 * when the model omitted explicit dimensions. Shared by the deck card, the
 * studio's thumbnails and stage, and the presentation view.
 */
export function stageFor(section: string): string {
  return (
    `<style>.kwrap>section.slide{width:${SLIDE_W}px;height:${SLIDE_H}px;` +
    'box-sizing:border-box;overflow:hidden;}</style>' +
    `<div class="kwrap" style="width:${SLIDE_W}px;height:${SLIDE_H}px;overflow:hidden;background:#fff">` +
    `${section}</div>`
  );
}

/**
 * The message's content with its deck replaced by `code` — the text around
 * the deck stays, and a deck the model emitted as several fences comes back
 * as the one fence the contract asks for. Content with no deck is returned
 * unchanged.
 */
export function replaceDeckInContent(content: string, code: string): string {
  const segments = mergeDeckSegments(splitDiagramSegments(content || ''));
  let replaced = false;
  return segments
    .map((seg, i) => {
      if (seg.type === 'text') return seg.text;
      // The parser consumed the newline after a closing fence; give it back
      // whenever something follows, so the prose keeps its spacing.
      const tail = i < segments.length - 1 ? '\n' : '';
      if (!replaced && seg.lang === 'html' && isDeck(seg.code)) {
        replaced = true;
        return fenceDeck(code) + tail;
      }
      return '```' + seg.lang + '\n' + seg.code.replace(/\n?$/, '\n') + '```' + tail;
    })
    .join('');
}

/** A deck as the chat renders it: one ```html fence around the whole thing. */
export function fenceDeck(html: string): string {
  return '```html\n' + (html || '').trim() + '\n```';
}

/**
 * A reply that is a bare deck (no fence) gets its fence back; anything else is
 * returned as is. The refiner is told to return "the complete artifact with no
 * code fences" — right for a document, wrong for a deck, which the chat only
 * pages when it arrives fenced.
 */
export function ensureDeckFence(reply: string): string {
  const text = reply || '';
  if (!isDeck(text)) return text;
  return splitDiagramSegments(text).some((seg) => seg.type === 'diagram') ? text : fenceDeck(text);
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

