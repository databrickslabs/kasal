/**
 * HTML slide-deck contract + parsing.
 *
 * A deck is a single ```html block whose slides are delimited by
 * `<section class="slide">…</section>`. Each slide is authored to fill a fixed
 * 1280×720 (16:9) logical stage with inline, self-contained styling. That
 * delimiter is what lets us page the deck on screen AND export it (one PDF page
 * / one PPTX slide per <section>). Freeform HTML/SVG is allowed *inside* a slide.
 */

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
