/**
 * Export an HTML slide deck to PDF or PowerPoint.
 *
 * Each slide (a full `<section class="slide">`) is rendered onto an offscreen
 * 1280×720 stage in the app document, then:
 *  - PDF: rasterized with html2canvas — one landscape page per slide (jsPDF).
 *    A PDF is a print artifact, so pixels are the right currency there.
 *  - PPTX: converted to EDITABLE PowerPoint — the rendered DOM is walked and
 *    each element becomes a real pptxgenjs primitive: backgrounds and cards
 *    become shapes, text becomes text boxes (size/weight/color/alignment
 *    carried over), inline SVG diagrams become embedded images. The result
 *    opens in PowerPoint as slides you can edit, not screenshots.
 *
 * The slide HTML is sanitized (scripts / event handlers / javascript: URLs
 * stripped) before it is injected into the app document, since — unlike the
 * sandboxed preview — this render happens in the app origin. Heavy libs are
 * imported dynamically so they only load when a download is triggered.
 */
import { SLIDE_H, SLIDE_W } from './htmlDeck';

// Strip anything executable before injecting agent HTML into the app document.
function sanitizeForRender(html: string): string {
  return html
    .replace(/<script[\s\S]*?<\/script>/gi, '')
    .replace(/<\/?(?:iframe|object|embed|link|meta)\b[^>]*>/gi, '')
    .replace(/\son\w+\s*=\s*"[^"]*"/gi, '')
    .replace(/\son\w+\s*=\s*'[^']*'/gi, '')
    .replace(/\son\w+\s*=\s*[^\s>]+/gi, '')
    .replace(/javascript:/gi, '');
}

function mountSlide(sectionHtml: string): HTMLDivElement {
  const host = document.createElement('div');
  host.style.cssText =
    `position:fixed;left:-100000px;top:0;width:${SLIDE_W}px;height:${SLIDE_H}px;` +
    'overflow:hidden;background:#fff;';
  host.innerHTML =
    `<style>.kwrap>section.slide{width:${SLIDE_W}px;height:${SLIDE_H}px;` +
    'box-sizing:border-box;overflow:hidden;}</style>' +
    `<div class="kwrap" style="width:${SLIDE_W}px;height:${SLIDE_H}px;overflow:hidden;background:#fff">` +
    `${sanitizeForRender(sectionHtml)}</div>`;
  document.body.appendChild(host);
  return host;
}

async function renderSlideCanvas(sectionHtml: string): Promise<HTMLCanvasElement> {
  const html2canvas = (await import('html2canvas')).default;
  const host = mountSlide(sectionHtml);
  try {
    // Let layout/webfonts settle before capture.
    await new Promise((r) => setTimeout(r, 40));
    return await html2canvas(host, {
      width: SLIDE_W,
      height: SLIDE_H,
      scale: 2,
      backgroundColor: '#ffffff',
      useCORS: true,
      logging: false,
    });
  } finally {
    host.remove();
  }
}

export async function downloadDeckPdf(slides: string[], filename = 'presentation.pdf'): Promise<void> {
  const { jsPDF } = await import('jspdf');
  const pdf = new jsPDF({ orientation: 'landscape', unit: 'px', format: [SLIDE_W, SLIDE_H] });
  for (let i = 0; i < slides.length; i++) {
    const canvas = await renderSlideCanvas(slides[i]);
    if (i > 0) pdf.addPage([SLIDE_W, SLIDE_H], 'landscape');
    pdf.addImage(canvas.toDataURL('image/jpeg', 0.92), 'JPEG', 0, 0, SLIDE_W, SLIDE_H);
  }
  pdf.save(filename);
}

// ---------------------------------------------------------------------------
// Editable PPTX: rendered DOM -> pptxgenjs primitives
// ---------------------------------------------------------------------------

// 1280px stage ↔ 13.333in PowerPoint wide layout: exactly 96 px/in.
const PX_PER_IN = 96;
/** px → inches on the 13.333×7.5in stage. Exported for tests. */
export const pxToIn = (px: number): number => Math.round((px / PX_PER_IN) * 1000) / 1000;
/** CSS px → PowerPoint points (72pt/in over 96px/in). Exported for tests. */
export const pxToPt = (px: number): number => Math.round(px * 0.75 * 10) / 10;

/** 'rgb(a)…' / '#hex' → 'RRGGBB' (pptx color), or null when transparent. */
export function cssColorToPptx(color: string | null | undefined): string | null {
  const c = (color || '').trim();
  if (!c || c === 'transparent') return null;
  const hex = c.match(/^#([0-9a-f]{6}|[0-9a-f]{3})\b/i);
  if (hex) {
    let h = hex[1];
    if (h.length === 3) h = h.split('').map((ch) => ch + ch).join('');
    return h.toUpperCase();
  }
  const rgb = c.match(/^rgba?\(\s*(\d+)[,\s]+(\d+)[,\s]+(\d+)(?:[,\s/]+([\d.]+))?/i);
  if (rgb) {
    if (rgb[4] !== undefined && parseFloat(rgb[4]) === 0) return null; // fully transparent
    return [rgb[1], rgb[2], rgb[3]]
      .map((n) => Math.min(255, parseInt(n, 10)).toString(16).padStart(2, '0'))
      .join('')
      .toUpperCase();
  }
  return null;
}

/** First color of a CSS gradient — PowerPoint shapes get a flat approximation. */
export function gradientFirstColor(backgroundImage: string | null | undefined): string | null {
  const m = (backgroundImage || '').match(/(#[0-9a-f]{3,8}|rgba?\([^)]*\))/i);
  return m ? cssColorToPptx(m[1]) : null;
}

/** True when the element only contains inline-ish content, so its innerText is
 *  one coherent text block (a heading, a bullet, a label). Exported for tests. */
export function isTextBlock(el: HTMLElement): boolean {
  if (!el.innerText || !el.innerText.trim()) return false;
  for (const child of Array.from(el.children)) {
    const d = getComputedStyle(child as HTMLElement).display;
    if (d !== 'inline' && d !== 'inline-block' && d !== 'inline-flex' && d !== 'none') return false;
  }
  return true;
}

interface StageRect { left: number; top: number }

function rectOf(el: Element, stage: StageRect) {
  const r = el.getBoundingClientRect();
  return {
    x: r.left - stage.left,
    y: r.top - stage.top,
    w: r.width,
    h: r.height,
  };
}

function visible(el: HTMLElement): boolean {
  const cs = getComputedStyle(el);
  if (cs.display === 'none' || cs.visibility === 'hidden' || parseFloat(cs.opacity || '1') === 0) return false;
  const r = el.getBoundingClientRect();
  return r.width >= 1 && r.height >= 1;
}

async function svgToDataUrl(svg: SVGElement, w: number, h: number): Promise<string | null> {
  try {
    const xml = new XMLSerializer().serializeToString(svg);
    const url = 'data:image/svg+xml;charset=utf-8,' + encodeURIComponent(xml);
    const img = new Image();
    await new Promise<void>((res, rej) => {
      img.onload = () => res();
      img.onerror = () => rej(new Error('svg load failed'));
      img.src = url;
    });
    const canvas = document.createElement('canvas');
    canvas.width = Math.max(2, Math.round(w * 2));
    canvas.height = Math.max(2, Math.round(h * 2));
    const ctx = canvas.getContext('2d');
    if (!ctx) return null;
    ctx.drawImage(img, 0, 0, canvas.width, canvas.height);
    return canvas.toDataURL('image/png');
  } catch {
    return null;
  }
}

/* eslint-disable @typescript-eslint/no-explicit-any -- pptxgenjs slide API */
async function addSlideFromDom(pptxSlide: any, section: HTMLElement): Promise<void> {
  const stageR = section.getBoundingClientRect();
  const stage: StageRect = { left: stageR.left, top: stageR.top };

  // Slide background from the section itself (solid color, or a gradient's
  // first stop as a flat approximation).
  const sectionCs = getComputedStyle(section);
  const bg = cssColorToPptx(sectionCs.backgroundColor) ?? gradientFirstColor(sectionCs.backgroundImage);
  if (bg) pptxSlide.background = { color: bg };

  // PASS 1 — shapes and images, in document order so parents paint under
  // children. Text is deliberately NOT emitted here so every box lands
  // beneath every label.
  const skipInside = new Set<Element>();
  const all = Array.from(section.querySelectorAll<HTMLElement>('*'));
  for (const el of all) {
    if ([...skipInside].some((s) => s.contains(el))) continue;
    if (el instanceof SVGElement) {
      // An inline SVG (a drawn diagram) embeds as one image; its internals are
      // not walked.
      const r = rectOf(el, stage);
      const data = await svgToDataUrl(el as unknown as SVGElement, r.w, r.h);
      if (data) {
        pptxSlide.addImage({ data, x: pxToIn(r.x), y: pxToIn(r.y), w: pxToIn(r.w), h: pxToIn(r.h) });
      }
      skipInside.add(el);
      continue;
    }
    if (!(el instanceof HTMLElement) || !visible(el)) continue;
    if (el.tagName === 'IMG') {
      const src = (el as HTMLImageElement).src || '';
      if (src.startsWith('data:')) {
        const r = rectOf(el, stage);
        pptxSlide.addImage({ data: src, x: pxToIn(r.x), y: pxToIn(r.y), w: pxToIn(r.w), h: pxToIn(r.h) });
      }
      continue;
    }
    const cs = getComputedStyle(el);
    const fill = cssColorToPptx(cs.backgroundColor) ?? gradientFirstColor(cs.backgroundImage);
    const borderC = parseFloat(cs.borderTopWidth || '0') >= 1 ? cssColorToPptx(cs.borderTopColor) : null;
    if (!fill && !borderC) continue;
    const r = rectOf(el, stage);
    if (r.w < 2 || r.h < 2) continue;
    const radius = parseFloat(cs.borderTopLeftRadius || '0');
    pptxSlide.addShape(radius >= 2 ? 'roundRect' : 'rect', {
      x: pxToIn(r.x),
      y: pxToIn(r.y),
      w: pxToIn(r.w),
      h: pxToIn(r.h),
      fill: fill ? { color: fill } : { type: 'none' },
      line: borderC ? { color: borderC, width: pxToPt(parseFloat(cs.borderTopWidth)) } : { type: 'none' },
      rectRadius: radius >= 2 ? Math.min(pxToIn(radius), 0.2) : undefined,
    });
  }

  // PASS 2 — text blocks, on top. A handled block's subtree is skipped so
  // nested inline markup does not double-emit.
  const textHandled = new Set<Element>();
  for (const el of all) {
    if (!(el instanceof HTMLElement) || el instanceof SVGElement) continue;
    if ([...textHandled].some((s) => s.contains(el))) continue;
    if ([...skipInside].some((s) => s.contains(el))) continue;
    if (!visible(el) || !isTextBlock(el)) continue;
    const cs = getComputedStyle(el);
    const r = rectOf(el, stage);
    let text = el.innerText.replace(/\s+\n/g, '\n').trim();
    if (cs.textTransform === 'uppercase') text = text.toUpperCase();
    const weight = parseInt(cs.fontWeight, 10) || 400;
    pptxSlide.addText(text, {
      x: pxToIn(r.x),
      y: pxToIn(r.y),
      w: Math.max(pxToIn(r.w), 0.3),
      h: Math.max(pxToIn(r.h), 0.25),
      fontSize: pxToPt(parseFloat(cs.fontSize) || 16),
      color: cssColorToPptx(cs.color) ?? '111111',
      bold: weight >= 600,
      italic: cs.fontStyle === 'italic',
      align: (['left', 'center', 'right', 'justify'].includes(cs.textAlign) ? cs.textAlign : 'left') as any,
      valign: 'top',
      fontFace: (cs.fontFamily || '').split(',')[0]?.replace(/["']/g, '').trim() || undefined,
      margin: 0,
      // Prevent PowerPoint enlarging a box whose crude metrics disagree.
      fit: 'shrink',
    });
    textHandled.add(el);
  }
}
/* eslint-enable @typescript-eslint/no-explicit-any */

export async function downloadDeckPptx(slides: string[], filename = 'presentation.pptx'): Promise<void> {
  const PptxGenJS = (await import('pptxgenjs')).default;
  const pptx = new PptxGenJS();
  pptx.defineLayout({ name: 'KAS_16X9', width: 13.333, height: 7.5 });
  pptx.layout = 'KAS_16X9';
  for (const sectionHtml of slides) {
    const host = mountSlide(sectionHtml);
    try {
      await new Promise((r) => setTimeout(r, 40)); // let layout settle
      const section = host.querySelector<HTMLElement>('section.slide') ?? host;
      await addSlideFromDom(pptx.addSlide(), section);
    } catch (err) {
      // A slide the walker cannot convert falls back to a screenshot rather
      // than being dropped — the deck stays complete, that one slide is not
      // editable.
      console.error('[deckExport] editable conversion failed; slide falls back to image', err);
      const canvas = await renderSlideCanvas(sectionHtml);
      pptx.addSlide().addImage({ data: canvas.toDataURL('image/png'), x: 0, y: 0, w: 13.333, h: 7.5 });
    } finally {
      host.remove();
    }
  }
  await pptx.writeFile({ fileName: filename });
}
