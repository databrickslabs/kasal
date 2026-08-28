/**
 * Export an HTML slide deck to PDF or PowerPoint.
 *
 * Each slide (a full `<section class="slide">`) is rendered onto an offscreen
 * 1280×720 stage in the app document and rasterized with html2canvas, then:
 *  - PDF: one landscape page per slide (jsPDF).
 *  - PPTX: one full-bleed slide image per slide (pptxgenjs).
 *
 * The rasterized image preserves the exact rendered look. The slide HTML is
 * sanitized (scripts / event handlers / javascript: URLs stripped) before it is
 * injected into the app document, since — unlike the sandboxed preview — this
 * render happens in the app origin. Heavy libs are imported dynamically so they
 * only load when a download is triggered.
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

async function renderSlideCanvas(sectionHtml: string): Promise<HTMLCanvasElement> {
  const html2canvas = (await import('html2canvas')).default;
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

export async function downloadDeckPptx(slides: string[], filename = 'presentation.pptx'): Promise<void> {
  const PptxGenJS = (await import('pptxgenjs')).default;
  const pptx = new PptxGenJS();
  pptx.defineLayout({ name: 'KAS_16X9', width: 13.333, height: 7.5 });
  pptx.layout = 'KAS_16X9';
  for (const section of slides) {
    const canvas = await renderSlideCanvas(section);
    pptx.addSlide().addImage({
      data: canvas.toDataURL('image/png'),
      x: 0,
      y: 0,
      w: 13.333,
      h: 7.5,
    });
  }
  await pptx.writeFile({ fileName: filename });
}
