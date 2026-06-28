import React from 'react';
import { createRoot } from 'react-dom/client';
import html2canvas from 'html2canvas';
import { jsPDF } from 'jspdf';
import A2uiSurface from '../components/Chat/A2uiSurface';
import type { Surface } from '../../../shared/a2ui';

/** Deck slides export at 16:9 landscape; documents at a readable page width. */
const SLIDE_W = 1280;
const SLIDE_H = 720;
const DOC_W = 1100;
/** html2canvas raster scale — 2× keeps text crisp in the PDF. */
const RASTER_SCALE = 2;

/** Mount a surface offscreen, rasterize it, and unmount. The container is
 *  parked far off-viewport (NOT display:none — html2canvas needs layout). Renders
 *  through the SAME A2uiSurface as the live preview, so the PDF matches on screen
 *  (workspace branding + any per-surface "Look" restyle on surface.theme apply). */
async function rasterizeSurface(
  surface: Surface,
  width: number,
  height?: number,
): Promise<HTMLCanvasElement> {
  const container = document.createElement('div');
  container.style.cssText =
    `position:fixed;left:-10000px;top:0;width:${width}px;` +
    (height ? `height:${height}px;overflow:hidden;` : '');
  document.body.appendChild(container);
  const root = createRoot(container);
  try {
    // hideDownloads: never bake the deck "PowerPoint" / table "CSV" control
    // buttons into the rasterized page.
    root.render(<A2uiSurface surface={surface} hideDownloads />);
    // Two frames: one for React's commit, one for layout/paint to settle.
    await new Promise((r) => requestAnimationFrame(() => requestAnimationFrame(r)));
    return await html2canvas(container, {
      scale: RASTER_SCALE,
      backgroundColor: null,
      logging: false,
    });
  } finally {
    root.unmount();
    container.remove();
  }
}

/** Slide ids of a presentation surface, or null when it is not a deck. */
function slideIdsOf(surface: Surface): string[] | null {
  const byId = Object.fromEntries(surface.components.map((c) => [c.id, c]));
  const root = byId[surface.root];
  if (!root || root.component !== 'SlideDeck') return null;
  const children = Array.isArray(root.children) ? root.children : [];
  const ids = children.filter((id) => byId[id]);
  return ids.length > 0 ? ids : null;
}

/**
 * Download the rendered surface as a PDF file (no print dialog):
 *  - a presentation exports in LANDSCAPE, one slide per page (each slide is
 *    re-rendered offscreen with that slide as the root, so every slide is
 *    captured — not just the one currently shown);
 *  - any other deliverable exports as a single page sized to its content, so
 *    nothing is cut at arbitrary page breaks.
 */
export async function downloadSurfacePdf(surface: Surface, title: string): Promise<void> {
  const filename = `${(title || 'kasal-app').replace(/[\\/:*?"<>|]/g, '').trim() || 'kasal-app'}.pdf`;
  const slideIds = slideIdsOf(surface);

  if (slideIds) {
    const pdf = new jsPDF({
      orientation: 'landscape',
      unit: 'px',
      format: [SLIDE_W, SLIDE_H],
      hotfixes: ['px_scaling'],
    });
    for (let i = 0; i < slideIds.length; i += 1) {
      // Each slide becomes the surface root → the stage (theme background,
      // padding) wraps every slide exactly like the live preview.
      const canvas = await rasterizeSurface({ ...surface, root: slideIds[i] }, SLIDE_W, SLIDE_H);
      if (i > 0) pdf.addPage([SLIDE_W, SLIDE_H], 'landscape');
      pdf.addImage(canvas.toDataURL('image/png'), 'PNG', 0, 0, SLIDE_W, SLIDE_H);
    }
    pdf.save(filename);
    return;
  }

  const canvas = await rasterizeSurface(surface, DOC_W);
  const pageW = canvas.width / RASTER_SCALE;
  const pageH = canvas.height / RASTER_SCALE;
  const pdf = new jsPDF({
    orientation: pageW > pageH ? 'landscape' : 'portrait',
    unit: 'px',
    format: [pageW, pageH],
    hotfixes: ['px_scaling'],
  });
  pdf.addImage(canvas.toDataURL('image/png'), 'PNG', 0, 0, pageW, pageH);
  pdf.save(filename);
}
