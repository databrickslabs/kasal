import React from 'react';
import { createRoot } from 'react-dom/client';
import html2canvas from 'html2canvas';
import { jsPDF } from 'jspdf';
import A2uiSurface from '../components/Chat/A2uiSurface';
import type { Surface } from '../../../shared/a2ui';

/** Documents export at a readable page width. */
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
  // CRITICAL: the chat's Tailwind utilities are scoped under `.kasal-chat-root`
  // (tailwind.config `important: '.kasal-chat-root'`), so without this class NONE
  // of the grid/flex/rounded/background/spacing utilities apply and a dashboard
  // rasterizes as unstyled, stacked text. The class (not the id) is what the
  // utilities — and the exported app — key off.
  container.className = 'kasal-chat-root';
  container.style.cssText =
    `position:fixed;left:-10000px;top:0;width:${width}px;` +
    (height ? `height:${height}px;overflow:hidden;` : '');
  document.body.appendChild(container);
  const root = createRoot(container);
  try {
    // hideDownloads: never bake the deck "PowerPoint" / table "CSV" control
    // buttons into the rasterized page.
    root.render(<A2uiSurface surface={surface} hideDownloads />);
    // Let React commit + layout settle (2 frames), THEN wait out recharts' mount
    // animation and ResponsiveContainer measure so charts are fully drawn (not
    // blank or mid-animation) when html2canvas snapshots the DOM.
    await new Promise((r) => requestAnimationFrame(() => requestAnimationFrame(r)));
    await new Promise((r) => setTimeout(r, 500));
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

/**
 * Download the rendered surface as a PDF file (no print dialog): a single page
 * sized to the content, so nothing is cut at arbitrary page breaks.
 */
export async function downloadSurfacePdf(surface: Surface, title: string): Promise<void> {
  const filename = `${(title || 'kasal-app').replace(/[\\/:*?"<>|]/g, '').trim() || 'kasal-app'}.pdf`;

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
