import { afterEach, describe, expect, it, vi } from 'vitest';
import { downloadDeckPdf, downloadDeckPptx, sanitizeForRender } from './deckExport';

const capture = vi.hoisted(() => ({ html: '' }));
vi.mock('html2canvas', () => ({ default: vi.fn(async (host: HTMLElement) => {
  capture.html = host.innerHTML;
  return { toDataURL: () => 'data:image/png;base64,AA==' };
}) }));
vi.mock('jspdf', () => ({ jsPDF: class {
  addImage() {} save() {} addPage() {}
} }));
vi.mock('pptxgenjs', () => ({ default: class {
  defineLayout() {} async writeFile() {}
  addSlide() {
    capture.html = document.querySelector('.kwrap')?.innerHTML ?? '';
    return { addShape() {}, addText() {}, addImage() {} };
  }
} }));

const attack = '<section class="slide"><svg/onload="window.__security_review_marker=1"></svg></section>';
afterEach(() => { document.body.innerHTML = ''; capture.html = ''; vi.unstubAllGlobals(); });

describe('deck export security', () => {
  it.each([
    attack,
    '<section class="slide"><img src="x" onerror=alert(1)></section>',
    '<section class="slide"><a href="java&#x73;cript:alert(1)">link</a></section>',
    '<section class="slide"><svg><a xlink:href="javascript:alert(1)">link</a></svg></section>',
    '<section class="slide"><script>alert(1)</script><iframe srcdoc="bad"></iframe></section>',
  ])('removes active content through the HTML parser: %s', (html) => {
    const host = document.createElement('div');
    host.innerHTML = sanitizeForRender(html);
    expect(host.querySelector('script, iframe, object, embed')).toBeNull();
    for (const node of host.querySelectorAll('*')) {
      for (const attr of node.attributes) {
        expect(attr.name).not.toMatch(/^on/i);
        expect(attr.value).not.toMatch(/javascript:/i);
      }
    }
  });

  it('keeps slide styling, text, image data and inert SVG diagrams', () => {
    const host = document.createElement('div');
    host.innerHTML = sanitizeForRender('<section class="slide" style="padding:20px;color:red"><h1>Title</h1><img src="data:image/png;base64,AA=="><svg viewBox="0 0 10 10"><path d="M0 0L10 10" stroke="blue"/></svg></section>');
    expect(host.querySelector('section')?.style.padding).toBe('20px');
    expect(host.querySelector('h1')?.textContent).toBe('Title');
    expect(host.querySelector('img')?.getAttribute('src')).toMatch(/^data:image\/png/);
    expect(host.querySelector('path')?.getAttribute('stroke')).toBe('blue');
  });

  it.each([['PDF', downloadDeckPdf], ['PowerPoint', downloadDeckPptx]] as const)(
    'sanitizes the actual %s export document and removes the temporary host', async (_name, download) => {
      // jsdom does not load SVG image blobs; exercise the export DOM while
      // making that separate image conversion fail immediately.
      vi.stubGlobal('Image', class {
        onerror: (() => void) | null = null;
        set src(_value: string) { this.onerror?.(); }
      });
      await download([attack]);
      expect(capture.html).toContain('<svg');
      expect(capture.html).not.toMatch(/onload|__security_review_marker/);
      expect(document.querySelector('.kwrap')).toBeNull();
    },
  );
});
