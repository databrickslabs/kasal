import { afterEach, describe, expect, it, vi } from 'vitest';
import { downloadDeckPdf, downloadDeckPptx, sanitizeForRender, standaloneDeckHtml } from './deckExport';

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

  it('builds a styled standalone HTML player without agent-authored active content', () => {
    const html = standaloneDeckHtml(
      '<style>.slide{color:red}</style>' + attack + '<section class="slide"><h1>Two</h1></section>',
    );
    const doc = new DOMParser().parseFromString(html, 'text/html');
    expect(doc.querySelectorAll('#deck-stage > section.slide')).toHaveLength(2);
    expect(doc.querySelector('style')?.textContent).toContain('.slide');
    expect(doc.querySelectorAll('script')).toHaveLength(1);
    expect(doc.querySelector('script')?.textContent).not.toContain('__security_review_marker');
    expect(doc.querySelector('svg')?.hasAttribute('onload')).toBe(false);
    expect(doc.querySelector('#deck-controls')).not.toBeNull();
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

describe('deck export image policy', () => {
  const remote = 'https://images.example.org';
  const slide =
    '<section class="slide">' +
    `<img src="${remote}/x.png?d=1" alt="Revenue chart">` +
    '<img src="data:image/png;base64,AA==" alt="inline">' +
    `<img src="/assets/a.png" srcset="${remote}/2x.png 2x" alt="local">` +
    `<div id="bg" style="color: red; background: url(${remote}/bg.png) center"></div>` +
    `<div id="bg2" style="background-image: url('data:image/png;base64,AA==')"></div>` +
    `<table background="${remote}/t.png"><tr><td>x</td></tr></table>` +
    `<svg><image href="${remote}/i.png"></image><rect fill="url(#g)" width="1" height="1"></rect></svg>` +
    '<a href="https://example.com/page">source</a>' +
    '</section>';

  it('keeps nothing that would fetch from another host; inline and same-origin stay', () => {
    const out = sanitizeForRender(slide);
    expect(out).not.toContain('images.example.org');
    const host = document.createElement('div');
    host.innerHTML = out;
    expect(host.textContent).toContain('Revenue chart');
    expect([...host.querySelectorAll('img')].map((i) => i.getAttribute('src'))).toEqual([
      'data:image/png;base64,AA==',
      '/assets/a.png',
    ]);
    // A style naming a remote resource is dropped whole, not edited.
    expect(host.querySelector('#bg')?.hasAttribute('style')).toBe(false);
    expect(host.querySelector('#bg2')?.getAttribute('style')).toContain('data:image/png');
    expect(host.querySelector('rect')?.getAttribute('fill')).toBe('url(#g)');
    expect(host.querySelector('a')?.getAttribute('href')).toBe('https://example.com/page');
  });

  it.each([
    ['an unclosed, unquoted url(', `color: red; background: url(${remote}/a.png`],
    ['an unclosed, quoted url(', `color: red; background: url('${remote}/a.png`],
    ['image-set()', `background-image: image-set('${remote}/a.png' 1x)`],
    ['-webkit-image-set()', `background-image: -webkit-image-set(url(/x.png) 1x, '${remote}/a.png' 2x)`],
    ['an escaped url(', `background: u\\72l(${remote}/a.png)`],
  ])('drops a style that could still fetch through %s', (_form, style) => {
    const host = document.createElement('div');
    host.innerHTML = sanitizeForRender(`<section class="slide"><div id="t" style="${style.replace(/"/g, '&quot;')}">x</div></section>`);
    expect(host.querySelector('#t')?.hasAttribute('style')).toBe(false);
    expect(host.innerHTML).not.toContain('images.example.org');
  });

  it('keeps a style whose every url() is local', () => {
    const style = "color: red; background: url(/assets/a.png) no-repeat, url('data:image/png;base64,AA==')";
    const host = document.createElement('div');
    host.innerHTML = sanitizeForRender(`<section class="slide"><div id="t" style="${style.replace(/"/g, '&quot;')}">x</div></section>`);
    const el = host.querySelector('#t') as HTMLElement;
    expect(el.style.color).toBe('red');
    expect(el.getAttribute('style')).toContain('/assets/a.png');
    expect(el.getAttribute('style')).toContain('data:image/png');
  });

  it('applies to the document the PDF export rasterizes', async () => {
    await downloadDeckPdf([slide]);
    expect(capture.html).toContain('Revenue chart');
    expect(capture.html).not.toContain('images.example.org');
  });

  it('leaves other DOMPurify callers alone', async () => {
    const DOMPurify = (await import('dompurify')).default;
    expect(DOMPurify.sanitize(`<img src="${remote}/x.png">`)).toContain('images.example.org');
  });
});
