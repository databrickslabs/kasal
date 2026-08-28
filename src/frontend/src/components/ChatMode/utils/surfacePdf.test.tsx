import { vi, describe, it, expect, beforeEach } from 'vitest';
import { downloadSurfacePdf } from './surfacePdf';
import type { Surface } from '../../../shared/a2ui';

// ---------------------------------------------------------------------------
// Mocks — rasterization and PDF assembly are third-party; we verify the
// orchestration: offscreen render, page-per-slide, orientation, cleanup.
// ---------------------------------------------------------------------------

// vi.hoisted: the mock factories run when the mocked modules are first
// imported (before this file's top-level consts initialize), so the spies
// must be created in a hoisted block. vi.fn() is `new`-constructible; the
// implementation's return value becomes the constructed instance.
const { html2canvasMock, addImage, addPage, save, jsPDFCtor } = vi.hoisted(() => {
  const addImage = vi.fn();
  const addPage = vi.fn();
  const save = vi.fn();
  return {
    html2canvasMock: vi.fn(),
    addImage,
    addPage,
    save,
    // a regular function (not an arrow) so `new jsPDF(...)` is constructible;
    // returning an object makes it the constructed instance
    jsPDFCtor: vi.fn(function jsPDFMock() {
      return { addImage, addPage, save };
    }),
  };
});
vi.mock('html2canvas', () => ({ default: html2canvasMock }));
vi.mock('jspdf', () => ({ jsPDF: jsPDFCtor }));

const fakeCanvas = (width: number, height: number) => ({
  width,
  height,
  toDataURL: vi.fn(() => `data:image/png;base64,${width}x${height}`),
});

beforeEach(() => {
  vi.clearAllMocks();
  // Default raster: a 2×-scaled landscape slide capture.
  html2canvasMock.mockResolvedValue(fakeCanvas(2560, 1440));
});

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------


const docSurface = (): Surface => ({
  surfaceKind: 'document',
  root: 'root',
  components: [
    { id: 'root', component: 'Column', children: ['t'] },
    { id: 't', component: 'Text', text: 'Report body' },
  ],
  dataModel: {},
});

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('downloadSurfacePdf — other deliverables', () => {
  it('exports a single content-sized portrait page', async () => {
    html2canvasMock.mockResolvedValue(fakeCanvas(2200, 6000)); // tall report at 2× scale

    await downloadSurfacePdf(docSurface(), 'Quarterly Report');

    expect(jsPDFCtor.mock.calls[0][0]).toMatchObject({
      orientation: 'portrait',
      format: [1100, 3000],
    });
    expect(addImage).toHaveBeenCalledTimes(1);
    expect(addPage).not.toHaveBeenCalled();
    expect(save).toHaveBeenCalledWith('Quarterly Report.pdf');
  });

  it('uses landscape when the content is wider than tall', async () => {
    html2canvasMock.mockResolvedValue(fakeCanvas(2200, 1000));
    await downloadSurfacePdf(docSurface(), 'Wide');
    expect(jsPDFCtor.mock.calls[0][0]).toMatchObject({ orientation: 'landscape' });
  });

  it('treats a SlideDeck root with no resolvable slides as a plain document', async () => {
    const surface: Surface = {
      surfaceKind: 'presentation',
      root: 'root',
      components: [{ id: 'root', component: 'SlideDeck', children: ['missing'] }],
      dataModel: {},
    };
    await downloadSurfacePdf(surface, 'Empty Deck');
    expect(html2canvasMock).toHaveBeenCalledTimes(1); // single-page path
    expect(addPage).not.toHaveBeenCalled();
  });

  it('sanitizes the filename and falls back when the title is empty', async () => {
    await downloadSurfacePdf(docSurface(), 'a/b:c*d?"<>|');
    expect(save).toHaveBeenCalledWith('abcd.pdf');

    save.mockClear();
    await downloadSurfacePdf(docSurface(), '');
    expect(save).toHaveBeenCalledWith('kasal-app.pdf');

    // A title that sanitizes down to nothing also falls back.
    save.mockClear();
    await downloadSurfacePdf(docSurface(), '???');
    expect(save).toHaveBeenCalledWith('kasal-app.pdf');
  });

  it('treats a SlideDeck root with malformed children (or a missing root) as a plain document', async () => {
    const malformed: Surface = {
      surfaceKind: 'presentation',
      root: 'root',
      components: [{ id: 'root', component: 'SlideDeck', children: 'nope' as never }],
      dataModel: {},
    };
    await downloadSurfacePdf(malformed, 'X');
    expect(addPage).not.toHaveBeenCalled(); // single-page document path

    vi.clearAllMocks();
    html2canvasMock.mockResolvedValue(fakeCanvas(2200, 1000));
    const missingRoot: Surface = { surfaceKind: 'document', root: 'ghost', components: [], dataModel: {} };
    await downloadSurfacePdf(missingRoot, 'Y');
    expect(addPage).not.toHaveBeenCalled();
  });

  it('cleans the offscreen container up even when rasterization fails', async () => {
    html2canvasMock.mockRejectedValue(new Error('canvas boom'));
    await expect(downloadSurfacePdf(docSurface(), 'X')).rejects.toThrow('canvas boom');
    expect(document.body.querySelectorAll('div').length).toBe(0);
    expect(save).not.toHaveBeenCalled();
  });
});
