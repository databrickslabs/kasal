import { describe, it, expect } from 'vitest';
import {
  buildMdSandboxCell,
  hasDiagram,
  notebookSafe,
  splitDiagramSegments,
} from './mdSandboxDiagram';

describe('notebookSafe', () => {
  it('strips blank / whitespace-only lines but keeps indentation', () => {
    const src = '<div>\n\n  <svg></svg>\n   \n</div>';
    expect(notebookSafe(src)).toBe('<div>\n  <svg></svg>\n</div>');
  });
});

describe('buildMdSandboxCell', () => {
  it('prefixes the %md-sandbox magic and sanitizes blank lines', () => {
    expect(buildMdSandboxCell('<div>\n\n<b>x</b>\n</div>')).toBe(
      '%md-sandbox\n<div>\n<b>x</b>\n</div>',
    );
  });
});

describe('splitDiagramSegments', () => {
  it('returns a single text segment when there is no diagram', () => {
    const segs = splitDiagramSegments('just some prose');
    expect(segs).toEqual([{ type: 'text', text: 'just some prose' }]);
    expect(hasDiagram(segs)).toBe(false);
  });

  it('ignores non-diagram fences (bare / other languages)', () => {
    expect(hasDiagram(splitDiagramSegments('```\ncode\n```'))).toBe(false);
    expect(hasDiagram(splitDiagramSegments('```python\nx=1\n```'))).toBe(false);
  });

  it('extracts a closed html block with surrounding prose', () => {
    const content = 'Here you go:\n```html\n<svg></svg>\n```\nDone.';
    const segs = splitDiagramSegments(content);
    expect(segs).toEqual([
      { type: 'text', text: 'Here you go:\n' },
      { type: 'diagram', code: '<svg></svg>', lang: 'html', closed: true },
      { type: 'text', text: 'Done.' },
    ]);
  });

  it('marks an unclosed (streaming) block as not closed and takes the rest as its body', () => {
    const content = 'building:\n```html\n<svg><rect';
    const segs = splitDiagramSegments(content);
    expect(segs[0]).toEqual({ type: 'text', text: 'building:\n' });
    expect(segs[1]).toEqual({ type: 'diagram', code: '<svg><rect', lang: 'html', closed: false });
  });

  it('supports svg fences and multiple blocks', () => {
    const content = '```svg\n<svg/>\n```\nmid\n```html\n<div/>\n```';
    const segs = splitDiagramSegments(content);
    const diagrams = segs.filter((s) => s.type === 'diagram');
    expect(diagrams).toHaveLength(2);
    expect(diagrams[0]).toMatchObject({ lang: 'svg', closed: true });
    expect(diagrams[1]).toMatchObject({ lang: 'html', closed: true });
  });
});
