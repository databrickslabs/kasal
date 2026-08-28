import { describe, expect, it } from 'vitest';
import { iframeDoc, sanitizePartialHtml } from './scaledFrame';

describe('sanitizePartialHtml', () => {
  it('returns complete html unchanged', () => {
    const html = '<section class="slide" style="color:#fff">Hi <b>there</b></section>';
    expect(sanitizePartialHtml(html)).toBe(html);
  });

  it('cuts a tag truncated inside an attribute quote (the real-world case)', () => {
    // Exactly how an output-capped deck ends: mid-style-attribute. Everything
    // after the last complete tag must be dropped, or the parser swallows the
    // frame markup that follows.
    const html =
      '<section class="slide"><div>ok</div>' +
      '<div style="text-transform:uppercase;margin-bottom:';
    expect(sanitizePartialHtml(html)).toBe('<section class="slide"><div>ok</div>');
  });

  it('cuts back to before an unclosed style element', () => {
    const html = '<div>ok</div><style>.a{color:red;';
    expect(sanitizePartialHtml(html)).toBe('<div>ok</div>');
  });

  it('keeps a closed style element intact', () => {
    const html = '<style>.a{color:red}</style><div>ok</div>';
    expect(sanitizePartialHtml(html)).toBe(html);
  });

  it('handles > inside quoted attributes', () => {
    const html = '<div data-x="a > b">t</div>';
    expect(sanitizePartialHtml(html)).toBe(html);
  });
});

describe('iframeDoc', () => {
  it('places the fit script BEFORE the content so partial content cannot swallow it', () => {
    const doc = iframeDoc('<div>x</div>', 'f1');
    expect(doc.indexOf('function fit()')).toBeGreaterThan(-1);
    expect(doc.indexOf('function fit()')).toBeLessThan(doc.indexOf('<div id="ksz">'));
  });

  it('sanitizes truncated content so the document stays well-formed', () => {
    const doc = iframeDoc('<div style="margin-bottom:', 'f2');
    // The incomplete tag is gone; the wrapper closes normally.
    expect(doc).not.toContain('margin-bottom:');
    expect(doc).toContain('</div></div></body></html>');
  });
});
