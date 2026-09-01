import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, renderHook } from '@testing-library/react';
import { iframeDoc, sanitizePartialHtml, useScaledFrameHeight, useThrottledPreview } from './scaledFrame';

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

describe('useScaledFrameHeight (streaming = monotonic)', () => {
  const post = (id: string, h: number) =>
    act(() => {
      window.dispatchEvent(
        new MessageEvent('message', { data: { t: 'kasal-frame-height', id, h } }),
      );
    });

  it('only grows while streaming, then follows exactly after the stream ends', () => {
    const { result, rerender } = renderHook(
      ({ streaming }: { streaming: boolean }) => useScaledFrameHeight(120, streaming),
      { initialProps: { streaming: true } },
    );
    const id = result.current.frameId;
    post(id, 300);
    expect(result.current.height).toBe(308);
    // A partial layout measuring smaller must NOT shrink the frame mid-stream.
    post(id, 100);
    expect(result.current.height).toBe(308);
    post(id, 400);
    expect(result.current.height).toBe(408);
    // Stream over: the final fit sets the exact height, shrinking included.
    rerender({ streaming: false });
    post(id, 250);
    expect(result.current.height).toBe(258);
  });

  it('ignores messages for other frames', () => {
    const { result } = renderHook(() => useScaledFrameHeight(120, true));
    post('someone-else', 999);
    expect(result.current.height).toBe(120);
  });
});

describe('useThrottledPreview', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it('passes the value straight through when not streaming', () => {
    const { result, rerender } = renderHook(
      ({ value, streaming }: { value: string; streaming: boolean }) =>
        useThrottledPreview(value, streaming, 400),
      { initialProps: { value: 'a', streaming: false } },
    );
    rerender({ value: 'b', streaming: false });
    expect(result.current).toBe('b');
  });

  it('rebuilds at most every interval while streaming, latest value wins', () => {
    vi.setSystemTime(100_000);
    const { result, rerender } = renderHook(
      ({ value, streaming }: { value: string; streaming: boolean }) =>
        useThrottledPreview(value, streaming, 400),
      { initialProps: { value: 'a', streaming: true } },
    );
    expect(result.current).toBe('a'); // first mount flushes
    rerender({ value: 'ab', streaming: true });
    expect(result.current).toBe('a'); // within the window: held back
    rerender({ value: 'abc', streaming: true });
    expect(result.current).toBe('a');
    act(() => {
      vi.advanceTimersByTime(400);
    });
    expect(result.current).toBe('abc'); // one rebuild, latest chunk
  });

  it('flushes immediately when the stream ends', () => {
    vi.setSystemTime(100_000);
    const { result, rerender } = renderHook(
      ({ value, streaming }: { value: string; streaming: boolean }) =>
        useThrottledPreview(value, streaming, 400),
      { initialProps: { value: 'a', streaming: true } },
    );
    rerender({ value: 'ab', streaming: true });
    expect(result.current).toBe('a');
    rerender({ value: 'ab-final', streaming: false });
    expect(result.current).toBe('ab-final');
  });
});
