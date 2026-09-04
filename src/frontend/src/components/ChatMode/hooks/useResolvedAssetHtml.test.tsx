import { describe, expect, it, beforeEach } from 'vitest';
import { renderHook, waitFor } from '@testing-library/react';
import { useAssetDataUrl, useResolvedAssetHtml } from './useResolvedAssetHtml';
import { LOADING_PIXEL } from '../utils/assetRefs';

// A plain delegate rather than a vi.fn: a spy that RETURNS a rejected promise
// fails the test after it has finished, even when the hook handled the
// rejection — the spy records the settled result itself.
const calls: string[] = [];
let impl: (id: string) => Promise<string> = () => Promise.resolve('');
vi.mock('../../../api/chat/AssetService', () => ({
  AssetService: {
    dataUrl: (id: string) => {
      calls.push(id);
      return impl(id);
    },
  },
}));
import { vi } from 'vitest';

describe('useResolvedAssetHtml', () => {
  beforeEach(() => {
    calls.length = 0;
  });

  it('returns html without references untouched and fetches nothing', () => {
    const { result } = renderHook(() => useResolvedAssetHtml('<p>hi</p>'));
    expect(result.current).toBe('<p>hi</p>');
    expect(calls).toEqual([]);
  });

  it('stands in the loading pixel, then the bytes once they arrive', async () => {
    impl = () => Promise.resolve('data:image/png;base64,QQ==');
    const html = '<img src="asset:abc123def">';
    const { result } = renderHook(() => useResolvedAssetHtml(html));
    expect(result.current).toBe(`<img src="${LOADING_PIXEL}">`);
    await waitFor(() => expect(result.current).toBe('<img src="data:image/png;base64,QQ==">'));
    expect(calls).toEqual(['abc123def']);
  });

  it('a failed fetch leaves the pixel and the page alive', async () => {
    let settled = false;
    impl = () =>
      new Promise((_, reject) => {
        setTimeout(() => {
          settled = true;
          reject(new Error('403'));
        }, 0);
      });
    const { result } = renderHook(() => useResolvedAssetHtml('<img src="asset:abc123def">'));
    await waitFor(() => expect(settled).toBe(true));
    expect(result.current).toContain(LOADING_PIXEL);
    expect(calls).toEqual(['abc123def']);
  });

  it('useAssetDataUrl resolves one id', async () => {
    impl = () => Promise.resolve('data:x');
    const { result } = renderHook(() => useAssetDataUrl('abc123def'));
    await waitFor(() => expect(result.current).toBe('data:x'));
  });
});
