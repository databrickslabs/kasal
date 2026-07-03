import { vi, describe, it, expect, beforeEach } from 'vitest';
import { renderHook, act, waitFor } from '@testing-library/react';

// Mock the HTTP layer only — we exercise the REAL UIConfigService (cache +
// subscribe + notify) and the REAL hook, so this proves the whole re-theme chain.
const mockGet = vi.fn();
const mockPut = vi.fn();
vi.mock('../../../config/api/ApiConfig', () => ({
  apiClient: {
    get: (...args: unknown[]) => mockGet(...args),
    put: (...args: unknown[]) => mockPut(...args),
  },
}));

import { useA2uiThemes } from './useA2uiThemes';
import { UIConfigService, UIConfigUpdate } from '../../../api/UIConfigService';

const cfg = (themes: Record<string, unknown>, enabled = true) => ({
  data: { enabled, catalog_type: 'full', style_json: JSON.stringify({ themes }) },
});

// The service holds a static session cache/subscriber set — reset it between tests.
beforeEach(() => {
  vi.clearAllMocks();
  const svc = UIConfigService as unknown as {
    cache: unknown;
    inflight: unknown;
    listeners: Set<unknown>;
  };
  svc.cache = undefined;
  svc.inflight = null;
  svc.listeners = new Set();
});

describe('useA2uiThemes', () => {
  it('returns the configured palettes when the UI config is enabled', async () => {
    mockGet.mockResolvedValue(cfg({ default: { accent: '#123456' }, presentation: { accent: '#654321' } }));

    const { result } = renderHook(() => useA2uiThemes());

    await waitFor(() =>
      expect(result.current).toEqual({
        default: { accent: '#123456' },
        presentation: { accent: '#654321' },
      }),
    );
  });

  it('stays null when the config is disabled, malformed, has no themes, or fails', async () => {
    for (const bad of [
      { data: { enabled: false, style_json: '{"themes":{"default":{"accent":"#fff"}}}' } },
      { data: { enabled: true, style_json: 'not-json{' } },
      { data: { enabled: true, style_json: '{"other": 1}' } },
    ]) {
      const svc = UIConfigService as unknown as { cache: unknown; inflight: unknown };
      svc.cache = undefined;
      svc.inflight = null;
      mockGet.mockResolvedValueOnce(bad);
      const { result, unmount } = renderHook(() => useA2uiThemes());
      await waitFor(() => expect(mockGet).toHaveBeenCalled());
      expect(result.current).toBeNull();
      unmount();
    }

    (UIConfigService as unknown as { cache: unknown; inflight: unknown }).cache = undefined;
    (UIConfigService as unknown as { inflight: unknown }).inflight = null;
    mockGet.mockRejectedValueOnce(new Error('network down'));
    const { result } = renderHook(() => useA2uiThemes());
    await waitFor(() => expect(mockGet).toHaveBeenCalled());
    expect(result.current).toBeNull();
  });

  // The regression this fix targets: an admin edits a palette in the Configurator
  // while a chat surface is already mounted. Before the fix, the session cache was
  // never invalidated, so the surface kept the OLD palette until a hard reload.
  it('re-themes an already-mounted surface when the config is saved — no reload', async () => {
    mockGet.mockResolvedValue(cfg({ default: { accent: '#111111' } }));
    mockPut.mockResolvedValue(cfg({ default: { accent: '#222222' } }));

    const { result } = renderHook(() => useA2uiThemes());
    await waitFor(() => expect(result.current).toEqual({ default: { accent: '#111111' } }));

    const payload: UIConfigUpdate = {
      enabled: true,
      catalog_type: 'full',
      catalog_json: null,
      style_json: JSON.stringify({ themes: { default: { accent: '#222222' } } }),
    };
    await act(async () => {
      await UIConfigService.updateConfig(payload);
    });

    // The mounted surface picks up the new accent immediately, without remounting.
    await waitFor(() => expect(result.current).toEqual({ default: { accent: '#222222' } }));
  });

  it('shares one fetch across many mounted surfaces (session cache)', async () => {
    mockGet.mockResolvedValue(cfg({ default: { accent: '#abcabc' } }));

    const a = renderHook(() => useA2uiThemes());
    const b = renderHook(() => useA2uiThemes());
    await waitFor(() => expect(a.result.current).toEqual({ default: { accent: '#abcabc' } }));
    await waitFor(() => expect(b.result.current).toEqual({ default: { accent: '#abcabc' } }));

    expect(mockGet).toHaveBeenCalledTimes(1);
  });
});
