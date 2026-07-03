import { vi, describe, it, expect, beforeEach } from 'vitest';
import { renderHook, waitFor } from '@testing-library/react';
import { useWorkspaceThemes } from './useWorkspaceThemes';

const mockGetConfig = vi.fn();
// useWorkspaceThemes now delegates to useA2uiThemes, which reads the shared
// UIConfigService cache (peek) and its change subscription in addition to getConfig.
vi.mock('../../../api/UIConfigService', () => ({
  UIConfigService: {
    getConfig: (...args: unknown[]) => mockGetConfig(...args),
    peek: () => undefined,
    subscribe: () => () => {},
  },
}));

beforeEach(() => {
  vi.clearAllMocks();
});

describe('useWorkspaceThemes', () => {
  it('returns the configured palettes when the UI config is enabled', async () => {
    mockGetConfig.mockResolvedValue({
      enabled: true,
      style_json: JSON.stringify({
        themes: { default: { accent: '#123456' }, presentation: { accent: '#654321' } },
      }),
    });

    const { result } = renderHook(() => useWorkspaceThemes());

    await waitFor(() => {
      expect(result.current).toEqual({
        default: { accent: '#123456' },
        presentation: { accent: '#654321' },
      });
    });
  });

  it('stays null when the config is disabled', async () => {
    mockGetConfig.mockResolvedValue({ enabled: false, style_json: '{"themes":{}}' });

    const { result } = renderHook(() => useWorkspaceThemes());

    await waitFor(() => expect(mockGetConfig).toHaveBeenCalled());
    expect(result.current).toBeNull();
  });

  it('stays null when style_json is malformed', async () => {
    mockGetConfig.mockResolvedValue({ enabled: true, style_json: 'not-json{' });

    const { result } = renderHook(() => useWorkspaceThemes());

    await waitFor(() => expect(mockGetConfig).toHaveBeenCalled());
    expect(result.current).toBeNull();
  });

  it('stays null when style_json has no themes object', async () => {
    mockGetConfig.mockResolvedValue({ enabled: true, style_json: '{"other": 1}' });

    const { result } = renderHook(() => useWorkspaceThemes());

    await waitFor(() => expect(mockGetConfig).toHaveBeenCalled());
    expect(result.current).toBeNull();
  });

  it('stays null when the config fetch fails', async () => {
    mockGetConfig.mockRejectedValue(new Error('network down'));

    const { result } = renderHook(() => useWorkspaceThemes());

    await waitFor(() => expect(mockGetConfig).toHaveBeenCalled());
    expect(result.current).toBeNull();
  });
});
