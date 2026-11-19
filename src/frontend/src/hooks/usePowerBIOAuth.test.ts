import { describe, it, expect, beforeEach, vi } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { usePowerBIOAuth, isSameOrigin } from './usePowerBIOAuth';

describe('usePowerBIOAuth callback', () => {
  beforeEach(() => {
    sessionStorage.clear();
    vi.restoreAllMocks();
  });

  it('refuses a callback whose state is not the one this tab sent', async () => {
    const fetchSpy = vi.spyOn(globalThis, 'fetch');
    sessionStorage.setItem('powerbi_oauth_state', 'sent-by-this-tab');
    const { result } = renderHook(() => usePowerBIOAuth({ clientId: 'client' }));

    await act(async () => {
      await result.current.handleCallback('code', 'from-somewhere-else');
    });

    expect(fetchSpy).not.toHaveBeenCalled();
    expect(result.current.isAuthenticated).toBe(false);
    expect(result.current.error).toMatch(/state mismatch/);
    expect(sessionStorage.getItem('powerbi_oauth_state')).toBeNull();
  });

  it('refuses a callback without a state', async () => {
    const fetchSpy = vi.spyOn(globalThis, 'fetch');
    const { result } = renderHook(() => usePowerBIOAuth({ clientId: 'client' }));

    await act(async () => {
      await result.current.handleCallback('code', null);
    });

    expect(fetchSpy).not.toHaveBeenCalled();
    expect(result.current.isAuthenticated).toBe(false);
  });

  it('only returns into this app', () => {
    expect(isSameOrigin(`${window.location.origin}/workflow`)).toBe(true);
    expect(isSameOrigin('/workflow')).toBe(true);
    expect(isSameOrigin('https://example.com/steal')).toBe(false);
    expect(isSameOrigin('javascript:alert(1)')).toBe(false);
  });
});
