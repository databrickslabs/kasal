import { renderHook } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { useUnmountSafeTimeout } from './useUnmountSafeTimeout';

describe('useUnmountSafeTimeout', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it('runs the callback after the delay while mounted', () => {
    const { result } = renderHook(() => useUnmountSafeTimeout());
    const callback = vi.fn();
    result.current(callback, 100);
    vi.advanceTimersByTime(99);
    expect(callback).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(callback).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(0);
  });

  it('cancels every pending callback on unmount and leaves no timer behind', () => {
    const { result, unmount } = renderHook(() => useUnmountSafeTimeout());
    const first = vi.fn();
    const second = vi.fn();
    result.current(first, 100);
    result.current(second, 250);
    expect(vi.getTimerCount()).toBe(2);

    vi.advanceTimersByTime(50);
    unmount();

    expect(vi.getTimerCount()).toBe(0);
    vi.runAllTimers();
    expect(first).not.toHaveBeenCalled();
    expect(second).not.toHaveBeenCalled();
  });

  it('returns a stable function, so effects that use it do not re-subscribe', () => {
    const { result, rerender } = renderHook(() => useUnmountSafeTimeout());
    const initial = result.current;
    rerender();
    expect(result.current).toBe(initial);
  });

  it('returns an id that clearTimeout still cancels early', () => {
    const { result } = renderHook(() => useUnmountSafeTimeout());
    const callback = vi.fn();
    clearTimeout(result.current(callback, 100));
    vi.runAllTimers();
    expect(callback).not.toHaveBeenCalled();
  });
});
