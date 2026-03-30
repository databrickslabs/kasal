import { renderHook, act } from '@testing-library/react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { useExecutionHistoryResize } from './useExecutionHistoryResize';

describe('useExecutionHistoryResize', () => {
  let mockSetExecutionHistoryHeight: ReturnType<typeof vi.fn>;
  let mockSetHasManuallyResized: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    mockSetExecutionHistoryHeight = vi.fn();
    mockSetHasManuallyResized = vi.fn();
    Object.defineProperty(window, 'innerHeight', { value: 800, writable: true });
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('returns handleHistoryResizeStart function', () => {
    const { result } = renderHook(() =>
      useExecutionHistoryResize(mockSetExecutionHistoryHeight, mockSetHasManuallyResized)
    );

    expect(result.current.handleHistoryResizeStart).toBeDefined();
    expect(typeof result.current.handleHistoryResizeStart).toBe('function');
  });

  it('accepts maxHeightOverride parameter', () => {
    const maxHeightOverride = 300;
    const { result } = renderHook(() =>
      useExecutionHistoryResize(
        mockSetExecutionHistoryHeight,
        mockSetHasManuallyResized,
        maxHeightOverride
      )
    );

    expect(result.current.handleHistoryResizeStart).toBeDefined();
  });

  it('sets up mouse event listeners when resize starts', () => {
    const addEventSpy = vi.spyOn(document, 'addEventListener');

    const { result } = renderHook(() =>
      useExecutionHistoryResize(mockSetExecutionHistoryHeight, mockSetHasManuallyResized)
    );

    const mouseEvent = { preventDefault: vi.fn() } as unknown as React.MouseEvent;
    act(() => {
      result.current.handleHistoryResizeStart(mouseEvent);
    });

    expect(addEventSpy).toHaveBeenCalledWith('mousemove', expect.any(Function));
    expect(addEventSpy).toHaveBeenCalledWith('mouseup', expect.any(Function));

    // Clean up by dispatching mouseup
    document.dispatchEvent(new MouseEvent('mouseup'));
    addEventSpy.mockRestore();
  });

  it('sets hasManuallyResized on mouseup', () => {
    const { result } = renderHook(() =>
      useExecutionHistoryResize(mockSetExecutionHistoryHeight, mockSetHasManuallyResized)
    );

    const mouseEvent = { preventDefault: vi.fn() } as unknown as React.MouseEvent;
    act(() => {
      result.current.handleHistoryResizeStart(mouseEvent);
    });

    act(() => {
      document.dispatchEvent(new MouseEvent('mouseup'));
    });

    expect(mockSetHasManuallyResized).toHaveBeenCalledWith(true);
  });

  it('cleans up event listeners on unmount', () => {
    const removeEventSpy = vi.spyOn(document, 'removeEventListener');

    const { result, unmount } = renderHook(() =>
      useExecutionHistoryResize(mockSetExecutionHistoryHeight, mockSetHasManuallyResized)
    );

    const mouseEvent = { preventDefault: vi.fn() } as unknown as React.MouseEvent;
    act(() => {
      result.current.handleHistoryResizeStart(mouseEvent);
    });

    unmount();

    expect(removeEventSpy).toHaveBeenCalledWith('mousemove', expect.any(Function));
    expect(removeEventSpy).toHaveBeenCalledWith('mouseup', expect.any(Function));

    removeEventSpy.mockRestore();
  });
});
