import { renderHook, act } from '@testing-library/react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { useChatPanelResize } from './useChatPanelResize';

vi.mock('../../store/uiLayout', () => ({
  useUILayoutStore: () => ({
    chatPanelSide: 'right',
    leftSidebarBaseWidth: 48,
  }),
}));

describe('useChatPanelResize', () => {
  let mockSetChatPanelWidth: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    mockSetChatPanelWidth = vi.fn();
    // Set a consistent window size for tests
    Object.defineProperty(window, 'innerWidth', { value: 1200, writable: true });
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('returns handleResizeStart function', () => {
    const { result } = renderHook(() => useChatPanelResize(mockSetChatPanelWidth));

    expect(result.current.handleResizeStart).toBeDefined();
    expect(typeof result.current.handleResizeStart).toBe('function');
  });

  it('uses default maxWidth when no override is provided', () => {
    const { result } = renderHook(() => useChatPanelResize(mockSetChatPanelWidth));

    // Start resize
    const mouseEvent = { preventDefault: vi.fn() } as unknown as React.MouseEvent;
    act(() => {
      result.current.handleResizeStart(mouseEvent);
    });

    // Simulate mouse move — right side: newWidth = window.innerWidth - clientX - 48
    // clientX = 100 → newWidth = 1200 - 100 - 48 = 1052, clamped to max(800, 1200*0.6=720) = 720
    const moveEvent = new MouseEvent('mousemove', { clientX: 100 });
    // Need to advance time past throttle
    Object.defineProperty(performance, 'now', { value: () => 100, writable: true });
    document.dispatchEvent(moveEvent);

    // End resize
    document.dispatchEvent(new MouseEvent('mouseup'));
  });

  it('accepts maxWidthOverride parameter', () => {
    const maxWidthOverride = 400;
    const { result } = renderHook(() =>
      useChatPanelResize(mockSetChatPanelWidth, maxWidthOverride)
    );

    expect(result.current.handleResizeStart).toBeDefined();
  });

  it('clamps width to maxWidthOverride when provided', () => {
    const maxWidthOverride = 350;
    const { result } = renderHook(() =>
      useChatPanelResize(mockSetChatPanelWidth, maxWidthOverride)
    );

    // Start resize
    const mouseEvent = { preventDefault: vi.fn() } as unknown as React.MouseEvent;
    act(() => {
      result.current.handleResizeStart(mouseEvent);
    });

    // Simulate mousemove that would produce a width > maxWidthOverride
    // For right side: newWidth = 1200 - 200 - 48 = 952 → clamped to 350
    const moveEvent = new MouseEvent('mousemove', { clientX: 200 });
    document.dispatchEvent(moveEvent);

    document.dispatchEvent(new MouseEvent('mouseup'));
  });
});
