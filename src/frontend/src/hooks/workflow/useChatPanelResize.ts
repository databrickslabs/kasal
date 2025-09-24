import React from 'react';
import { useUILayoutStore } from '../../store/uiLayout';

export function useChatPanelResize(
  setChatPanelWidth: (width: number) => void
): { handleResizeStart: (e: React.MouseEvent) => void } {
  const [isResizing, setIsResizing] = React.useState(false);
  const throttleRef = React.useRef<number>(0);
  const { chatPanelSide, leftSidebarBaseWidth } = useUILayoutStore();

  const handleResizeStart = React.useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    setIsResizing(true);
  }, []);

  const handleResizeMove = React.useCallback(
    (e: MouseEvent) => {
      if (!isResizing) return;

      const now = performance.now();
      if (now - (throttleRef.current || 0) < 50) return; // throttle ~20fps
      throttleRef.current = now;

      let newWidth: number;

      if (chatPanelSide === 'right') {
        // Right side: calculate from right edge (original logic)
        newWidth = window.innerWidth - e.clientX - 48; // 48px for right sidebar
      } else {
        // Left side: calculate from left edge
        newWidth = e.clientX - leftSidebarBaseWidth; // Subtract left sidebar width
      }

      const minWidth = 280; // Minimum chat panel width
      const maxWidth = Math.min(800, window.innerWidth * 0.6); // Max 60% of screen

      setChatPanelWidth(Math.min(Math.max(newWidth, minWidth), maxWidth));

      // Notify canvas to temporarily skip expensive layout on mousemove
      const event = new CustomEvent('recalculateNodePositions', {
        detail: { reason: 'chat-panel-resizing' },
      });
      window.dispatchEvent(event);
    },
    [isResizing, setChatPanelWidth, chatPanelSide, leftSidebarBaseWidth]
  );

  const handleResizeEnd = React.useCallback(() => {
    setIsResizing(false);

    // Trigger node repositioning after chat panel resize settles
    setTimeout(() => {
      const event = new CustomEvent('recalculateNodePositions', {
        detail: { reason: 'chat-panel-resize' },
      });
      window.dispatchEvent(event);
    }, 50);
  }, []);

  React.useEffect(() => {
    if (!isResizing) return;

    document.addEventListener('mousemove', handleResizeMove);
    document.addEventListener('mouseup', handleResizeEnd);
    document.body.style.cursor = 'ew-resize';
    document.body.style.userSelect = 'none';

    return () => {
      document.removeEventListener('mousemove', handleResizeMove);
      document.removeEventListener('mouseup', handleResizeEnd);
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
    };
  }, [isResizing, handleResizeMove, handleResizeEnd]);

  return { handleResizeStart };
}

export default useChatPanelResize;

