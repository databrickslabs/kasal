import React from 'react';

export function useExecutionHistoryResize(
  setExecutionHistoryHeight: (height: number) => void,
  setHasManuallyResized: (resized: boolean) => void
): { handleHistoryResizeStart: (e: React.MouseEvent) => void } {
  const [isResizingHistory, setIsResizingHistory] = React.useState(false);
  const throttleRef = React.useRef<number>(0);

  const handleHistoryResizeStart = React.useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    setIsResizingHistory(true);
  }, []);

  const handleHistoryResizeMove = React.useCallback(
    (e: MouseEvent) => {
      if (!isResizingHistory) return;

      const now = performance.now();
      if (now - (throttleRef.current || 0) < 50) return; // throttle ~20fps
      throttleRef.current = now;

      const newHeight = window.innerHeight - e.clientY;
      const minHeight = 100; // Minimum height for execution history
      const maxHeight = Math.min(500, window.innerHeight * 0.5); // Max 50% of screen or 500px

      setExecutionHistoryHeight(Math.min(Math.max(newHeight, minHeight), maxHeight));
    },
    [isResizingHistory, setExecutionHistoryHeight]
  );

  const handleHistoryResizeEnd = React.useCallback(() => {
    setIsResizingHistory(false);
    setHasManuallyResized(true); // User has manually resized, stop auto-adjusting

    // Trigger viewport recalculation after resize
    window.dispatchEvent(new CustomEvent('recalculateNodePositions', {
      detail: { reason: 'execution-history-resize' }
    }));
  }, [setHasManuallyResized]);

  React.useEffect(() => {
    if (!isResizingHistory) return;

    document.addEventListener('mousemove', handleHistoryResizeMove);
    document.addEventListener('mouseup', handleHistoryResizeEnd);
    document.body.style.cursor = 'ns-resize';
    document.body.style.userSelect = 'none';

    return () => {
      document.removeEventListener('mousemove', handleHistoryResizeMove);
      document.removeEventListener('mouseup', handleHistoryResizeEnd);
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
    };
  }, [isResizingHistory, handleHistoryResizeMove, handleHistoryResizeEnd]);

  return { handleHistoryResizeStart };
}

export default useExecutionHistoryResize;

