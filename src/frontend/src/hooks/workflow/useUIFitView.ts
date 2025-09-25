import React from 'react';
import type { ReactFlowInstance, Node } from 'reactflow';
import { CanvasLayoutManager } from '../../utils/CanvasLayoutManager';
import { useUILayoutStore } from '../../store/uiLayout';

export function useUIFitView(params: {
  nodes: Node[];
  crewFlowInstanceRef: React.MutableRefObject<ReactFlowInstance | null>;
  flowFlowInstanceRef: React.MutableRefObject<ReactFlowInstance | null>;
}) {
  const { nodes, crewFlowInstanceRef, flowFlowInstanceRef } = params;

  // UI-aware fitView function that respects canvas boundaries (crew canvas)
  const handleUIAwareFitView = React.useCallback(() => {
    if (!crewFlowInstanceRef.current || nodes.length === 0) return;

    // Create layout manager and get the most current UI state
    const layoutManager = new CanvasLayoutManager();
    const currentUIState = useUILayoutStore.getState().getUILayoutState();

    // Force update screen dimensions to current window size
    currentUIState.screenWidth = window.innerWidth;
    currentUIState.screenHeight = window.innerHeight;

    layoutManager.updateUIState(currentUIState);
    const canvasArea = layoutManager.getAvailableCanvasArea('crew');

    console.log('[FitView] Canvas area:', canvasArea);
    console.log('[FitView] Chat panel width:', currentUIState.chatPanelWidth);
    console.log('[FitView] Chat panel side:', currentUIState.chatPanelSide);

    // Calculate bounds of all nodes
    let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
    nodes.forEach((node) => {
      if (node.position) {
        minX = Math.min(minX, node.position.x);
        minY = Math.min(minY, node.position.y);
        maxX = Math.max(maxX, node.position.x + (node.width || 200));
        maxY = Math.max(maxY, node.position.y + (node.height || 150));
      }
    });

    if (minX === Infinity || minY === Infinity) return;

    const nodesWidth = maxX - minX;
    const nodesHeight = maxY - minY;

    const padding = 50;
    const zoomX = (canvasArea.width - 2 * padding) / nodesWidth;
    const zoomY = (canvasArea.height - 2 * padding) / nodesHeight;
    const zoom = Math.min(zoomX, zoomY, 1.5); // Cap at 1.5x to prevent over-zooming

    console.log('[FitView] Calculated zoom:', zoom, 'zoomX:', zoomX, 'zoomY:', zoomY);
    console.log('[FitView] Nodes bounds:', { minX, minY, maxX, maxY, nodesWidth, nodesHeight });

    // Calculate center of canvas area in screen coordinates with visual balance adjustments
    // When chat panel is on the right, shift center point to the right to account for visual weight
    // When chat panel is on the left, shift center point to the left to keep nodes visible
    // When execution history is visible at bottom, shift center point up
    const chatPanelOffset = currentUIState.chatPanelVisible && !currentUIState.chatPanelCollapsed
      ? (currentUIState.chatPanelSide === 'right'
          ? currentUIState.chatPanelWidth * 0.15   // Shift right when chat is on right
          : -currentUIState.chatPanelWidth * 0.55) // Shift significantly to left when chat is on left
      : 0;

    const executionHistoryOffset = currentUIState.executionHistoryVisible
      ? currentUIState.executionHistoryHeight * 0.2
      : 0;

    const canvasCenterX = canvasArea.x + canvasArea.width / 2 + chatPanelOffset;
    const canvasCenterY = canvasArea.y + canvasArea.height / 2 - executionHistoryOffset;

    // Calculate center of nodes in flow coordinates
    const nodesCenterX = minX + nodesWidth / 2;
    const nodesCenterY = minY + nodesHeight / 2;

    // Calculate viewport position to center nodes in canvas area
    // viewport.x = where we want the center in screen coords - where the node center will be with zoom
    const viewportX = canvasCenterX - nodesCenterX * zoom;
    const viewportY = canvasCenterY - nodesCenterY * zoom;

    console.log('[FitView] Offsets - chat:', chatPanelOffset, 'execution:', executionHistoryOffset);
    console.log('[FitView] Setting viewport:', { x: viewportX, y: viewportY, zoom });

    crewFlowInstanceRef.current.setViewport(
      { x: viewportX, y: viewportY, zoom },
      { duration: 800 }
    );
  }, [nodes, crewFlowInstanceRef]);

  // Internal fitView function to handle both canvas instances
  const handleFitViewToNodesInternal = React.useCallback(() => {
    // Use UI-aware fit view for crew canvas
    handleUIAwareFitView();

    // Standard fit view for flow canvas
    if (flowFlowInstanceRef.current) {
      try {
        setTimeout(() => {
          flowFlowInstanceRef.current?.fitView({
            padding: 0.2,
            includeHiddenNodes: false,
            duration: 800,
          });
        }, 100);
      } catch {
        // ignore
      }
    }
  }, [handleUIAwareFitView, flowFlowInstanceRef]);

  return { handleUIAwareFitView, handleFitViewToNodesInternal } as const;
}

export default useUIFitView;

