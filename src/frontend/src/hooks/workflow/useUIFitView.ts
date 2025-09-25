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

    const padding = 20;
    const zoomX = (canvasArea.width - 2 * padding) / nodesWidth;
    const zoomY = (canvasArea.height - 2 * padding) / nodesHeight;
    const zoom = Math.min(zoomX, zoomY, 2.0); // Fit to area without extra amplification

    // Calculate viewport position to center nodes within the available canvas area
    // The CanvasLayoutManager already accounts for chat panel position in canvasArea.x and canvasArea.width
    // Increase left bias when chat is docked left (move content further left)
    const leftBias = currentUIState.chatPanelVisible && currentUIState.chatPanelSide === 'left'
      ? -(40 + Math.min(240, Math.max(0, (currentUIState.chatPanelWidth || 0) - 300) * 0.6))
      : 0;

    const viewportX =
      canvasArea.x + (canvasArea.width - nodesWidth * zoom) / 2 - minX * zoom + leftBias;
    const viewportY =
      canvasArea.y + (canvasArea.height - nodesHeight * zoom) / 2 - minY * zoom;

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

