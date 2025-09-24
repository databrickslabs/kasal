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

    const layoutManager = new CanvasLayoutManager();
    const currentUIState = useUILayoutStore.getState().getUILayoutState();
    layoutManager.updateUIState(currentUIState);

    const canvasArea = layoutManager.getAvailableCanvasArea('crew');

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
    const baseZoom = Math.min(zoomX, zoomY, 2.5);
    const zoom = baseZoom * 1.5; // Zoom in 50% more

    // Dynamic offset based on chat panel position
    const chatOffset = currentUIState.chatPanelVisible && currentUIState.chatPanelSide === 'right' ? 250 : -150;
    const viewportX =
      canvasArea.x + (canvasArea.width - nodesWidth * zoom) / 2 - minX * zoom + chatOffset;
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

