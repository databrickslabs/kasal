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

    // Calculate bounds of all nodes
    let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
    nodes.forEach((node) => {
      if (node.position) {
        const nodeWidth = node.width || 200;
        const nodeHeight = node.height || 150;
        minX = Math.min(minX, node.position.x);
        minY = Math.min(minY, node.position.y);
        maxX = Math.max(maxX, node.position.x + nodeWidth);
        maxY = Math.max(maxY, node.position.y + nodeHeight);
      }
    });

    if (minX === Infinity || minY === Infinity) return;

    const nodesWidth = maxX - minX;
    const nodesHeight = maxY - minY;

    const padding = 50;
    const zoomX = (canvasArea.width - 2 * padding) / nodesWidth;
    const zoomY = (canvasArea.height - 2 * padding) / nodesHeight;
    const zoom = Math.min(zoomX, zoomY, 1.5); // Cap at 1.5x to prevent over-zooming

    // Calculate center of canvas area in screen coordinates with visual balance adjustments
    const chatPanelOffset = currentUIState.chatPanelVisible && !currentUIState.chatPanelCollapsed
      ? (currentUIState.chatPanelSide === 'right'
          ? currentUIState.chatPanelWidth * 0.15
          : -currentUIState.chatPanelWidth * 0.55)
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
    const viewportX = canvasCenterX - nodesCenterX * zoom;
    const viewportY = canvasCenterY - nodesCenterY * zoom;

    crewFlowInstanceRef.current.setViewport(
      { x: viewportX, y: viewportY, zoom },
      { duration: 800 }
    );
  }, [nodes, crewFlowInstanceRef]);

  // UI-aware fitView for flow canvas
  const handleFlowUIAwareFitView = React.useCallback(() => {
    if (!flowFlowInstanceRef.current) return;

    const layoutManager = new CanvasLayoutManager();
    const currentUIState = useUILayoutStore.getState().getUILayoutState();

    // Update screen dimensions
    currentUIState.screenWidth = window.innerWidth;
    currentUIState.screenHeight = window.innerHeight;

    layoutManager.updateUIState(currentUIState);
    // Get available canvas area (calculated internally)
    layoutManager.getAvailableCanvasArea('flow');

    // Calculate padding based on execution history visibility
    const basePadding = 0.2;
    const executionHistoryPadding = currentUIState.executionHistoryVisible
      ? currentUIState.executionHistoryHeight / currentUIState.screenHeight
      : 0;

    // Adjust padding to account for execution history from bottom
    flowFlowInstanceRef.current.fitView({
      padding: basePadding + executionHistoryPadding * 0.5,
      includeHiddenNodes: false,
      duration: 800,
    });
  }, [flowFlowInstanceRef]);

  // Internal fitView function to handle both canvas instances
  const handleFitViewToNodesInternal = React.useCallback(() => {
    // Use UI-aware fit view for crew canvas
    handleUIAwareFitView();

    // Use UI-aware fit view for flow canvas
    if (flowFlowInstanceRef.current) {
      try {
        setTimeout(() => {
          handleFlowUIAwareFitView();
        }, 100);
      } catch {
        // ignore
      }
    }
  }, [handleUIAwareFitView, handleFlowUIAwareFitView, flowFlowInstanceRef]);

  return { handleUIAwareFitView, handleFitViewToNodesInternal } as const;
}

export default useUIFitView;

