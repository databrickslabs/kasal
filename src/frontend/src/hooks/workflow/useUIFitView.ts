import React from 'react';
import type { ReactFlowInstance, Node } from 'reactflow';
import { CanvasLayoutManager } from '../../features/workflow/canvas/lib/CanvasLayoutManager';
import { useCanvasViewportResize } from './useCanvasViewportResize';
import { useUILayoutStore } from '../../store/uiLayout';

export function useUIFitView(params: {
  nodes: Node[];
  crewFlowInstanceRef: React.MutableRefObject<ReactFlowInstance | null>;
  flowFlowInstanceRef: React.MutableRefObject<ReactFlowInstance | null>;
}) {
  const { nodes, crewFlowInstanceRef, flowFlowInstanceRef } = params;

  // UI-aware fitView function that respects canvas boundaries (crew canvas)
  const handleUIAwareFitView = React.useCallback(() => {
    if (!crewFlowInstanceRef.current) return;
    const currentNodes = crewFlowInstanceRef.current.getNodes().filter(node => !node.hidden);
    if (!currentNodes.length) return;

    // Create layout manager and get the most current UI state
    const layoutManager = new CanvasLayoutManager();
    const currentUIState = useUILayoutStore.getState().getUILayoutState();

    // Force update screen dimensions to current window size
    currentUIState.screenWidth = window.innerWidth;
    currentUIState.screenHeight = window.innerHeight;

    layoutManager.updateUIState(currentUIState);
    // The ReactFlow element already excludes the session rail and conversation.
    // Measure it directly so opening the rail doesn't shift the viewport twice.
    const bounds = document.querySelector('[data-crew-container] .react-flow')?.getBoundingClientRect();
    const canvasArea = bounds?.width && bounds?.height
      ? { x: bounds.left, y: bounds.top, width: bounds.width, height: bounds.height }
      : layoutManager.getAvailableCanvasArea('crew');

    // Calculate bounds of all nodes
    let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
    currentNodes.forEach((node) => {
      if (node.position) {
        const nodeWidth = node.width || (node.type === 'taskNode' ? 270 : 200);
        const nodeHeight = node.height || (node.type === 'taskNode' ? 230 : 210);
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
    const zoom = Math.max(0.01, Math.min(zoomX, zoomY, 1.5)); // Cap at 1.5x to prevent over-zooming

    // Convert the usable screen area to ReactFlow's local viewport coordinates.
    const canvasCenterX = canvasArea.x + canvasArea.width / 2 - (bounds?.left || 0);
    const canvasCenterY = canvasArea.y + canvasArea.height / 2 - (bounds?.top || 0);

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
  }, [crewFlowInstanceRef]);

  const previousIds = React.useRef(new Set<string>());
  const pendingFit = React.useRef(false);
  React.useEffect(() => {
    const ids = new Set(nodes.filter(node => !node.hidden).map(node => node.id));
    if ([...ids].some(id => !previousIds.current.has(id))) pendingFit.current = true;
    previousIds.current = ids;
    if (!pendingFit.current || !ids.size) return;
    let frame: number;
    let attempts = 0;
    const fitAfterMeasurement = () => {
      const measured = crewFlowInstanceRef.current?.getNodes().filter(node => !node.hidden) || [];
      const ready = measured.length === ids.size && measured.every(node => ids.has(node.id) && node.width && node.height);
      if (ready || ++attempts >= 30) {
        if (measured.length) { pendingFit.current = false; handleUIAwareFitView(); }
      } else {
        frame = requestAnimationFrame(fitAfterMeasurement);
      }
    };
    frame = requestAnimationFrame(fitAfterMeasurement);
    return () => cancelAnimationFrame(frame);
  }, [nodes, crewFlowInstanceRef, handleUIAwareFitView]);

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

    flowFlowInstanceRef.current.fitView({
      padding: 0.2,
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

  useCanvasViewportResize(handleUIAwareFitView, handleFlowUIAwareFitView);

  return { handleUIAwareFitView, handleFitViewToNodesInternal } as const;
}

export default useUIFitView;

