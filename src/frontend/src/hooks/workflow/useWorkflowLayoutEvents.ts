import React from 'react';
import type { ReactFlowInstance, Node } from 'reactflow';
import { CanvasLayoutManager } from '../../utils/CanvasLayoutManager';
import { useUILayoutStore } from '../../store/uiLayout';

export function useWorkflowLayoutEvents(params: {
  nodes: Node[];
  setNodes: React.Dispatch<React.SetStateAction<Node[]>>;
  crewFlowInstanceRef: React.MutableRefObject<ReactFlowInstance | null>;
  flowFlowInstanceRef: React.MutableRefObject<ReactFlowInstance | null>;
  handleUIAwareFitView: () => void;
  handleFitViewToNodesInternal: () => void;
}) {
  const {
    nodes,
    setNodes,
    crewFlowInstanceRef,
    flowFlowInstanceRef: _flowFlowInstanceRef,
    handleUIAwareFitView,
    handleFitViewToNodesInternal,
  } = params;

  // Auto-fit crew nodes with specific zoom based on provided layout bounds
  const handleAutoFitCrewNodes = React.useCallback(
    (event: CustomEvent) => {
      const { layoutBounds } = event.detail || {};
      if (!layoutBounds || !crewFlowInstanceRef.current) return;

      try {
        const layoutManager = new CanvasLayoutManager();
        const currentUIState = useUILayoutStore.getState().getUILayoutState();
        layoutManager.updateUIState(currentUIState);

        const canvasArea = layoutManager.getAvailableCanvasArea('crew');

        const padding = 20;
        const zoomX = (canvasArea.width - 2 * padding) / layoutBounds.width;
        const zoomY = (canvasArea.height - 2 * padding) / layoutBounds.height;
        const baseZoom = Math.min(zoomX, zoomY, 2.2);
        const zoom = baseZoom * 1.5; // Zoom in 50% more

        // Dynamic offset based on chat panel position
        const chatOffset = currentUIState.chatPanelVisible && currentUIState.chatPanelSide === 'right' ? 240 : -150;
        const viewportX =
          canvasArea.x +
          (canvasArea.width - layoutBounds.width * zoom) / 2 -
          layoutBounds.x * zoom +
          chatOffset;
        const viewportY =
          canvasArea.y +
          (canvasArea.height - layoutBounds.height * zoom) / 2 -
          layoutBounds.y * zoom;

        crewFlowInstanceRef.current.setViewport({ x: viewportX, y: viewportY, zoom });

        // Backup fitView using calculated zoom
        setTimeout(() => {
          crewFlowInstanceRef.current?.fitView({
            padding: 0.1,
            includeHiddenNodes: false,
            duration: 1000,
            maxZoom: zoom,
          });
        }, 100);
      } catch {
        // Fallback to regular fit view
        handleFitViewToNodesInternal();
      }
    },
    [crewFlowInstanceRef, handleFitViewToNodesInternal]
  );

  // Handle automatic node repositioning when UI layout changes
  const handleRecalculateNodePositions = React.useCallback(
    (event?: Event) => {
      if (nodes.length === 0) return;

      const customEvent = event as CustomEvent | undefined;
      const reason = customEvent?.detail?.reason as string | undefined;

      // Only reorganize for specific reasons, not general UI changes
      if (reason !== 'chat-panel-resize' && reason !== 'execution-history-resize') {
        return;
      }

      const layoutManager = new CanvasLayoutManager({ margin: 20, minNodeSpacing: 50 });
      const currentUIState = useUILayoutStore.getState().getUILayoutState();
      layoutManager.updateUIState(currentUIState);

      const reorganizedNodes = layoutManager.reorganizeNodes(nodes, 'crew');
      setNodes(reorganizedNodes);

      // Trigger UI-aware fit view after repositioning
      setTimeout(() => {
        handleUIAwareFitView();
      }, 100);
    },
    [nodes, setNodes, handleUIAwareFitView]
  );

  // Register global event listeners
  React.useEffect(() => {
    const fitViewListener = () => handleFitViewToNodesInternal();
    const autoFitListener = (e: Event) => handleAutoFitCrewNodes(e as CustomEvent);
    const recalcListener = (e: Event) => handleRecalculateNodePositions(e);

    window.addEventListener('fitViewToNodesInternal', fitViewListener);
    window.addEventListener('autoFitCrewNodes', autoFitListener as EventListener);
    window.addEventListener('recalculateNodePositions', recalcListener as EventListener);

    return () => {
      window.removeEventListener('fitViewToNodesInternal', fitViewListener);
      window.removeEventListener('autoFitCrewNodes', autoFitListener as EventListener);
      window.removeEventListener('recalculateNodePositions', recalcListener as EventListener);
    };
  }, [handleFitViewToNodesInternal, handleAutoFitCrewNodes, handleRecalculateNodePositions]);

  // Apply custom viewport when page loads/refreshes with existing nodes
  React.useEffect(() => {
    let initialViewportApplied = false;

    const applyInitialViewport = () => {
      if (!initialViewportApplied && crewFlowInstanceRef.current && nodes.length > 0) {
        initialViewportApplied = true;
        handleUIAwareFitView();
      }
    };

    const handleCrewFlowInitialized = () => {
      setTimeout(applyInitialViewport, 200);
    };

    window.addEventListener('crewFlowInitialized', handleCrewFlowInitialized);

    if (nodes.length > 0 && crewFlowInstanceRef.current) {
      const timer = setTimeout(applyInitialViewport, 500);
      return () => {
        clearTimeout(timer);
        window.removeEventListener('crewFlowInitialized', handleCrewFlowInitialized);
      };
    }

    return () => {
      window.removeEventListener('crewFlowInitialized', handleCrewFlowInitialized);
    };
  }, [nodes.length, handleUIAwareFitView, crewFlowInstanceRef]);

  return {
    handleAutoFitCrewNodes,
    handleRecalculateNodePositions,
  } as const;
}

export default useWorkflowLayoutEvents;

