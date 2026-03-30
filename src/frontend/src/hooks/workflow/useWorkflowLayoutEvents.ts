import React from 'react';
import type { ReactFlowInstance, Node, Edge } from 'reactflow';
import { CanvasLayoutManager } from '../../utils/CanvasLayoutManager';
import { useUILayoutStore } from '../../store/uiLayout';

export function useWorkflowLayoutEvents(params: {
  nodes: Node[];
  edges: Edge[];
  setNodes: React.Dispatch<React.SetStateAction<Node[]>>;
  setEdges: React.Dispatch<React.SetStateAction<Edge[]>>;
  flowNodes: Node[];
  flowEdges: Edge[];
  setFlowNodes: React.Dispatch<React.SetStateAction<Node[]>>;
  setFlowEdges: React.Dispatch<React.SetStateAction<Edge[]>>;
  areFlowsVisible: boolean;
  crewFlowInstanceRef: React.MutableRefObject<ReactFlowInstance | null>;
  flowFlowInstanceRef: React.MutableRefObject<ReactFlowInstance | null>;
  handleUIAwareFitView: () => void;
  handleFitViewToNodesInternal: () => void;
}) {
  const {
    nodes,
    edges: _edges,
    setNodes,
    setEdges,
    flowNodes,
    flowEdges,
    setFlowNodes,
    setFlowEdges,
    areFlowsVisible,
    crewFlowInstanceRef,
    flowFlowInstanceRef,
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
        const zoom = Math.min(zoomX, zoomY, 2.0); // Fit to area without extra amplification

        // Calculate viewport position to center nodes within the available canvas area
        // The CanvasLayoutManager already accounts for chat panel position in canvasArea.x and canvasArea.width
        const viewportX =
          canvasArea.x +
          (canvasArea.width - layoutBounds.width * zoom) / 2 -
          layoutBounds.x * zoom;
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
      // Context-aware: check which canvas is visible and reorganize accordingly
      const currentNodes = areFlowsVisible ? flowNodes : nodes;
      const currentEdges = areFlowsVisible ? flowEdges : _edges;
      const currentSetNodes = areFlowsVisible ? setFlowNodes : setNodes;
      const currentSetEdges = areFlowsVisible ? setFlowEdges : setEdges;
      const canvasType = areFlowsVisible ? 'flow' : 'crew';

      if (currentNodes.length === 0) return;

      const customEvent = event as CustomEvent | undefined;
      const reason = customEvent?.detail?.reason as string | undefined;

      // Only handle specific reasons, not general UI changes
      if (
        reason !== 'chat-panel-resize' &&
        reason !== 'execution-history-resize' &&
        reason !== 'layout-orientation-toggle'
      ) {
        return;
      }

      // For chat panel and execution history resize: just recenter viewport, don't move nodes
      if (reason === 'chat-panel-resize' || reason === 'execution-history-resize') {
        setTimeout(() => {
          if (areFlowsVisible) {
            // Flow canvas: use UI-aware fitView that accounts for execution history
            if (flowFlowInstanceRef.current) {
              const layoutManager = new CanvasLayoutManager();
              const currentUIState = useUILayoutStore.getState().getUILayoutState();
              currentUIState.screenWidth = window.innerWidth;
              currentUIState.screenHeight = window.innerHeight;
              layoutManager.updateUIState(currentUIState);

              // Calculate padding based on execution history
              const basePadding = 0.2;
              const executionHistoryPadding = currentUIState.executionHistoryVisible
                ? currentUIState.executionHistoryHeight / currentUIState.screenHeight
                : 0;

              flowFlowInstanceRef.current.fitView({
                padding: basePadding + executionHistoryPadding * 0.5,
                includeHiddenNodes: false,
                duration: 800,
              });
            }
          } else {
            // Crew canvas: use UI-aware fitView that accounts for chat panel and execution history
            handleUIAwareFitView();
          }
        }, 100);
        return;
      }

      // For layout orientation toggle: reorganize nodes AND recenter viewport
      const layoutManager = new CanvasLayoutManager({ margin: 20, minNodeSpacing: 50 });
      const currentUIState = useUILayoutStore.getState().getUILayoutState();

      // Force update screen dimensions to current window size
      currentUIState.screenWidth = window.innerWidth;
      currentUIState.screenHeight = window.innerHeight;

      layoutManager.updateUIState(currentUIState);

      const reorganizedNodes = layoutManager.reorganizeNodes(currentNodes, canvasType, currentEdges);
      currentSetNodes(reorganizedNodes);

      // Retarget edge handles to match orientation
      if (reason === 'layout-orientation-toggle') {
        const currentLayout = useUILayoutStore.getState().layoutOrientation;

        currentSetEdges(prevEdges => prevEdges.map(e => {
          const sourceNode = reorganizedNodes.find(n => n.id === e.source);
          const targetNode = reorganizedNodes.find(n => n.id === e.target);

          // Agent-to-task edges: change based on layout orientation
          if (sourceNode?.type === 'agentNode' && targetNode?.type === 'taskNode') {
            const agentSourceHandle = currentLayout === 'vertical' ? 'bottom' : 'right';
            const taskTargetHandle = currentLayout === 'vertical' ? 'top' : 'left';
            return { ...e, sourceHandle: agentSourceHandle, targetHandle: taskTargetHandle };
          }

          // Task-to-task edges: ALWAYS horizontal (right → left) regardless of layout
          if (sourceNode?.type === 'taskNode' && targetNode?.type === 'taskNode') {
            return { ...e, sourceHandle: 'right', targetHandle: 'left' };
          }

          // Flow canvas: crewNode-to-crewNode edges - change based on layout orientation
          if (sourceNode?.type === 'crewNode' && targetNode?.type === 'crewNode') {
            const sourceHandle = currentLayout === 'vertical' ? 'bottom' : 'right';
            const targetHandle = currentLayout === 'vertical' ? 'top' : 'left';
            return { ...e, sourceHandle, targetHandle };
          }

          return e;
        }));
      }

      // Trigger context-aware fit view after repositioning
      // Increased delay to ensure DOM updates are complete before fitView
      setTimeout(() => {
        if (areFlowsVisible) {
          // Flow canvas: use UI-aware fitView that accounts for execution history
          if (flowFlowInstanceRef.current) {
            const layoutManager = new CanvasLayoutManager();
            const currentUIState = useUILayoutStore.getState().getUILayoutState();
            currentUIState.screenWidth = window.innerWidth;
            currentUIState.screenHeight = window.innerHeight;
            layoutManager.updateUIState(currentUIState);

            const basePadding = 0.2;
            const executionHistoryPadding = currentUIState.executionHistoryVisible
              ? currentUIState.executionHistoryHeight / currentUIState.screenHeight
              : 0;

            flowFlowInstanceRef.current.fitView({
              padding: basePadding + executionHistoryPadding * 0.5,
              includeHiddenNodes: false,
              duration: 800,
            });
          }
        } else {
          // Crew canvas: use UI-aware fitView that accounts for chat panel and execution history
          handleUIAwareFitView();
        }
      }, 300);
    },
    [nodes, flowNodes, _edges, flowEdges, setNodes, setFlowNodes, setEdges, setFlowEdges, areFlowsVisible, flowFlowInstanceRef, handleUIAwareFitView]
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

