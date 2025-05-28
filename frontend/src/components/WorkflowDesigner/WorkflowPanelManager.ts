import { useState, useCallback, useEffect } from 'react';
import { Node, ReactFlowInstance } from 'reactflow';

// Panel state constants
export const PANEL_STATE = {
  SPLIT: 'split',
  LEFT_ONLY: 'left-only',
  RIGHT_ONLY: 'right-only',
  FLOWS_HIDDEN: 'flows-hidden'
};

export interface PanelManagerResult {
  panelPosition: number;
  setPanelPosition: React.Dispatch<React.SetStateAction<number>>;
  isDraggingPanel: boolean;
  setIsDraggingPanel: React.Dispatch<React.SetStateAction<boolean>>;
  panelState: string;
  setPanelState: React.Dispatch<React.SetStateAction<string>>;
  areFlowsVisible: boolean;
  setAreFlowsVisible: React.Dispatch<React.SetStateAction<boolean>>;
  handlePanelDragStart: (e: React.MouseEvent) => void;
  handleSnapToLeft: () => void;
  handleSnapToRight: () => void;
  handleResetPanel: () => void;
  toggleFlowsVisibility: () => void;
  showRunHistory: boolean;
  setShowRunHistory: React.Dispatch<React.SetStateAction<boolean>>;
  showChatPanel: boolean;
  setShowChatPanel: React.Dispatch<React.SetStateAction<boolean>>;
  toggleChatPanel: () => void;
}

export const usePanelManager = (): PanelManagerResult => {
  // Panel position and drag state (0-100 representing percentage)
  const [panelPosition, setPanelPosition] = useState<number>(50);
  const [isDraggingPanel, setIsDraggingPanel] = useState(false);
  const [panelState, setPanelState] = useState(PANEL_STATE.SPLIT);
  const [areFlowsVisible, setAreFlowsVisible] = useState(false);
  const [showRunHistory, setShowRunHistory] = useState(true);
  const [showChatPanel, setShowChatPanel] = useState(false);
  
  // Panel transition functions
  const handlePanelDragStart = useCallback(() => {
    setIsDraggingPanel(true);
  }, []);
  
  const handleSnapToLeft = useCallback(() => {
    setPanelState(PANEL_STATE.RIGHT_ONLY);
    setPanelPosition(5); // Nearly all the way left
  }, []);
  
  const handleSnapToRight = useCallback(() => {
    setPanelState(PANEL_STATE.LEFT_ONLY);
    setPanelPosition(95); // Nearly all the way right
  }, []);
  
  const handleResetPanel = useCallback(() => {
    setPanelState(PANEL_STATE.SPLIT);
    setPanelPosition(50);
  }, []);

  // Handler to toggle flows visibility
  const toggleFlowsVisibility = useCallback(() => {
    setAreFlowsVisible(prev => {
      // When hiding the flow panel, reset divider position to 50%
      if (prev) {
        setPanelPosition(50);
        
        // Force update the grid template immediately
        setTimeout(() => {
          const container = document.querySelector('[data-crew-container]')?.parentElement;
          if (container) {
            container.style.gridTemplateColumns = '1fr';
          }
        }, 0);
      } else {
        // When showing flow panel, update grid template
        setTimeout(() => {
          const container = document.querySelector('[data-crew-container]')?.parentElement;
          if (container) {
            container.style.gridTemplateColumns = `50% 50%`;
          }
        }, 0);
      }
      
      return !prev;
    });
  }, []);

  // Handler to toggle chat panel visibility
  const toggleChatPanel = useCallback(() => {
    setShowChatPanel(prev => !prev);
  }, []);

  // Set up event listeners for dragging outside the component
  useEffect(() => {
    if (isDraggingPanel) {
      const handleMouseMove = (e: MouseEvent) => {
        // Get canvas dimensions
        const canvas = document.querySelector('.react-flow') as HTMLElement;
        if (!canvas) return;
        
        const rect = canvas.getBoundingClientRect();
        const newPosition = ((e.clientX - rect.left) / rect.width) * 100;
        
        // Limit the range between 10% and 90%
        const clampedPosition = Math.max(10, Math.min(90, newPosition));
        setPanelPosition(clampedPosition);
      };
      
      const handleMouseUp = () => {
        setIsDraggingPanel(false);
      };
      
      document.addEventListener('mousemove', handleMouseMove);
      document.addEventListener('mouseup', handleMouseUp);
      
      return () => {
        document.removeEventListener('mousemove', handleMouseMove);
        document.removeEventListener('mouseup', handleMouseUp);
      };
    }
  }, [isDraggingPanel]);
  
  return {
    panelPosition,
    setPanelPosition,
    isDraggingPanel,
    setIsDraggingPanel,
    panelState,
    setPanelState,
    areFlowsVisible,
    setAreFlowsVisible,
    handlePanelDragStart,
    handleSnapToLeft,
    handleSnapToRight,
    handleResetPanel,
    toggleFlowsVisibility,
    showRunHistory,
    setShowRunHistory,
    showChatPanel,
    setShowChatPanel,
    toggleChatPanel,
  };
};

// Update node positions when panel configuration changes
export const useNodePositioning = (
  nodes: Node[],
  setNodes: React.Dispatch<React.SetStateAction<Node[]>>,
  isDraggingPanel: boolean,
  areFlowsVisible: boolean,
  panelState: string,
  manuallyPositionedNodes: string[],
  crewFlowInstanceRef: React.MutableRefObject<ReactFlowInstance | null>,
  flowFlowInstanceRef: React.MutableRefObject<ReactFlowInstance | null>,
  updateNodePositionsTimeoutRef: React.MutableRefObject<ReturnType<typeof setTimeout> | null>,
  unmountedRef: React.MutableRefObject<boolean>
) => {
  useEffect(() => {
    // Skip positioning if there are no nodes or flag is disabled
    if (!nodes.length) return;

    // Skip updating positions during active drag operations
    if (isDraggingPanel) return;

    // Create a set of manually positioned node IDs
    const manuallyPositionedSet = new Set(manuallyPositionedNodes);

    // Skip node positioning for recently added nodes to avoid interference
    const minTimeSinceAddition = 1000; // 1 second
    const now = Date.now();
    
    const recentlyAddedNodeIds = new Set(
      nodes
        .filter(node => 
          node.data?.addedTimestamp && 
          now - node.data.addedTimestamp < minTimeSinceAddition
        )
        .map(node => node.id)
    );
    
    // Helper function to get the active flow instance
    const getActiveFlowInstance = () => {
      if (areFlowsVisible && panelState !== PANEL_STATE.LEFT_ONLY && flowFlowInstanceRef.current) {
        return flowFlowInstanceRef.current;
      } else if (crewFlowInstanceRef.current) {
        return crewFlowInstanceRef.current;
      }
      return null;
    };

    // Get a reference to the active flow instance for positioning
    const activeFlowInstance = getActiveFlowInstance();
    if (!activeFlowInstance) return;
    
    // Debounce the function to reduce the number of calls
    if (updateNodePositionsTimeoutRef.current) {
      clearTimeout(updateNodePositionsTimeoutRef.current);
    }

    updateNodePositionsTimeoutRef.current = setTimeout(() => {
      if (unmountedRef.current) return; // Skip if component is unmounted
      
      let hasChanges = false;
      const nodesWithValidPositions = nodes.map(node => {
        // Skip any manually positioned nodes or recently added nodes
        if (manuallyPositionedSet.has(node.id) || recentlyAddedNodeIds.has(node.id)) {
          return node;
        }
        
        // Skip if position is already valid
        if (
          node.position && 
          isFinite(node.position.x) && 
          isFinite(node.position.y) &&
          Math.abs(node.position.x) < 10000 &&
          Math.abs(node.position.y) < 10000
        ) {
          return node;
        }

        hasChanges = true;
        return {
          ...node,
          position: {
            x: Math.random() * 300, 
            y: Math.random() * 300
          }
        };
      });

      // Only update if we actually changed anything
      if (hasChanges) {
        setNodes(nodesWithValidPositions);
        
        // After setting nodes with valid positions, fit view if necessary
        // Use a longer timeout to ensure nodes are rendered properly
        setTimeout(() => {
          if (unmountedRef.current) return;
          
          const instance = getActiveFlowInstance();
          if (instance) {
            instance.fitView({ padding: 0.2, includeHiddenNodes: false });
          }
        }, 300);
      }
    }, 200); // 200ms debounce

    return () => {
      if (updateNodePositionsTimeoutRef.current) {
        clearTimeout(updateNodePositionsTimeoutRef.current);
      }
    };
  }, [
    nodes, 
    setNodes, 
    panelState,
    areFlowsVisible,
    isDraggingPanel,
    manuallyPositionedNodes,
    crewFlowInstanceRef,
    flowFlowInstanceRef,
    updateNodePositionsTimeoutRef,
    unmountedRef
  ]);
}; 