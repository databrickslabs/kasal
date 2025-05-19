import { useEffect, useRef, useMemo } from 'react';
import { Node, Edge, ReactFlowInstance } from 'reactflow';
import { ShortcutConfig } from '../../types/shortcuts';
import { useRunResult } from './useExecutionResult';
import { useRunHistory } from './useExecutionHistory';
import { useWorkflowStore } from '../../store/workflow';
import { useCrewExecutionStore } from '../../store/crewExecution';

/**
 * Default shortcut configurations
 */
export const DEFAULT_SHORTCUTS: ShortcutConfig[] = [
  // Canvas Operations
  { action: 'deleteSelected', keys: ['Delete'], description: 'Delete selected nodes or edges' },
  { action: 'deleteSelected', keys: ['Backspace'], description: 'Delete selected nodes or edges' },
  { action: 'clearCanvas', keys: ['d', 'd'], description: 'Clear entire canvas (vim-style)' },
  { action: 'clearCanvas', keys: ['Alt', 'c'], description: 'Clear entire canvas' },
  { action: 'fitView', keys: ['v', 'f'], description: 'Fit view to all nodes (vim-style)' },
  { action: 'fitView', keys: ['Control', '0'], description: 'Fit view to all nodes' },
  { action: 'zoomIn', keys: ['Control', '='], description: 'Zoom in' },
  { action: 'zoomOut', keys: ['Control', '-'], description: 'Zoom out' },
  { action: 'toggleFullscreen', keys: ['f'], description: 'Toggle fullscreen mode' },
  
  // Edit Operations
  { action: 'undo', keys: ['Control', 'z'], description: 'Undo last action' },
  { action: 'redo', keys: ['Control', 'Shift', 'z'], description: 'Redo last undone action' },
  { action: 'redo', keys: ['Control', 'y'], description: 'Redo last undone action' },
  { action: 'selectAll', keys: ['Control', 'a'], description: 'Select all nodes' },
  { action: 'copy', keys: ['Control', 'c'], description: 'Copy selected nodes' },
  { action: 'paste', keys: ['Control', 'v'], description: 'Paste copied nodes' },
  
  // Agent Operations
  { action: 'openAgentDialog', keys: ['g', 'a'], description: 'Open Generate Agent dialog' },
  { action: 'openTaskDialog', keys: ['g', 't'], description: 'Open Generate Task dialog' },
  { action: 'openCrewPlanningDialog', keys: ['g', 'c'], description: 'Open Generate Crew Plan dialog' },
  { action: 'generateConnections', keys: ['c', 'c'], description: 'Generate connections between agents and tasks' },
  
  // Crew Operations
  { action: 'openSaveCrew', keys: ['s', 's'], description: 'Open Save Crew dialog' },
  { action: 'executeCrew', keys: ['e', 'c'], description: 'Execute Crew' },
  { action: 'executeFlow', keys: ['e', 'f'], description: 'Execute Flow' },
  { action: 'showRunResult', keys: ['s', 'r'], description: 'Show Run Result dialog' },
  { action: 'openCrewFlowDialog', keys: ['l', 'c'], description: 'Open Crew/Flow selection dialog' },
  { action: 'openFlowDialog', keys: ['l', 'f'], description: 'Open Flow selection dialog' },
  
  // Agent Configuration
  { action: 'changeLLMForAllAgents', keys: ['l', 'l', 'm'], description: 'Change LLM model for all agents' },
  { action: 'changeMaxRPMForAllAgents', keys: ['m', 'a', 'x', 'r'], description: 'Change Max RPM for all agents' },
  { action: 'changeToolsForAllAgents', keys: ['t', 'o', 'o', 'l'], description: 'Change tools for all agents' },
  
  // Quick Access Dialogs
  { action: 'openLLMDialog', keys: ['l', 'l', 'm'], description: 'Open LLM Selection dialog' },
  { action: 'openToolDialog', keys: ['t', 'o', 'o', 'l'], description: 'Open Tool Selection dialog' },
  { action: 'openMaxRPMDialog', keys: ['m', 'a', 'x', 'r'], description: 'Open Max RPM Selection dialog' },
  { action: 'openMCPConfigDialog', keys: ['m', 'c', 'p', 's'], description: 'Open MCP Configuration dialog' },
];

interface UseShortcutsOptions {
  shortcuts?: ShortcutConfig[];
  flowInstance?: ReactFlowInstance | null;
  onDeleteSelected?: (selectedNodes: Node[], selectedEdges: Edge[]) => void;
  onClearCanvas?: () => void;
  onUndo?: () => void;
  onRedo?: () => void;
  onSelectAll?: () => void;
  onCopy?: () => void;
  onPaste?: () => void;
  onZoomIn?: () => void;
  onZoomOut?: () => void;
  onFitView?: () => void;
  onToggleFullscreen?: () => void;
  onOpenAgentDialog?: () => void;
  onOpenTaskDialog?: () => void;
  onOpenCrewPlanningDialog?: () => void;
  onGenerateConnections?: () => void;
  onOpenSaveCrew?: () => void;
  onExecuteCrew?: () => void;
  onExecuteFlow?: () => void;
  onOpenCrewFlowDialog?: () => void;
  onOpenFlowDialog?: () => void;
  onChangeLLMForAllAgents?: () => void;
  onChangeMaxRPMForAllAgents?: () => void;
  onChangeToolsForAllAgents?: () => void;
  onOpenLLMDialog?: () => void;
  onOpenToolDialog?: () => void;
  onOpenMaxRPMDialog?: () => void;
  onOpenMCPConfigDialog?: () => void;
  disabled?: boolean;
  useWorkflowStore?: boolean;
}

/**
 * Custom hook for handling keyboard shortcuts in canvas components
 */
const useShortcuts = ({
  shortcuts = DEFAULT_SHORTCUTS,
  flowInstance,
  onDeleteSelected,
  onClearCanvas,
  onUndo,
  onRedo,
  onSelectAll,
  onCopy,
  onPaste,
  onZoomIn,
  onZoomOut,
  onFitView,
  onToggleFullscreen,
  onOpenAgentDialog,
  onOpenTaskDialog,
  onOpenCrewPlanningDialog,
  onGenerateConnections,
  onOpenSaveCrew,
  onExecuteCrew,
  onExecuteFlow,
  onOpenCrewFlowDialog,
  onOpenFlowDialog,
  onChangeLLMForAllAgents,
  onChangeMaxRPMForAllAgents,
  onChangeToolsForAllAgents,
  onOpenLLMDialog,
  onOpenToolDialog,
  onOpenMaxRPMDialog,
  onOpenMCPConfigDialog,
  disabled = false,
  useWorkflowStore: enableWorkflowStore = false
}: UseShortcutsOptions) => {
  // Store the current key sequence
  const keySequence = useRef<string[]>([]);
  const sequenceTimer = useRef<NodeJS.Timeout | null>(null);
  const handlerRef = useRef<HandlerMap | null>(null);
  
  const { showRunResult } = useRunResult();
  const { runs, fetchRuns } = useRunHistory();

  // Get workflow state and actions from the store if enabled
  const workflowStore = useWorkflowStore();
  const { nodes: workflowNodes, edges: workflowEdges, setNodes, setEdges, clearWorkflow } = workflowStore;

  // Get crew execution state and actions
  const crewExecutionStore = useCrewExecutionStore();
  const { 
    executeCrew: executeCrewAction, 
    executeFlow: executeFlowAction,
    setErrorMessage,
    setShowError
  } = crewExecutionStore;

  // Memoize nodes and edges to prevent unnecessary re-renders
  const nodes = useMemo(() => workflowNodes, [workflowNodes]);
  const edges = useMemo(() => workflowEdges, [workflowEdges]);

  // Memoize the filter functions to prevent unnecessary re-renders
  const filterNodes = useMemo(() => {
    return (node: Node, selectedNodes: Node[]) => !selectedNodes.some(n => n.id === node.id);
  }, []);

  const filterEdges = useMemo(() => {
    return (edge: Edge, selectedEdges: Edge[]) => !selectedEdges.some(e => e.id === edge.id);
  }, []);

  // Define the type for the handlers object
  type HandlerMap = {
    [key: string]: () => void | Promise<void>;
  };

  // Memoize the handler map to prevent unnecessary re-renders
  const handlers: HandlerMap = useMemo(() => {
    // Helper function to validate crew execution requirements
    const validateCrewExecution = (currentNodes: Node[]): boolean => {
      const hasAgentNodes = currentNodes.some(node => node.type === 'agentNode');
      const hasTaskNodes = currentNodes.some(node => node.type === 'taskNode');
      
      if (!hasAgentNodes || !hasTaskNodes) {
        setErrorMessage('Crew execution requires at least one agent and one task node');
        setShowError(true);
        return false;
      }
      
      return true;
    };

    // Helper function to validate agent nodes existence
    const validateAgentNodes = (currentNodes: Node[]): boolean => {
      const hasAgentNodes = currentNodes.some(node => node.type === 'agentNode');
      
      if (!hasAgentNodes) {
        setErrorMessage('This operation requires at least one agent node');
        setShowError(true);
        return false;
      }
      
      return true;
    };

    const baseHandlers: HandlerMap = {
      'deleteSelected': () => {
        if (onDeleteSelected && flowInstance) {
          const selectedNodes = flowInstance.getNodes().filter(node => node.selected);
          const selectedEdges = flowInstance.getEdges().filter(edge => edge.selected);
          console.log("Deleting selected:", { 
            nodes: selectedNodes.length, 
            edges: selectedEdges.length,
            nodeIds: selectedNodes.map(n => n.id),
            edgeIds: selectedEdges.map(e => e.id)
          });
          onDeleteSelected(selectedNodes, selectedEdges);
        } else if (enableWorkflowStore && flowInstance) {
          const selectedNodes = flowInstance.getNodes().filter(node => node.selected);
          const selectedEdges = flowInstance.getEdges().filter(edge => edge.selected);
          console.log("Deleting selected (via store):", { 
            nodes: selectedNodes.length, 
            edges: selectedEdges.length,
            nodeIds: selectedNodes.map(n => n.id),
            edgeIds: selectedEdges.map(e => e.id)
          });
          
          if (selectedNodes.length > 0) {
            setNodes(nodes.filter(node => filterNodes(node, selectedNodes)));
          }
          
          if (selectedEdges.length > 0) {
            setEdges(edges.filter(edge => filterEdges(edge, selectedEdges)));
          }
        }
      },
      'clearCanvas': () => {
        if (onClearCanvas) {
          onClearCanvas();
        } else if (enableWorkflowStore) {
          // Ensure both nodes and edges are cleared
          setNodes([]);
          setEdges([]);
          clearWorkflow();
        }
      },
      'fitView': () => onFitView?.(),
      'openAgentDialog': () => onOpenAgentDialog?.(),
      'openTaskDialog': () => onOpenTaskDialog?.(),
      'openCrewPlanningDialog': () => onOpenCrewPlanningDialog?.(),
      'openSaveCrew': () => onOpenSaveCrew?.(),
      'executeCrew': async () => {
        if (onExecuteCrew) {
          onExecuteCrew();
        } else if (nodes && edges) {
          console.log('Shortcut: Executing crew with stored nodes/edges');
          try {
            if (validateCrewExecution(nodes)) {
              await executeCrewAction(nodes, edges);
            }
          } catch (error) {
            console.error('Error executing crew:', error);
            setErrorMessage(error instanceof Error ? error.message : 'Error executing crew');
            setShowError(true);
          }
        }
      },
      'executeFlow': async () => {
        if (onExecuteFlow) {
          onExecuteFlow();
        } else if (nodes && edges) {
          console.log('Shortcut: Executing flow with stored nodes/edges');
          await executeFlowAction(nodes, edges);
        }
      },
      'showRunResult': async () => {
        await fetchRuns();
        if (runs[0]) {
          showRunResult(runs[0]);
        }
      },
      'openCrewFlowDialog': () => onOpenCrewFlowDialog?.(),
      'openFlowDialog': () => onOpenFlowDialog?.(),
      'undo': () => onUndo?.(),
      'redo': () => onRedo?.(),
      'selectAll': () => onSelectAll?.(),
      'copy': () => onCopy?.(),
      'paste': () => onPaste?.(),
      'zoomIn': () => onZoomIn?.(),
      'zoomOut': () => onZoomOut?.(),
      'toggleFullscreen': () => onToggleFullscreen?.(),
      'generateConnections': () => onGenerateConnections?.(),
      'changeLLMForAllAgents': () => {
        if (onChangeLLMForAllAgents) {
          onChangeLLMForAllAgents();
        } else if (nodes) {
          console.log('Shortcut: Changing LLM for all agents');
          if (validateAgentNodes(nodes)) {
            onOpenLLMDialog?.();
          }
        }
      },
      'changeMaxRPMForAllAgents': () => {
        if (onChangeMaxRPMForAllAgents) {
          onChangeMaxRPMForAllAgents();
        } else if (nodes) {
          console.log('Shortcut: Changing Max RPM for all agents');
          if (validateAgentNodes(nodes)) {
            onOpenMaxRPMDialog?.();
          }
        }
      },
      'changeToolsForAllAgents': () => {
        if (onChangeToolsForAllAgents) {
          onChangeToolsForAllAgents();
        } else if (nodes) {
          console.log('Shortcut: Changing tools for all agents');
          if (validateAgentNodes(nodes)) {
            onOpenToolDialog?.();
          }
        }
      },
      'openLLMDialog': () => onOpenLLMDialog?.(),
      'openToolDialog': () => onOpenToolDialog?.(),
      'openMaxRPMDialog': () => onOpenMaxRPMDialog?.(),
      'openMCPConfigDialog': () => onOpenMCPConfigDialog?.()
    };

    return baseHandlers;
  }, [
    onDeleteSelected, flowInstance, enableWorkflowStore, nodes, edges, setNodes, setEdges, filterNodes, filterEdges,
    onClearCanvas, clearWorkflow, onFitView, onOpenAgentDialog, onOpenTaskDialog, onOpenCrewPlanningDialog,
    onOpenSaveCrew, onExecuteCrew, onExecuteFlow, onOpenCrewFlowDialog, onOpenFlowDialog, onUndo, onRedo, onSelectAll,
    onCopy, onPaste, onZoomIn, onZoomOut, onToggleFullscreen, onGenerateConnections,
    onChangeLLMForAllAgents, onChangeMaxRPMForAllAgents, onChangeToolsForAllAgents,
    onOpenLLMDialog, onOpenToolDialog, onOpenMaxRPMDialog, onOpenMCPConfigDialog,
    executeCrewAction, executeFlowAction, 
    showRunResult, fetchRuns, runs, setErrorMessage, setShowError
  ]);

  // Keep the handlerRef updated with the latest handlers
  useEffect(() => {
    handlerRef.current = handlers;
  }, [handlers]);

  // Add and remove event listeners
  useEffect(() => {
    console.log('useShortcuts - Setting up event listener');
    
    // Create stable references to the values used in the effect
    const currentDisabled = disabled;
    const currentShortcuts = shortcuts;
    
    // console.log('useShortcuts - Render cycle dependencies:', {
    //   handlersKeys: Object.keys(handlerRef.current || {}),
    //   disabledValue: currentDisabled,
    //   shortcutsCount: currentShortcuts.length
    // });
    
    const handleKeyDown = (event: KeyboardEvent) => {
      // console.log('useShortcuts - Key event captured:', event.key);
      
      if (currentDisabled) {
        // console.log('useShortcuts - Shortcuts are disabled');
        return;
      }

      // Check if a dialog is open
      const hasOpenDialog = document.querySelector('.MuiDialog-root') !== null;
      if (hasOpenDialog) {
        // console.log('useShortcuts - Dialog is open, blocking shortcuts');
        return;
      }

      // Rest of key handling logic
      const isShortcutKey = currentShortcuts.some(shortcut => 
        shortcut.keys.some(key => key.toLowerCase() === event.key.toLowerCase())
      );
      
      if (isShortcutKey) {
        // Add the key to the sequence
        const newKeySequence = [...keySequence.current, event.key.toLowerCase()];
        // console.log('useShortcuts - New key sequence:', newKeySequence);

        // Check if the sequence matches any shortcut
        const matchedShortcut = currentShortcuts.find(shortcut => 
          shortcut.keys.every((key, index) => 
            key.toLowerCase() === newKeySequence[index]
          )
        );

        if (matchedShortcut) {
          // console.log('useShortcuts - Matched shortcut:', matchedShortcut);
          // Execute the handler if it exists
          const handler = handlerRef.current ? handlerRef.current[matchedShortcut.action] : null;
          if (handler) {
            // console.log('useShortcuts - Executing handler for:', matchedShortcut.action);
            handler();
            // Clear the sequence after executing the handler
            keySequence.current = [];
          } else {
            // console.log('useShortcuts - No handler found for:', matchedShortcut.action);
          }
        } else {
          // Update the sequence
          keySequence.current = newKeySequence;
        }

        // Prevent default behavior for shortcut keys
        event.preventDefault();
      } else {
        // Clear the sequence if a non-shortcut key is pressed
        // console.log('useShortcuts - Non-shortcut key pressed, clearing sequence');
        keySequence.current = [];
      }
    };

    window.addEventListener('keydown', handleKeyDown);
    // console.log('useShortcuts - Event listener attached');
    
    // Capture the current timer value
    const currentTimer = sequenceTimer.current;
    
    return () => {
      // console.log('useShortcuts - Cleaning up event listener');
      window.removeEventListener('keydown', handleKeyDown);
      if (currentTimer) {
        clearTimeout(currentTimer);
      }
    };
  // Only disabled and shortcuts are needed as dependencies
  // handlers are accessed through handlerRef.current to avoid re-registering the event listener
  }, [disabled, shortcuts]);

  // Add mount/unmount logging
  useEffect(() => {
    // console.log('useShortcuts - Hook mounted');
    return () => {
      // console.log('useShortcuts - Hook unmounted');
    };
  }, []);

  return { shortcuts };
};

export default useShortcuts; 