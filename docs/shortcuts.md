# Keyboard Shortcuts System Documentation

## Overview

The keyboard shortcuts system in the CrewAI Manager application provides a powerful way to interact with the workflow designer through keyboard commands. This document covers the technical implementation details and component usage of the shortcuts system.

## Technical Architecture

### Core Components

1. **useShortcuts Hook** (`frontend/src/hooks/global/useShortcuts.ts`)
   - Main hook that manages keyboard shortcut functionality
   - Handles key sequence tracking and matching
   - Manages dialog state and shortcut enabling/disabling
   - Provides debugging capabilities
   - Integrates with Zustand state management

2. **Shortcuts UI Components**
   - `ShortcutsCircle.tsx`: Modal display of available shortcuts
   - `ShortcutsToggle.tsx`: Button to show/hide shortcuts
   - `ShortcutsStore`: Zustand store for managing shortcuts state

3. **Type Definitions** (`frontend/src/types/shortcuts.ts`)
   - Defines shortcut actions, configurations, and context types

### Key Features

- **Multi-key Sequences**: Support for complex key combinations
- **Dialog Awareness**: Automatically disables shortcuts when dialogs are open
- **Debugging Support**: Comprehensive logging for troubleshooting
- **Extensible Design**: Easy to add new shortcuts and handlers
- **State Management**: Integration with Zustand for global state
- **Validation**: Pre-action validation for node operations
- **Error Handling**: Improved error management via Zustand store

## Component Usage

### 1. CrewCanvas

**Location**: `frontend/src/components/WorkflowDesigner/CrewCanvas.tsx`

**Usage**:
```typescript
const { shortcuts } = useShortcuts({
  flowInstance: reactFlowInstanceRef.current,
  onDeleteSelected: handleDeleteSelected,
  onClearCanvas: handleClear,
  onZoomIn: () => reactFlowInstanceRef.current?.zoomIn(),
  onZoomOut: () => reactFlowInstanceRef.current?.zoomOut(),
  onFitView: () => reactFlowInstanceRef.current?.fitView({ padding: 0.2 }),
  onExecuteCrew: handleExecuteCrew,
  onExecuteFlow: () => executeCrew(nodes, edges),
  onOpenAgentDialog: () => setIsAgentGenerationDialogOpen(true),
  onOpenTaskDialog: () => setIsTaskGenerationDialogOpen(true),
  onOpenCrewPlanningDialog: () => setIsCrewPlanningDialogOpen(true),
  onGenerateConnections: handleGenerateConnections,
  onOpenSaveCrew: () => setIsSaveCrewDialogOpen(true),
  onOpenCrewFlowDialog: () => setIsCrewFlowDialogOpen(true),
  onChangeLLMForAllAgents: handleChangeLLM,
  onChangeMaxRPMForAllAgents: handleChangeMaxRPM,
  onChangeToolsForAllAgents: handleChangeTools,
  onOpenLLMDialog: () => setIsLLMSelectionDialogOpen(true),
  onOpenToolDialog: () => setIsToolDialogOpen(true),
  onOpenMaxRPMDialog: () => setIsMaxRPMSelectionDialogOpen(true),
  disabled: false,
  useWorkflowStore: true
});
```

**Handled Actions**:
- Canvas operations (delete, clear, fit view)
- Agent and task management
- Dialog control
- Execution commands with validation

### 2. FlowCanvas

**Location**: `frontend/src/components/WorkflowDesigner/FlowCanvas.tsx`

**Usage**:
```typescript
const { shortcuts: _shortcuts } = useShortcuts({
  flowInstance: reactFlowInstanceRef.current,
  onDeleteSelected: handleDeleteSelected,
  onClearCanvas: handleClearCanvas,
  onFitView: () => {
    if (reactFlowInstanceRef.current) {
      reactFlowInstanceRef.current.fitView({ padding: 0.2 });
    }
  },
  disabled: isRendering || hasError
});
```

**Handled Actions**:
- Basic canvas operations
- Node and edge management
- View control

### 3. WorkflowDesigner

**Location**: `frontend/src/components/WorkflowDesigner/WorkflowDesigner.tsx`

**Integration**:
- Coordinates between CrewCanvas and FlowCanvas
- Manages overall workflow state
- Handles shortcut-related UI components

## Default Shortcuts

### Canvas Operations
| Action | Keys | Description |
|--------|------|-------------|
| Delete Selected | `Delete` or `Backspace` | Delete selected nodes/edges |
| Clear Canvas | `d`, `d` | Clear entire canvas (vim-style) |
| Clear Canvas | `Alt`, `c` | Clear entire canvas |
| Fit View | `v`, `f` | Fit view to all nodes (vim-style) |
| Fit View | `Control`, `0` | Fit view to all nodes |
| Zoom In | `Control`, `=` | Zoom in |
| Zoom Out | `Control`, `-` | Zoom out |
| Toggle Fullscreen | `f` | Toggle fullscreen mode |

### Edit Operations
| Action | Keys | Description |
|--------|------|-------------|
| Undo | `Control`, `z` | Undo last action |
| Redo | `Control`, `Shift`, `z` | Redo last undone action |
| Redo | `Control`, `y` | Redo last undone action |
| Select All | `Control`, `a` | Select all nodes |
| Copy | `Control`, `c` | Copy selected nodes |
| Paste | `Control`, `v` | Paste copied nodes |

### Agent Operations
| Action | Keys | Description |
|--------|------|-------------|
| Open Agent Dialog | `g`, `a` | Open Generate Agent dialog |
| Open Task Dialog | `g`, `t` | Open Generate Task dialog |
| Open Crew Planning | `g`, `c` | Open Generate Crew Plan dialog |
| Generate Connections | `c`, `c` | Generate connections between agents/tasks |

### Crew Operations
| Action | Keys | Description |
|--------|------|-------------|
| Execute Crew | `e`, `c` | Execute Crew (validates agent & task nodes) |
| Execute Flow | `e`, `f` | Execute Flow |
| Show Run Result | `s`, `r` | Show Run Result dialog |
| Open Crew/Flow Dialog | `l`, `c` | Open Crew/Flow selection dialog |

### Agent Configuration
| Action | Keys | Description |
|--------|------|-------------|
| Change LLM | `l`, `l`, `m` | Change LLM model for all agents (validates agent nodes) |
| Change Max RPM | `m`, `a`, `x`, `r` | Change Max RPM for all agents (validates agent nodes) |
| Change Tools | `t`, `o`, `o`, `l` | Change tools for all agents (validates agent nodes) |

## Implementation Details

### Key Sequence Tracking

The system uses a `keySequence` ref to track multi-key shortcuts:
```typescript
const keySequence = useRef<string[]>([]);
```

### Dialog State Management

Shortcuts are automatically disabled when dialogs are open:
```typescript
const hasOpenDialog = document.querySelector('.MuiDialog-root') !== null;
if (hasOpenDialog) {
  console.log('useShortcuts - Dialog is open, blocking shortcuts');
  return;
}
```

### Handler Management with React Refs

Handlers are memoized and accessed via refs to prevent unnecessary re-renders:
```typescript
// Define a stable ref for handlers
const handlerRef = useRef<HandlerMap | null>(null);

// Memoize handlers
const handlers = useMemo(() => ({
  'deleteSelected': () => { /* ... */ },
  'clearCanvas': () => { /* ... */ },
  // ... other handlers
}), [/* dependencies */]);

// Keep the handlerRef updated with the latest handlers
useEffect(() => {
  handlerRef.current = handlers;
}, [handlers]);

// Use handlerRef.current in event listeners
const handler = handlerRef.current ? handlerRef.current[matchedShortcut.action] : null;
if (handler) {
  handler();
}
```

### Validation Logic

The shortcuts system now includes built-in validation for operations:

```typescript
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
```

### Zustand State Integration

The shortcuts system integrates with Zustand stores for state management:

```typescript
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

// Get error store for global error management
const errorStore = useErrorStore();
```

### Error Handling

Enhanced error handling with Zustand's error store:

```typescript
try {
  // Execute crew action
  if (validateCrewExecution(currentNodes)) {
    executeCrewAction(currentNodes, currentEdges);
  }
} catch (error) {
  console.error('Error executing crew:', error);
  setErrorMessage(`Error executing crew: ${error instanceof Error ? error.message : 'Unknown error'}`);
  setShowError(true);
}
```

### Debugging Support

The system provides detailed logging:
```typescript
console.log('useShortcuts - Key event captured:', event.key);
console.log('useShortcuts - Render cycle dependencies:', {
  handlersKeys: Object.keys(handlerRef.current || {}),
  disabledValue: currentDisabled,
  shortcutsCount: currentShortcuts.length
});
```

## Best Practices

1. **Adding New Shortcuts**:
   - Add the action type to `ShortcutAction` in `shortcuts.ts`
   - Add the shortcut configuration to `DEFAULT_SHORTCUTS`
   - Implement the handler in the appropriate component
   - Add validation logic if needed

2. **Handler Implementation**:
   - Use `useCallback` for handler functions
   - Include proper validation and error handling
   - Add appropriate logging for debugging
   - Implement graceful failure mechanisms

3. **Dependency Management**:
   - Use React refs to avoid excessive re-renders
   - Carefully manage dependencies in `useMemo` hooks
   - Use ESLint's `react-hooks/exhaustive-deps` rule with care
   - Consider using `// eslint-disable-next-line react-hooks/exhaustive-deps` when necessary

4. **State Management Integration**:
   - Use Zustand stores for shared state
   - Access store actions directly in handlers
   - Prefer store actions over local state updates
   - Maintain consistency between local and global state

5. **Testing Shortcuts**:
   - Test in different contexts (with/without dialogs)
   - Verify multi-key sequences work correctly
   - Check for conflicts with existing shortcuts
   - Test validation logic with various node configurations

## Troubleshooting

Common issues and solutions:

1. **Shortcuts Not Working**:
   - Check if a dialog is open
   - Verify the component is focused
   - Check console logs for debugging information
   - Ensure validations are passing

2. **Handler Not Executing**:
   - Verify the handler is properly registered
   - Check for errors in the handler implementation
   - Ensure the shortcut sequence is correctly matched
   - Verify that validation conditions are met

3. **Performance Issues**:
   - Check for unnecessary re-renders
   - Verify handler memoization
   - Monitor key sequence tracking
   - Use React DevTools to check for render cycles

4. **Validation Always Failing**:
   - Check node types in the validation functions
   - Verify node data structure
   - Log node content to debug issues
   - Ensure error messages are being correctly set

## Future Improvements

1. **Planned Enhancements**:
   - Custom shortcut configuration
   - Shortcut conflict detection
   - Visual shortcut editor
   - Shortcut learning mode
   - Enhanced validation feedback
   - Full integration with all Zustand stores

2. **Technical Debt**:
   - Further improve handler management
   - Enhance type safety
   - Extend debugging capabilities
   - Optimize performance
   - Refine validation logic 