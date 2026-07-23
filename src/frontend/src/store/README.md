# State Management

## Zustand Stores

### Workflow Store

The workflow state has been migrated from Redux to Zustand for simpler state management.

The workflow store manages the state for the workflow editor, including:
- Nodes and edges for the flow diagram
- Context menu state
- Flow configuration
- Node positions

#### Usage

```typescript
// Direct usage in components
import { useWorkflowStore } from '../store/workflow';

const MyComponent = () => {
  // Access state
  const { nodes, edges } = useWorkflowStore();
  
  // Access actions
  const { setNodes, addEdge, clearCanvas } = useWorkflowStore();
  
  // ...
};
```

#### Preferred Hook Usage

While you can use the store directly, it's recommended to use the provided hook for workflow state management:

```typescript
// Using the workflow hook
import { useWorkflowRedux } from '../hooks/workflow/useWorkflowRedux';

const MyComponent = () => {
  const { 
    nodes, 
    edges, 
    setNodes,
    onNodesChange,
    handleClearCanvas,
    // ...other workflow functions
  } = useWorkflowRedux({ 
    showErrorMessage: (msg) => console.error(msg) 
  });
  
  // ...
};
```

### Error Store

The application also uses a Zustand store for error state management.

```typescript
import { useError } from '../hooks/global/useError';

const MyComponent = () => {
  const { 
    showError, 
    errorMessage, 
    handleCloseError, 
    showErrorMessage 
  } = useError();
  
  // ...
};
```

### Shortcuts Store

The Shortcuts store has been migrated from React Context to Zustand for improved performance and consistency.

The store manages the keyboard shortcuts configuration and visibility:
- List of configured shortcuts
- Visibility state for the shortcuts panel
- Actions to toggle and update shortcuts

#### Usage

```typescript
// Direct usage in components
import { useShortcutsStore } from '../store/shortcuts';

const MyComponent = () => {
  // Access state
  const { shortcuts, showShortcuts } = useShortcutsStore();
  
  // Access actions
  const { setShortcuts, toggleShortcuts, setShortcutsVisible } = useShortcutsStore();
  
  // ...
};
```

#### Migration from Context

The ShortcutsContext has been completely replaced by the Zustand store. The `useShortcutsContext` hook
has been updated to use the Zustand store internally, ensuring backward compatibility with existing components.

If you were previously using:

```typescript
import { useShortcutsContext } from '../hooks/context/useShortcutsContext';

const { shortcuts, showShortcuts, toggleShortcuts } = useShortcutsContext();
```

This will still work, but direct use of the Zustand store is recommended for new code:

```typescript
import { useShortcutsStore } from '../store/shortcuts';

const { shortcuts, showShortcuts, toggleShortcuts } = useShortcutsStore();
```

## Redux Store

The application still maintains a Redux store for potential future Redux slices. Currently, it contains no active slices as the workflow state has been migrated to Zustand.

If you need to add a new Redux slice, add it to the `store/index.ts` file's reducer configuration. 

### Node Actions Store

The Node Actions store has been migrated from React Context to Zustand for consistent state management.

The store manages the handlers for node operations in the workflow editor:
- Agent edit actions
- Task edit actions
- Node deletion actions

#### Usage

```typescript
// Direct usage in components
import { useNodeActionsStore } from '../store/nodeActions';

const MyComponent = () => {
  // Access actions
  const { handleAgentEdit, handleTaskEdit, handleDeleteNode } = useNodeActionsStore();
  
  // Set custom handlers
  const { setHandleAgentEdit } = useNodeActionsStore();
  setHandleAgentEdit((agentId) => {
    console.log('Custom agent edit handler', agentId);
    // Implement your custom logic here
  });
  
  // ...
};
```

#### Preferred Hook Usage

For consistency, use the provided hook:

```typescript
// Using the node actions hook
import { useNodeActions } from '../hooks/global/useNodeActions';

const MyComponent = () => {
  const { 
    handleAgentEdit,
    handleTaskEdit,
    handleDeleteNode,
    setHandleAgentEdit
    // ...other functions
  } = useNodeActions();
  
  // ...
};
``` 