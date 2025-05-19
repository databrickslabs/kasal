# State Management with Zustand

This document provides an overview of the state management system used in the CrewAI Manager application, which primarily uses Zustand for state management.

## Introduction to Zustand

Zustand is a small, fast, and scalable state management solution for React applications. The application has migrated from Redux to Zustand for most of its state management needs to simplify state logic and improve performance.

## Store Structure

Each Zustand store in the application follows a similar pattern:

1. A TypeScript interface defining the state shape and actions
2. An initial state object
3. A store creation using `create` from Zustand
4. Action implementations that modify the state

## Available Stores

### Workflow Store (`workflow.ts`)

Manages the state for the workflow editor, including nodes, edges, and UI configuration.

**State:**
- `nodes`: Array of workflow nodes
- `edges`: Array of connections between nodes
- `selectedEdges`: Currently selected edges
- `contextMenu`: Context menu state (position and related edge)
- `flowConfig`: Configuration for the workflow
- `draggedNodeIds`: IDs of nodes being dragged
- `manuallyPositionedNodes`: IDs of nodes positioned manually
- `hasSeenTutorial`: Flag indicating if user has seen the tutorial
- `hasSeenHandlebar`: Flag indicating if user has seen the handlebar
- `uiState`: State for UI elements like minimap visibility

**Key Actions:**
- `setNodes`: Update the nodes in the workflow
- `setEdges`: Update the edges in the workflow
- `addEdge`: Add a new connection to the workflow
- `deleteEdge`: Remove an edge from the workflow
- `clearCanvas`: Clear all nodes and edges
- `updateNodePosition`: Update a node's position

### Node Decorator Store (`nodeDecorator.ts`)

Manages the state for highlighting active and completed tasks in the workflow.

**State:**
- `currentTaskId`: ID of the currently active task
- `completedTaskIds`: Array of completed task IDs

**Key Actions:**
- `setCurrentTaskId`: Update the current task ID
- `setCompletedTaskIds`: Update the completed tasks list
- `addCompletedTaskId`: Add a task to the completed list
- `clearCompletedTaskIds`: Clear all completed tasks
- `decorateNodesWithActiveStatus`: Apply active/completed status to nodes

### Error Store (`error.ts`)

Manages application-wide error state.

**State:**
- `showError`: Flag indicating if an error should be displayed
- `errorMessage`: Content of the error message

**Key Actions:**
- `showErrorMessage`: Display an error message
- `clearError`: Hide and clear the current error
- `resetError`: Reset error state to initial values

### API Keys Store (`apiKeys.ts`)

Manages API key secrets for integrations.

**State:**
- `secrets`: Array of API key secrets
- `loading`: Flag indicating if keys are being loaded
- `error`: Error message, if any

**Key Actions:**
- `fetchAPIKeys`: Load API keys from the backend
- `updateSecrets`: Update the list of secrets
- `clearError`: Clear any error messages

### Job Management Store (`jobManagement.ts`)

Manages state related to job execution.

**State:**
- `jobId`: ID of the current job
- `isRunning`: Flag indicating if a job is currently running
- `selectedModel`: Selected model for the job
- `planningEnabled`: Flag indicating if planning is enabled
- `schemaDetectionEnabled`: Flag indicating if schema detection is enabled
- `tools`: Available tools for the job
- `selectedTools`: IDs of selected tools

**Key Actions:**
- `setJobId`: Update the current job ID
- `setIsRunning`: Update the running state
- `setSelectedModel`: Set the model to use
- `setTools`: Update the available tools
- `setSelectedTools`: Update the selected tools
- `resetJobManagement`: Reset the state to initial values

### Model Config Store (`modelConfig.ts`)

Manages configuration for LLM models.

**State:**
- `models`: Available models
- `currentEditModel`: Currently edited model
- `editDialogOpen`: Flag indicating if edit dialog is open
- `isNewModel`: Flag indicating if a new model is being created
- `loading`: Flag indicating if models are being loaded
- `saving`: Flag indicating if model changes are being saved
- `modelsChanged`: Flag indicating if models have been modified
- `searchTerm`: Search term for filtering models
- `databricksEnabled`: Flag indicating if Databricks is enabled
- `error`: Error message, if any
- `refreshKey`: Key used to trigger refreshes
- `activeTab`: Currently active tab in the model config UI

**Key Actions:**
- `setModels`: Update the available models
- `setCurrentEditModel`: Set the model being edited
- `setEditDialogOpen`: Open/close the edit dialog
- `setLoading`: Update the loading state
- `setSaving`: Update the saving state
- `setModelsChanged`: Mark models as changed/unchanged
- `resetModelConfig`: Reset the state to initial values

### Run Result Store (`runResult.ts`)

Manages the state for displaying run results.

**State:**
- `selectedRun`: Currently selected run result
- `isOpen`: Flag indicating if the run result view is open

**Key Actions:**
- `showRunResult`: Display a run result
- `closeRunResult`: Close the run result view
- `setSelectedRun`: Set the selected run

### Theme Store (`theme.ts`)

Manages the application theme.

**State:**
- `mode`: Current theme mode (light/dark)
- `primaryColor`: Primary theme color
- `secondaryColor`: Secondary theme color

**Key Actions:**
- `setMode`: Change the theme mode
- `setPrimaryColor`: Change the primary color
- `setSecondaryColor`: Change the secondary color
- `resetTheme`: Reset to default theme settings

## Best Practices

1. **Use Custom Hooks**: Instead of directly using the stores in components, use the provided custom hooks from the `hooks` directory.

2. **Minimize Renders**: Be careful about which parts of the state you subscribe to. Only subscribe to the specific pieces of state your component needs.

3. **Action Consistency**: Use the provided actions to modify state rather than attempting to modify state directly.

4. **Type Safety**: Take advantage of TypeScript interfaces to ensure type safety when working with store state.

## Migration from Redux

Most of the application state has been migrated from Redux to Zustand, but there is still a placeholder Redux store in place for backward compatibility. New state should be implemented using Zustand stores.

## Persist Middleware

Some stores (like the workflow store) use Zustand's persist middleware to save state to localStorage. This allows state to persist across page reloads.

```typescript
export const useWorkflowStore = create<WorkflowState>()(
  persist(
    (set, get) => ({
      // Store implementation
    }),
    {
      name: 'workflow-storage',
      partialize: (state) => ({
        // Only persisted parts of the state
        nodes: state.nodes,
        edges: state.edges,
        hasSeenTutorial: state.hasSeenTutorial,
        hasSeenHandlebar: state.hasSeenHandlebar,
        manuallyPositionedNodes: state.manuallyPositionedNodes
      }),
    }
  )
);
``` 


# Zustand Store Implementation Guide

This document provides guidance for implementing new Zustand stores in the CrewAI Manager application.

## Basic Structure

A typical Zustand store implementation follows this structure:

```typescript
import { create } from 'zustand';
import { persist } from 'zustand/middleware'; // Optional, for persisted stores

// 1. Define the state and actions interface
interface MyStoreState {
  // State properties
  someValue: string;
  loading: boolean;
  error: string | null;
  
  // Actions
  setSomeValue: (value: string) => void;
  setLoading: (loading: boolean) => void;
  setError: (error: string | null) => void;
  resetStore: () => void;
}

// 2. Define initial state
const initialState = {
  someValue: '',
  loading: false,
  error: null,
};

// 3. Create the store
export const useMyStore = create<MyStoreState>((set) => ({
  // Include the initial state
  ...initialState,
  
  // Implement actions
  setSomeValue: (value: string) => 
    set(() => ({ someValue: value })),
  
  setLoading: (loading: boolean) => 
    set(() => ({ loading })),
  
  setError: (error: string | null) => 
    set(() => ({ error })),
  
  resetStore: () => 
    set(() => ({ ...initialState })),
}));
```

## Implementing a Persisted Store

For stores that need to persist their state across page reloads:

```typescript
export const useMyPersistedStore = create<MyStoreState>()(
  persist(
    (set) => ({
      // Store implementation with initial state and actions
      ...initialState,
      
      setSomeValue: (value: string) => 
        set(() => ({ someValue: value })),
      
      // ... other actions
    }),
    {
      name: 'my-persisted-store', // Name used in localStorage
      partialize: (state) => ({
        // Only include the properties you want to persist
        someValue: state.someValue,
        // Exclude transient state like loading or error
      }),
    }
  )
);
```

## Using `get` to Access Current State

Sometimes an action needs access to the current state:

```typescript
export const useCounterStore = create<CounterState>((set, get) => ({
  count: 0,
  
  increment: () => 
    set((state) => ({ count: state.count + 1 })),
  
  // Alternative using get to access current state
  incrementAlternative: () => {
    const currentCount = get().count;
    set({ count: currentCount + 1 });
  },
  
  reset: () => 
    set({ count: 0 }),
}));
```

## Handling Async Actions

For stores that need to interact with APIs:

```typescript
import { create } from 'zustand';
import { YourAPIService } from '../api';

interface DataState {
  data: any[];
  loading: boolean;
  error: string | null;
  
  fetchData: () => Promise<void>;
  clearData: () => void;
}

export const useDataStore = create<DataState>((set) => ({
  data: [],
  loading: false,
  error: null,
  
  fetchData: async () => {
    set({ loading: true, error: null });
    
    try {
      const apiService = YourAPIService.getInstance();
      const data = await apiService.getData();
      set({ data, loading: false });
    } catch (error) {
      set({ 
        loading: false, 
        error: error instanceof Error ? error.message : 'An unexpected error occurred'
      });
    }
  },
  
  clearData: () => 
    set({ data: [] }),
}));
```

## Implementing Computed Values

Zustand doesn't have built-in computed values like some other state management libraries, but you can implement them in a custom hook:

```typescript
// Store definition
interface TodoState {
  todos: { id: number; text: string; completed: boolean }[];
  addTodo: (text: string) => void;
  toggleTodo: (id: number) => void;
}

export const useTodoStore = create<TodoState>((set) => ({
  todos: [],
  
  addTodo: (text: string) => 
    set((state) => ({ 
      todos: [...state.todos, { id: Date.now(), text, completed: false }] 
    })),
  
  toggleTodo: (id: number) => 
    set((state) => ({ 
      todos: state.todos.map(todo => 
        todo.id === id ? { ...todo, completed: !todo.completed } : todo
      ) 
    })),
}));

// Custom hook with computed values
import { useMemo } from 'react';

export const useTodoWithComputed = () => {
  const { todos, addTodo, toggleTodo } = useTodoStore();
  
  const completedTodos = useMemo(() => 
    todos.filter(todo => todo.completed), [todos]);
  
  const incompleteTodos = useMemo(() => 
    todos.filter(todo => !todo.completed), [todos]);
  
  const totalTodos = useMemo(() => 
    todos.length, [todos]);
  
  return {
    todos,
    completedTodos,
    incompleteTodos,
    totalTodos,
    addTodo,
    toggleTodo,
  };
};
```

## Combining Multiple Stores

To share functionality between stores or combine multiple stores, use custom hooks:

```typescript
import { useUserStore } from './userStore';
import { useProductStore } from './productStore';

export const useShoppingCart = () => {
  const { userId, isAuthenticated } = useUserStore();
  const { products, fetchProductById } = useProductStore();
  
  // Implement shopping cart logic using both stores
  
  return {
    // Return combined state and functions
  };
};
```

## Best Practices

1. **Separating Concerns**: Each store should focus on a specific domain (user, products, UI state, etc.).

2. **Naming Conventions**:
   - Store hooks: `useXXXStore`
   - Custom hooks wrapping stores: `useXXX`
   - Boolean state: `isXXX` or `hasXXX`
   - Action functions: `setXXX`, `updateXXX`, `resetXXX`

3. **Type Safety**: Always define TypeScript interfaces for state and action parameters.

4. **Immutability**: Always treat state as immutable and create new objects/arrays when updating.

5. **Reusability**: Prefer small, focused stores that can be combined with custom hooks.

6. **Avoid Derived State**: Don't store values that can be derived from other state.

7. **Documentation**: Document each store's purpose, state structure, and action behaviors.

## Example: Complete Store Implementation

Here's a complete example of a Zustand store for user authentication:

```typescript
import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import { AuthService } from '../api/AuthService';

interface User {
  id: string;
  username: string;
  email: string;
  role: 'admin' | 'user';
}

interface AuthState {
  // State
  user: User | null;
  isAuthenticated: boolean;
  token: string | null;
  loading: boolean;
  error: string | null;
  
  // Actions
  login: (username: string, password: string) => Promise<void>;
  logout: () => void;
  checkAuth: () => Promise<void>;
  clearError: () => void;
}

const initialState = {
  user: null,
  isAuthenticated: false,
  token: null,
  loading: false,
  error: null,
};

export const useAuthStore = create<AuthState>()(
  persist(
    (set) => ({
      ...initialState,
      
      login: async (username: string, password: string) => {
        set({ loading: true, error: null });
        
        try {
          const authService = AuthService.getInstance();
          const { user, token } = await authService.login(username, password);
          
          set({ 
            user,
            token,
            isAuthenticated: true,
            loading: false
          });
        } catch (error) {
          set({ 
            loading: false, 
            error: error instanceof Error ? error.message : 'Login failed'
          });
        }
      },
      
      logout: () => {
        const authService = AuthService.getInstance();
        authService.logout();
        
        set({ 
          user: null,
          token: null,
          isAuthenticated: false,
          error: null
        });
      },
      
      checkAuth: async () => {
        set({ loading: true });
        
        try {
          const authService = AuthService.getInstance();
          const { user, token } = await authService.checkAuth();
          
          set({ 
            user,
            token,
            isAuthenticated: true,
            loading: false
          });
        } catch (error) {
          set({ 
            user: null,
            token: null,
            isAuthenticated: false,
            loading: false
          });
        }
      },
      
      clearError: () => 
        set({ error: null }),
    }),
    {
      name: 'auth-storage',
      partialize: (state) => ({
        token: state.token,
      }),
    }
  )
); 



# Custom Hooks

This document provides an overview of the custom hooks used in the CrewAI Manager application to interact with Zustand stores.

## Introduction

The application uses custom hooks to provide a clean interface between components and Zustand stores. These hooks encapsulate store interactions and provide additional functionality and type safety.

## Hook Structure

Most custom hooks in the application follow a similar pattern:

1. Import state and actions from a Zustand store
2. Create callback functions for actions (using `useCallback`)
3. Return a simplified interface with state and wrapped action handlers

## Global Hooks

### useError

**File**: `src/hooks/global/useError.ts`

Provides access to the application-wide error handling system.

```typescript
import { useError } from '../hooks/global/useError';

const MyComponent = () => {
  const { 
    showError,          // Boolean indicating if an error is being shown
    errorMessage,       // String containing the error message
    handleCloseError,   // Function to close the error dialog
    showErrorMessage    // Function to show a new error message
  } = useError();
  
  // Example usage
  const handleSubmit = async () => {
    try {
      await submitData();
    } catch (error) {
      showErrorMessage('Failed to submit data: ' + error.message);
    }
  };
};
```

### useAPIKeys

**File**: `src/hooks/global/useAPIKeys.ts`

Manages API keys for external services.

```typescript
import { useAPIKeys } from '../hooks/global/useAPIKeys';

const MyComponent = () => {
  const { 
    secrets,        // Array of API key secrets
    loading,        // Boolean indicating if keys are being loaded
    error,          // Error message, if any
    updateSecrets   // Function to update the list of secrets
  } = useAPIKeys();
  
  // The hook automatically fetches API keys on mount
};
```

### useModelConfig

**File**: `src/hooks/global/useModelConfig.ts`

Manages configuration for LLM models.

```typescript
import { useModelConfig } from '../hooks/global/useModelConfig';

const MyComponent = () => {
  const {
    models,                  // Available models
    currentEditModel,        // Currently edited model
    editDialogOpen,          // Is edit dialog open
    isNewModel,              // Is a new model being created
    loading,                 // Are models being loaded
    saving,                  // Are changes being saved
    handleSetModels,         // Update models
    handleSetCurrentEditModel, // Set model being edited
    handleSetEditDialogOpen, // Open/close edit dialog
    // ... other state and actions
  } = useModelConfig();
};
```

### useRunResult

**File**: `src/hooks/global/useRunResult.ts`

Manages the state for displaying execution run results.

```typescript
import { useRunResult } from '../hooks/global/useRunResult';

const MyComponent = () => {
  const {
    selectedRun,       // Currently selected run result
    isOpen,            // Is run result view open
    showRunResult,     // Show a run result
    closeRunResult,    // Close run result view
    setSelectedRun     // Set selected run
  } = useRunResult();
  
  // Example usage
  const handleRunClick = (run) => {
    showRunResult(run);
  };
};
```

## Workflow Hooks

### useNodeDecorator

**File**: `src/hooks/workflow/useNodeDecorator.ts`

Provides methods to decorate nodes with active and completed status.

```typescript
import { useNodeDecorator } from '../hooks/workflow/useNodeDecorator';

const MyComponent = () => {
  const {
    currentTaskId,            // Currently active task ID
    completedTaskIds,         // List of completed task IDs
    setCurrentTaskId,         // Update active task
    setCompletedTaskIds,      // Set all completed tasks
    addCompletedTaskId,       // Add a completed task
    clearCompletedTaskIds,    // Clear completed tasks
    decorateNodes             // Apply decoration to nodes
  } = useNodeDecorator();
  
  // Example usage
  const updateNodeVisuals = (nodes, edges) => {
    const decoratedNodes = decorateNodes(nodes, edges);
    setNodes(decoratedNodes);
  };
};
```

### useWorkflowRedux

**File**: `src/hooks/workflow/useWorkflowRedux.ts`

Provides access to the workflow editor state and operations.

```typescript
import { useWorkflowRedux } from '../hooks/workflow/useWorkflowRedux';

const MyComponent = () => {
  const {
    nodes,               // Workflow nodes
    edges,               // Workflow edges
    selectedEdges,       // Selected edges
    handleClearCanvas,   // Clear the canvas
    handleAddEdge,       // Add an edge
    handleDeleteEdge,    // Delete an edge
    onNodesChange,       // Handle node changes
    onEdgesChange,       // Handle edge changes
    // ... other state and actions
  } = useWorkflowRedux({
    showErrorMessage: (msg) => console.error(msg)
  });
};
```

### useCrewExecution

**File**: `src/hooks/workflow/useCrewExecution.ts`

Manages the execution of CrewAI workflows.

```typescript
import { useCrewExecution } from '../hooks/workflow/useCrewExecution';

const MyComponent = () => {
  const {
    isExecuting,         // Is a workflow being executed
    jobId,               // Current job ID
    selectedModel,       // Selected model for execution
    handleSetJobId,      // Set job ID
    handleSetIsExecuting, // Set execution state
    handleExecuteCrew,   // Execute a crew
    // ... other state and actions
  } = useCrewExecution();
  
  // Example usage
  const startExecution = async () => {
    await handleExecuteCrew(crewConfig);
  };
};
```

### useJobManagement

**File**: `src/hooks/workflow/useJobManagement.ts`

Manages job execution state and tools.

```typescript
import { useJobManagement } from '../hooks/workflow/useJobManagement';

const MyComponent = () => {
  const {
    jobId,                // Current job ID
    isRunning,            // Is a job running
    selectedModel,        // Selected model
    tools,                // Available tools
    selectedTools,        // Selected tools
    handleSetJobId,       // Set job ID
    handleSetIsRunning,   // Set running state
    handleSetSelectedTools, // Set selected tools
    // ... other state and actions
  } = useJobManagement();
};
```

## Context Hooks

### useThemeContext

**File**: `src/hooks/context/useThemeContext.ts`

Provides access to the application theme settings.

```typescript
import { useThemeContext } from '../hooks/context/useThemeContext';

const MyComponent = () => {
  const {
    mode,               // Current theme mode (light/dark)
    primaryColor,       // Primary theme color
    secondaryColor,     // Secondary theme color
    handleModeChange,   // Change theme mode
    handlePrimaryColorChange, // Change primary color
    handleSecondaryColorChange, // Change secondary color
    handleResetTheme    // Reset to default theme
  } = useThemeContext();
  
  // Example usage
  const toggleTheme = () => {
    handleModeChange(mode === 'light' ? 'dark' : 'light');
  };
};
```

## Best Practices

1. **Use Component-Specific Hooks**: For complex components, consider creating component-specific hooks that use global hooks internally.

2. **Minimize Dependencies**: Keep the dependency array of `useCallback` and `useEffect` as small as possible to prevent unnecessary rerenders.

3. **Handle Side Effects in Hooks**: Manage API calls and side effects inside hooks rather than components when possible.

4. **Consistent Naming**: Use the `handleXXX` naming convention for action handlers and `isXXX` for boolean state.

5. **Type Safety**: Ensure all hooks have proper TypeScript typing for parameters and return values.

## Example: Building a Custom Hook

Here's an example of how a custom hook is structured:

```typescript
import { useCallback } from 'react';
import { useYourZustandStore } from '../../store/yourStore';

export const useYourCustomHook = () => {
  // Get state and actions from Zustand store
  const { 
    someState, 
    someAction, 
    anotherState,
    anotherAction 
  } = useYourZustandStore();

  // Create callback functions for actions
  const handleSomeAction = useCallback((param: string) => {
    // Additional logic if needed
    someAction(param);
  }, [someAction]);

  const handleAnotherAction = useCallback(() => {
    // Additional logic if needed
    anotherAction();
  }, [anotherAction]);

  // Return simplified interface
  return {
    someState,
    anotherState,
    handleSomeAction,
    handleAnotherAction,
  };
};
```

# Migration Examples

## Migrating from Utility Functions to Zustand

As part of the application's evolution, we are moving utility functions that manage state to Zustand stores. This section provides an example of how to migrate a utility function to a Zustand store.

### Case Study: Node Decorator

The Node Decorator system was originally implemented as a utility function in `src/utils/nodeDecorator.ts`. It has been migrated to a Zustand store to provide better state management and integration with the React component lifecycle.

#### Before: Utility Function Approach

```typescript
// src/utils/nodeDecorator.ts (original)
import { Node, Edge } from 'reactflow';

export const decorateNodesWithActiveStatus = (
  nodes: Node[], 
  edges?: Edge[], 
  currentTaskId?: string,
  completedTaskIds: string[] = []
): Node[] => {
  // Implementation details
  // ...
};

// Usage in a component
import { decorateNodesWithActiveStatus } from '../utils/nodeDecorator';

const MyComponent = () => {
  const [nodes, setNodes] = useState<Node[]>([]);
  const [edges, setEdges] = useState<Edge[]>([]);
  const [currentTaskId, setCurrentTaskId] = useState<string | null>(null);
  const [completedTasks, setCompletedTasks] = useState<string[]>([]);
  
  // When task status changes
  useEffect(() => {
    const decoratedNodes = decorateNodesWithActiveStatus(
      nodes,
      edges,
      currentTaskId || undefined,
      completedTasks
    );
    setNodes(decoratedNodes);
  }, [currentTaskId, completedTasks]);
};
```

#### After: Zustand Store Approach

```typescript
// src/store/nodeDecorator.ts
import { create } from 'zustand';
import { Node, Edge } from 'reactflow';

interface NodeDecoratorState {
  currentTaskId: string | undefined;
  completedTaskIds: string[];
  
  setCurrentTaskId: (taskId: string | undefined) => void;
  setCompletedTaskIds: (taskIds: string[]) => void;
  decorateNodesWithActiveStatus: (nodes: Node[], edges?: Edge[]) => Node[];
}

export const useNodeDecoratorStore = create<NodeDecoratorState>((set, get) => ({
  // State and actions implementation
  // ...
}));

// src/hooks/workflow/useNodeDecorator.ts
import { useCallback } from 'react';
import { useNodeDecoratorStore } from '../../store/nodeDecorator';

export const useNodeDecorator = () => {
  // Custom hook implementation
  // ...
};

// Usage in a component
import { useNodeDecorator } from '../hooks/workflow/useNodeDecorator';

const MyComponent = () => {
  const [nodes, setNodes] = useState<Node[]>([]);
  const [edges, setEdges] = useState<Edge[]>([]);
  const { 
    setCurrentTaskId, 
    setCompletedTaskIds, 
    decorateNodes 
  } = useNodeDecorator();
  
  // When task status changes
  useEffect(() => {
    // Update the store state
    setCurrentTaskId(taskId);
    setCompletedTaskIds(completedTasks);
    
    // Apply decoration
    const decoratedNodes = decorateNodes(nodes, edges);
    setNodes(decoratedNodes);
  }, [taskId, completedTasks]);
};
```

### Benefits of Migration

1. **State Persistence**: Zustand stores can persist state across rerenders and component unmounts
2. **Centralized State**: State is managed in a single location rather than being passed through props
3. **Performance**: Zustand is optimized for performance and reduces unnecessary rerenders
4. **Type Safety**: TypeScript interfaces ensure proper state and action usage
5. **Testing**: Easier to test state changes and actions in isolation
6. **Reusability**: State logic can be easily shared between components

### Migration Process

1. **Create a Zustand store**: Define state and actions in a new store
2. **Create a custom hook**: Provide a clean interface to the store
3. **Update components**: Replace direct function calls with hook usage
4. **Provide backward compatibility**: Update the original utility to use the store internally
5. **Document the migration**: Add documentation on the new approach

For detailed examples, see:
- [Node Decorator Store](../src/store/nodeDecorator.ts)
- [Node Decorator Hook](../src/hooks/workflow/useNodeDecorator.ts)
- [Node Decorator Documentation](./nodeDecorator.md) 