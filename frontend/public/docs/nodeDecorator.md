# Node Decoration System

This document describes the Node Decoration system in the CrewAI Manager application, which highlights active and completed tasks in the workflow editor.

## Overview

The Node Decoration system is responsible for:

1. Tracking which tasks are currently active during job execution
2. Tracking which tasks have been completed
3. Updating the visual state of nodes in the workflow to reflect their status
4. Providing a consistent interface for components to interact with this functionality

## Implementation

The system consists of:

1. A Zustand store (`nodeDecorator.ts`) that maintains state and provides decoration logic
2. A custom hook (`useNodeDecorator.ts`) that provides a clean interface to the store
3. Integration with other components and hooks (like `useTaskHighlighter.ts`) that consume this functionality

## Store: nodeDecorator.ts

The Node Decorator store maintains the current state of task execution and provides methods to update and query this state.

### State

- `currentTaskId`: The ID of the task currently being executed
- `completedTaskIds`: An array of IDs for tasks that have been completed

### Actions

- `setCurrentTaskId`: Update the currently active task
- `setCompletedTaskIds`: Set the full list of completed tasks
- `addCompletedTaskId`: Add a single task to the completed list
- `clearCompletedTaskIds`: Clear the list of completed tasks
- `decorateNodesWithActiveStatus`: Apply the current state to a set of nodes, returning updated nodes with visual indicators

## Hook: useNodeDecorator.ts

The custom hook provides a convenient interface to the store with memoized handlers.

### Usage

```typescript
import { useNodeDecorator } from '../hooks/workflow/useNodeDecorator';
import { Node, Edge } from 'reactflow';

const MyComponent = () => {
  const { 
    // State
    currentTaskId,              // Currently active task ID
    completedTaskIds,           // List of completed task IDs
    
    // Actions
    setCurrentTaskId,           // Set the current task
    setCompletedTaskIds,        // Set completed tasks
    addCompletedTaskId,         // Add a task to completed list
    clearCompletedTaskIds,      // Clear completed tasks
    decorateNodes               // Apply decoration to nodes
  } = useNodeDecorator();
  
  // Example: Update nodes with active/completed status
  const updateNodes = (nodes: Node[], edges: Edge[]) => {
    const decoratedNodes = decorateNodes(nodes, edges);
    // Use the decorated nodes...
  };
};
```

## Integration with Task Highlighter

The Node Decorator system is integrated with the Task Highlighter, which monitors job execution and updates the node decoration state accordingly.

### useTaskHighlighter.ts

This hook:

1. Monitors active runs from the Run Status store
2. Updates the Node Decorator store when task status changes
3. Applies node decoration to the workflow nodes
4. Notifies parent components of job status changes

## Visual Indicators

The decoration system applies the following visual states to nodes:

- `isActive: true`: Indicates a task is currently being executed
- `isCompleted: true`: Indicates a task has been completed

These flags are used by the node components to apply appropriate styling.

## Task ID Matching

The system handles various task ID formats for compatibility:

- Direct matches: `taskId === currentTaskId`
- Prefix matches: `currentTaskId.startsWith('task_')` for format `task_task-XXX`
- Inclusion matches: `currentTaskId.includes(taskId)`

## Agent Node Decoration

Agent nodes are decorated based on their connections to task nodes:

- If an agent is connected to an active task, it is marked as active
- If an agent is connected to a completed task, it is marked as completed

This provides visual feedback about which agents are involved in the current task execution. 