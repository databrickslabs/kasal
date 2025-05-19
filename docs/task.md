# Tasks Component Documentation

This document provides an overview of the Tasks components in the CrewAI Manager frontend application.

## Overview

The Tasks components are responsible for creating, managing, editing, and visualizing tasks within the CrewAI system. These components handle task configuration, tool assignment, conditional execution settings, and advanced parameters like caching, callbacks, and error handling.

## Components Structure

### TaskNode

`TaskNode.tsx` implements a visual node component used in the workflow editor to represent a task within the workflow.

**Key Features:**
- Displays task information (name, description, expected output)
- Provides visual feedback for active or completed tasks
- Handles task deletion and editing
- Supports drag and drop interactions
- Manages connections to other nodes (like agents or other tasks)
- Supports connecting tasks in sequence through double-clicking

### TaskForm

`TaskForm.tsx` provides a comprehensive form for creating and editing tasks.

**Key Features:**
- Input fields for basic task properties (name, description, expected output)
- Tools selection
- Agent assignment
- Async execution configuration
- Context definition (relationships with other tasks)
- Integration with advanced configuration
- AI-assisted task generation

### TaskDialog

`TaskDialog.tsx` implements a dialog for managing saved tasks.

**Key Features:**
- Lists all saved tasks
- Allows selection of tasks to add to the workflow
- Provides actions to delete tasks
- Button to create new tasks
- Multi-selection capabilities

### SavedTasks

`SavedTasks.tsx` displays a table of all saved tasks with their properties and actions to edit or delete them.

**Key Features:**
- Tabular view of tasks with their properties
- Edit and delete functionality
- Displays associated agent
- Notification system for operation feedback

### TaskGenerationDialog

`TaskGenerationDialog.tsx` provides an AI-assisted task generation interface.

**Key Features:**
- Text prompt input for describing the desired task
- Model selection for generation
- Error handling and feedback
- Handles keyboard shortcuts (Enter to generate)

### TaskAdvancedConfig

`TaskAdvancedConfig.tsx` provides advanced configuration options for tasks.

**Key Features:**
- Execution settings (async, priority, timeout)
- Caching configuration (enable/disable, TTL)
- Output formatting (JSON schema, file path)
- Error handling strategy (retry on fail, max retries)
- Callback function selection
- Human input requirement setting
- Conditional execution configuration

## Data Flow

1. Tasks are created using the `TaskForm` component, either from scratch or using AI generation via `TaskGenerationDialog`
2. Saved tasks can be viewed and managed in `SavedTasks` or selected for use in workflows via `TaskDialog`
3. Tasks are visualized in the workflow using `TaskNode` components
4. Advanced task configurations are managed through the `TaskAdvancedConfig` component within the task form

## Key Interfaces

The Tasks components use several key interfaces:

- **Task**: Represents a task with properties like name, description, expected output, and configurations
- **TaskFormData**: Represents the form data structure used for editing tasks
- **TaskAdvancedConfigProps**: Represents the props for the advanced configuration component
- **TaskCallbackOption**: Represents available callback functions for tasks

## Services Integration

The Tasks components integrate with several services:

- **TaskService**: For CRUD operations on tasks
- **ToolService**: For retrieving available tools to assign to tasks
- **AgentService**: For retrieving available agents to assign tasks to
- **ModelService**: For retrieving available LLM models for task generation
- **GenerateService**: For AI-assisted task generation

## Conditional Execution

Tasks can be configured with conditional execution through:

- **Condition types**: Predefined conditions like 'is_data_missing'
- **Error handling strategies**: Different approaches to handle errors (retry, ignore, fail)
- **Human input requirement**: Option to require human confirmation before proceeding

## Best Practices

When working with the Tasks components:

1. Use clear, descriptive names and detailed descriptions for tasks
2. Configure appropriate error handling strategies based on task criticality
3. Set meaningful timeout values to prevent workflow blockages
4. Use async execution for tasks that don't need immediate results
5. Leverage AI generation for quick task creation
6. Use the proper output configuration based on the task's output format
7. Set appropriate caching policies to improve performance and reduce redundant operations
8. Keep UI consistency with the rest of the application using MUI components 