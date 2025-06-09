# Agents Component Documentation

This document provides an overview of the Agents components in the CrewAI Manager frontend application.

## Overview

The Agents components are responsible for creating, managing, editing, and visualizing agents within the CrewAI system. They handle agent configuration, tool selection, LLM model selection, knowledge source management, and more.

## Components Structure

### AgentNode

`AgentNode.tsx` implements a visual node component used in the workflow editor to represent an agent within the workflow.

**Key Features:**
- Displays agent information (name, role, goal)
- Provides visual feedback when an agent is active or completed
- Handles agent deletion and editing
- Supports drag and drop interactions
- Manages connections to other nodes (like tasks)

### AgentForm

`AgentForm.tsx` provides a comprehensive form for creating and editing agents.

**Key Features:**
- Input fields for basic agent properties (name, role, goal, backstory)
- LLM model selection
- Tools selection
- Configuration for agent parameters (max iterations, RPM, execution time)
- Memory and delegation settings
- Knowledge source management
- System/prompt/response template customization
- AI-assisted template generation

### AgentDialog

`AgentDialog.tsx` implements a dialog for managing saved agents.

**Key Features:**
- Lists all saved agents
- Allows selection of agents to add to the workflow
- Provides actions to delete agents
- Button to create new agents

### SavedAgents

`SavedAgents.tsx` displays a table of all saved agents with their properties and actions to edit or delete them.

**Key Features:**
- Tabular view of agents with their properties
- Edit and delete functionality
- Display of agent features (like memory)

### AgentGenerationDialog

`AgentGenerationDialog.tsx` provides an AI-assisted agent generation interface.

**Key Features:**
- Text prompt input for describing the desired agent
- Model selection for generation
- Tool selection for the generated agent
- Error handling and feedback

### ToolSelectionDialog

`ToolSelectionDialog.tsx` implements a dialog for selecting tools to assign to agents or tasks.

**Key Features:**
- Lists available tools with descriptions
- Search functionality for finding tools
- Multi-selection capabilities
- Target selection (which agents/tasks to apply tools to)

### LLMSelectionDialog

`LLMSelectionDialog.tsx` provides a dialog for selecting an LLM (Language Learning Model) for agents.

**Key Features:**
- Lists available models
- Shows model provider information
- Handles model selection and application

### MaxRPMSelectionDialog

`MaxRPMSelectionDialog.tsx` allows setting the maximum RPM (Requests Per Minute) for agents.

**Key Features:**
- Predefined RPM options with descriptions
- Applies selected RPM to agents

### KnowledgeSourcesSection

`KnowledgeSourcesSection.tsx` manages knowledge sources for agents.

**Key Features:**
- Supports multiple knowledge source types (text, file, URL, etc.)
- File upload functionality
- Configuration of chunking parameters
- File existence verification

## Data Flow

1. Agents are created using the `AgentForm` component, either from scratch or using AI generation via `AgentGenerationDialog`
2. Saved agents can be viewed and managed in `SavedAgents` or selected for use in workflows via `AgentDialog`
3. Agents are visualized in the workflow using `AgentNode` components
4. Agent properties like tools, LLM model, and max RPM can be modified through dedicated dialogs

## Key Interfaces

The Agents components use several key interfaces:

- **Agent**: Represents an agent with properties like name, role, goal, backstory, and configurations
- **Tool**: Represents a tool that can be assigned to agents
- **KnowledgeSource**: Represents a source of knowledge for an agent (text, file, URL)
- **Models**: Represents available LLM models that can be used by agents

## Services Integration

The Agents components integrate with several services:

- **AgentService**: For CRUD operations on agents
- **ToolService**: For retrieving available tools
- **ModelService**: For retrieving available LLM models
- **GenerateService**: For AI-assisted agent and template generation
- **UploadService**: For handling file uploads for knowledge sources

## Best Practices

When working with the Agents components:

1. Always use the appropriate dialog for specific configurations (tools, LLM, RPM)
2. Leverage AI generation for quick agent creation
3. Ensure proper error handling, especially for file uploads and API operations
4. Use the KnowledgeSourcesSection for managing agent knowledge sources
5. Keep UI consistency with the rest of the application using MUI components 