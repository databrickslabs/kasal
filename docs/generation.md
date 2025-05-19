# Generation Features in Kasal

## Overview

Kasal provides powerful AI-assisted generation capabilities to help users quickly create agents, tasks, and crew plans. This document covers how to use these generation features through both the UI and keyboard shortcuts.

## Generation Capabilities

### 1. Generate Agents

The Agent Generation feature allows you to create agents with specific roles, goals, and backstories using natural language descriptions.

#### How to Access Agent Generation

**Through UI:**
1. Navigate to the workflow designer canvas
2. Click the "Generate Agent" button in the toolbar
3. The Agent Generation dialog will open

**Using Keyboard Shortcuts:**
- Press `g` followed by `a` (vim-style shortcut for "generate agent")

#### Using Agent Generation

1. In the Agent Generation dialog, provide:
   - **Description**: Describe the type of agent you want to create (e.g., "A data analysis expert who can review financial data")
   - **Additional Context** (optional): Add any specific requirements or context

2. Click "Generate" to create the agent

3. Review and edit the generated agent properties:
   - **Name**: The agent's identifier
   - **Role**: The agent's professional role
   - **Goal**: What the agent aims to accomplish
   - **Backstory**: The agent's background and context

4. Click "Save" to add the agent to your workflow

#### Example Agent Description

```
A cybersecurity expert specializing in network vulnerabilities and threat detection. 
The agent should be able to analyze network traffic patterns and identify potential security risks.
```

### 2. Generate Tasks

The Task Generation feature helps you create well-defined tasks for your agents to perform.

#### How to Access Task Generation

**Through UI:**
1. Navigate to the workflow designer canvas
2. Click the "Generate Task" button in the toolbar
3. The Task Generation dialog will open

**Using Keyboard Shortcuts:**
- Press `g` followed by `t` (vim-style shortcut for "generate task")

#### Using Task Generation

1. In the Task Generation dialog, provide:
   - **Description**: Describe the task you want to create (e.g., "Analyze website traffic data and identify patterns")
   - **Additional Context** (optional): Add any specific requirements or context

2. Click "Generate" to create the task

3. Review and edit the generated task properties:
   - **Name**: The task's identifier
   - **Description**: Detailed explanation of what the task entails
   - **Expected Output**: The expected result or deliverable
   - **Tools** (optional): Any tools required for the task

4. Click "Save" to add the task to your workflow

#### Example Task Description

```
Review the latest quarterly financial reports and create a summary of key performance indicators,
highlighting any significant changes from the previous quarter.
```

### 3. Generate Crew Plans

The Crew Plan Generation feature helps you design an entire workflow by automatically creating and connecting multiple agents and tasks.

#### How to Access Crew Plan Generation

**Through UI:**
1. Navigate to the workflow designer canvas
2. Click the "Generate Crew Plan" button in the toolbar
3. The Crew Plan Generation dialog will open

**Using Keyboard Shortcuts:**
- Press `g` followed by `c` (vim-style shortcut for "generate crew")

#### Using Crew Plan Generation

1. In the Crew Plan Generation dialog, provide:
   - **Objective**: The overall goal you want to achieve (e.g., "Build a comprehensive market analysis report for a new product launch")
   - **Requirements** (optional): Any specific requirements or constraints
   - **Additional Context** (optional): Any background information that might be helpful

2. Click "Generate" to create the crew plan

3. Review the generated plan, which will include:
   - Multiple agents with defined roles
   - Tasks assigned to those agents
   - Connections between agents and tasks showing workflow

4. Click "Apply" to add the entire crew plan to your workflow

#### Example Crew Plan Objective

```
Create a comprehensive content marketing strategy for a new SaaS product, 
including audience analysis, content calendar, and distribution channels.
```

## Automatic Connection Generation

After creating multiple agents and tasks, you can automatically generate logical connections between them.

#### How to Generate Connections

**Through UI:**
1. Create multiple agents and tasks on the canvas
2. Click the "Generate Connections" button in the toolbar

**Using Keyboard Shortcuts:**
- Press `c` followed by `c` (vim-style shortcut for "create connections")

This feature analyzes the roles of your agents and the requirements of your tasks, then creates appropriate connections between them based on their compatibility.

## Shortcut Quick Reference

| Feature | Shortcut | Description |
|---------|----------|-------------|
| Generate Agent | `g`, `a` | Open the Agent Generation dialog |
| Generate Task | `g`, `t` | Open the Task Generation dialog |
| Generate Crew Plan | `g`, `c` | Open the Crew Plan Generation dialog |
| Generate Connections | `c`, `c` | Automatically connect agents and tasks |
| Execute Crew | `e`, `c` | Execute the current crew workflow |

## Best Practices for Generation

1. **Be Specific**: The more specific your descriptions, the better the generated results
2. **Iterate**: Generate initial components, then refine them for best results
3. **Provide Context**: Include domain-specific information for more accurate generation
4. **Review Outputs**: Always review generated components before executing workflows
5. **Use Templates**: Save successful generations as templates for future use

## Troubleshooting

### Common Issues

- **Vague Generations**: If the generated components are too generic, provide more specific descriptions
- **Incorrect Connections**: If connections don't make sense, try regenerating with more explicit role definitions
- **Generation Errors**: If you encounter errors during generation, check your API configuration and connectivity

### Getting Help

If you encounter issues with the generation features:

1. Check the application logs for detailed error messages
2. Ensure your LLM API keys are correctly configured
3. Try simplifying your descriptions if complex ones are causing issues 