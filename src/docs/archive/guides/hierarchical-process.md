# Hierarchical Process in Kasal

## Overview

Hierarchical Process is a CrewAI feature that enables manager-based task delegation and coordination. In this process type, a manager agent (or manager LLM) oversees the workflow, delegating tasks to specialist agents and ensuring quality control.

## Key Concepts

### Sequential vs Hierarchical Process

- **Sequential Process**: Tasks are executed one after another in a linear fashion. Each task completes before the next begins.
- **Hierarchical Process**: A manager coordinates the workflow, intelligently delegating tasks to the most suitable agents and validating outcomes.

### Manager Configuration

The hierarchical process requires one of:
1. **Manager LLM**: A language model that acts as the manager
2. **Manager Agent**: A custom agent with specific role, goal, and backstory to manage the crew

## Implementation in Kasal

### Backend Support

The backend now fully supports hierarchical process through:

1. **Process Type Handling**: Converts string process types to CrewAI Process enum
2. **Manager LLM Support**: Configures a language model as the crew manager
3. **Manager Agent Support**: Allows custom agent definition for management
4. **Automatic Configuration**: Falls back to appropriate defaults when needed

### Configuration Flow

1. Frontend sends process type in execution configuration
2. If hierarchical, includes optional manager_llm or manager_agent settings
3. Backend processes configuration and creates appropriate crew
4. Manager coordinates task execution and delegation

## Usage Examples

### Basic Hierarchical Process

```python
# Configuration sent from frontend
{
    "inputs": {
        "process": "hierarchical",
        "manager_llm": "gpt-4o"
    }
}
```

### Custom Manager Agent

```python
# Configuration with custom manager agent
{
    "inputs": {
        "process": "hierarchical",
        "manager_agent": {
            "role": "Project Manager",
            "goal": "Efficiently coordinate team efforts",
            "backstory": "Experienced project manager skilled in delegation",
            "allow_delegation": true
        }
    }
}
```

## Benefits

### Better Task Coordination
- Manager ensures tasks are delegated to the most suitable agents
- Validates outcomes before proceeding
- Handles complex dependencies between tasks

### Quality Control
- Manager reviews and validates agent outputs
- Can request revisions or additional work
- Ensures consistency across the workflow

### Intelligent Delegation
- Manager understands agent capabilities
- Assigns tasks based on agent expertise
- Balances workload across the team

## Configuration Options

### Process Type
- `sequential` (default): Linear task execution
- `hierarchical`: Manager-coordinated execution

### Manager Settings

#### Manager LLM
- Specify the model to use as manager
- Examples: "gpt-4o", "claude-3-opus", "databricks-llama-4-maverick"

#### Manager Agent
Custom agent configuration with:
- `role`: Manager's role description
- `goal`: What the manager aims to achieve
- `backstory`: Context and experience
- `allow_delegation`: Must be `true` for hierarchical process

### Planning and Reasoning
- Can be combined with hierarchical process
- Manager handles both coordination and planning/reasoning

## Best Practices

### When to Use Hierarchical Process

1. **Complex Workflows**: Multiple interdependent tasks
2. **Quality Critical**: Need validation and review
3. **Dynamic Delegation**: Tasks require different expertise
4. **Large Teams**: Many agents with varied capabilities

### When to Use Sequential Process

1. **Simple Workflows**: Linear, well-defined task sequence
2. **Speed Priority**: Minimal coordination overhead
3. **Small Teams**: Few agents with clear responsibilities
4. **Predictable Tasks**: Fixed execution order

## Technical Implementation

### Backend Changes

1. **crew_preparation.py**:
   - Import Process enum from crewai
   - Handle process type conversion
   - Support manager_agent creation
   - Configure manager_llm when needed

2. **config_adapter.py**:
   - Pass through hierarchical configuration
   - Preserve manager settings from frontend

### Frontend Integration (Planned)

1. **Process Type Selector**: Dropdown for sequential/hierarchical
2. **Manager Configuration**: Optional settings when hierarchical selected
3. **Model Selection**: Choose manager LLM from available models
4. **Custom Agent**: Form for defining manager agent properties

## Troubleshooting

### Common Issues

1. **No Manager Specified**:
   - Hierarchical process requires either manager_llm or manager_agent
   - System will attempt to use default model if available

2. **Delegation Not Working**:
   - Ensure manager_agent has `allow_delegation: true`
   - Check that specialist agents are properly configured

3. **Performance Considerations**:
   - Hierarchical process has more overhead than sequential
   - Manager adds extra LLM calls for coordination

## Future Enhancements

1. **UI Support**: Visual process type selection and manager configuration
2. **Manager Templates**: Pre-configured manager agents for common scenarios
3. **Delegation Visualization**: Show task delegation flow in UI
4. **Performance Metrics**: Track manager efficiency and delegation patterns

## API Reference

### Execution Configuration

```typescript
interface ExecutionInputs {
    process?: 'sequential' | 'hierarchical';
    manager_llm?: string;
    manager_agent?: {
        role: string;
        goal: string;
        backstory: string;
        allow_delegation: boolean;
    };
    // ... other inputs
}
```

### Backend Processing

The backend automatically:
1. Converts process string to Process enum
2. Creates manager_agent if configuration provided
3. Sets manager_llm if specified or uses defaults
4. Configures crew with appropriate settings

## Conclusion

Hierarchical Process in Kasal provides powerful workflow orchestration capabilities through manager-based coordination. By leveraging CrewAI's built-in features, Kasal enables sophisticated multi-agent collaboration with intelligent task delegation and quality control.