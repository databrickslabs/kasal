# Flow Components

This directory contains components related to Flow management in the application, leveraging crewAI Flow concepts.

⚠️ **Note**: CrewAI Flow concepts are experimental and not completely tested. Use with caution in production environments.

## Components

- **FlowForm.tsx**: Main form for creating and editing flows
- **ConditionForm.tsx**: Component for specifying flow conditions (AND, OR, Router)
- **EdgeStateForm.tsx**: Component for managing edge state in the flow

## CrewAI Flow Concepts

The components in this directory implement crewAI Flow concepts, allowing users to create sophisticated flow control mechanisms:

### Flow Type Options

- **Normal**: Standard flow node without special decorators
- **Start (@start)**: Starting point of a flow, marked with the `@start()` decorator
- **Listen (@listen)**: Node that listens for events from other nodes, using the `@listen()` decorator
- **Router (@router)**: Node that implements conditional routing using the `@router()` decorator

### Condition Types

The ConditionForm component allows users to select from the following condition types:

1. **AND Condition**: 
   - Uses the `and_` function in crewAI
   - Triggers only when ALL specified methods emit an output
   - Example: `@listen(and_(method1, method2))`

2. **OR Condition**:
   - Uses the `or_` function in crewAI
   - Triggers when ANY of the specified methods emit an output
   - Example: `@listen(or_(method1, method2))`

3. **Router Condition**:
   - Uses the `@router()` decorator to implement conditional routing
   - Routes execution based on return values like "success" or "failure"
   - Example:
     ```python
     @router(start_method)
     def route_method(self):
         if self.state.success:
             return "success"
         else:
             return "failure"
     ```

## Usage

1. Create a new flow by selecting the appropriate flow type
2. If using a Start, Listen, or Router type, configure the condition settings
3. For Listen type flows, specify which nodes to listen to
4. For Router flows, define the routing condition

## Best Practices

- Use Start nodes as entry points for your flows
- Connect Listen nodes to appropriate upstream nodes
- Use AND conditions when all previous steps must be completed
- Use OR conditions when any previous step can trigger the next action
- Use Router conditions for branching logic based on success/failure scenarios

## Further Reading

For more information about crewAI Flows, visit the [official documentation](https://docs.crewai.com/concepts/flows). 