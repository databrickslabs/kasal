# Edge Configuration System

## Overview

This directory contains centralized configuration for all edge-related functionality in the Kasal workflow designer. The edge configuration system provides a single source of truth for edge styling, animation, and behavior.

## Architecture

### File Structure

```
src/frontend/src/config/
├── edgeConfig.ts       # Centralized edge configuration
├── index.ts            # Configuration exports
└── README.md           # This file
```

### Key Concepts

1. **Edge Categories**: Different types of edges based on source and target nodes
2. **Centralized Styling**: All edge styles defined in one place
3. **Automatic Animation**: Edges are automatically animated based on their category
4. **Consistent Handles**: Default handle positions determined by edge type

## Edge Categories

### 1. Agent-to-Task Edges
- **Source**: Agent nodes (`agent-*`)
- **Target**: Task nodes (`task-*`)
- **Style**: Blue (#2196f3), dashed (12px), animated
- **Handles**: `agent.right → task.left`
- **Label**: "assigned"

### 2. Task-to-Task Edges
- **Source**: Task nodes (`task-*`)
- **Target**: Task nodes (`task-*`)
- **Style**: Blue (#2196f3), dashed (12px), animated
- **Handles**: `task.right → task.left`
- **Label**: "dependency"

### 3. Flow Edges
- **Source**: Flow nodes (`flow-*`)
- **Target**: Flow nodes (`flow-*`)
- **Style**: Purple (#9c27b0), dashed (5px), animated
- **Handles**: Auto-determined
- **Label**: "state"

### 4. Crew-to-Crew Edges
- **Source**: Crew nodes
- **Target**: Crew nodes
- **Style**: Blue (#2196f3), solid, not animated
- **Handles**: Auto-determined
- **Label**: "flow"

## Usage

### Creating Edges

```typescript
import { createEdge } from '../utils/edgeUtils';

// Create an edge with automatic configuration
const edge = createEdge(
  { source: 'agent-123', target: 'task-456' },
  'default',  // type
  true,       // animated (optional - auto-detected if not provided)
  {}          // custom style overrides
);

// Result:
// - Automatically sets sourceHandle: 'right', targetHandle: 'left'
// - Automatically applies blue color and dashed style
// - Automatically enables animation
```

### Getting Edge Styles

```typescript
import { getEdgeStyle } from '../config/edgeConfig';

// Get style for a specific edge
const style = getEdgeStyle('agent-123', 'task-456', true);

// Result: Complete CSSProperties object with all styling
```

### Getting Edge Labels

```typescript
import { getEdgeLabel } from '../config/edgeConfig';

const label = getEdgeLabel('task-123', 'task-456');
// Returns: "dependency"

const label2 = getEdgeLabel('agent-123', 'task-456');
// Returns: "assigned"
```

### Checking Animation

```typescript
import { shouldEdgeBeAnimated } from '../config/edgeConfig';

const shouldAnimate = shouldEdgeBeAnimated('agent-123', 'task-456');
// Returns: true (agent-to-task edges are animated)

const shouldAnimate2 = shouldEdgeBeAnimated('crew-123', 'crew-456');
// Returns: false (crew-to-crew edges are not animated)
```

## Components

### AnimatedEdge Component

The default edge component that uses centralized configuration:

```typescript
// src/frontend/src/components/Common/AnimatedEdge.tsx
import { getEdgeStyle, getEdgeLabel, edgeColors } from '../../config/edgeConfig';

// Automatically applies correct styling based on source/target
const edgeStyles = getEdgeStyle(source, target, animated, style);
const label = getEdgeLabel(source, target);
```

### CrewEdge Component

Specialized edge for crew-to-crew connections:

```typescript
// src/frontend/src/components/Flow/CrewEdge.tsx
import { edgeColors, EdgeCategory, getEdgeStyleConfig } from '../../config/edgeConfig';

// Uses crew-specific styling
const crewEdgeStyle = getEdgeStyleConfig(EdgeCategory.CREW_TO_CREW, false, style);
```

## Color Palette

All edge colors are defined in `edgeConfig.ts`:

```typescript
export const edgeColors = {
  primary: '#2196f3',      // Blue - agent-task and task-task
  flow: '#9c27b0',         // Purple - flow edges
  dependency: '#ff9800',   // Orange - task dependencies
  crew: '#2196f3',         // Blue - crew-to-crew
  hover: '#1976d2',        // Darker blue for hover
  delete: '#666',          // Gray for delete buttons
};
```

## Animation

Edge animation is defined using MUI keyframes:

```typescript
export const edgeAnimations = {
  flow: keyframes`
    from {
      stroke-dashoffset: 24;
    }
    to {
      stroke-dashoffset: 0;
    }
  `,
};
```

Animation is automatically applied to:
- Agent-to-task edges
- Task-to-task edges
- Flow edges (when animated prop is true)

## Extending the System

### Adding a New Edge Category

1. Add the category to `EdgeCategory` enum:
```typescript
export enum EdgeCategory {
  // ... existing categories
  NEW_CATEGORY = 'new-category',
}
```

2. Update `getEdgeCategory` function:
```typescript
export const getEdgeCategory = (source?: string, target?: string): EdgeCategory => {
  // ... existing logic
  
  if (isNewCategory) {
    return EdgeCategory.NEW_CATEGORY;
  }
  
  // ...
};
```

3. Add styling in `getEdgeStyleConfig`:
```typescript
case EdgeCategory.NEW_CATEGORY:
  return {
    ...baseConfig,
    stroke: edgeColors.newColor,
    strokeDasharray: '8',
    animation: animated ? `${edgeAnimations.flow} 0.5s linear infinite` : 'none',
  };
```

### Adding a New Color

1. Add to `edgeColors` object:
```typescript
export const edgeColors = {
  // ... existing colors
  newColor: '#ff5722',  // Description
};
```

2. Use in components:
```typescript
import { edgeColors } from '../../config/edgeConfig';

const style = {
  stroke: edgeColors.newColor,
};
```

## Best Practices

1. **Always use centralized configuration**: Don't hardcode colors or styles in components
2. **Use helper functions**: Leverage `getEdgeStyle`, `getEdgeLabel`, etc.
3. **Let the system auto-detect**: Don't manually set `animated` unless you need to override
4. **Use edge categories**: Think in terms of categories, not individual properties
5. **Document changes**: Update this README when adding new categories or colors

## Migration Guide

### From Old System to New System

**Before:**
```typescript
const edge = {
  id: 'edge-1',
  source: 'agent-123',
  target: 'task-456',
  type: 'default',
  animated: true,
  style: {
    stroke: '#2196f3',
    strokeDasharray: '12',
    animation: 'flowAnimation 0.5s linear infinite',
  },
};
```

**After:**
```typescript
import { createEdge } from '../utils/edgeUtils';

const edge = createEdge(
  { source: 'agent-123', target: 'task-456' },
  'default'
);
// All styling and animation applied automatically!
```

## Troubleshooting

### Edges not animating

1. Check if `animated` prop is true
2. Verify edge category is correct (use `getEdgeCategory`)
3. Check if `shouldEdgeBeAnimated` returns true for your edge type
4. Ensure edge style includes animation property

### Wrong colors

1. Verify source/target node IDs match expected patterns
2. Check `getEdgeCategory` logic
3. Ensure no style overrides are interfering

### Handles not connecting correctly

1. Check `getDefaultHandles` function
2. Verify node IDs start with correct prefixes (`agent-`, `task-`, etc.)
3. Ensure handles exist on the nodes

## Related Files

- `src/frontend/src/utils/edgeUtils.ts` - Edge creation utilities
- `src/frontend/src/components/Common/AnimatedEdge.tsx` - Default edge component
- `src/frontend/src/components/Flow/CrewEdge.tsx` - Crew edge component
- `src/frontend/src/store/workflow.ts` - Edge state management

