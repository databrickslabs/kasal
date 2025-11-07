/**
 * Centralized Edge Configuration
 * 
 * This file contains all edge-related styling, animation, and type definitions.
 * It provides a single source of truth for edge appearance and behavior.
 */

import { CSSProperties } from 'react';
import { keyframes } from '@mui/system';

// ============================================================================
// EDGE TYPES
// ============================================================================

export enum EdgeType {
  DEFAULT = 'default',
  CREW = 'crewEdge',
}

export enum EdgeCategory {
  AGENT_TO_TASK = 'agent-to-task',
  TASK_TO_TASK = 'task-to-task',
  FLOW = 'flow',
  CREW_TO_CREW = 'crew-to-crew',
}

// ============================================================================
// ANIMATION DEFINITIONS
// ============================================================================

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

// ============================================================================
// COLOR PALETTE
// ============================================================================

export const edgeColors = {
  primary: '#2196f3',      // Blue - for task-task edges
  agentToTask: '#1565c0',  // Darker blue - for agent-to-task edges
  flow: '#9c27b0',         // Purple - for flow edges
  dependency: '#ff9800',   // Orange - for task dependencies
  crew: '#2196f3',         // Blue - for crew-to-crew edges
  hover: '#1976d2',        // Darker blue for hover states
  delete: '#666',          // Gray for delete buttons
};

// ============================================================================
// STYLE CONFIGURATIONS
// ============================================================================

interface EdgeStyleConfig {
  strokeWidth: number;
  stroke: string;
  strokeDasharray?: string;
  animation?: string;
  filter?: string;
  zIndex?: number;
  pointerEvents?: 'none' | 'auto';
}

/**
 * Get the edge category based on source and target node IDs
 */
export const getEdgeCategory = (source?: string, target?: string): EdgeCategory => {
  if (!source || !target) return EdgeCategory.AGENT_TO_TASK;
  
  const isSourceFlow = source.includes('flow');
  const isTargetFlow = target.includes('flow');
  const isSourceAgent = source.startsWith('agent-');
  const isSourceTask = source.startsWith('task-');
  const isTargetTask = target.startsWith('task-');
  
  if (isSourceFlow && isTargetFlow) {
    return EdgeCategory.FLOW;
  }
  
  if (isSourceAgent && isTargetTask) {
    return EdgeCategory.AGENT_TO_TASK;
  }
  
  if (isSourceTask && isTargetTask) {
    return EdgeCategory.TASK_TO_TASK;
  }
  
  return EdgeCategory.CREW_TO_CREW;
};

/**
 * Get the base style configuration for an edge category
 */
export const getEdgeStyleConfig = (
  category: EdgeCategory,
  animated = false,
  customStyle: CSSProperties = {}
): EdgeStyleConfig => {
  const baseConfig: EdgeStyleConfig = {
    strokeWidth: 2,
    stroke: edgeColors.primary,
    filter: 'drop-shadow(0 1px 2px rgba(33, 150, 243, 0.3))',
    zIndex: 0,
    pointerEvents: 'none',
  };
  
  switch (category) {
    case EdgeCategory.FLOW:
      return {
        ...baseConfig,
        stroke: edgeColors.flow,
        strokeDasharray: '5',
        animation: animated ? `${edgeAnimations.flow} 0.5s linear infinite` : 'none',
      };

    case EdgeCategory.AGENT_TO_TASK:
      return {
        ...baseConfig,
        stroke: edgeColors.primary, // Same color as task-to-task
        strokeDasharray: '0', // Explicitly set to '0' for solid line
        animation: 'none',
      };

    case EdgeCategory.TASK_TO_TASK:
      return {
        ...baseConfig,
        stroke: edgeColors.primary,
        strokeDasharray: '12', // Dashed line
        animation: animated ? `${edgeAnimations.flow} 0.5s linear infinite` : 'none',
      };

    case EdgeCategory.CREW_TO_CREW:
      return {
        ...baseConfig,
        stroke: edgeColors.crew,
      };

    default:
      return baseConfig;
  }
};

/**
 * Get the complete edge style including custom overrides
 */
export const getEdgeStyle = (
  source?: string,
  target?: string,
  animated = false,
  customStyle: CSSProperties = {}
): CSSProperties => {
  const category = getEdgeCategory(source, target);
  const baseStyle = getEdgeStyleConfig(category, animated);
  
  return {
    ...baseStyle,
    ...customStyle,
  } as CSSProperties;
};

// ============================================================================
// EDGE LABELS
// ============================================================================

export const getEdgeLabel = (source?: string, target?: string): string => {
  const category = getEdgeCategory(source, target);
  
  switch (category) {
    case EdgeCategory.FLOW:
      return 'state';
    case EdgeCategory.TASK_TO_TASK:
      return 'dependency';
    case EdgeCategory.AGENT_TO_TASK:
      return 'assigned';
    case EdgeCategory.CREW_TO_CREW:
      return 'flow';
    default:
      return '';
  }
};

// ============================================================================
// EDGE PROPERTIES
// ============================================================================

/**
 * Determine if an edge should be animated based on its category
 */
export const shouldEdgeBeAnimated = (source?: string, target?: string): boolean => {
  const category = getEdgeCategory(source, target);

  // Only animate task-to-task edges (not agent-to-task)
  return category === EdgeCategory.TASK_TO_TASK;
};

/**
 * Get the default handles for an edge based on source, target, and layout
 */
export const getDefaultHandles = (
  source?: string,
  target?: string,
  layout?: 'horizontal' | 'vertical'
): {
  sourceHandle: string | null;
  targetHandle: string | null;
} => {
  if (!source || !target) {
    return { sourceHandle: null, targetHandle: null };
  }

  const isAgentToTask = source.startsWith('agent-') && target.startsWith('task-');
  const isTaskToTask = source.startsWith('task-') && target.startsWith('task-');

  // Agent-to-task edges: vertical layout uses bottom->top, horizontal uses right->left
  if (isAgentToTask) {
    if (layout === 'vertical') {
      return {
        sourceHandle: 'bottom',
        targetHandle: 'top',
      };
    } else {
      return {
        sourceHandle: 'right',
        targetHandle: 'left',
      };
    }
  }

  // Task-to-task edges always use horizontal connectors (right->left)
  if (isTaskToTask) {
    return {
      sourceHandle: 'right',
      targetHandle: 'left',
    };
  }

  return { sourceHandle: null, targetHandle: null };
};

// ============================================================================
// EDGE VALIDATION
// ============================================================================

/**
 * Validate if an edge connection is allowed
 */
export const isValidEdgeConnection = (
  sourceType?: string,
  targetType?: string
): boolean => {
  // Add validation rules here
  // For now, allow all connections
  return true;
};

// ============================================================================
// EXPORTS
// ============================================================================

export default {
  EdgeType,
  EdgeCategory,
  edgeAnimations,
  edgeColors,
  getEdgeCategory,
  getEdgeStyleConfig,
  getEdgeStyle,
  getEdgeLabel,
  shouldEdgeBeAnimated,
  getDefaultHandles,
  isValidEdgeConnection,
};

