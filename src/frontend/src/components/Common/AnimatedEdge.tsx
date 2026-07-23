import React, { useState, useCallback } from 'react';
import { BaseEdge, EdgeProps, getSmoothStepPath, EdgeText } from 'reactflow';
import { Dialog, DialogContent } from '@mui/material';
import { EdgeStateForm } from '../Flow';
import { getEdgeStyle, getEdgeLabel, edgeColors } from '../../config/edgeConfig';
import { useRunStatusStore } from '../../store/runStatus';

interface ExtendedEdgeProps extends EdgeProps {
  data?: {
    stateType?: 'structured' | 'unstructured';
    stateDefinition?: string;
    stateData?: Record<string, unknown>;
  };
  animated?: boolean;
}

const AnimatedEdge: React.FC<ExtendedEdgeProps> = ({
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  style = {},
  markerEnd,
  source,
  target,
  id,
  data,
  animated = false
}) => {
  const [isHovered, setIsHovered] = useState(false);
  const [isEditing, setIsEditing] = useState(false);
  // Only flow the dashes while a job is actually running; otherwise the edge is
  // rendered static (no moving animation) even if it's an "animated" edge.
  // hasRunningJobs stays true for the whole run (via SSE/polling), unlike the
  // crew store's isExecuting which only flips true briefly while submitting.
  const hasRunningJobs = useRunStatusStore(state => state.hasRunningJobs);

  const [edgePath, labelX, labelY] = getSmoothStepPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
    borderRadius: 16,
    offset: 12,
  });

  // Check if both source and target are flow nodes
  const isFlowEdge = source?.includes('flow') && target?.includes('flow');

  // Get label from centralized config
  let label: string;
  if (isFlowEdge && data?.stateType) {
    label = `${data.stateType} state`;
  } else {
    label = getEdgeLabel(source, target);
  }

  // Calculate position for UI elements
  const deleteButtonX = labelX + 50;
  const deleteButtonY = labelY;
  const settingsButtonX = labelX + 75;
  const settingsButtonY = labelY;

  // Calculate the bounds for the hover area with increased padding
  const minX = Math.min(sourceX, targetX) - 8;
  const maxX = Math.max(sourceX, targetX) + 8;
  const minY = Math.min(sourceY, targetY) - 10;
  const maxY = Math.max(sourceY, targetY) + 10;

  // Calculate the width and height for the path hover area
  const width = maxX - minX;
  const height = maxY - minY;

  // Get edge styles from centralized configuration.
  // The stored edge `style` may already bake in the flow animation, and ReactFlow's
  // built-in `.animated` class also animates the path — so force the `animation`
  // property here, gated on the crew actually running. An inline `animation: 'none'`
  // overrides both the stored style and ReactFlow's default animated-edge CSS.
  const computedStyles = getEdgeStyle(source, target, animated, style);
  const edgeStyles = {
    ...computedStyles,
    // While running, leave the animation unset so ReactFlow's built-in `.animated`
    // class drives the dash flow; when idle, force `none` to override both that
    // class CSS and any animation baked into the stored edge style.
    animation: animated && hasRunningJobs ? undefined : 'none',
  };

  const handleMouseEnter = useCallback(() => {
    if (!isHovered) {
      setIsHovered(true);
    }
  }, [isHovered]);

  const handleMouseLeave = useCallback((event: React.MouseEvent) => {
    const rect = (event.target as SVGElement).getBoundingClientRect();
    const x = event.clientX;
    const y = event.clientY;
    
    // Only set isHovered to false if we're actually leaving the area
    if (x < rect.left || x > rect.right || y < rect.top || y > rect.bottom) {
      setIsHovered(false);
    }
  }, []);

  const handleDelete = useCallback((event: React.MouseEvent) => {
    event.stopPropagation();
    const deleteEvent = new CustomEvent('edge:delete', { detail: { id } });
    window.dispatchEvent(deleteEvent);
  }, [id]);

  const handleEditState = useCallback((event: React.MouseEvent) => {
    event.stopPropagation();
    setIsEditing(true);
  }, []);

  const handleStateUpdate = useCallback((updatedState: {
    stateType: 'structured' | 'unstructured';
    stateDefinition: string;
    stateData?: Record<string, unknown>;
  }) => {
    const updateEvent = new CustomEvent('edge:update', { 
      detail: { 
        id,
        data: updatedState
      } 
    });
    window.dispatchEvent(updateEvent);
    setIsEditing(false);
  }, [id]);

  return (
    <>
      <g>
        {/* Path hover area only covers the actual path */}
        <rect
          x={minX}
          y={minY}
          width={width}
          height={height}
          fill="transparent"
          onMouseEnter={handleMouseEnter}
          onMouseLeave={handleMouseLeave}
          style={{ pointerEvents: 'all' }}
        />
        <BaseEdge
          path={edgePath}
          markerEnd={markerEnd}
          style={edgeStyles}
        />
        {isHovered && (
          <g style={{ pointerEvents: 'none' }}>
            <EdgeText
              x={labelX}
              y={labelY}
              label={label}
              labelStyle={{
                fill: isFlowEdge ? '#9c27b0' : '#2196f3',
                fontSize: '12px',
                fontWeight: 600,
              }}
              labelBgStyle={{
                fill: 'white',
                fillOpacity: 0.8,
              }}
              labelBgPadding={[2, 4]}
              labelBgBorderRadius={4}
            />
            
            {/* Delete button with smaller hit area */}
            <g 
              transform={`translate(${deleteButtonX}, ${deleteButtonY})`}
              onClick={handleDelete}
              style={{ cursor: 'pointer', pointerEvents: 'all' }}
            >
              {/* Smaller clickable area */}
              <rect
                x="-8"
                y="-8"
                width="16"
                height="16"
                fill="white"
                fillOpacity="0.9"
                rx="2"
                pointerEvents="all"
              />
              <line
                x1="-4"
                y1="-4"
                x2="4"
                y2="4"
                stroke={edgeColors.delete}
                strokeWidth="1.5"
                opacity="0.8"
                pointerEvents="none"
              />
              <line
                x1="4"
                y1="-4"
                x2="-4"
                y2="4"
                stroke={edgeColors.delete}
                strokeWidth="1.5"
                opacity="0.8"
                pointerEvents="none"
              />
            </g>
            
            {/* Settings button with smaller hit area (only for flow edges) */}
            {isFlowEdge && (
              <g 
                transform={`translate(${settingsButtonX}, ${settingsButtonY})`}
                onClick={handleEditState}
                style={{ cursor: 'pointer', pointerEvents: 'all' }}
              >
                {/* Smaller clickable area */}
                <rect
                  x="-8"
                  y="-8"
                  width="16"
                  height="16"
                  fill="white"
                  fillOpacity="0.9"
                  rx="2"
                  pointerEvents="all"
                />
                <circle
                  cx="0"
                  cy="0"
                  r="4"
                  fill="none"
                  stroke={edgeColors.delete}
                  strokeWidth="1.5"
                  opacity="0.8"
                  pointerEvents="none"
                />
                <line
                  x1="0"
                  y1="-3"
                  x2="0"
                  y2="-6"
                  stroke={edgeColors.delete}
                  strokeWidth="1.5"
                  opacity="0.8"
                  pointerEvents="none"
                />
                <line
                  x1="0"
                  y1="3"
                  x2="0"
                  y2="6"
                  stroke={edgeColors.delete}
                  strokeWidth="1.5"
                  opacity="0.8"
                  pointerEvents="none"
                />
                <line
                  x1="-3"
                  y1="0"
                  x2="-6"
                  y2="0"
                  stroke={edgeColors.delete}
                  strokeWidth="1.5"
                  opacity="0.8"
                  pointerEvents="none"
                />
                <line
                  x1="3"
                  y1="0"
                  x2="6"
                  y2="0"
                  stroke={edgeColors.delete}
                  strokeWidth="1.5"
                  opacity="0.8"
                  pointerEvents="none"
                />
              </g>
            )}
          </g>
        )}
      </g>
      
      {/* Dialog for state configuration */}
      {isFlowEdge && (
        <Dialog open={isEditing} onClose={() => setIsEditing(false)} maxWidth="md">
          <DialogContent>
            <EdgeStateForm
              initialData={data}
              onCancel={() => setIsEditing(false)}
              onSubmit={handleStateUpdate}
            />
          </DialogContent>
        </Dialog>
      )}
    </>
  );
};

export default AnimatedEdge; 