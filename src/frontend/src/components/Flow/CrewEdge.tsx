import React from 'react';
import { BaseEdge, EdgeProps, getSmoothStepPath } from 'reactflow';
import { Box } from '@mui/material';
import { FlowConfiguration } from '../../types/flow';
import { edgeColors, EdgeCategory, getEdgeStyleConfig } from '../../config/edgeConfig';

interface CrewEdgeData {
  label?: string;
  stateType?: string;
  conditionType?: string;
  flowConfig?: FlowConfiguration;
}

const CrewEdge: React.FC<EdgeProps<CrewEdgeData>> = ({
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  style = {},
  markerEnd,
  data,
}) => {
  const [edgePath, labelX, labelY] = getSmoothStepPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
    borderRadius: 16,
  });

  // Get crew edge style from centralized config
  const crewEdgeStyle = getEdgeStyleConfig(EdgeCategory.CREW_TO_CREW, false, style);

  return (
    <g>
      <BaseEdge
        path={edgePath}
        markerEnd={markerEnd}
        style={crewEdgeStyle}
      />
      
      {/* Edge label with improved visibility */}
      {data?.label && (
        <foreignObject
          width={250}  // Increased width for longer task names
          height={60}  // Increased height for better readability
          x={labelX - 125}
          y={labelY - 30}
          requiredExtensions="http://www.w3.org/1999/xhtml"
        >
          <Box
            sx={{
              display: 'flex',
              justifyContent: 'center',
              alignItems: 'center',
              width: '100%',
              height: '100%',
              fontSize: '12px',  // Increased font size
              padding: '6px 12px',
              borderRadius: '8px',
              backgroundColor: 'rgba(255, 255, 255, 0.95)',  // More opaque background
              border: '1px solid rgba(33, 150, 243, 0.3)',  // Subtle border
              boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
              textAlign: 'center',
              position: 'relative',
              zIndex: 5,
              fontWeight: 'bold',
              wordBreak: 'break-word',
              overflow: 'hidden',
              color: edgeColors.hover,
              transition: 'all 0.2s ease',
              '&:hover': {
                backgroundColor: 'rgba(255, 255, 255, 1)',
                boxShadow: '0 4px 8px rgba(0,0,0,0.15)',
                transform: 'translateY(-1px)',
              }
            }}
          >
            {data.label}
          </Box>
        </foreignObject>
      )}
    </g>
  );
};

export default CrewEdge; 