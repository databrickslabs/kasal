import React from 'react';
import { BaseEdge, EdgeProps, getSmoothStepPath } from 'reactflow';
import { Box } from '@mui/material';
import WarningIcon from '@mui/icons-material/Warning';
import { FlowConfiguration } from '../../types/flow';
import { edgeColors, EdgeCategory, getEdgeStyleConfig } from '../../config/edgeConfig';

export type FlowLogicType = 'AND' | 'OR' | 'ROUTER' | 'NONE';

interface CrewEdgeData {
  label?: string;
  stateType?: string;
  conditionType?: string;
  flowConfig?: FlowConfiguration;
  logicType?: FlowLogicType;
  routerCondition?: string;
  description?: string;
  configured?: boolean;
  listenToTaskIds?: string[];
  targetTaskIds?: string[];
  mergeGroupId?: string; // ID identifying which merge group this edge belongs to
  isMerged?: boolean; // Flag indicating this is part of a merged edge group
  mergeGroupSize?: number; // Total number of edges in this merge group
  isLastInGroup?: boolean; // Flag indicating this edge should show indicators (arrow, warning, labels)
}

const CrewEdge: React.FC<EdgeProps<CrewEdgeData>> = (props) => {
  const {
    id: _id,
    source: _source,
    target: _target,
    sourceX,
    sourceY,
    targetX,
    targetY,
    sourcePosition,
    targetPosition,
    style = {},
    markerEnd,
    data,
    selected,
  } = props;

  // Check if this edge is part of a merged group
  const isMerged = data?.isMerged && data?.mergeGroupId;
  const isLastInGroup = data?.isLastInGroup ?? false;
  const shouldShowIndicators = !isMerged || isLastInGroup; // Show indicators on regular edges or last in merge group

  const [edgePath, labelX, labelY] = getSmoothStepPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
    borderRadius: 16,
  });

  // Determine if edge is configured
  // An edge is configured if it has tasks selected, even if logic type is NONE
  const hasTasksConfigured =
    (data?.listenToTaskIds && data.listenToTaskIds.length > 0) &&
    (data?.targetTaskIds && data.targetTaskIds.length > 0);

  const isConfigured = hasTasksConfigured;
  const logicType = data?.logicType || 'NONE';

  // Get style based on configuration status
  const getEdgeColor = () => {
    if (selected) return '#2196f3'; // Blue when selected
    if (!isConfigured) return '#ff9800'; // Orange for unconfigured

    switch (logicType) {
      case 'AND':
        return '#4caf50'; // Green for AND
      case 'OR':
        return '#9c27b0'; // Purple for OR
      case 'ROUTER':
        return '#f44336'; // Red for ROUTER
      case 'NONE':
        return '#607d8b'; // Grey for NONE (configured but simple flow)
      default:
        return edgeColors.crew; // Default crew edge color
    }
  };

  // Get crew edge style from centralized config
  const crewEdgeStyle = {
    ...getEdgeStyleConfig(EdgeCategory.CREW_TO_CREW, selected || false, style),
    stroke: getEdgeColor(),
    strokeWidth: selected ? 3 : isConfigured ? 2 : 2,
    strokeDasharray: !isConfigured ? '5,5' : undefined, // Dashed if unconfigured
  };

  return (
    <g>
      {/* Render single path - ReactFlow provides correct sourceX/sourceY for each edge */}
      <BaseEdge
        path={edgePath}
        markerEnd={shouldShowIndicators ? markerEnd : undefined} // Only show arrow on last edge in merge group or on regular edges
        style={crewEdgeStyle}
      />

      {/* Configuration indicator - only show when NOT configured */}
      {!isConfigured && shouldShowIndicators && (
        <foreignObject
          width={24}
          height={24}
          x={labelX - 12}
          y={labelY - 12}
          className="edge-config-indicator"
          style={{ cursor: 'pointer', pointerEvents: 'all' }}
        >
          <Box
            sx={{
              display: 'flex',
              justifyContent: 'center',
              alignItems: 'center',
              width: '100%',
              height: '100%',
              animation: 'pulse 2s infinite',
              '@keyframes pulse': {
                '0%, 100%': {
                  opacity: 1,
                },
                '50%': {
                  opacity: 0.6,
                },
              },
              '&:hover': {
                transform: 'scale(1.2)',
                transition: 'transform 0.2s ease',
              },
            }}
          >
            <WarningIcon
              sx={{
                fontSize: 24,
                color: '#ff9800',
                filter: 'drop-shadow(0 0 2px white) drop-shadow(0 0 2px white)',
              }}
            />
          </Box>
        </foreignObject>
      )}

      {/* Edge label with task names (if provided) - only show on one edge per merge group */}
      {data?.label && shouldShowIndicators && (
        <foreignObject
          width={250}
          height={24}
          x={labelX - 125}
          y={labelY - 45}
          requiredExtensions="http://www.w3.org/1999/xhtml"
          style={{ pointerEvents: 'none' }}
        >
          <Box
            sx={{
              display: 'flex',
              justifyContent: 'center',
              alignItems: 'center',
              width: '100%',
              height: '100%',
              fontSize: '10px',
              padding: '2px 8px',
              borderRadius: '4px',
              backgroundColor: 'rgba(255, 255, 255, 0.9)',
              border: '1px solid rgba(0, 0, 0, 0.1)',
              textAlign: 'center',
              color: 'text.secondary',
              fontStyle: 'italic',
              overflow: 'hidden',
              textOverflow: 'ellipsis',
              whiteSpace: 'nowrap',
            }}
          >
            {data.label}
          </Box>
        </foreignObject>
      )}

      {/* Description tooltip (if configured and has description) - only show on one edge per merge group */}
      {isConfigured && data?.description && shouldShowIndicators && (
        <foreignObject
          width={200}
          height={40}
          x={labelX - 100}
          y={labelY + 15}
          style={{ pointerEvents: 'none' }}
        >
          <Box
            sx={{
              fontSize: '9px',
              padding: '4px 8px',
              borderRadius: '4px',
              backgroundColor: 'rgba(0, 0, 0, 0.7)',
              color: 'white',
              textAlign: 'center',
              opacity: selected ? 1 : 0,
              transition: 'opacity 0.2s',
              overflow: 'hidden',
              textOverflow: 'ellipsis',
              whiteSpace: 'nowrap',
            }}
          >
            {data.description}
          </Box>
        </foreignObject>
      )}
    </g>
  );
};

export default CrewEdge;
