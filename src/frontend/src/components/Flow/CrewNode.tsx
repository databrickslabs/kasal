import React, { useState } from 'react';
import { Handle, Position, NodeProps, useReactFlow } from 'reactflow';
import { Box, Typography, IconButton, Tooltip, useTheme, CircularProgress } from '@mui/material';
import DeleteIcon from '@mui/icons-material/Delete';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import ErrorIcon from '@mui/icons-material/Error';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import { FlowConfiguration } from '../../types/flow';
import { useUILayoutStore } from '../../store/uiLayout';
import { useFlowExecutionStore } from '../../store/flowExecutionStore';

interface Task {
  id: string;
  name: string;
  description?: string;
}

interface CrewNodeData {
  id: string;
  label: string;
  crewName: string;
  crewId: string | number;
  flowConfig?: FlowConfiguration;
  selectedTasks?: Task[];
  allTasks?: Task[];
  order?: number; // Track creation order for maintaining sequence during layout toggles
}

const CrewNode: React.FC<NodeProps<CrewNodeData>> = ({ data, selected, id, isConnectable }) => {
  const { crewName, selectedTasks = [] } = data;
  const [isHovered, setIsHovered] = useState(false);
  const theme = useTheme();
  const layoutOrientation = useUILayoutStore(state => state.layoutOrientation);

  // Get execution state for this crew node
  const crewNodeStates = useFlowExecutionStore(state => state.crewNodeStates);

  // Find the execution state for this crew node by matching crew name
  // Try multiple key formats for robust matching
  const nodeLabel = data.label || crewName || '';
  let executionState = crewNodeStates.get(nodeLabel)
    || crewNodeStates.get(crewName)
    || crewNodeStates.get(nodeLabel.toLowerCase())
    || crewNodeStates.get(crewName.toLowerCase());

  // If still not found, search through all keys for a case-insensitive match
  if (!executionState && crewNodeStates.size > 0) {
    const nodeLabelLower = nodeLabel.toLowerCase();
    const crewNameLower = crewName.toLowerCase();
    for (const [key, state] of crewNodeStates.entries()) {
      const keyLower = key.toLowerCase();
      if (keyLower === nodeLabelLower || keyLower === crewNameLower ||
          keyLower.includes(nodeLabelLower) || nodeLabelLower.includes(keyLower)) {
        executionState = state;
        break;
      }
    }
  }

  const { deleteElements } = useReactFlow();

  // Determine the effective status: only use per-crew trace state.
  // Do NOT fall back to global flowStatus — crew nodes that never ran
  // (e.g. unmatched router branches) should stay idle, not show completed/failed.
  const effectiveStatus: string | undefined = executionState?.status;

  // Get status-based styling
  const getStatusStyles = (): {
    borderColor?: string;
    animation?: string;
    opacity?: number;
    background?: string;
  } => {
    if (!effectiveStatus) {
      return {}; // No per-crew status resolved — stay idle
    }

    switch (effectiveStatus) {
      case 'running':
        return {
          borderColor: theme.palette.info.main,
          animation: 'pulse 2s infinite',
        };
      case 'completed':
        return {
          borderColor: theme.palette.success.main,
        };
      case 'failed':
        return {
          borderColor: theme.palette.error.main,
        };
      case 'pending':
        return {
          opacity: 0.7,
        };
      default:
        return {};
    }
  };

  // Get status icon
  const getStatusIcon = () => {
    if (!effectiveStatus) return null;

    const iconStyle = { fontSize: 16 };

    switch (effectiveStatus) {
      case 'running':
        return <CircularProgress size={14} sx={{ color: theme.palette.info.main }} />;
      case 'completed':
        return <CheckCircleIcon sx={{ ...iconStyle, color: theme.palette.success.main }} />;
      case 'failed':
        return <ErrorIcon sx={{ ...iconStyle, color: theme.palette.error.main }} />;
      case 'pending':
        return <PlayArrowIcon sx={{ ...iconStyle, color: theme.palette.text.secondary, opacity: 0.5 }} />;
      default:
        return null;
    }
  };

  // Generate tooltip content showing selected tasks
  const taskTooltip = selectedTasks.length > 0
    ? `Selected tasks:\n${selectedTasks.map(t => `• ${t.name}`).join('\n')}`
    : crewName;
  
  const handleDelete = (event: React.MouseEvent) => {
    event.stopPropagation(); // Prevent node selection
    deleteElements({ nodes: [{ id }] });
  };

  const statusStyles = getStatusStyles();
  const statusIcon = getStatusIcon();

  return (
    <Tooltip title={taskTooltip} placement="top" arrow>
      <Box
        sx={{
          width: '100%',
          height: '100%',
          display: 'flex',
          flexDirection: 'column',
          justifyContent: 'center',
          alignItems: 'center',
          borderRadius: '8px',
          border: `1px solid ${statusStyles.borderColor || (selected ? theme.palette.primary.main : theme.palette.grey[300])}`,
          background: statusStyles.background || (selected
            ? `${theme.palette.primary.light}20`
            : theme.palette.background.paper),
          boxShadow: (selected
            ? `0 0 0 2px ${theme.palette.primary.main}`
            : `0 2px 4px ${theme.palette.mode === 'light' ? 'rgba(0, 0, 0, 0.05)' : 'rgba(0, 0, 0, 0.2)'}`),
          position: 'relative',
          transition: 'all 0.2s ease',
          overflow: 'visible',
          cursor: 'pointer',
          opacity: statusStyles.opacity || 1,
          animation: statusStyles.animation || 'none',
          '@keyframes pulse': {
            '0%': { boxShadow: '0 0 0 0 rgba(33, 150, 243, 0.4)' },
            '70%': { boxShadow: '0 0 0 10px rgba(33, 150, 243, 0)' },
            '100%': { boxShadow: '0 0 0 0 rgba(33, 150, 243, 0)' },
          },
          '&:hover': {
            boxShadow: '0 4px 8px rgba(0, 0, 0, 0.15)',
          }
        }}
        onMouseEnter={() => setIsHovered(true)}
        onMouseLeave={() => setIsHovered(false)}
      >
        {/* Target handles - for incoming connections */}
        {/* Top handle - for vertical layout */}
        <Handle
          type="target"
          position={Position.Top}
          id="top"
          isConnectable={isConnectable}
          style={{
            background: '#2196f3',
            width: '7px',
            height: '7px',
            opacity: layoutOrientation === 'vertical' ? 1 : 0,
            pointerEvents: layoutOrientation === 'vertical' ? 'all' : 'none'
          }}
        />

        {/* Left handle - for horizontal layout */}
        <Handle
          type="target"
          position={Position.Left}
          id="left"
          isConnectable={isConnectable}
          style={{
            background: '#2196f3',
            width: '7px',
            height: '7px',
            opacity: layoutOrientation === 'horizontal' ? 1 : 0,
            pointerEvents: layoutOrientation === 'horizontal' ? 'all' : 'none'
          }}
        />

        {/* Source handles - for outgoing connections */}
        {/* Bottom handle - for vertical layout */}
        <Handle
          type="source"
          position={Position.Bottom}
          id="bottom"
          isConnectable={isConnectable}
          style={{
            background: '#4caf50',
            width: '7px',
            height: '7px',
            opacity: layoutOrientation === 'vertical' ? 1 : 0,
            pointerEvents: layoutOrientation === 'vertical' ? 'all' : 'none'
          }}
        />

        {/* Right handle - for horizontal layout */}
        <Handle
          type="source"
          position={Position.Right}
          id="right"
          isConnectable={isConnectable}
          style={{
            background: '#4caf50',
            width: '7px',
            height: '7px',
            opacity: layoutOrientation === 'horizontal' ? 1 : 0,
            pointerEvents: layoutOrientation === 'horizontal' ? 'all' : 'none'
          }}
        />
        {/* Status icon badge */}
        {statusIcon && (
          <Box
            sx={{
              position: 'absolute',
              top: -8,
              left: -8,
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              width: 24,
              height: 24,
              borderRadius: '50%',
              backgroundColor: theme.palette.background.paper,
              boxShadow: '0 0 4px rgba(0,0,0,0.2)',
              zIndex: 10,
            }}
          >
            {statusIcon}
          </Box>
        )}

          <Typography
            variant="subtitle2"
            textAlign="center"
            fontWeight="bold"
            sx={{
              color: effectiveStatus === 'running'
                ? theme.palette.info.main
                : effectiveStatus === 'completed'
                  ? theme.palette.success.main
                  : effectiveStatus === 'failed'
                    ? theme.palette.error.main
                    : theme.palette.primary.main,
              padding: '0 8px',
              width: '100%',
              // Truncate long names with ellipsis (max 2 lines)
              display: '-webkit-box',
              WebkitLineClamp: 2,
              WebkitBoxOrient: 'vertical',
              overflow: 'hidden',
              textOverflow: 'ellipsis',
              lineHeight: 1.3,
              fontSize: '0.8rem',
            }}
          >
            {data.label || 'Unnamed Crew'}
          </Typography>
        
        {(isHovered || selected) && (
          <Box
            sx={{
              position: 'absolute',
              top: -6,
              right: -6,
              display: 'flex',
              gap: 0.5,
            }}
          >
            <IconButton
              size="small"
              onClick={handleDelete}
              sx={{
                backgroundColor: 'rgba(255, 255, 255, 0.9)',
                '&:hover': {
                  backgroundColor: '#ffebee', // light red for delete
                },
                boxShadow: '0 0 4px rgba(0,0,0,0.2)',
                width: 24,
                height: 24,
              }}
            >
              <DeleteIcon fontSize="small" color="error" />
            </IconButton>
          </Box>
        )}
      </Box>
    </Tooltip>
  );
};

export default CrewNode;