import React, { useState } from 'react';
import { Handle, Position, NodeProps, useReactFlow } from 'reactflow';
import { Box, Typography, IconButton, Tooltip, useTheme } from '@mui/material';
import DeleteIcon from '@mui/icons-material/Delete';
import { FlowConfiguration } from '../../types/flow';
import { useUILayoutStore } from '../../store/uiLayout';

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

  const { deleteElements } = useReactFlow();

  // Generate tooltip content showing selected tasks
  const taskTooltip = selectedTasks.length > 0
    ? `Selected tasks:\n${selectedTasks.map(t => `• ${t.name}`).join('\n')}`
    : crewName;
  
  const handleDelete = (event: React.MouseEvent) => {
    event.stopPropagation(); // Prevent node selection
    deleteElements({ nodes: [{ id }] });
  };

  return (
    <Tooltip title={taskTooltip} placement="top" arrow>
      <Box
        sx={{
          width: 140,
          height: 80,
          display: 'flex',
          flexDirection: 'column',
          justifyContent: 'center',
          alignItems: 'center',
          borderRadius: '12px',
          border: `1px solid ${theme.palette.divider}`,
          background: selected
            ? `${theme.palette.primary.light}20`
            : theme.palette.background.paper,
          boxShadow: selected
            ? `0 0 0 2px ${theme.palette.primary.main}`
            : 1,
          position: 'relative',
          transition: 'all 0.2s ease',
          overflow: 'visible',
          cursor: 'pointer',
          '&:hover': {
            boxShadow: 4,
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
          <Typography
            variant="subtitle1"
            textAlign="center"
            fontWeight="bold"
            sx={{
              color: theme.palette.primary.main,
              wordBreak: 'break-word',
              padding: '0 5px',
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