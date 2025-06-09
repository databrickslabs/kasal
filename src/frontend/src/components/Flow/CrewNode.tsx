import React, { useState } from 'react';
import { Handle, Position, NodeProps, useReactFlow } from 'reactflow';
import { Box, Typography, IconButton } from '@mui/material';
import DeleteIcon from '@mui/icons-material/Delete';
import { FlowConfiguration } from '../../types/flow';

interface CrewNodeData {
  id: string;
  label: string;
  crewName: string;
  crewId: string | number;
  flowConfig?: FlowConfiguration;
}

const CrewNode: React.FC<NodeProps<CrewNodeData>> = ({ data, selected, id, isConnectable }) => {
  const { crewName } = data;
  const [isHovered, setIsHovered] = useState(false);
  
  const { deleteElements } = useReactFlow();
  
  // Generate a deterministic color based on crew name
  const getNodeColor = (name: string) => {
    const colors = [
      '#4caf50', // green
      '#2196f3', // blue
      '#ff9800', // orange
      '#e91e63', // pink
      '#9c27b0', // purple
      '#00bcd4', // cyan
      '#ff5722', // deep orange
      '#607d8b', // blue grey
    ];
    
    // Simple hash function to get a consistent color for the same name
    let hash = 0;
    for (let i = 0; i < name.length; i++) {
      hash = name.charCodeAt(i) + ((hash << 5) - hash);
    }
    
    return colors[Math.abs(hash) % colors.length];
  };
  
  const nodeColor = getNodeColor(crewName);
  
  const handleDelete = (event: React.MouseEvent) => {
    event.stopPropagation(); // Prevent node selection
    deleteElements({ nodes: [{ id }] });
  };

  // Common styles for all handles
  const handleStyle = {
    width: 8,
    height: 8,
    background: '#555',
    border: '2px solid #fff',
    cursor: 'crosshair',
  };
  
  return (
    <>
      {/* Top handles */}
      <Handle
        type="source"
        position={Position.Top}
        id="top"
        isConnectable={isConnectable}
        style={{ 
          ...handleStyle,
          top: -4,
        }}
      />
      
      <Handle
        type="target"
        position={Position.Top}
        id="top-target"
        isConnectable={isConnectable}
        style={{ 
          ...handleStyle,
          top: -4,
        }}
      />
      
      {/* Right handles */}
      <Handle
        type="source"
        position={Position.Right}
        id="right"
        isConnectable={isConnectable}
        style={{ 
          ...handleStyle,
          right: -4,
        }}
      />
      
      <Handle
        type="target"
        position={Position.Right}
        id="right-target"
        isConnectable={isConnectable}
        style={{ 
          ...handleStyle,
          right: -4,
        }}
      />
      
      {/* Bottom handles */}
      <Handle
        type="source"
        position={Position.Bottom}
        id="bottom"
        isConnectable={isConnectable}
        style={{ 
          ...handleStyle,
          bottom: -4,
        }}
      />
      
      <Handle
        type="target"
        position={Position.Bottom}
        id="bottom-target"
        isConnectable={isConnectable}
        style={{ 
          ...handleStyle,
          bottom: -4,
        }}
      />
      
      {/* Left handles */}
      <Handle
        type="source"
        position={Position.Left}
        id="left"
        isConnectable={isConnectable}
        style={{ 
          ...handleStyle,
          left: -4,
        }}
      />
      
      <Handle
        type="target"
        position={Position.Left}
        id="left-target"
        isConnectable={isConnectable}
        style={{ 
          ...handleStyle,
          left: -4,
        }}
      />
      
      {/* Circle node */}
      <Box
        sx={{
          width: 140,
          height: 80,
          display: 'flex',
          justifyContent: 'center',
          alignItems: 'center',
          borderRadius: '8px',
          border: selected ? '2px solid #1976d2' : '1px solid #e0e0e0',
          bgcolor: nodeColor,
          color: 'white',
          boxShadow: selected ? 3 : 1,
          position: 'relative',
          transition: 'all 0.2s ease',
          overflow: 'visible',
          fontWeight: 'bold',
          cursor: 'pointer',
          '&:hover': {
            boxShadow: 4,
          }
        }}
        onMouseEnter={() => setIsHovered(true)}
        onMouseLeave={() => setIsHovered(false)}
      >
        <Typography 
          variant="subtitle1" 
          textAlign="center" 
          fontWeight="bold"
          sx={{ 
            color: 'white', 
            textShadow: '1px 1px 2px rgba(0,0,0,0.4)',
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
    </>
  );
};

export default CrewNode;