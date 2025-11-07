import React, { useState } from 'react';
import { Handle, Position } from 'reactflow';
import { Box, Typography, Theme } from '@mui/material';
import SupervisorAccountIcon from '@mui/icons-material/SupervisorAccount';
import { useUILayoutStore } from '../../store/uiLayout';

interface ManagerNodeData {
  label: string;
  llm?: string;
  isActive?: boolean;
  isCompleted?: boolean;
}

const ManagerNode: React.FC<{ data: ManagerNodeData; id: string }> = ({ data, id }) => {
  const [isSelected, setIsSelected] = useState(false);
  
  // Get current layout orientation
  const layoutOrientation = useUILayoutStore(state => state.layoutOrientation);

  const handleNodeClick = (event: React.MouseEvent) => {
    event.stopPropagation();
    setIsSelected(true);
  };

  const getManagerNodeStyles = () => ({
    padding: '12px',
    borderRadius: '12px',
    border: (theme: Theme) => `2px solid ${theme.palette.warning.main}`,
    background: (theme: Theme) => 
      `linear-gradient(135deg, ${theme.palette.warning.light}15 0%, ${theme.palette.warning.main}25 100%)`,
    minWidth: '200px',
    maxWidth: '200px',
    minHeight: '150px',
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'center',
    justifyContent: 'center',
    gap: '8px',
    boxShadow: isSelected 
      ? '0 0 0 2px rgba(255, 152, 0, 0.5)' 
      : '0 2px 8px rgba(0,0,0,0.1)',
    transition: 'all 0.2s ease-in-out',
    '&:hover': {
      boxShadow: '0 4px 12px rgba(255, 152, 0, 0.3)',
      transform: 'translateY(-2px)',
    },
  });

  return (
    <Box
      sx={getManagerNodeStyles()}
      onClick={handleNodeClick}
      data-nodeid={id}
      data-nodetype="manager"
      data-selected={isSelected ? 'true' : 'false'}
    >
      {/* Top handle - visible only in vertical layout (for incoming connections from above if needed) */}
      <Handle
        type="target"
        position={Position.Top}
        id="top"
        style={{
          background: '#ff9800',
          width: '7px',
          height: '7px',
          opacity: layoutOrientation === 'vertical' ? 1 : 0,
          pointerEvents: layoutOrientation === 'vertical' ? 'all' : 'none'
        }}
      />

      {/* Left handle - visible only in horizontal layout (for incoming connections from left if needed) */}
      <Handle
        type="target"
        position={Position.Left}
        id="left"
        style={{
          background: '#ff9800',
          width: '7px',
          height: '7px',
          opacity: layoutOrientation === 'horizontal' ? 1 : 0,
          pointerEvents: layoutOrientation === 'horizontal' ? 'all' : 'none'
        }}
      />

      {/* Bottom handle - visible only in vertical layout (connects to agents below) */}
      <Handle
        type="source"
        position={Position.Bottom}
        id="bottom"
        style={{
          background: '#ff9800',
          width: '7px',
          height: '7px',
          opacity: layoutOrientation === 'vertical' ? 1 : 0,
          pointerEvents: layoutOrientation === 'vertical' ? 'all' : 'none'
        }}
      />

      {/* Right handle - visible only in horizontal layout (connects to agents on right) */}
      <Handle
        type="source"
        position={Position.Right}
        id="right"
        style={{
          background: '#ff9800',
          width: '7px',
          height: '7px',
          opacity: layoutOrientation === 'horizontal' ? 1 : 0,
          pointerEvents: layoutOrientation === 'horizontal' ? 'all' : 'none'
        }}
      />

      {/* Manager Icon */}
      <Box sx={{
        backgroundColor: (theme: Theme) => `${theme.palette.warning.main}20`,
        borderRadius: '50%',
        padding: '8px',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        border: (theme: Theme) => `2px solid ${theme.palette.warning.main}`,
      }}>
        <SupervisorAccountIcon sx={{ 
          color: (theme: Theme) => theme.palette.warning.main, 
          fontSize: '1.5rem' 
        }} />
      </Box>

      {/* Manager Badge */}
      <Box sx={{
        backgroundColor: (theme: Theme) => theme.palette.warning.main,
        color: 'white',
        padding: '2px 8px',
        borderRadius: '4px',
        fontSize: '0.65rem',
        fontWeight: 600,
        textTransform: 'uppercase',
        letterSpacing: '0.5px',
      }}>
        Manager
      </Box>

      {/* Manager Label */}
      <Typography 
        variant="body2" 
        sx={{ 
          fontWeight: 600,
          textAlign: 'center',
          color: (theme: Theme) => theme.palette.text.primary,
          fontSize: '0.875rem',
          maxWidth: '180px',
          overflow: 'hidden',
          textOverflow: 'ellipsis',
          whiteSpace: 'nowrap',
        }}
      >
        {data.label}
      </Typography>

      {/* LLM Display */}
      {data.llm && (
        <Box sx={{
          display: 'flex',
          alignItems: 'center',
          gap: 0.5,
          backgroundColor: (theme: Theme) => `${theme.palette.warning.main}15`,
          padding: '2px 6px',
          borderRadius: '4px',
          border: (theme: Theme) => `1px solid ${theme.palette.warning.main}40`,
        }}>
          <Typography variant="caption" sx={{
            color: (theme: Theme) => theme.palette.warning.dark,
            fontSize: '0.65rem',
            fontWeight: 500,
            textAlign: 'center',
            maxWidth: '150px',
            overflow: 'hidden',
            textOverflow: 'ellipsis',
            whiteSpace: 'nowrap',
          }}>
            {data.llm}
          </Typography>
        </Box>
      )}

      {/* Status Indicators */}
      {data.isActive && (
        <Box sx={{
          position: 'absolute',
          top: 8,
          right: 8,
          width: 8,
          height: 8,
          borderRadius: '50%',
          backgroundColor: '#4caf50',
          animation: 'pulse 2s infinite',
          '@keyframes pulse': {
            '0%, 100%': { opacity: 1 },
            '50%': { opacity: 0.5 },
          },
        }} />
      )}

      {data.isCompleted && (
        <Box sx={{
          position: 'absolute',
          top: 8,
          right: 8,
          width: 8,
          height: 8,
          borderRadius: '50%',
          backgroundColor: '#2196f3',
        }} />
      )}
    </Box>
  );
};

export default ManagerNode;

