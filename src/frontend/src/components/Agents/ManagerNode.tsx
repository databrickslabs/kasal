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
    width: 160,
    minHeight: 140,
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'center',
    justifyContent: 'space-between',
    gap: 0.1,
    position: 'relative',
    background: (theme: Theme) => isSelected
      ? `${theme.palette.primary.light}20`
      : theme.palette.background.paper,
    borderRadius: '12px',
    boxShadow: (theme: Theme) => isSelected
      ? `0 0 0 2px ${theme.palette.primary.main}`
      : `0 2px 4px ${theme.palette.mode === 'light'
        ? 'rgba(0, 0, 0, 0.1)'
        : 'rgba(0, 0, 0, 0.4)'}`,
    border: (theme: Theme) => `1px solid ${isSelected
      ? theme.palette.primary.main
      : theme.palette.primary.light}`,
    transition: 'all 0.3s ease',
    padding: '16px 8px',
    '&:hover': {
      boxShadow: (theme: Theme) => `0 4px 12px ${theme.palette.mode === 'light'
        ? 'rgba(0, 0, 0, 0.2)'
        : 'rgba(0, 0, 0, 0.6)'}`,
      transform: 'translateY(-1px)',
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
          background: '#2196f3',
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
          background: '#2196f3',
          width: '7px',
          height: '7px',
          opacity: layoutOrientation === 'horizontal' ? 1 : 0,
          pointerEvents: layoutOrientation === 'horizontal' ? 'all' : 'none'
        }}
      />

      {/* Manager Icon */}
      <Box sx={{
        backgroundColor: (theme: Theme) => `${theme.palette.primary.main}20`,
        borderRadius: '50%',
        padding: '8px',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        border: (theme: Theme) => `2px solid ${theme.palette.primary.main}`,
      }}>
        <SupervisorAccountIcon sx={{
          color: (theme: Theme) => theme.palette.primary.main,
          fontSize: '1.5rem'
        }} />
      </Box>

      {/* Manager Label */}
      <Typography
        variant="body2"
        sx={{
          fontWeight: 500,
          textAlign: 'center',
          color: (theme: Theme) => theme.palette.primary.main,
          maxWidth: '140px',
          overflow: 'hidden',
          textOverflow: 'ellipsis',
          whiteSpace: 'nowrap',
        }}
      >
        Manager Agent
      </Typography>

      {/* LLM Display */}
      {data.llm && (
        <Box sx={{
          background: (theme: Theme) => `linear-gradient(135deg, ${theme.palette.primary.main}15, ${theme.palette.primary.main}30)`,
          borderRadius: '4px',
          padding: '2px 6px',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          mt: 0.25,
          mb: 0.25,
          border: (theme: Theme) => `1px solid ${theme.palette.primary.main}20`,
          boxShadow: (theme: Theme) => `0 1px 2px ${theme.palette.primary.main}10`,
          transition: 'all 0.2s ease',
          maxWidth: '120px',
          '&:hover': {
            background: (theme: Theme) => `linear-gradient(135deg, ${theme.palette.primary.main}25, ${theme.palette.primary.main}40)`,
            boxShadow: (theme: Theme) => `0 2px 4px ${theme.palette.primary.main}15`,
          }
        }}>
          <Typography variant="caption" sx={{
            color: (theme: Theme) => theme.palette.primary.main,
            fontSize: '0.65rem',
            fontWeight: 500,
            textAlign: 'center',
            maxWidth: '100px',
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
          backgroundColor: (theme: Theme) => theme.palette.primary.main,
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
          backgroundColor: (theme: Theme) => theme.palette.success.main,
        }} />
      )}
    </Box>
  );
};

export default ManagerNode;

