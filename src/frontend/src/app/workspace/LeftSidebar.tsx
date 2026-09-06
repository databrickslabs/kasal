import React, { useEffect, useCallback } from 'react';
import { Box, Paper, Tooltip, IconButton } from '@mui/material';
import {
  CleaningServices as ClearIcon,
  ZoomIn as ZoomInIcon,
  ZoomOut as ZoomOutIcon,
  CenterFocusStrong as FitViewIcon,
  SwapHoriz as SwapHorizIcon,
} from '@mui/icons-material';
import SidebarAccountActions from '../../components/SidebarAccountActions';
import { useUILayoutStore } from '../../store/uiLayout';

interface LeftSidebarProps {
  onClearCanvas: () => void;
  onZoomIn: () => void;
  onZoomOut: () => void;
  onFitView: () => void;
  onToggleInteractivity: () => void;
  setIsConfigurationDialogOpen?: (open: boolean) => void;
  onOpenLogsDialog?: () => void;
  onToggleExecutionHistory?: () => void;
  showRunHistory?: boolean;
  executionHistoryHeight?: number;
}

const LeftSidebar: React.FC<LeftSidebarProps> = ({
  onClearCanvas, onZoomIn, onZoomOut, onFitView, setIsConfigurationDialogOpen,
}) => {
  const { layoutOrientation, setLayoutOrientation, setLeftSidebarExpanded } = useUILayoutStore();
  useEffect(() => { setLeftSidebarExpanded(false); }, [setLeftSidebarExpanded]);

  const toggleLayoutOrientation = useCallback(() => {
    setLayoutOrientation(layoutOrientation === 'horizontal' ? 'vertical' : 'horizontal');
    setTimeout(() => {
      window.dispatchEvent(new CustomEvent('recalculateNodePositions', { detail: { reason: 'layout-orientation-toggle' } }));
    }, 50);
  }, [layoutOrientation, setLayoutOrientation]);

  return (
    <Box data-tour="left-sidebar" sx={{ position: 'absolute', top: 48, bottom: 0, left: 0, zIndex: 5, display: 'flex' }}>
      <Paper elevation={0} sx={{ width: 48, height: '100%', background: 'transparent', borderRadius: 0, border: 0, boxShadow: 'none', display: 'flex', flexDirection: 'column', alignItems: 'center', pt: 1 }}>
        <Tooltip title="Clear Canvas" placement="right">
          <IconButton
            onClick={onClearCanvas}
            sx={{
              width: 40,
              height: 40,
              mb: 1,
              color: 'text.secondary',
              borderRadius: 0,
              transition: 'all 0.2s ease-in-out',
              '&:hover': {
                backgroundColor: 'action.hover',
                color: 'text.primary'
              }
            }}
          >
            <ClearIcon fontSize="small" />
          </IconButton>
        </Tooltip>
        <Tooltip title="Fit View" placement="right">
          <IconButton
            onClick={onFitView}
            sx={{
              width: 40,
              height: 40,
              mb: 1,
              color: 'text.secondary',
              borderRadius: 0,
              transition: 'all 0.2s ease-in-out',
              '&:hover': {
                backgroundColor: 'action.hover',
                color: 'text.primary'
              }
            }}
          >
            <FitViewIcon fontSize="small" />
          </IconButton>
        </Tooltip>
        <Tooltip
          title={`Current: ${layoutOrientation === 'horizontal' ? 'Horizontal' : 'Vertical'} Layout (Click to toggle)`}
          placement="right"
        >
          <IconButton
            onClick={toggleLayoutOrientation}
            sx={{
              width: 40,
              height: 40,
              mb: 1,
              color: 'text.secondary',
              borderRadius: 0,
              transition: 'all 0.2s ease-in-out',
              '&:hover': {
                backgroundColor: 'action.hover',
                color: 'text.primary'
              }
            }}
          >
            <SwapHorizIcon fontSize="small" />
          </IconButton>
        </Tooltip>
        <Tooltip title="Zoom In" placement="right">
          <IconButton
            onClick={onZoomIn}
            sx={{
              width: 40,
              height: 40,
              mb: 1,
              color: 'text.secondary',
              borderRadius: 0,
              transition: 'all 0.2s ease-in-out',
              '&:hover': {
                backgroundColor: 'action.hover',
                color: 'text.primary'
              }
            }}
          >
            <ZoomInIcon fontSize="small" />
          </IconButton>
        </Tooltip>
        <Tooltip title="Zoom Out" placement="right">
          <IconButton
            onClick={onZoomOut}
            sx={{
              width: 40,
              height: 40,
              mb: 1,
              color: 'text.secondary',
              borderRadius: 0,
              transition: 'all 0.2s ease-in-out',
              '&:hover': {
                backgroundColor: 'action.hover',
                color: 'text.primary'
              }
            }}
          >
            <ZoomOutIcon fontSize="small" />
          </IconButton>
        </Tooltip>
        <SidebarAccountActions onOpenSettings={setIsConfigurationDialogOpen ? () => setIsConfigurationDialogOpen(true) : undefined} />
      </Paper>
    </Box>
  );
};

export default LeftSidebar;
