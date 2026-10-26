import React, { useState } from 'react';
import { Controls, ControlButton } from 'reactflow';
import DeleteSweepIcon from '@mui/icons-material/DeleteSweep';
import ZoomInIcon from '@mui/icons-material/ZoomIn';
import ZoomOutIcon from '@mui/icons-material/ZoomOut';
import FitScreenIcon from '@mui/icons-material/FitScreen';
import PanToolIcon from '@mui/icons-material/PanTool';
import { useTheme } from '@mui/material/styles';

export interface CanvasControlsProps {
  onClearCanvas: () => void;
  onZoomIn?: () => void;
  onZoomOut?: () => void;
  onFitView?: () => void;
  onToggleInteractivity?: () => void;
  isHorizontal?: boolean;
  isLeftToRight?: boolean;
  children?: React.ReactNode; // For additional buttons
}

const CanvasControls: React.FC<CanvasControlsProps> = ({
  onClearCanvas,
  onZoomIn,
  onZoomOut,
  onFitView,
  onToggleInteractivity,
  isHorizontal = true,
  isLeftToRight = true,
  children,
}) => {
  const [hoveredButton, setHoveredButton] = useState<string | null>(null);
  const theme = useTheme();

  const buttonStyle = {
    width: '24px',
    height: '24px',
    padding: '4px',
    fontSize: '12px',
    opacity: 0.8,
    backgroundColor: theme.palette.background.paper,
    border: `1px solid ${theme.palette.divider}`,
    cursor: 'pointer',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    transition: 'all 0.2s ease-in-out'
  };

  const dynamicButtonStyle = (buttonId: string) => ({
    ...buttonStyle,
    backgroundColor: hoveredButton === buttonId ? theme.palette.action.hover : theme.palette.background.paper,
    border: `1px solid ${theme.palette.divider}`,
    color: theme.palette.text.primary,
  });

  return (
    <Controls
      style={{
        position: 'absolute',
        top: 0,
        left: 0,
        zIndex: 10,
        pointerEvents: 'all',
        padding: '4px',
        backgroundColor: theme.palette.mode === 'dark' 
          ? 'rgba(0, 0, 0, 0.8)' 
          : 'rgba(255, 255, 255, 0.9)',
        borderRadius: '4px',
        boxShadow: theme.shadows[1],
        width: 'fit-content',
        height: 'fit-content',
        display: 'flex',
        flexDirection: 'column',
        gap: '4px'
      }}
    >
      {onZoomIn && (
        <ControlButton
          onClick={onZoomIn}
          title="Zoom In"
          style={dynamicButtonStyle('zoomIn')}
          onMouseEnter={() => setHoveredButton('zoomIn')}
          onMouseLeave={() => setHoveredButton(null)}
        >
          <ZoomInIcon sx={{ fontSize: 16, color: theme.palette.text.primary }} />
        </ControlButton>
      )}
      
      {onZoomOut && (
        <ControlButton
          onClick={onZoomOut}
          title="Zoom Out"
          style={dynamicButtonStyle('zoomOut')}
          onMouseEnter={() => setHoveredButton('zoomOut')}
          onMouseLeave={() => setHoveredButton(null)}
        >
          <ZoomOutIcon sx={{ fontSize: 16, color: theme.palette.text.primary }} />
        </ControlButton>
      )}
      
      {onFitView && (
        <ControlButton
          onClick={onFitView}
          title="Fit View"
          style={dynamicButtonStyle('fitView')}
          onMouseEnter={() => setHoveredButton('fitView')}
          onMouseLeave={() => setHoveredButton(null)}
        >
          <FitScreenIcon sx={{ fontSize: 16, color: theme.palette.text.primary }} />
        </ControlButton>
      )}
      
      {onToggleInteractivity && (
        <ControlButton
          onClick={onToggleInteractivity}
          title="Toggle Interactivity"
          style={dynamicButtonStyle('interactivity')}
          onMouseEnter={() => setHoveredButton('interactivity')}
          onMouseLeave={() => setHoveredButton(null)}
        >
          <PanToolIcon sx={{ fontSize: 16, color: theme.palette.text.primary }} />
        </ControlButton>
      )}
      
      <ControlButton
        onClick={onClearCanvas}
        title="Clear Canvas"
        style={dynamicButtonStyle('clear')}
        onMouseEnter={() => setHoveredButton('clear')}
        onMouseLeave={() => setHoveredButton(null)}
      >
        <DeleteSweepIcon sx={{ fontSize: 16, color: theme.palette.text.primary }} />
      </ControlButton>
      
      {children}
    </Controls>
  );
};

export default CanvasControls; 