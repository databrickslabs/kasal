import React, { useState } from 'react';
import { ControlButton } from 'reactflow';
import AccountTreeIcon from '@mui/icons-material/AccountTree';
import { useTheme } from '@mui/material/styles';
import CanvasControls, { CanvasControlsProps } from './CanvasControls';
import { CircularProgress } from '@mui/material';

interface CrewCanvasControlsProps extends CanvasControlsProps {
  onGenerateConnections: () => Promise<void>;
  isGeneratingConnections?: boolean;
}

const CrewCanvasControls: React.FC<CrewCanvasControlsProps> = ({
  onGenerateConnections,
  isGeneratingConnections = false,
  ...canvasControlsProps
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
    cursor: isGeneratingConnections ? 'wait' : 'pointer',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    transition: 'all 0.2s ease-in-out'
  };

  const dynamicButtonStyle = (buttonId: string) => ({
    ...buttonStyle,
    backgroundColor: hoveredButton === buttonId && !isGeneratingConnections 
      ? theme.palette.action.hover 
      : theme.palette.background.paper,
    border: `1px solid ${theme.palette.divider}`,
    color: theme.palette.text.primary,
  });

  return (
    <CanvasControls {...canvasControlsProps}>
      <ControlButton
        onClick={isGeneratingConnections ? undefined : onGenerateConnections}
        title={isGeneratingConnections ? "Generating Connections..." : "Generate Connections"}
        style={dynamicButtonStyle('generate')}
        onMouseEnter={() => !isGeneratingConnections && setHoveredButton('generate')}
        onMouseLeave={() => setHoveredButton(null)}
      >
        {isGeneratingConnections ? (
          <CircularProgress size={16} color="primary" />
        ) : (
          <AccountTreeIcon sx={{ fontSize: 16, color: theme.palette.text.primary }} />
        )}
      </ControlButton>
    </CanvasControls>
  );
};

export default CrewCanvasControls; 