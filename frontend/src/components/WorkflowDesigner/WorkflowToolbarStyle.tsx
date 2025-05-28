import React from 'react';
import { Box, IconButton, Tooltip, styled } from '@mui/material';
import ChevronLeftIcon from '@mui/icons-material/ChevronLeft';
import ChevronRightIcon from '@mui/icons-material/ChevronRight';
import ExpandLessIcon from '@mui/icons-material/ExpandLess';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';

// Styled component for the panel toggle buttons
const ToggleIconButton = styled(IconButton)(({ theme }) => ({
  backgroundColor: theme.palette.mode === 'dark' ? 'rgba(255,255,255,0.08)' : 'rgba(0,0,0,0.04)',
  borderRadius: '4px',
  padding: '4px',
  '&:hover': {
    backgroundColor: theme.palette.mode === 'dark' ? 'rgba(255,255,255,0.15)' : 'rgba(0,0,0,0.08)',
  },
}));

// Container for the toggle buttons
const ToggleButtonContainer = styled(Box)(({ theme }) => ({
  position: 'absolute',
  zIndex: 10,
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
  backgroundColor: theme.palette.mode === 'dark' ? 'rgba(0,0,0,0.6)' : 'rgba(255,255,255,0.8)',
  borderRadius: '4px',
  padding: '2px',
  boxShadow: '0 1px 3px rgba(0,0,0,0.2)',
}));

interface PanelToggleProps {
  isVisible: boolean;
  togglePanel: () => void;
  position: 'bottom' | 'right';
  tooltip: string;
}

// Component for toggling bottom panel (run history)
export const BottomPanelToggle: React.FC<PanelToggleProps> = ({ 
  isVisible, 
  togglePanel,
  tooltip
}) => {
  return (
    <ToggleButtonContainer
      sx={{
        position: 'fixed',
        bottom: isVisible ? '200px' : '0px',
        left: '50%',
        transform: 'translateX(-50%)',
        transition: 'bottom 0.3s ease',
        borderTopLeftRadius: '6px',
        borderTopRightRadius: '6px',
        borderBottomLeftRadius: isVisible ? '6px' : 0,
        borderBottomRightRadius: isVisible ? '6px' : 0,
        zIndex: 1000,
      }}
    >
      <Tooltip title={tooltip}>
        <ToggleIconButton onClick={togglePanel} size="small">
          {isVisible ? <ExpandMoreIcon fontSize="small" /> : <ExpandLessIcon fontSize="small" />}
        </ToggleIconButton>
      </Tooltip>
    </ToggleButtonContainer>
  );
};

// Component for toggling right panel (flows)
export const RightPanelToggle: React.FC<PanelToggleProps> = ({ 
  isVisible, 
  togglePanel,
  tooltip
}) => {
  return (
    <ToggleButtonContainer
      sx={{
        top: '50%',
        right: isVisible ? 'auto' : 0,
        left: isVisible ? 'calc(100% - 24px)' : 'auto',
        transform: 'translateY(-50%)',
        transition: 'left 0.3s ease, right 0.3s ease',
      }}
    >
      <Tooltip title={tooltip}>
        <ToggleIconButton onClick={togglePanel} size="small">
          {isVisible ? <ChevronRightIcon fontSize="small" /> : <ChevronLeftIcon fontSize="small" />}
        </ToggleIconButton>
      </Tooltip>
    </ToggleButtonContainer>
  );
};

// Component for toggling chat panel (AI Assistant)
export const ChatPanelToggle: React.FC<PanelToggleProps> = ({ 
  isVisible, 
  togglePanel,
  tooltip
}) => {
  return (
    <ToggleButtonContainer
      sx={{
        position: 'fixed',
        top: '50%',
        right: isVisible ? '350px' : 0,
        transform: 'translateY(-50%)',
        transition: 'right 0.3s ease',
        zIndex: 1000,
      }}
    >
      <Tooltip title={tooltip}>
        <ToggleIconButton onClick={togglePanel} size="small">
          {isVisible ? <ChevronRightIcon fontSize="small" /> : <ChevronLeftIcon fontSize="small" />}
        </ToggleIconButton>
      </Tooltip>
    </ToggleButtonContainer>
  );
}; 