import React from 'react';
import { IconButton, Tooltip } from '@mui/material';
import KeyboardIcon from '@mui/icons-material/Keyboard';
import { useShortcutsContext } from '../hooks/context/useShortcutsContext';

interface ShortcutsToggleProps {
  size?: 'small' | 'medium' | 'large';
}

const ShortcutsToggle: React.FC<ShortcutsToggleProps> = ({ 
  size = 'medium'
}) => {
  const { toggleShortcuts, showShortcuts } = useShortcutsContext();

  return (
    <Tooltip title={showShortcuts ? "Hide Keyboard Shortcuts" : "Show Keyboard Shortcuts"}>
      <IconButton
        onClick={toggleShortcuts}
        color="primary"
        size={size}
        sx={{
          backgroundColor: showShortcuts ? 'primary.main' : 'background.paper',
          color: showShortcuts ? 'primary.contrastText' : 'primary.main',
          boxShadow: 1,
          '&:hover': {
            backgroundColor: showShortcuts ? 'primary.dark' : 'primary.light',
            color: 'primary.contrastText',
          },
        }}
        aria-label="keyboard shortcuts"
      >
        <KeyboardIcon fontSize={size} />
      </IconButton>
    </Tooltip>
  );
};

export default ShortcutsToggle; 