import React from 'react';
import { Box, IconButton, Tooltip, useTheme } from '@mui/material';
import SettingsIcon from '@mui/icons-material/Settings';
import ThemeModeIcon from './ThemeModeIcon';
import GroupSelector from '../features/groups/components/GroupSelector';
import { useThemeStore } from '../store/theme';

/** Shared footer keeps account controls at the same height in every workspace. */
const SidebarAccountActions: React.FC<{ onOpenSettings?: () => void; showLabel?: boolean }> = ({ onOpenSettings, showLabel = false }) => {
  const isDarkMode = useThemeStore(state => state.isDarkMode);
  const toggleTheme = useThemeStore(state => state.toggleTheme);
  const theme = useTheme();
  const themeLabel = isDarkMode ? 'Switch to light mode' : 'Switch to dark mode';
  // Chat's button reset takes precedence over generated MUI classes.
  const buttonStyle = { width: showLabel ? '100%' : 40, height: 40, padding: showLabel ? '8px 10px' : 0, color: theme.palette.text.secondary, fontSize: 13, fontWeight: 500 };
  const buttonSx = { borderRadius: 2, color: 'text.secondary', justifyContent: showLabel ? 'flex-start' : 'center', gap: 1.25, fontSize: 13, fontWeight: 500 };

  return (
    <Box data-tour="workspace-account-actions" sx={{ mt: 'auto', width: '100%', flexShrink: 0, display: 'flex', flexDirection: 'column', alignItems: showLabel ? 'stretch' : 'center', gap: '4px', pt: '8px', pb: '12px', px: showLabel ? '8px' : 0 }}>
      {onOpenSettings && <Tooltip title="Configuration" placement="right">
        <IconButton aria-label="Configuration" data-tour="configuration-button" onClick={onOpenSettings} style={buttonStyle} sx={buttonSx}>
          <SettingsIcon sx={{ fontSize: 20 }} />
          {showLabel && <span>Configuration</span>}
        </IconButton>
      </Tooltip>}
      <Tooltip title={themeLabel} placement="right">
        <IconButton aria-label={themeLabel} onClick={() => { void toggleTheme(); }} style={buttonStyle} sx={buttonSx}>
          <ThemeModeIcon dark={isDarkMode} size={20} />
          {showLabel && <span>{isDarkMode ? 'Light mode' : 'Dark mode'}</span>}
        </IconButton>
      </Tooltip>
      <Box sx={{ height: 40, width: showLabel ? '100%' : 40, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        <GroupSelector showLabel={showLabel} />
      </Box>
    </Box>
  );
};

export default SidebarAccountActions;
