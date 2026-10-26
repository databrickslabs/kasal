import React from 'react';
import { Box, IconButton, useTheme } from '@mui/material';
import { Settings } from 'lucide-react';
import ThemeModeIcon from './ThemeModeIcon';
import GroupSelector from '../features/groups/components/GroupSelector';
import { useThemeStore } from '../store/theme';

/** One account row when expanded; a consistent icon column in the collapsed rail. */
const SidebarAccountActions: React.FC<{
  onOpenSettings?: () => void;
  showLabel?: boolean;
}> = ({ onOpenSettings, showLabel = false }) => {
  const isDarkMode = useThemeStore(state => state.isDarkMode);
  const toggleTheme = useThemeStore(state => state.toggleTheme);
  const theme = useTheme();
  const themeLabel = isDarkMode ? 'Switch to light mode' : 'Switch to dark mode';
  const buttonStyle = { width: 40, height: 40, padding: 0, color: theme.palette.text.secondary };
  const buttonSx = { flexShrink: 0, borderRadius: 2.5, color: 'text.secondary', '&:hover': { bgcolor: 'action.hover' } };
  const settings = onOpenSettings && <IconButton aria-label="Configuration" data-tour="configuration-button"
    onClick={onOpenSettings} style={buttonStyle} sx={buttonSx}>
    <Settings size={20} strokeWidth={1.8} />
  </IconButton>;
  const appearance = <IconButton aria-label={themeLabel} onClick={() => { void toggleTheme(); }} style={buttonStyle} sx={buttonSx}>
    <ThemeModeIcon dark={isDarkMode} size={20} />
  </IconButton>;
  const teamspace = <Box sx={{ minWidth: 0, height: 40, flex: showLabel ? 1 : undefined, width: showLabel ? undefined : 40, display: 'flex', alignItems: 'center' }}>
    <GroupSelector showLabel={showLabel} />
  </Box>;

  return <Box data-tour="workspace-account-actions" sx={{
    mt: 'auto', width: '100%', boxSizing: 'border-box', flexShrink: 0, display: 'flex',
    flexDirection: showLabel ? 'row' : 'column', alignItems: 'center',
    gap: showLabel ? 0.25 : 0.5, p: 1,
  }}>
    {showLabel ? <>{teamspace}{appearance}{settings}</> : <>{settings}{appearance}{teamspace}</>}
  </Box>;
};

export default SidebarAccountActions;
