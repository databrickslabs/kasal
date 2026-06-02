import React from 'react';
import { Box, IconButton, Tooltip } from '@mui/material';
import MenuIcon from '@mui/icons-material/Menu';
import { useAppStore } from './store/appStore';

/**
 * Rendered inside the app TabBar (the top bar) when Chat mode is active, so the
 * sidebar toggle (hamburger) sits on the SAME row as the grid mode-switcher and
 * the workspace selector — instead of the chat rendering its own second header
 * bar below the app bar.
 *
 * This lives outside #kasal-chat-root, so it is styled with MUI to match the
 * surrounding app bar rather than the chat's Tailwind theme.
 */
const ChatModeHeaderSlot: React.FC = () => {
  const sidebarOpen = useAppStore((s) => s.sidebarOpen);
  const toggleSidebar = useAppStore((s) => s.toggleSidebar);

  return (
    <Box sx={{ display: 'flex', alignItems: 'center', pl: 0.5, pr: 1 }}>
      <Tooltip title={sidebarOpen ? 'Hide chat history' : 'Show chat history'} placement="bottom">
        <IconButton
          onClick={toggleSidebar}
          size="small"
          sx={{
            color: 'text.secondary',
            borderRadius: 1.5,
            '&:hover': { backgroundColor: 'action.hover', color: 'text.primary' },
          }}
        >
          <MenuIcon fontSize="small" />
        </IconButton>
      </Tooltip>
    </Box>
  );
};

export default ChatModeHeaderSlot;
