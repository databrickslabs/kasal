import React from 'react';
import { IconButton } from '@mui/material';
import { useAppStore } from './store/appStore';

/**
 * The ONE chat-sidebar toggle, rendered top-left in the app bar (TabBar
 * leftSlot) while chat mode is active. It lives OUTSIDE #kasal-chat-root, so
 * it is MUI-styled to blend with the (transparent) bar; the panel glyph
 * matches the chat's own iconography. Serves both directions — collapse when
 * the sidebar is open, expand when it shows the slim rail.
 */
const SidebarToggle: React.FC = () => {
  const sidebarOpen = useAppStore((s) => s.sidebarOpen);
  const toggleSidebar = useAppStore((s) => s.toggleSidebar);
  return (
    <IconButton
      onClick={toggleSidebar}
      size="small"
      aria-label={sidebarOpen ? 'Hide chat history' : 'Show chat history'}
      sx={{
        ml: 0.5,
        borderRadius: 2,
        color: 'text.secondary',
        '&:hover': { backgroundColor: 'action.hover', color: 'text.primary' },
      }}
    >
      <svg width="18" height="18" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.7}>
        <rect x="3" y="4.5" width="18" height="15" rx="2.5" />
        <path strokeLinecap="round" d="M9.5 4.5v15" />
      </svg>
    </IconButton>
  );
};

export default SidebarToggle;
