import React from 'react';
import { IconButton } from '@mui/material';
import { useAppStore } from './store/appStore';

/** Shared collapse/expand control at the top of the sessions sidebar. */
const SidebarToggle: React.FC = () => {
  const sidebarOpen = useAppStore((s) => s.sidebarOpen);
  const toggleSidebar = useAppStore((s) => s.toggleSidebar);
  return (
    <IconButton
      onClick={toggleSidebar}
      size="small"
      aria-label={sidebarOpen ? 'Hide sessions' : 'Show sessions'}
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
