import React, { useState } from 'react';
import {
  Box,
  IconButton,
  Menu,
  MenuItem,
  ListItemIcon,
  ListItemText,
  Typography,
} from '@mui/material';
import {
  GridViewRounded as GridIcon,
  SmartToy as CrewIcon,
  AccountTree as FlowModeIcon,
  ChatBubbleOutline as ChatIcon,
  Check as CheckIcon,
} from '@mui/icons-material';
import { usePermissionStore } from '../../store/permissions';
import { useUILayoutStore, AppMode } from '../../store/uiLayout';
import { useFlowConfigStore } from '../../store/flowConfig';
import { useTabManagerStore } from '../../store/tabManager';

interface ModeOption {
  mode: AppMode;
  label: string;
  description: string;
  icon: React.ReactNode;
}

/**
 * Top-level workspace mode switcher. Lives at the right-most side of the TabBar
 * (just before the workspace/group selector). A single grid-icon button opens a
 * menu to switch the whole app between the Crew, Flow, and Chat workspaces.
 */
// Kasal's brand accent (chat.css --accent), used ONLY while chat mode is
// active — in chat the MUI blue reads as a foreign product, and on the
// builder canvases the reverse is true (the canvases are MUI-themed, so the
// menus keep their original primary colors there). CSS vars can't reach
// here (outside #kasal-chat-root).
const KASAL_ACCENT = '#FF3621';
const KASAL_ACCENT_SOFT = 'rgba(255, 54, 33, 0.08)';
const KASAL_ACCENT_SOFT_HOVER = 'rgba(255, 54, 33, 0.12)';

const ModeSwitcher: React.FC = () => {
  const appMode = useUILayoutStore((s) => s.appMode);
  const setAppMode = useUILayoutStore((s) => s.setAppMode);
  const { kasalFlowEnabled } = useFlowConfigStore();

  const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null);
  const open = Boolean(anchorEl);

  const allOptions: ModeOption[] = [
    {
      mode: 'crew',
      label: 'Agent Builder',
      description: 'Design and run agent crews',
      icon: <CrewIcon fontSize="small" />,
    },
    {
      mode: 'flow',
      label: 'Flow Builder',
      description: 'Build multi-crew workflows',
      icon: <FlowModeIcon fontSize="small" />,
    },
    {
      mode: 'chat',
      label: 'Chat',
      description: 'Converse with Kasal',
      icon: <ChatIcon fontSize="small" />,
    },
  ];

  // Hide the Flow option when the CrewAI flow feature is disabled.
  const options = allOptions.filter(
    (opt) => opt.mode !== 'flow' || kasalFlowEnabled,
  );

  // Capability-gated per surface (Access screen / role default) — a user
  // without a builder doesn't see its entry, and a one-entry menu is no
  // menu, so the switcher hides itself entirely.
  const allowAgent = usePermissionStore((st) => st.allowAgentBuilder);
  const allowFlow = usePermissionStore((st) => st.allowFlowBuilder);
  const visibleOptions = options.filter(
    (o) =>
      o.mode === 'chat' ||
      (o.mode === 'crew' && allowAgent) ||
      (o.mode === 'flow' && allowFlow),
  );

  const activeOption = options.find((o) => o.mode === appMode) || options[0];
  // Chat follows the Kasal accent; the builder canvases keep MUI primary.
  const isChat = appMode === 'chat';
  const accent = isChat ? KASAL_ACCENT : 'primary.main';
  const accentSoft = isChat ? KASAL_ACCENT_SOFT : 'action.selected';
  const accentSoftHover = isChat ? KASAL_ACCENT_SOFT_HOVER : 'action.selected';

  const handleOpen = (event: React.MouseEvent<HTMLElement>) => {
    setAnchorEl(event.currentTarget);
  };

  const handleClose = () => {
    setAnchorEl(null);
  };

  const handleSelect = (mode: AppMode) => {
    // Land on a tab that holds this kind of work when there is one. Chat is not
    // a tab view mode, so it keeps the plain behaviour.
    if (mode === 'crew' || mode === 'flow') {
      useTabManagerStore.getState().activateTabForViewMode(mode);
    }
    setAppMode(mode);
    handleClose();
  };

  if (visibleOptions.length < 2) {
    return null;
  }

  return (
    <>
      <IconButton
          id="mode-switcher-button"
          aria-controls={open ? 'mode-switcher-menu' : undefined}
          aria-haspopup="true"
          aria-expanded={open ? 'true' : undefined}
          onClick={handleOpen}
          size="small"
          aria-label={`Workspace mode: ${activeOption.label}`}
          sx={{
            ml: 0.5,
            p: 0.75,
            borderRadius: 2,
            color: open ? accent : 'text.secondary',
            backgroundColor: open ? accentSoft : 'transparent',
            transition: 'all 0.2s ease',
            '&:hover': {
              backgroundColor: 'action.hover',
              color: 'text.primary',
            },
          }}
        >
          <GridIcon fontSize="small" />
        </IconButton>

      <Menu
        id="mode-switcher-menu"
        anchorEl={anchorEl}
        open={open}
        onClose={handleClose}
        disableScrollLock
        anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
        transformOrigin={{ vertical: 'top', horizontal: 'right' }}
        slotProps={{
          paper: {
            elevation: 0,
            sx: {
              minWidth: 250,
              mt: 1,
              borderRadius: 3,
              border: '1px solid',
              borderColor: 'divider',
              boxShadow: '0 12px 32px rgba(16,24,40,0.10), 0 2px 8px rgba(16,24,40,0.06)',
            },
          },
        }}
        MenuListProps={{ 'aria-labelledby': 'mode-switcher-button', sx: { py: 0.75 } }}
      >
        <Box sx={{ px: 2, pt: 1, pb: 0.5 }}>
          <Typography
            sx={{ fontSize: 11, fontWeight: 600, letterSpacing: '0.08em', textTransform: 'uppercase' }}
            color="text.secondary"
          >
            Switch Mode
          </Typography>
        </Box>
        {visibleOptions.map((option) => {
          const isSelected = option.mode === appMode;
          return (
            <MenuItem
              key={option.mode}
              onClick={() => handleSelect(option.mode)}
              selected={isSelected}
              sx={{
                minHeight: 50,
                mx: 0.75,
                px: 1.5,
                py: 1,
                borderRadius: 2,
                '&.Mui-selected': {
                  backgroundColor: accentSoft,
                  '&:hover': { backgroundColor: accentSoftHover },
                },
              }}
            >
              <ListItemIcon
                sx={{ minWidth: 36, color: isSelected ? accent : 'text.secondary' }}
              >
                {option.icon}
              </ListItemIcon>
              <ListItemText
                primary={
                  <Typography variant="body2" sx={{ fontWeight: isSelected ? 600 : 400 }}>
                    {option.label}
                  </Typography>
                }
                secondary={
                  <Typography variant="caption" color="text.secondary">
                    {option.description}
                  </Typography>
                }
                sx={{ my: 0 }}
              />
              {isSelected && <CheckIcon fontSize="small" sx={{ color: accent, ml: 1 }} />}
            </MenuItem>
          );
        })}
      </Menu>
    </>
  );
};

export default ModeSwitcher;
