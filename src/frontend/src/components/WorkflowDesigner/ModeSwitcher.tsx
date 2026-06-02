import React, { useState } from 'react';
import {
  Box,
  IconButton,
  Tooltip,
  Menu,
  MenuItem,
  ListItemIcon,
  ListItemText,
  Typography,
} from '@mui/material';
import {
  GridViewRounded as GridIcon,
  Groups as CrewIcon,
  AccountTree as FlowModeIcon,
  ChatBubbleOutline as ChatIcon,
  Check as CheckIcon,
} from '@mui/icons-material';
import { useUILayoutStore, AppMode } from '../../store/uiLayout';
import { useFlowConfigStore } from '../../store/flowConfig';

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
const ModeSwitcher: React.FC = () => {
  const appMode = useUILayoutStore((s) => s.appMode);
  const setAppMode = useUILayoutStore((s) => s.setAppMode);
  const { crewAIFlowEnabled } = useFlowConfigStore();

  const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null);
  const open = Boolean(anchorEl);

  const allOptions: ModeOption[] = [
    {
      mode: 'crew',
      label: 'Crew',
      description: 'Design and run agent crews',
      icon: <CrewIcon fontSize="small" />,
    },
    {
      mode: 'flow',
      label: 'Flow',
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
    (opt) => opt.mode !== 'flow' || crewAIFlowEnabled,
  );

  const activeOption = options.find((o) => o.mode === appMode) || options[0];

  const handleOpen = (event: React.MouseEvent<HTMLElement>) => {
    setAnchorEl(event.currentTarget);
  };

  const handleClose = () => {
    setAnchorEl(null);
  };

  const handleSelect = (mode: AppMode) => {
    setAppMode(mode);
    handleClose();
  };

  return (
    <>
      <Tooltip title={`Workspace mode: ${activeOption.label}`} enterDelay={400} placement="bottom">
        <IconButton
          id="mode-switcher-button"
          aria-controls={open ? 'mode-switcher-menu' : undefined}
          aria-haspopup="true"
          aria-expanded={open ? 'true' : undefined}
          onClick={handleOpen}
          size="small"
          sx={{
            ml: 0.5,
            p: 0.75,
            borderRadius: 1.5,
            color: open ? 'primary.main' : 'text.secondary',
            backgroundColor: open ? 'action.selected' : 'transparent',
            transition: 'all 0.2s ease',
            '&:hover': {
              backgroundColor: 'action.hover',
              color: 'primary.main',
            },
          }}
        >
          <GridIcon fontSize="small" />
        </IconButton>
      </Tooltip>

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
            elevation: 3,
            sx: {
              minWidth: 240,
              mt: 0.5,
              filter: 'drop-shadow(0px 2px 8px rgba(0,0,0,0.08))',
            },
          },
        }}
        MenuListProps={{ 'aria-labelledby': 'mode-switcher-button', sx: { py: 0.5 } }}
      >
        <Box sx={{ px: 2, py: 1, borderBottom: 1, borderColor: 'divider' }}>
          <Typography variant="subtitle2" color="text.secondary">
            Switch Mode
          </Typography>
        </Box>
        {options.map((option) => {
          const isSelected = option.mode === appMode;
          return (
            <MenuItem
              key={option.mode}
              onClick={() => handleSelect(option.mode)}
              selected={isSelected}
              sx={{
                minHeight: 52,
                px: 2,
                py: 1,
                '&.Mui-selected': {
                  backgroundColor: 'action.selected',
                  '&:hover': { backgroundColor: 'action.selected' },
                },
              }}
            >
              <ListItemIcon
                sx={{ minWidth: 36, color: isSelected ? 'primary.main' : 'text.secondary' }}
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
              {isSelected && <CheckIcon fontSize="small" sx={{ color: 'primary.main', ml: 1 }} />}
            </MenuItem>
          );
        })}
      </Menu>
    </>
  );
};

export default ModeSwitcher;
