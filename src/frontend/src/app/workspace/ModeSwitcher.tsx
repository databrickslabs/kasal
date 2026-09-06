import React, { useState, useId } from 'react';
import {
  Box,
  Button,
  Menu,
  MenuItem,
  ListItemIcon,
  ListItemText,
  Typography,
  useTheme,
} from '@mui/material';
import { ChevronDown, Bot, Workflow, MessageCircle, Check } from 'lucide-react';
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

/** Shared workspace selector beside the composer controls. */
const ModeSwitcher: React.FC<{ placement?: 'up' | 'down' }> = ({ placement = 'up' }) => {
  const id = useId();
  const dark = useTheme().palette.mode === 'dark';
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
      icon: <Bot size={17} strokeWidth={1.7} />,
    },
    {
      mode: 'flow',
      label: 'Flow Builder',
      description: 'Build multi-crew workflows',
      icon: <Workflow size={17} strokeWidth={1.7} />,
    },
    {
      mode: 'chat',
      label: 'Chat',
      description: 'Converse with Kasal',
      icon: <MessageCircle size={17} strokeWidth={1.7} />,
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
  const accent = 'text.primary';
  const accentSoft = 'action.selected';
  const accentSoftHover = 'action.hover';

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
      <Button
        id={`${id}-button`}
        aria-controls={open ? `${id}-menu` : undefined}
        aria-haspopup="menu"
        aria-expanded={open ? 'true' : undefined}
        onClick={handleOpen}
        size="small"
        color="inherit"
        aria-label={`Workspace mode: ${activeOption.label}`}
        startIcon={activeOption.icon}
        endIcon={<ChevronDown size={14} strokeWidth={1.7} style={{ transform: open ? 'rotate(180deg)' : undefined, transition: 'transform 150ms' }} />}
        disableRipple
        style={{ padding: '0 10px', height: 32, fontSize: 12, lineHeight: 1, borderRadius: 12, color: dark ? '#C4CDD5' : '#586570', backgroundColor: dark ? '#232930' : '#F5F7FA', border: 'none' }}
        sx={{
          minWidth: 0, flexShrink: 0, px: 1, py: 0.5, borderRadius: 2,
          fontSize: 12, fontWeight: 500, lineHeight: 1.6, textTransform: 'none',
          color: 'text.secondary', bgcolor: open ? 'action.selected' : 'transparent',
          '& .MuiButton-startIcon': { mr: 0.75, '& > *': { fontSize: 16 } },
          '& .MuiButton-endIcon': { ml: 0.5 },
          '&:hover': { bgcolor: 'action.hover', color: 'text.primary' },
        }}
      >
        {activeOption.label}
      </Button>

      <Menu
        id={`${id}-menu`}
        anchorEl={anchorEl}
        open={open}
        onClose={handleClose}
        disableScrollLock
        anchorOrigin={{ vertical: placement === 'up' ? 'top' : 'bottom', horizontal: 'left' }}
        transformOrigin={{ vertical: placement === 'up' ? 'bottom' : 'top', horizontal: 'left' }}
        slotProps={{
          paper: {
            elevation: 0,
            sx: {
              width: 284, maxWidth: 'calc(100vw - 24px)',
              ...(placement === 'up' ? { mt: -1 } : { mt: 1 }),
              borderRadius: '16px',
              border: 'none',
              boxShadow: dark ? '0 8px 32px rgba(0,0,0,.3)' : '0 8px 32px rgba(16,24,40,.12), 0 2px 6px rgba(16,24,40,.04)',
            },
          },
        }}
        MenuListProps={{ 'aria-labelledby': `${id}-button`, sx: { py: 0.75 } }}
      >
        <Box sx={{ px: 2, pt: 1, pb: 0.5 }}>
          <Typography
            sx={{ fontSize: 11, fontWeight: 500 }}
            color="text.secondary"
          >
            Switch mode
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
                minHeight: 58,
                mx: 0,
                px: 1.25,
                py: 1,
                borderRadius: '11px',
                gap: 1.25,
                '&.Mui-selected': {
                  backgroundColor: accentSoft,
                  '&:hover': { backgroundColor: accentSoftHover },
                },
              }}
            >
              <ListItemIcon
                sx={{ minWidth: '32px !important', width: 32, height: 32, borderRadius: '10px', display: 'flex', alignItems: 'center', justifyContent: 'center', bgcolor: 'action.hover', color: isSelected ? accent : 'text.secondary' }}
              >
                {option.icon}
              </ListItemIcon>
              <ListItemText
                primary={
                  <Typography variant="body2" sx={{ fontSize: 13, fontWeight: isSelected ? 600 : 500 }}>
                    {option.label}
                  </Typography>
                }
                secondary={
                  <Typography variant="caption" color="text.secondary" sx={{ fontSize: 11, lineHeight: 1.5 }}>
                    {option.description}
                  </Typography>
                }
                sx={{ my: 0 }}
              />
              {isSelected && <Check size={15} strokeWidth={2} style={{ flexShrink: 0 }} />}
            </MenuItem>
          );
        })}
      </Menu>
    </>
  );
};

export default ModeSwitcher;
