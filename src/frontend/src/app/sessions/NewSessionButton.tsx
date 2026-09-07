import React, { useId, useState } from 'react';
import { Box, Button, IconButton, Menu, MenuItem, Typography } from '@mui/material';
import { MessageSquare, Network, Plus, Workflow } from 'lucide-react';
import { usePermissionStore } from '../../store/permissions';
import { useFlowConfigStore } from '../../store/flowConfig';
import type { AppMode } from '../../store/uiLayout';
import { modeLabels, newWorkspaceSession } from './sessionNavigation';

const options = [
  { mode: 'chat' as const, Icon: MessageSquare, description: 'Ask questions and create content' },
  { mode: 'crew' as const, Icon: Network, description: 'Design and run agents and tasks' },
  { mode: 'flow' as const, Icon: Workflow, description: 'Connect crews into a workflow' },
];

/** Mode is chosen when starting work, never by relabelling an existing session. */
export default function NewSessionButton({ expanded, onCreated }: { expanded: boolean; onCreated?: () => void }) {
  const id = useId();
  const allowCrew = usePermissionStore(state => state.allowAgentBuilder);
  const allowFlow = usePermissionStore(state => state.allowFlowBuilder);
  const flowEnabled = useFlowConfigStore(state => state.kasalFlowEnabled);
  const [anchor, setAnchor] = useState<HTMLElement | null>(null);
  const available = options.filter(option => option.mode === 'chat'
    || (option.mode === 'crew' && allowCrew)
    || (option.mode === 'flow' && allowFlow && flowEnabled));
  const create = (mode: AppMode) => {
    newWorkspaceSession(mode);
    setAnchor(null);
    onCreated?.();
  };
  const open = (event: React.MouseEvent<HTMLElement>) => {
    if (available.length === 1) create('chat');
    else setAnchor(event.currentTarget);
  };
  const menuProps = {
    'aria-label': 'New session',
    'aria-haspopup': available.length > 1 ? 'menu' as const : undefined,
    'aria-controls': anchor ? `${id}-menu` : undefined,
    'aria-expanded': anchor ? true : undefined,
    'data-tour': 'new-session',
    onClick: open,
  };
  return (
    <>
      {expanded ? (
        <Button {...menuProps} disableRipple startIcon={<Plus size={17} />} color="inherit"
          sx={{ justifyContent: 'flex-start', flex: 1, borderRadius: 2, px: 1, py: 1, textTransform: 'none', fontSize: 13, fontWeight: 500, color: 'text.primary' }}>
          New session
        </Button>
      ) : (
        <IconButton {...menuProps} sx={{ width: 40, height: 40, borderRadius: 2 }}><Plus size={19} /></IconButton>
      )}
      <Menu id={`${id}-menu`} anchorEl={anchor} open={Boolean(anchor)} onClose={() => setAnchor(null)}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'left' }}
        transformOrigin={{ vertical: 'top', horizontal: 'left' }}
        slotProps={{ paper: { elevation: 0, sx: {
          mt: 0.5, p: 0.5, width: 288, maxWidth: 'calc(100vw - 24px)', borderRadius: 3,
          boxShadow: '0 8px 28px rgba(16,24,40,.14)',
        } } }} MenuListProps={{ 'aria-label': 'Choose session type', sx: { py: 0.25 } }}>
        {available.map(({ mode, Icon, description }) => (
          <MenuItem key={mode} onClick={() => create(mode)} sx={{ minHeight: 66, px: 1.5, py: 1.25, gap: 1.5, borderRadius: 2 }}>
            <Icon size={19} strokeWidth={1.6} style={{ flexShrink: 0 }} />
            <Box>
              <Typography sx={{ fontSize: 13, fontWeight: 500, lineHeight: 1.5 }}>{modeLabels[mode]}</Typography>
              <Typography sx={{ fontSize: 11.5, color: 'text.secondary', lineHeight: 1.6 }}>{description}</Typography>
            </Box>
          </MenuItem>
        ))}
      </Menu>
    </>
  );
}
