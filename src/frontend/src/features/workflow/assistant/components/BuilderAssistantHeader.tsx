import React, { useState } from 'react';
import { Box, IconButton, Menu, MenuItem, Tooltip, Typography } from '@mui/material';
import { ChevronDown, Ellipsis, PanelLeftClose, PanelRightClose, SquarePen, ArrowLeftRight } from 'lucide-react';

interface Props {
  sessionName: string;
  side: 'left' | 'right';
  dark: boolean;
  onNewChat: () => void;
  onHistory: () => void;
  onMove: () => void;
  onCollapse?: () => void;
}

/** The assistant's navigation stays separate from its conversation actions. */
export function BuilderAssistantHeader({ sessionName, side, dark, onNewChat, onHistory, onMove, onCollapse }: Props) {
  const [menuAnchor, setMenuAnchor] = useState<HTMLElement | null>(null);
  const ink = dark ? '#E8ECEF' : '#1B1F23';
  const muted = dark ? '#A0AAB4' : '#66717D';
  const hover = dark ? 'rgba(255,255,255,0.07)' : 'rgba(27,31,35,0.045)';
  const buttonSx = { width: 32, height: 32, borderRadius: '10px', color: muted, '&:hover': { color: ink, backgroundColor: hover } };
  const moveLabel = side === 'right' ? 'Move Chat to Left' : 'Move Chat to Right';
  return (
    <Box component="header" sx={{ display: 'flex', alignItems: 'center', gap: 0.5, px: 2, py: 1.5, flexShrink: 0, minHeight: 64 }}>
      <Box
        component="button"
        type="button"
        aria-label="Chat History"
        title={sessionName === 'New Chat' ? 'Conversations' : sessionName}
        onClick={onHistory}
        sx={{ display: 'flex', alignItems: 'center', gap: 1, mr: 'auto', minWidth: 0, maxWidth: '60%', border: 0, borderRadius: '10px', backgroundColor: 'transparent', color: ink, p: 0.75, cursor: 'pointer', fontFamily: 'inherit', '&:hover': { backgroundColor: hover }, '&:focus-visible': { outline: `2px solid ${muted}`, outlineOffset: 2 } }}
      >
        <Box component="img" src={`${import.meta.env.BASE_URL}kasal-icon-24.png`} alt="" sx={{ width: 22, height: 22, flexShrink: 0 }} />
        <Typography component="span" sx={{ fontSize: '0.9375rem', fontWeight: 600, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
          {sessionName === 'New Chat' ? 'Kasal' : sessionName}
        </Typography>
        <ChevronDown size={14} strokeWidth={1.6} style={{ flexShrink: 0, color: muted }} />
      </Box>
      <Tooltip title="New Chat">
        <IconButton aria-label="New Chat" onClick={onNewChat} sx={buttonSx}><SquarePen size={17} strokeWidth={1.6} /></IconButton>
      </Tooltip>
      <Tooltip title="Assistant options">
        <IconButton aria-label="Assistant options" aria-haspopup="menu" aria-expanded={Boolean(menuAnchor)} onClick={e => setMenuAnchor(e.currentTarget)} sx={buttonSx}><Ellipsis size={18} strokeWidth={1.6} /></IconButton>
      </Tooltip>
      <Tooltip title="Collapse Chat">
        <IconButton aria-label="Collapse Chat" onClick={onCollapse} sx={buttonSx}>
          {side === 'right' ? <PanelRightClose size={17} strokeWidth={1.6} /> : <PanelLeftClose size={17} strokeWidth={1.6} />}
        </IconButton>
      </Tooltip>
      <Menu anchorEl={menuAnchor} open={Boolean(menuAnchor)} onClose={() => setMenuAnchor(null)} slotProps={{ paper: { sx: { borderRadius: '12px', boxShadow: '0 8px 28px rgba(16,24,40,0.14)', bgcolor: dark ? '#232930' : '#FFFFFF', color: ink } } }}>
        <MenuItem aria-label={moveLabel} onClick={() => { setMenuAnchor(null); onMove(); }} sx={{ fontSize: '0.8125rem', gap: 1.25 }}><ArrowLeftRight size={16} strokeWidth={1.6} />{moveLabel}</MenuItem>
      </Menu>
    </Box>
  );
}
