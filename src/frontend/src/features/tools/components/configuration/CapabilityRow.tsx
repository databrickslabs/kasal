import React from 'react';
import { Box, ButtonBase, Typography } from '@mui/material';
import { Check, Plus } from 'lucide-react';

/** One row style for built-in tools, MCP servers and remote agents. */
export default function CapabilityRow({ name, description, selected, status, onClick, disabled, selectable }: {
  name: string; description?: string; selected?: boolean; status?: string;
  onClick?: () => void; disabled?: boolean; selectable?: boolean;
}) {
  return <ButtonBase component={onClick ? 'button' : 'div'} onClick={onClick} disabled={disabled}
    role={selectable ? 'checkbox' : undefined} aria-checked={selectable ? !!selected : undefined}
    sx={{ width: '100%', display: 'flex', gap: 1.25, textAlign: 'left', justifyContent: 'flex-start', px: 1.25, py: 1,
      borderRadius: 1.5, color: 'text.primary', bgcolor: selected ? 'action.selected' : 'transparent',
      '&:hover': { bgcolor: 'action.hover' }, '&.Mui-disabled': { opacity: 0.5 }, mb: 0.25 }}>
    <Box sx={{ border: '1px solid', borderColor: selected ? 'text.secondary' : 'divider', width: 20, height: 20,
      borderRadius: 1, display: 'grid', placeItems: 'center', flexShrink: 0 }}>
      {selected ? <Check size={13} /> : status === 'Add' || status === 'Connect' ? <Plus size={13} /> : null}
    </Box>
    <Box sx={{ minWidth: 0, flex: 1 }}>
      <Typography sx={{ fontSize: 13, fontWeight: 500, overflowWrap: 'anywhere' }}>{name}</Typography>
      {description && <Typography title={description} noWrap sx={{ fontSize: 11, color: 'text.secondary' }}>{description}</Typography>}
    </Box>
    {status && <Typography sx={{ fontSize: 11, color: 'text.secondary', flexShrink: 0 }}>{status}</Typography>}
  </ButtonBase>;
}
