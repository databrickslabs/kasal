import React from 'react';
import { Box, Button, type ButtonProps } from '@mui/material';

/** Shared typography and spacing for navigation in every mode's left sidebar. */
export default function SidebarAction({ label, icon, expanded = true, trailing, ...props }: Omit<ButtonProps, 'children'> & {
  label: string; icon: React.ReactNode; expanded?: boolean; trailing?: React.ReactNode;
}) {
  return <Button color="inherit" aria-label={label} {...props} sx={{ '&&': {
    mx: 0.75, my: 0.5, px: 1, py: 0.75, width: 'calc(100% - 12px)', minWidth: 0, minHeight: 36,
    gap: 1.25, justifyContent: expanded ? 'flex-start' : 'center', borderRadius: 2,
    color: 'text.secondary', fontFamily: theme => theme.typography.fontFamily,
    fontSize: 12, fontWeight: 500, lineHeight: '20px', letterSpacing: 0, textTransform: 'none',
    '&:hover': { bgcolor: 'action.hover', color: 'text.primary' },
  } }}>
    <Box component="span" sx={{ display: 'flex', alignItems: 'center', flexShrink: 0 }}>{icon}</Box>
    {expanded && <><Box component="span" sx={{ flex: 1, textAlign: 'left' }}>{label}</Box>{trailing}</>}
  </Button>;
}
