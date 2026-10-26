import React, { useId } from 'react';
import { Box, Typography } from '@mui/material';

/** Shared spacing and typography for every capability category. */
export default function CapabilitySection({ title, children }: {
  title?: string; children: React.ReactNode;
}) {
  const headingId = useId();
  return <Box component="section" aria-labelledby={title ? headingId : undefined} sx={{
    p: 1,
    '& > .MuiButton-root': {
      mt: 0.5, px: 1.25, py: 0.75, minHeight: 34,
      fontSize: 12, fontWeight: 500, textTransform: 'none', justifyContent: 'flex-start',
    },
  }}>
    {title && <Typography id={headingId} component="h3" sx={{
      m: 0, px: 1.25, mb: 0.75, fontSize: 12, fontWeight: 600, color: 'text.secondary',
    }}>{title}</Typography>}
    {children}
  </Box>;
}
