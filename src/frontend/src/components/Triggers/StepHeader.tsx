import React from 'react';
import { Box, Stack, Typography } from '@mui/material';

interface StepHeaderProps {
  /** The step number shown in the badge. */
  n: number;
  title: string;
  subtitle?: string;
}

/**
 * A numbered step header (badge + title + one-line subtitle) used to turn the
 * Event Triggers panel into a guided 1 → 2 → 3 flow.
 */
const StepHeader: React.FC<StepHeaderProps> = ({ n, title, subtitle }) => (
  <Stack direction="row" spacing={1.5} alignItems="center" sx={{ mb: 1 }}>
    <Box
      sx={{
        width: 26,
        height: 26,
        flexShrink: 0,
        borderRadius: '50%',
        bgcolor: 'primary.main',
        color: 'primary.contrastText',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        fontSize: 14,
        fontWeight: 700,
      }}
    >
      {n}
    </Box>
    <Box>
      <Typography variant="subtitle2" lineHeight={1.2}>
        {title}
      </Typography>
      {subtitle && (
        <Typography variant="caption" color="text.secondary">
          {subtitle}
        </Typography>
      )}
    </Box>
  </Stack>
);

export default StepHeader;
