/**
 * Loading placeholder for the run list.
 *
 * Same shell as the real table (header row, sticky head, small rows) so the
 * panel does not jump when the runs land — only the cells are pulsing bars.
 */

import React from 'react';
import {
  Box,
  Card,
  CardContent,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
} from '@mui/material';
import { useTranslation } from 'react-i18next';

const PULSE = {
  backgroundColor: (theme: { palette: { action: { hover: string } } }) =>
    theme.palette.action.hover,
  borderRadius: '4px',
  animation: 'pulse 1.5s ease-in-out infinite',
  '@keyframes pulse': {
    '0%': { opacity: 1 },
    '50%': { opacity: 0.4 },
    '100%': { opacity: 1 },
  },
} as const;

const HEAD_CELL = {
  py: 0.25,
  fontSize: '0.8125rem',
  backgroundColor: (theme: { palette: { background: { paper: string } } }) =>
    theme.palette.background.paper,
} as const;

const BODY_CELL = { py: 0.25, fontSize: '0.75rem' } as const;

export const ExecutionHistorySkeleton: React.FC = () => {
  const { t } = useTranslation();

  return (
    <Card sx={{ boxShadow: 'none', height: '100%' }}>
      <CardContent
        sx={{
          p: 0,
          height: '100%',
          '&:last-child': { pb: 0 },
          display: 'flex',
          flexDirection: 'column',
        }}
      >
        <TableContainer sx={{ flex: '1 1 auto', overflow: 'auto' }}>
          <Table size="small" stickyHeader>
            <TableHead>
              <TableRow>
                <TableCell sx={HEAD_CELL}>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                    {t('jobs.runName')}
                  </Box>
                </TableCell>
                <TableCell sx={HEAD_CELL}>{t('jobs.status')}</TableCell>
                <TableCell sx={HEAD_CELL}>{t('jobs.duration')}</TableCell>
                <TableCell sx={HEAD_CELL}>{t('jobs.date')}</TableCell>
                <TableCell sx={{ ...HEAD_CELL, textAlign: 'center' }}>
                  {t('jobs.actions')}
                </TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {Array.from({ length: 3 }, (_, index) => (
                <TableRow key={`skeleton-${index}`}>
                  <TableCell sx={BODY_CELL}>
                    <Box sx={{ ...PULSE, height: '1rem' }} />
                  </TableCell>
                  <TableCell sx={BODY_CELL}>
                    <Box sx={{ ...PULSE, height: '1.5rem', width: '60px', borderRadius: '12px' }} />
                  </TableCell>
                  <TableCell sx={BODY_CELL}>
                    <Box sx={{ ...PULSE, height: '1rem', width: '40px' }} />
                  </TableCell>
                  <TableCell sx={BODY_CELL}>
                    <Box sx={{ ...PULSE, height: '1rem', width: '80px' }} />
                  </TableCell>
                  <TableCell sx={BODY_CELL}>
                    <Box sx={{ ...PULSE, height: '1rem', width: '50px' }} />
                  </TableCell>
                  <TableCell sx={{ ...BODY_CELL, textAlign: 'center' }}>
                    <Box sx={{ ...PULSE, height: '1.5rem', width: '60px', margin: '0 auto' }} />
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      </CardContent>
    </Card>
  );
};

export default ExecutionHistorySkeleton;
