import React from 'react';
import { Box, Button, Typography } from '@mui/material';
import { Activity, Check, CirclePause, Layers, CircleAlert } from 'lucide-react';
import type { Run } from '../../../types/execution/run';

export type RunFilter = 'all' | 'active' | 'completed' | 'attention' | 'stopped';
const filters = [
  { id: 'all', label: 'All runs', icon: Layers },
  { id: 'active', label: 'In progress', icon: Activity },
  { id: 'completed', label: 'Completed', icon: Check },
  { id: 'attention', label: 'Needs attention', icon: CircleAlert },
  { id: 'stopped', label: 'Stopped', icon: CirclePause },
] as const;

export function matchesRunFilter(run: Run, filter: RunFilter): boolean {
  const status = run.status?.toLowerCase();
  switch (filter) {
    case 'active': return ['running', 'in_progress', 'pending', 'queued', 'preparing', 'stopping'].includes(status);
    case 'completed': return status === 'completed';
    case 'attention': return ['failed', 'rejected', 'waiting_for_approval'].includes(status);
    case 'stopped': return ['stopped', 'cancelled'].includes(status);
    default: return true;
  }
}

export default function RunActivityOverview({ runs, value, onChange }: {
  runs: Run[]; value: RunFilter; onChange: (value: RunFilter) => void;
}) {
  return <Box role="group" aria-label="Filter executions by status" sx={{ display: 'grid', gridTemplateColumns: 'repeat(5, minmax(0, 1fr))', gap: 0.5, mb: 2 }}>
    {filters.map(({ id, label, icon: Icon }) => <Button key={id} color="inherit" aria-pressed={value === id}
      onClick={() => onChange(id)} sx={{ display: 'flex', alignItems: 'flex-start', flexDirection: 'column', gap: 0.75,
        p: { xs: 1, sm: 1.5 }, minWidth: 0, borderRadius: 2.5, bgcolor: value === id ? 'action.selected' : 'transparent',
        textTransform: 'none', textAlign: 'left', color: value === id ? 'text.primary' : 'text.secondary' }}>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.75, fontSize: 11, minHeight: 32, lineHeight: 1.3 }}>
        <Box component="span" sx={{ display: { xs: 'none', sm: 'inline-flex' } }}><Icon size={14} /></Box>{label}
      </Box>
      <Typography component="span" sx={{ fontSize: { xs: 21, sm: 26 }, fontWeight: 500, letterSpacing: '-0.04em', lineHeight: 1.2, fontVariantNumeric: 'tabular-nums' }}>
        {runs.filter(run => matchesRunFilter(run, id)).length}
      </Typography>
    </Button>)}
  </Box>;
}
