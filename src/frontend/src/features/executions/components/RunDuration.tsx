import React, { useEffect, useState } from 'react';
import { Box, CircularProgress } from '@mui/material';
import { Clock3 } from 'lucide-react';
import { Run, calculateDuration, calculateDurationFromTraces } from '../../../api/execution/ExecutionHistoryService';

/** Prefer persisted timestamps; only legacy records need a trace lookup. */
export default function RunDuration({ run }: { run: Run }) {
  const [duration, setDuration] = useState('—');
  const [loading, setLoading] = useState(false);
  const active = ['running', 'in_progress', 'preparing', 'pending', 'queued', 'stopping', 'waiting_for_approval'].includes(run.status?.toLowerCase());
  useEffect(() => {
    let mounted = true;
    const status = run.status?.toUpperCase();
    const terminal = ['COMPLETED', 'FAILED', 'CANCELLED', 'STOPPED', 'REJECTED'].includes(status);
    const updateElapsed = () => setDuration(calculateDuration({ ...run, status: 'COMPLETED', completed_at: new Date().toISOString() }));
    setDuration('—');
    setLoading(false);
    if (active) {
      updateElapsed();
      const timer = window.setInterval(updateElapsed, 1000);
      return () => window.clearInterval(timer);
    }
    if (terminal && run.completed_at) {
      setDuration(calculateDuration({ ...run, status: 'COMPLETED' }));
    } else if (terminal) {
      setLoading(true);
      void calculateDurationFromTraces(run).then(value => {
        if (mounted) setDuration(value === '-' ? '—' : value);
      }).catch(() => { if (mounted) setDuration('—'); })
        .finally(() => { if (mounted) setLoading(false); });
    }
    return () => { mounted = false; };
  }, [run, active]);
  return <Box title={active ? 'Elapsed since this run was created' : 'Execution duration'} sx={{ display: 'inline-flex', alignItems: 'center', gap: 0.5, color: 'text.secondary', fontSize: 12, fontVariantNumeric: 'tabular-nums' }}>
    {loading ? <CircularProgress size={12} color="inherit" /> : <><Clock3 size={12} />{duration}{active ? ' elapsed' : ''}</>}
  </Box>;
}
