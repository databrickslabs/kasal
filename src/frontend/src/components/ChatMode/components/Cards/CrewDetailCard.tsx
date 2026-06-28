import React from 'react';
import Box from '@mui/material/Box';
import { CatalogLoadResult } from '../../types/dispatcher';
import { useExecutionStore } from '../../store/executionStore';
import { buttonResetSx, spinnerSx } from '../../chatSx';

interface CrewDetailCardProps {
  data: CatalogLoadResult;
  onExecute?: () => void;
}

const CrewDetailCard: React.FC<CrewDetailCardProps> = ({
  data,
  onExecute,
}) => {
  const busy = useExecutionStore((s) => s.isExecuting || s.isLoading);
  const { plan } = data;
  if (!plan) {
    return (
      <Box sx={{ fontSize: 14, my: 1, color: 'text.disabled' }}>
        No plan data available.
      </Box>
    );
  }

  const agentCount = plan.nodes?.filter(
    (n: unknown) =>
      (n as Record<string, unknown>).type === 'agentNode' ||
      (n as Record<string, string>).type === 'agent'
  ).length ?? 0;
  const taskCount = plan.nodes?.filter(
    (n: unknown) =>
      (n as Record<string, unknown>).type === 'taskNode' ||
      (n as Record<string, string>).type === 'task'
  ).length ?? 0;

  const labelSx = { fontWeight: 500, color: 'text.primary' } as const;

  return (
    <Box
      sx={{
        borderRadius: '12px',
        p: 2,
        my: 1.5,
        backgroundColor: 'background.paper',
        border: 1,
        borderColor: 'divider',
      }}
    >
      <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 1.5 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <Box
            sx={{
              width: 28,
              height: 28,
              borderRadius: '8px',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              color: '#fff',
              fontSize: 12,
              backgroundColor: 'primary.main',
            }}
          >
            <Box component="svg" sx={{ width: 14, height: 14 }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M18 18.72a9.094 9.094 0 003.741-.479 3 3 0 00-4.682-2.72m.94 3.198l.001.031c0 .225-.012.447-.037.666A11.944 11.944 0 0112 21c-2.17 0-4.207-.576-5.963-1.584A6.062 6.062 0 016 18.719m12 0a5.971 5.971 0 00-.941-3.197m0 0A5.995 5.995 0 0012 12.75a5.995 5.995 0 00-5.058 2.772m0 0a3 3 0 00-4.681 2.72 8.986 8.986 0 003.74.477m.94-3.197a5.971 5.971 0 00-.94 3.197M15 6.75a3 3 0 11-6 0 3 3 0 016 0zm6 3a2.25 2.25 0 11-4.5 0 2.25 2.25 0 014.5 0zm-13.5 0a2.25 2.25 0 11-4.5 0 2.25 2.25 0 014.5 0z" />
            </Box>
          </Box>
          <Box component="h4" sx={{ fontWeight: 600, fontSize: 14, color: 'text.primary' }}>
            {plan.name}
          </Box>
        </Box>
        {onExecute && (
          <Box
            component="button"
            onClick={onExecute}
            disabled={busy}
            sx={{
              ...buttonResetSx,
              color: '#fff',
              fontSize: 12,
              px: 1.75,
              py: 0.75,
              borderRadius: '8px',
              fontWeight: 500,
              transition: 'all 0.15s',
              display: 'flex',
              alignItems: 'center',
              gap: 0.75,
              backgroundColor: 'primary.main',
              '&:hover': { opacity: 0.9 },
              '&:disabled': { opacity: 0.5, cursor: 'not-allowed' },
            }}
          >
            {busy && <Box sx={spinnerSx} />}
            {busy ? 'Starting...' : 'Execute'}
          </Box>
        )}
      </Box>
      <Box sx={{ display: 'grid', gridTemplateColumns: 'repeat(2, minmax(0, 1fr))', gap: 1, fontSize: 14, color: 'text.secondary' }}>
        <Box>
          <Box component="span" sx={labelSx}>Process:</Box>{' '}
          {plan.process || 'sequential'}
        </Box>
        <Box>
          <Box component="span" sx={labelSx}>Agents:</Box>{' '}
          {agentCount}
        </Box>
        <Box>
          <Box component="span" sx={labelSx}>Tasks:</Box>{' '}
          {taskCount}
        </Box>
        <Box>
          <Box component="span" sx={labelSx}>Planning:</Box>{' '}
          {plan.planning ? 'Yes' : 'No'}
        </Box>
        {plan.memory !== undefined && (
          <Box>
            <Box component="span" sx={labelSx}>Memory:</Box>{' '}
            {plan.memory ? 'Yes' : 'No'}
          </Box>
        )}
        {plan.max_rpm !== undefined && (
          <Box>
            <Box component="span" sx={labelSx}>Max RPM:</Box>{' '}
            {plan.max_rpm}
          </Box>
        )}
      </Box>
    </Box>
  );
};

export default CrewDetailCard;
