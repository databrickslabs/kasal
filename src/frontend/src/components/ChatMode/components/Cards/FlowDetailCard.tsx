import React from 'react';
import Box from '@mui/material/Box';
import { FlowLoadResult } from '../../types/dispatcher';
import { useExecutionStore } from '../../store/executionStore';
import { buttonResetSx, spinnerSx } from '../../chatSx';

interface FlowDetailCardProps {
  data: FlowLoadResult;
  onExecute?: () => void;
}

const FlowDetailCard: React.FC<FlowDetailCardProps> = ({
  data,
  onExecute,
}) => {
  const busy = useExecutionStore((s) => s.isExecuting || s.isLoading);
  const { flow } = data;
  if (!flow) {
    return (
      <Box sx={{ fontSize: 14, my: 1, color: 'text.disabled' }}>
        No flow data available.
      </Box>
    );
  }

  const nodeCount = flow.nodes?.length ?? 0;
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
              <path strokeLinecap="round" strokeLinejoin="round" d="M7.5 21L3 16.5m0 0L7.5 12M3 16.5h13.5m0-13.5L21 7.5m0 0L16.5 12M21 7.5H7.5" />
            </Box>
          </Box>
          <Box component="h4" sx={{ fontWeight: 600, fontSize: 14, color: 'text.primary' }}>
            {flow.name}
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
          <Box component="span" sx={labelSx}>Nodes:</Box>{' '}
          {nodeCount}
        </Box>
        {flow.flow_config && (
          <Box>
            <Box component="span" sx={labelSx}>Config:</Box>{' '}
            {Object.keys(flow.flow_config).length} settings
          </Box>
        )}
      </Box>
    </Box>
  );
};

export default FlowDetailCard;
