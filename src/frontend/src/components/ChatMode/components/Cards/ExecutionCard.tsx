import React, { useRef, useEffect } from 'react';
import Box from '@mui/material/Box';
import { useTheme } from '@mui/material/styles';
import { ExecutionStatus } from '../../types/execution';
import { buttonResetSx } from '../../chatSx';

interface ExecutionCardProps {
  jobId: string;
  status: ExecutionStatus;
  traces: string[];
  result?: string;
  error?: string;
  onStop?: () => void;
}

interface StatusCfg {
  bg: string;
  border: string;
  text: string;
  dot: string;
  label: string;
}

// The queued/running/completed/failed states use fixed pastel palettes (a light
// status card regardless of theme). "stopped" follows the chat theme — its
// colours are filled in from the theme at render time.
const statusConfig: Record<ExecutionStatus, StatusCfg> = {
  queued: { bg: '#FFF8F0', border: '#FFD6A5', text: '#8A5A00', dot: '#E89B00', label: 'Queued' },
  running: { bg: '#EFF6FF', border: '#B3D4FC', text: '#1A56A0', dot: '#2979E5', label: 'Running' },
  completed: { bg: '#EEFBF3', border: '#A3E0B8', text: '#1A6B34', dot: '#00A972', label: 'Completed' },
  failed: { bg: '#FFF1F0', border: '#FFBDBA', text: '#B91C1C', dot: '#FF3621', label: 'Failed' },
  stopped: { bg: '', border: '', text: '', dot: '', label: 'Stopped' },
};

const ExecutionCard: React.FC<ExecutionCardProps> = ({
  jobId,
  status,
  traces,
  result,
  error,
  onStop,
}) => {
  const theme = useTheme();
  const safeStatus = status || 'queued';
  const base = statusConfig[safeStatus] || statusConfig.queued;
  const cfg: StatusCfg =
    safeStatus === 'stopped'
      ? {
          ...base,
          bg: theme.chat.bgSecondary,
          border: theme.palette.divider,
          text: theme.palette.text.secondary,
          dot: theme.chat.textMuted,
        }
      : base;
  const tracesEndRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    tracesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [traces]);

  return (
    <Box
      sx={{
        borderRadius: '12px',
        overflow: 'hidden',
        my: 1,
        boxShadow: '0 10px 15px -3px rgb(0 0 0 / 0.1), 0 4px 6px -4px rgb(0 0 0 / 0.1)',
        backgroundColor: cfg.bg,
        border: `1px solid ${cfg.border}`,
      }}
    >
      {/* Header */}
      <Box
        sx={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          px: 2,
          py: 1.25,
          borderBottom: `1px solid ${cfg.border}`,
        }}
      >
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <Box
            component="span"
            data-testid="execution-status-dot"
            data-running={safeStatus === 'running'}
            sx={{
              width: 10,
              height: 10,
              borderRadius: '9999px',
              backgroundColor: cfg.dot,
              ...(safeStatus === 'running' && {
                animation: 'kasalDotPulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite',
                '@keyframes kasalDotPulse': { '0%, 100%': { opacity: 1 }, '50%': { opacity: 0.5 } },
              }),
            }}
          />
          <Box component="span" sx={{ fontSize: 14, fontWeight: 500, color: cfg.text }}>
            Execution: {cfg.label}
          </Box>
          {jobId && (
            <Box component="span" sx={{ fontSize: 12, fontFamily: 'monospace', color: 'text.disabled' }}>
              {jobId.slice(0, 8)}...
            </Box>
          )}
        </Box>
        {(status === 'running' || status === 'queued') && onStop && (
          <Box
            component="button"
            onClick={onStop}
            sx={{
              ...buttonResetSx,
              fontSize: 12,
              px: 1.25,
              py: 0.5,
              borderRadius: '8px',
              fontWeight: 500,
              transition: 'opacity 0.15s',
              color: '#B91C1C',
              border: '1px solid #FFBDBA',
              backgroundColor: '#FFF1F0',
              '&:hover': { opacity: 0.8 },
            }}
          >
            Stop
          </Box>
        )}
      </Box>

      {/* Traces */}
      {traces.length > 0 && (
        <Box sx={{ px: 2, py: 1.25, maxHeight: 256, overflowY: 'auto' }}>
          <Box sx={{ '& > * + *': { mt: 0.5 } }}>
            {traces.map((trace, i) => (
              <Box key={i} sx={{ fontSize: 12, fontFamily: 'monospace', lineHeight: 1.625, color: 'text.secondary' }}>
                {trace}
              </Box>
            ))}
            <div ref={tracesEndRef} />
          </Box>
        </Box>
      )}

      {/* Result */}
      {result && (
        <Box sx={{ px: 2, py: 1.25, borderTop: `1px solid ${cfg.border}` }}>
          <Box sx={{ fontSize: 14, fontWeight: 500, mb: 0.5, color: '#1A6B34' }}>
            Result:
          </Box>
          <Box sx={{ fontSize: 14, whiteSpace: 'pre-wrap', color: 'text.primary' }}>
            {result}
          </Box>
        </Box>
      )}

      {/* Error */}
      {error && (
        <Box sx={{ px: 2, py: 1.25, borderTop: '1px solid #FFBDBA' }}>
          <Box sx={{ fontSize: 14, color: '#B91C1C' }}>
            {error}
          </Box>
        </Box>
      )}
    </Box>
  );
};

export default ExecutionCard;
