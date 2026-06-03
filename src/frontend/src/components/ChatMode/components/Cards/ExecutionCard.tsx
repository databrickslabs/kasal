import React, { useRef, useEffect } from 'react';
import { ExecutionStatus } from '../../types/execution';

interface ExecutionCardProps {
  jobId: string;
  status: ExecutionStatus;
  traces: string[];
  result?: string;
  error?: string;
  onStop?: () => void;
}

const statusConfig: Record<
  ExecutionStatus,
  { bg: string; border: string; text: string; dot: string; label: string }
> = {
  queued: {
    bg: '#FFF8F0',
    border: '#FFD6A5',
    text: '#8A5A00',
    dot: '#E89B00',
    label: 'Queued',
  },
  running: {
    bg: '#EFF6FF',
    border: '#B3D4FC',
    text: '#1A56A0',
    dot: '#2979E5',
    label: 'Running',
  },
  completed: {
    bg: '#EEFBF3',
    border: '#A3E0B8',
    text: '#1A6B34',
    dot: '#00A972',
    label: 'Completed',
  },
  failed: {
    bg: '#FFF1F0',
    border: '#FFBDBA',
    text: '#B91C1C',
    dot: '#FF3621',
    label: 'Failed',
  },
  stopped: {
    bg: 'var(--bg-secondary)',
    border: 'var(--border-color)',
    text: 'var(--text-secondary)',
    dot: 'var(--text-muted)',
    label: 'Stopped',
  },
};

const ExecutionCard: React.FC<ExecutionCardProps> = ({
  jobId,
  status,
  traces,
  result,
  error,
  onStop,
}) => {
  const safeStatus = status || 'queued';
  const cfg = statusConfig[safeStatus] || statusConfig.queued;
  const tracesEndRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    tracesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [traces]);

  return (
    <div
      className="rounded-xl overflow-hidden my-2 shadow-lg"
      style={{
        backgroundColor: cfg.bg,
        border: `1px solid ${cfg.border}`,
      }}
    >
      {/* Header */}
      <div
        className="flex items-center justify-between px-4 py-2.5"
        style={{ borderBottom: `1px solid ${cfg.border}` }}
      >
        <div className="flex items-center gap-2">
          <span
            className={`w-2.5 h-2.5 rounded-full ${safeStatus === 'running' ? 'animate-pulse' : ''}`}
            style={{ backgroundColor: cfg.dot }}
          />
          <span
            className="text-sm font-medium"
            style={{ color: cfg.text }}
          >
            Execution: {cfg.label}
          </span>
          {jobId && (
            <span
              className="text-xs font-mono"
              style={{ color: 'var(--text-muted)' }}
            >
              {jobId.slice(0, 8)}...
            </span>
          )}
        </div>
        {(status === 'running' || status === 'queued') && onStop && (
          <button
            onClick={onStop}
            className="text-xs px-2.5 py-1 rounded-lg font-medium transition-colors hover:opacity-80"
            style={{
              color: '#B91C1C',
              border: '1px solid #FFBDBA',
              backgroundColor: '#FFF1F0',
            }}
          >
            Stop
          </button>
        )}
      </div>

      {/* Traces */}
      {traces.length > 0 && (
        <div className="px-4 py-2.5 max-h-64 overflow-y-auto">
          <div className="space-y-1">
            {traces.map((trace, i) => (
              <div
                key={i}
                className="text-xs font-mono leading-relaxed"
                style={{ color: 'var(--text-secondary)' }}
              >
                {trace}
              </div>
            ))}
            <div ref={tracesEndRef} />
          </div>
        </div>
      )}

      {/* Result */}
      {result && (
        <div
          className="px-4 py-2.5"
          style={{ borderTop: `1px solid ${cfg.border}` }}
        >
          <div className="text-sm font-medium mb-1" style={{ color: '#1A6B34' }}>
            Result:
          </div>
          <div
            className="text-sm whitespace-pre-wrap"
            style={{ color: 'var(--text-primary)' }}
          >
            {result}
          </div>
        </div>
      )}

      {/* Error */}
      {error && (
        <div
          className="px-4 py-2.5"
          style={{ borderTop: '1px solid #FFBDBA' }}
        >
          <div className="text-sm" style={{ color: '#B91C1C' }}>
            {error}
          </div>
        </div>
      )}
    </div>
  );
};

export default ExecutionCard;
