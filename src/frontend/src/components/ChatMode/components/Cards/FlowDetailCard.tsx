import React from 'react';
import { FlowLoadResult } from '../../types/dispatcher';
import { useExecutionStore } from '../../store/executionStore';

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
      <div className="text-sm my-2" style={{ color: 'var(--text-muted)' }}>
        No flow data available.
      </div>
    );
  }

  const nodeCount = flow.nodes?.length ?? 0;

  return (
    <div
      className="rounded-xl p-4 my-3"
      style={{
        backgroundColor: 'var(--bg-input)',
        border: '1px solid var(--border-color)',
      }}
    >
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          <div
            className="w-7 h-7 rounded-lg flex items-center justify-center text-white text-xs"
            style={{ backgroundColor: 'var(--accent)' }}
          >
            <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M7.5 21L3 16.5m0 0L7.5 12M3 16.5h13.5m0-13.5L21 7.5m0 0L16.5 12M21 7.5H7.5" />
            </svg>
          </div>
          <h4
            className="font-semibold text-sm"
            style={{ color: 'var(--text-primary)' }}
          >
            {flow.name}
          </h4>
        </div>
        {onExecute && (
          <button
            onClick={onExecute}
            disabled={busy}
            className="text-white text-xs px-3.5 py-1.5 rounded-lg font-medium transition-all hover:opacity-90 disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-1.5"
            style={{ backgroundColor: 'var(--accent)' }}
          >
            {busy && (
              <div className="w-3 h-3 rounded-full border-2 border-white border-t-transparent animate-spin" />
            )}
            {busy ? 'Starting...' : 'Execute'}
          </button>
        )}
      </div>
      <div className="grid grid-cols-2 gap-2 text-sm" style={{ color: 'var(--text-secondary)' }}>
        <div>
          <span className="font-medium" style={{ color: 'var(--text-primary)' }}>Nodes:</span>{' '}
          {nodeCount}
        </div>
        {flow.flow_config && (
          <div>
            <span className="font-medium" style={{ color: 'var(--text-primary)' }}>Config:</span>{' '}
            {Object.keys(flow.flow_config).length} settings
          </div>
        )}
      </div>
    </div>
  );
};

export default FlowDetailCard;
