import React from 'react';
import { CatalogLoadResult } from '../../types/dispatcher';
import { useExecutionStore } from '../../store/executionStore';

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
      <div className="text-sm my-2" style={{ color: 'var(--text-muted)' }}>
        No plan data available.
      </div>
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
              <path strokeLinecap="round" strokeLinejoin="round" d="M18 18.72a9.094 9.094 0 003.741-.479 3 3 0 00-4.682-2.72m.94 3.198l.001.031c0 .225-.012.447-.037.666A11.944 11.944 0 0112 21c-2.17 0-4.207-.576-5.963-1.584A6.062 6.062 0 016 18.719m12 0a5.971 5.971 0 00-.941-3.197m0 0A5.995 5.995 0 0012 12.75a5.995 5.995 0 00-5.058 2.772m0 0a3 3 0 00-4.681 2.72 8.986 8.986 0 003.74.477m.94-3.197a5.971 5.971 0 00-.94 3.197M15 6.75a3 3 0 11-6 0 3 3 0 016 0zm6 3a2.25 2.25 0 11-4.5 0 2.25 2.25 0 014.5 0zm-13.5 0a2.25 2.25 0 11-4.5 0 2.25 2.25 0 014.5 0z" />
            </svg>
          </div>
          <h4
            className="font-semibold text-sm"
            style={{ color: 'var(--text-primary)' }}
          >
            {plan.name}
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
          <span className="font-medium" style={{ color: 'var(--text-primary)' }}>Process:</span>{' '}
          {plan.process || 'sequential'}
        </div>
        <div>
          <span className="font-medium" style={{ color: 'var(--text-primary)' }}>Agents:</span>{' '}
          {agentCount}
        </div>
        <div>
          <span className="font-medium" style={{ color: 'var(--text-primary)' }}>Tasks:</span>{' '}
          {taskCount}
        </div>
        <div>
          <span className="font-medium" style={{ color: 'var(--text-primary)' }}>Planning:</span>{' '}
          {plan.planning ? 'Yes' : 'No'}
        </div>
        {plan.memory !== undefined && (
          <div>
            <span className="font-medium" style={{ color: 'var(--text-primary)' }}>Memory:</span>{' '}
            {plan.memory ? 'Yes' : 'No'}
          </div>
        )}
        {plan.max_rpm !== undefined && (
          <div>
            <span className="font-medium" style={{ color: 'var(--text-primary)' }}>Max RPM:</span>{' '}
            {plan.max_rpm}
          </div>
        )}
      </div>
    </div>
  );
};

export default CrewDetailCard;
