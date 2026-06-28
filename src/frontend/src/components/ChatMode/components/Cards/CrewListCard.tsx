import React from 'react';
import { CatalogListResult } from '../../types/dispatcher';

interface CrewListCardProps {
  data: CatalogListResult;
  onCommand?: (command: string) => void;
}

const CrewListCard: React.FC<CrewListCardProps> = ({ data, onCommand }) => {
  if (!data.plans || data.plans.length === 0) {
    return (
      <div className="text-sm my-2" style={{ color: 'var(--text-muted)' }}>
        No crews found.
      </div>
    );
  }

  return (
    <div className="my-3 space-y-1.5">
      {data.plans.map((plan) => (
        <div
          key={plan.id}
          className="group flex items-center gap-3 rounded-lg px-3 py-2.5 transition-colors cursor-default"
          style={{
            backgroundColor: 'var(--bg-input)',
            border: '1px solid var(--border-color)',
          }}
        >
          {/* Icon */}
          <div
            className="w-8 h-8 rounded-lg flex items-center justify-center shrink-0"
            style={{ backgroundColor: 'var(--bg-secondary)' }}
          >
            <svg
              className="w-4 h-4"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
              strokeWidth={1.8}
              style={{ color: 'var(--accent)' }}
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                d="M18 18.72a9.094 9.094 0 003.741-.479 3 3 0 00-4.682-2.72m.94 3.198l.001.031c0 .225-.012.447-.037.666A11.944 11.944 0 0112 21c-2.17 0-4.207-.576-5.963-1.584A6.062 6.062 0 016 18.719m12 0a5.971 5.971 0 00-.941-3.197m0 0A5.995 5.995 0 0012 12.75a5.995 5.995 0 00-5.058 2.772m0 0a3 3 0 00-4.681 2.72 8.986 8.986 0 003.74.477m.94-3.197a5.971 5.971 0 00-.94 3.197M15 6.75a3 3 0 11-6 0 3 3 0 016 0zm6 3a2.25 2.25 0 11-4.5 0 2.25 2.25 0 014.5 0zm-13.5 0a2.25 2.25 0 11-4.5 0 2.25 2.25 0 014.5 0z"
              />
            </svg>
          </div>

          {/* Name + badges */}
          <div className="flex-1 min-w-0">
            <div
              className="text-sm font-medium truncate"
              style={{ color: 'var(--text-primary)' }}
            >
              {plan.name}
            </div>
            <div className="flex items-center gap-2 mt-0.5">
              <span
                className="text-[11px] leading-tight"
                style={{ color: 'var(--text-muted)' }}
              >
                {plan.agent_count ?? 0} agent{(plan.agent_count ?? 0) !== 1 ? 's' : ''}
              </span>
              <span style={{ color: 'var(--border-color)' }}>&middot;</span>
              <span
                className="text-[11px] leading-tight"
                style={{ color: 'var(--text-muted)' }}
              >
                {plan.task_count ?? 0} task{(plan.task_count ?? 0) !== 1 ? 's' : ''}
              </span>
            </div>
          </div>

          {/* Play button */}
          <button
            onClick={() => onCommand?.(`/run crew ${plan.name}`)}
            className="shrink-0 w-8 h-8 rounded-full flex items-center justify-center transition-all opacity-60 group-hover:opacity-100 hover:scale-110"
            style={{ backgroundColor: 'var(--accent)', color: '#fff' }}
            title={`Run ${plan.name}`}
          >
            <svg className="w-3.5 h-3.5 ml-0.5" viewBox="0 0 24 24" fill="currentColor">
              <path d="M8 5.14v14l11-7-11-7z" />
            </svg>
          </button>
        </div>
      ))}
    </div>
  );
};

export default CrewListCard;
