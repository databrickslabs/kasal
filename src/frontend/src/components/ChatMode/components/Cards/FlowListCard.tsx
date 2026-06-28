import React from 'react';
import { FlowListResult } from '../../types/dispatcher';

interface FlowListCardProps {
  data: FlowListResult;
  onCommand?: (command: string) => void;
}

const FlowListCard: React.FC<FlowListCardProps> = ({ data, onCommand }) => {
  if (!data.flows || data.flows.length === 0) {
    return (
      <div className="text-sm my-2" style={{ color: 'var(--text-muted)' }}>
        No flows found.
      </div>
    );
  }

  return (
    <div className="my-3 space-y-1.5">
      {data.flows.map((flow) => (
        <div
          key={flow.id}
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
                d="M7.5 21L3 16.5m0 0L7.5 12M3 16.5h13.5m0-13.5L21 7.5m0 0L16.5 12M21 7.5H7.5"
              />
            </svg>
          </div>

          {/* Name + badge */}
          <div className="flex-1 min-w-0">
            <div
              className="text-sm font-medium truncate"
              style={{ color: 'var(--text-primary)' }}
            >
              {flow.name}
            </div>
            <div className="flex items-center gap-2 mt-0.5">
              <span
                className="text-[11px] leading-tight"
                style={{ color: 'var(--text-muted)' }}
              >
                {flow.node_count ?? 0} node{(flow.node_count ?? 0) !== 1 ? 's' : ''}
              </span>
            </div>
          </div>

          {/* Play button */}
          <button
            onClick={() => onCommand?.(`/run flow ${flow.name}`)}
            className="shrink-0 w-8 h-8 rounded-full flex items-center justify-center transition-all opacity-60 group-hover:opacity-100 hover:scale-110"
            style={{ backgroundColor: 'var(--accent)', color: '#fff' }}
            title={`Run ${flow.name}`}
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

export default FlowListCard;
