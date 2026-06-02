import React from 'react';
import { GeneratedAgent } from '../../types/dispatcher';

interface AgentCardProps {
  agent: GeneratedAgent;
}

const AgentCard: React.FC<AgentCardProps> = ({ agent }) => {
  return (
    <div
      className="rounded-xl p-4 my-3"
      style={{
        backgroundColor: 'var(--bg-input)',
        border: '1px solid var(--border-color)',
      }}
    >
      <div className="flex items-center gap-2 mb-2.5">
        <div
          className="w-7 h-7 rounded-lg flex items-center justify-center text-white text-xs"
          style={{ backgroundColor: 'var(--accent)' }}
        >
          <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M15.75 6a3.75 3.75 0 11-7.5 0 3.75 3.75 0 017.5 0zM4.501 20.118a7.5 7.5 0 0114.998 0A17.933 17.933 0 0112 21.75c-2.676 0-5.216-.584-7.499-1.632z" />
          </svg>
        </div>
        <h4
          className="font-semibold text-sm"
          style={{ color: 'var(--text-primary)' }}
        >
          {agent.name}
        </h4>
        <span
          className="text-[11px] px-2 py-0.5 rounded-full font-medium"
          style={{
            backgroundColor: 'var(--bg-secondary)',
            color: 'var(--text-secondary)',
          }}
        >
          {agent.role}
        </span>
      </div>
      <div className="space-y-1.5 text-sm" style={{ color: 'var(--text-secondary)' }}>
        <p>
          <span className="font-medium" style={{ color: 'var(--text-primary)' }}>Goal:</span>{' '}
          {agent.goal}
        </p>
        <p>
          <span className="font-medium" style={{ color: 'var(--text-primary)' }}>Backstory:</span>{' '}
          {agent.backstory}
        </p>
        {agent.tools && agent.tools.length > 0 && (
          <div className="flex flex-wrap gap-1.5 mt-2">
            <span className="font-medium" style={{ color: 'var(--text-primary)' }}>Tools:</span>
            {agent.tools.map((tool, i) => (
              <span
                key={i}
                className="text-[11px] px-2 py-0.5 rounded-md font-medium"
                style={{
                  backgroundColor: 'var(--bg-secondary)',
                  color: 'var(--accent)',
                }}
              >
                {tool}
              </span>
            ))}
          </div>
        )}
      </div>
    </div>
  );
};

export default AgentCard;
