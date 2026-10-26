import React from 'react';
import { GeneratedAgent } from '../../types/dispatcher';

interface AgentCardProps {
  agent: GeneratedAgent;
}

const AgentCard: React.FC<AgentCardProps> = ({ agent }) => {
  // Agent-builder style: one flowing line — **Name** — Role <goal>. No field
  // labels, no backstory/tools.
  return (
    <div
      className="my-2 text-[15px] leading-[1.7]"
      style={{ color: 'var(--text-primary)' }}
    >
      <span className="font-semibold">{agent.name}</span>
      {agent.role ? ` — ${agent.role}` : ''}
      {agent.goal ? ` ${agent.goal}` : ''}
    </div>
  );
};

export default AgentCard;
