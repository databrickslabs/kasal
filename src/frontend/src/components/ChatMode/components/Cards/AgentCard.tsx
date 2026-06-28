import React from 'react';
import { GeneratedAgent } from '../../types/dispatcher';
import { useExecutionStore } from '../../store/executionStore';
import { useSessionStore } from '../../store/sessionStore';

interface AgentCardProps {
  agent: GeneratedAgent;
}

const AgentCard: React.FC<AgentCardProps> = ({ agent }) => {
  // Pulse the text only WHILE the viewed session's run is still going ("working"
  // feel); once it completes the animation stops and the text settles.
  const running = useExecutionStore((s) => s.isExecuting || s.isGenerating);
  const owner = useExecutionStore((s) => s.executionOwnerSessionId);
  const currentSession = useSessionStore((s) => s.currentSessionId);
  const active = running && owner === currentSession;

  // Agent-builder style: one flowing line — **Name** — Role <goal>. No field
  // labels, no backstory/tools.
  return (
    <div
      className={`my-2 text-[15px] leading-[1.7]${active ? ' animate-pulse' : ''}`}
      style={{ color: 'var(--text-primary)' }}
    >
      <span className="font-semibold">{agent.name}</span>
      {agent.role ? ` — ${agent.role}` : ''}
      {agent.goal ? ` ${agent.goal}` : ''}
    </div>
  );
};

export default AgentCard;
