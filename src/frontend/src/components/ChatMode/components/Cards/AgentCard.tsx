import React from 'react';
import Box from '@mui/material/Box';
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
  // labels, no backstory/tools. (Styled via the chat MUI theme.)
  return (
    <Box
      sx={{
        my: 1,
        fontSize: 15,
        lineHeight: 1.7,
        color: 'text.primary',
        ...(active && {
          animation: 'agentCardPulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite',
          '@keyframes agentCardPulse': { '0%, 100%': { opacity: 1 }, '50%': { opacity: 0.5 } },
        }),
      }}
    >
      <Box component="span" sx={{ fontWeight: 600 }}>{agent.name}</Box>
      {agent.role ? ` — ${agent.role}` : ''}
      {agent.goal ? ` ${agent.goal}` : ''}
    </Box>
  );
};

export default AgentCard;
