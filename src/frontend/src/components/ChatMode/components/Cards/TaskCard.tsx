import React from 'react';
import Box from '@mui/material/Box';
import { GeneratedTask } from '../../types/dispatcher';
import { useExecutionStore } from '../../store/executionStore';
import { useSessionStore } from '../../store/sessionStore';

interface TaskCardProps {
  task: GeneratedTask;
}

const TaskCard: React.FC<TaskCardProps> = ({ task }) => {
  // Pulse only WHILE the viewed session's run is still going; stop on completion.
  const running = useExecutionStore((s) => s.isExecuting || s.isGenerating);
  const owner = useExecutionStore((s) => s.executionOwnerSessionId);
  const currentSession = useSessionStore((s) => s.currentSessionId);
  const active = running && owner === currentSession;

  // Agent-builder style: one flowing line — **Task name** <description>. No
  // field labels, no expected-output/tools. (Styled via the chat MUI theme.)
  return (
    <Box
      sx={{
        my: 1,
        fontSize: 15,
        lineHeight: 1.7,
        color: 'text.primary',
        ...(active && {
          animation: 'taskCardPulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite',
          '@keyframes taskCardPulse': { '0%, 100%': { opacity: 1 }, '50%': { opacity: 0.5 } },
        }),
      }}
    >
      <Box component="span" sx={{ fontWeight: 600 }}>{task.name}</Box>
      {task.description ? ` ${task.description}` : ''}
    </Box>
  );
};

export default TaskCard;
