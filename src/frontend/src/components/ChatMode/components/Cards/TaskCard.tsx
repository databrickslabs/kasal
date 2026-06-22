import React from 'react';
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
  // field labels, no expected-output/tools.
  return (
    <div
      className={`my-2 text-[15px] leading-[1.7]${active ? ' animate-pulse' : ''}`}
      style={{ color: 'var(--text-primary)' }}
    >
      <span className="font-semibold">{task.name}</span>
      {task.description ? ` ${task.description}` : ''}
    </div>
  );
};

export default TaskCard;
