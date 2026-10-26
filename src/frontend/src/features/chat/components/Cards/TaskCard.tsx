import React from 'react';
import { GeneratedTask } from '../../types/dispatcher';

interface TaskCardProps {
  task: GeneratedTask;
}

const TaskCard: React.FC<TaskCardProps> = ({ task }) => {
  // Agent-builder style: one flowing line — **Task name** <description>. No
  // field labels, no expected-output/tools.
  return (
    <div
      className="my-2 text-[15px] leading-[1.7]"
      style={{ color: 'var(--text-primary)' }}
    >
      <span className="font-semibold">{task.name}</span>
      {task.description ? ` ${task.description}` : ''}
    </div>
  );
};

export default TaskCard;
