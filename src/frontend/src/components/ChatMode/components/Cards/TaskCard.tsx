import React from 'react';
import { GeneratedTask } from '../../types/dispatcher';

interface TaskCardProps {
  task: GeneratedTask;
}

const TaskCard: React.FC<TaskCardProps> = ({ task }) => {
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
            <path strokeLinecap="round" strokeLinejoin="round" d="M9 12h3.75M9 15h3.75M9 18h3.75m3 .75H18a2.25 2.25 0 002.25-2.25V6.108c0-1.135-.845-2.098-1.976-2.192a48.424 48.424 0 00-1.123-.08m-5.801 0c-.065.21-.1.433-.1.664 0 .414.336.75.75.75h4.5a.75.75 0 00.75-.75 2.25 2.25 0 00-.1-.664m-5.8 0A2.251 2.251 0 0113.5 2.25H15c1.012 0 1.867.668 2.15 1.586m-5.8 0c-.376.023-.75.05-1.124.08C9.095 4.01 8.25 4.973 8.25 6.108V8.25m0 0H4.875c-.621 0-1.125.504-1.125 1.125v11.25c0 .621.504 1.125 1.125 1.125h9.75c.621 0 1.125-.504 1.125-1.125V9.375c0-.621-.504-1.125-1.125-1.125H8.25zM6.75 12h.008v.008H6.75V12zm0 3h.008v.008H6.75V15zm0 3h.008v.008H6.75V18z" />
          </svg>
        </div>
        <h4
          className="font-semibold text-sm"
          style={{ color: 'var(--text-primary)' }}
        >
          {task.name}
        </h4>
      </div>
      <div className="space-y-1.5 text-sm" style={{ color: 'var(--text-secondary)' }}>
        <p>
          <span className="font-medium" style={{ color: 'var(--text-primary)' }}>Description:</span>{' '}
          {task.description}
        </p>
        <p>
          <span className="font-medium" style={{ color: 'var(--text-primary)' }}>Expected Output:</span>{' '}
          {task.expected_output}
        </p>
        {task.tools && task.tools.length > 0 && (
          <div className="flex flex-wrap gap-1.5 mt-2">
            <span className="font-medium" style={{ color: 'var(--text-primary)' }}>Tools:</span>
            {task.tools.map((tool, i) => (
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

export default TaskCard;
