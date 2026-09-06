import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import TaskCard from './TaskCard';
import { GeneratedTask } from '../../types/dispatcher';

describe('TaskCard', () => {
  const baseTask: GeneratedTask = {
    name: 'Analyze Data',
    description: 'Analyze the incoming dataset',
    expected_output: 'A summary report',
  };

  // The card renders the agent-builder one-line format: **Task name** <description>.
  // No field labels, and expected-output/tools are intentionally NOT shown.
  it('renders the task name and description as one clean line', () => {
    const { container } = render(<TaskCard task={baseTask} />);

    expect(screen.getByText('Analyze Data')).toBeInTheDocument(); // name (bold span)
    expect(container.textContent).toContain('Analyze the incoming dataset');
  });

  it('does NOT show field labels, expected output, or tools', () => {
    const { container } = render(
      <TaskCard task={{ ...baseTask, tools: ['SerperDevTool', 'FileReadTool'] }} />,
    );

    expect(screen.queryByText('Description:')).not.toBeInTheDocument();
    expect(screen.queryByText('Expected Output:')).not.toBeInTheDocument();
    expect(screen.queryByText('Tools:')).not.toBeInTheDocument();
    expect(container.textContent).not.toContain('A summary report');
    expect(container.textContent).not.toContain('SerperDevTool');
  });

  it('renders just the name when description is absent', () => {
    const { container } = render(<TaskCard task={{ name: 'Solo Task', description: '', expected_output: '' }} />);
    expect(screen.getByText('Solo Task')).toBeInTheDocument();
    expect(container.textContent?.trim()).toBe('Solo Task');
  });
});
