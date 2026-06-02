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

  it('renders the task name, description and expected output', () => {
    render(<TaskCard task={baseTask} />);

    expect(screen.getByText('Analyze Data')).toBeInTheDocument();
    expect(screen.getByText('Description:')).toBeInTheDocument();
    expect(screen.getByText(/Analyze the incoming dataset/)).toBeInTheDocument();
    expect(screen.getByText('Expected Output:')).toBeInTheDocument();
    expect(screen.getByText(/A summary report/)).toBeInTheDocument();
  });

  it('renders tools when tools array is present and non-empty', () => {
    const task: GeneratedTask = {
      ...baseTask,
      tools: ['SerperDevTool', 'FileReadTool'],
    };

    render(<TaskCard task={task} />);

    expect(screen.getByText('Tools:')).toBeInTheDocument();
    expect(screen.getByText('SerperDevTool')).toBeInTheDocument();
    expect(screen.getByText('FileReadTool')).toBeInTheDocument();
  });

  it('does not render the tools section when tools is undefined', () => {
    render(<TaskCard task={baseTask} />);

    expect(screen.queryByText('Tools:')).not.toBeInTheDocument();
  });

  it('does not render the tools section when tools is an empty array', () => {
    const task: GeneratedTask = {
      ...baseTask,
      tools: [],
    };

    render(<TaskCard task={task} />);

    expect(screen.queryByText('Tools:')).not.toBeInTheDocument();
  });
});
