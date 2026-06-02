import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import AgentCard from './AgentCard';
import { GeneratedAgent } from '../../types/dispatcher';

describe('AgentCard', () => {
  const baseAgent: GeneratedAgent = {
    name: 'Researcher',
    role: 'Senior Research Analyst',
    goal: 'Find the best information',
    backstory: 'An experienced analyst.',
  };

  it('renders full agent props including tools', () => {
    const agent: GeneratedAgent = {
      ...baseAgent,
      id: 'agent-1',
      llm: 'gpt-4',
      tools: ['SerperDevTool', 'FileReadTool'],
    };

    render(<AgentCard agent={agent} />);

    expect(screen.getByText('Researcher')).toBeInTheDocument();
    expect(screen.getByText('Senior Research Analyst')).toBeInTheDocument();
    expect(screen.getByText('Goal:')).toBeInTheDocument();
    expect(screen.getByText(/Find the best information/)).toBeInTheDocument();
    expect(screen.getByText('Backstory:')).toBeInTheDocument();
    expect(screen.getByText(/An experienced analyst\./)).toBeInTheDocument();

    // Tools section rendered
    expect(screen.getByText('Tools:')).toBeInTheDocument();
    expect(screen.getByText('SerperDevTool')).toBeInTheDocument();
    expect(screen.getByText('FileReadTool')).toBeInTheDocument();
  });

  it('renders minimal agent props with tools undefined', () => {
    render(<AgentCard agent={baseAgent} />);

    expect(screen.getByText('Researcher')).toBeInTheDocument();
    // Tools section should NOT render when tools is undefined
    expect(screen.queryByText('Tools:')).not.toBeInTheDocument();
  });

  it('does not render tools section when tools array is empty', () => {
    const agent: GeneratedAgent = {
      ...baseAgent,
      tools: [],
    };

    render(<AgentCard agent={agent} />);

    expect(screen.queryByText('Tools:')).not.toBeInTheDocument();
  });
});
