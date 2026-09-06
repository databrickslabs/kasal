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

  // The card renders the agent-builder one-line format: **Name** — Role <goal>.
  // No field labels, and backstory/tools are intentionally NOT shown.
  it('renders name, role and goal as one clean line', () => {
    const { container } = render(<AgentCard agent={{ ...baseAgent, tools: ['SerperDevTool'] }} />);

    expect(screen.getByText('Researcher')).toBeInTheDocument(); // name (bold span)
    expect(container.textContent).toContain('Senior Research Analyst');
    expect(container.textContent).toContain('Find the best information');
  });

  it('does NOT show field labels, backstory, or tools', () => {
    const { container } = render(<AgentCard agent={{ ...baseAgent, tools: ['SerperDevTool', 'FileReadTool'] }} />);

    expect(screen.queryByText('Goal:')).not.toBeInTheDocument();
    expect(screen.queryByText('Backstory:')).not.toBeInTheDocument();
    expect(screen.queryByText('Tools:')).not.toBeInTheDocument();
    expect(container.textContent).not.toContain('An experienced analyst');
    expect(container.textContent).not.toContain('SerperDevTool');
  });

  it('renders just the name when role/goal are absent', () => {
    const { container } = render(<AgentCard agent={{ name: 'Solo', role: '', goal: '', backstory: '' }} />);
    expect(screen.getByText('Solo')).toBeInTheDocument();
    expect(container.textContent?.trim()).toBe('Solo');
  });
});
