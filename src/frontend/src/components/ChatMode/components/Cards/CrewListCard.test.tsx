import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import CrewListCard from './CrewListCard';
import { CatalogListResult } from '../../types/dispatcher';

describe('CrewListCard', () => {
  it('renders "No crews found." when plans is undefined', () => {
    const data = { type: 'catalog_list', message: '' } as unknown as CatalogListResult;
    render(<CrewListCard data={data} />);
    expect(screen.getByText('No crews found.')).toBeInTheDocument();
  });

  it('renders "No crews found." when plans is an empty array', () => {
    const data: CatalogListResult = {
      type: 'catalog_list',
      plans: [],
      message: '',
    };
    render(<CrewListCard data={data} />);
    expect(screen.getByText('No crews found.')).toBeInTheDocument();
  });

  it('renders plans with singular agent/task labels when counts are 1', () => {
    const data: CatalogListResult = {
      type: 'catalog_list',
      plans: [
        { id: 'p1', name: 'Crew One', agent_count: 1, task_count: 1 },
      ],
      message: '',
    };
    render(<CrewListCard data={data} />);
    expect(screen.getByText('Crew One')).toBeInTheDocument();
    expect(screen.getByText('1 agent')).toBeInTheDocument();
    expect(screen.getByText('1 task')).toBeInTheDocument();
  });

  it('renders plans with plural agent/task labels when counts are not 1', () => {
    const data: CatalogListResult = {
      type: 'catalog_list',
      plans: [
        { id: 'p2', name: 'Crew Two', agent_count: 3, task_count: 5 },
      ],
      message: '',
    };
    render(<CrewListCard data={data} />);
    expect(screen.getByText('3 agents')).toBeInTheDocument();
    expect(screen.getByText('5 tasks')).toBeInTheDocument();
  });

  it('defaults counts to 0 (plural) when agent_count/task_count are undefined', () => {
    const data: CatalogListResult = {
      type: 'catalog_list',
      plans: [
        { id: 'p3', name: 'Crew Three' },
      ],
      message: '',
    };
    render(<CrewListCard data={data} />);
    expect(screen.getByText('0 agents')).toBeInTheDocument();
    expect(screen.getByText('0 tasks')).toBeInTheDocument();
  });

  it('invokes onCommand with the run command when the play button is clicked', () => {
    const onCommand = vi.fn();
    const data: CatalogListResult = {
      type: 'catalog_list',
      plans: [
        { id: 'p4', name: 'Crew Four', agent_count: 2, task_count: 2 },
      ],
      message: '',
    };
    render(<CrewListCard data={data} onCommand={onCommand} />);
    const button = screen.getByTitle('Run Crew Four');
    fireEvent.click(button);
    expect(onCommand).toHaveBeenCalledTimes(1);
    expect(onCommand).toHaveBeenCalledWith('/run crew Crew Four');
  });

  it('does not throw when the play button is clicked without an onCommand handler', () => {
    const data: CatalogListResult = {
      type: 'catalog_list',
      plans: [
        { id: 'p5', name: 'Crew Five', agent_count: 0, task_count: 0 },
      ],
      message: '',
    };
    render(<CrewListCard data={data} />);
    const button = screen.getByTitle('Run Crew Five');
    expect(() => fireEvent.click(button)).not.toThrow();
  });
});
