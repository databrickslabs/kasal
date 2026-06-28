import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import FlowListCard from './FlowListCard';
import { FlowListResult } from '../../types/dispatcher';

describe('FlowListCard', () => {
  it('renders "No flows found." when flows is an empty array', () => {
    const data: FlowListResult = {
      type: 'flow_list',
      flows: [],
      message: 'none',
    };
    render(<FlowListCard data={data} />);
    expect(screen.getByText('No flows found.')).toBeInTheDocument();
  });

  it('renders "No flows found." when flows is undefined', () => {
    // Force the falsy `data.flows` branch.
    const data = {
      type: 'flow_list',
      message: 'none',
    } as unknown as FlowListResult;
    render(<FlowListCard data={data} />);
    expect(screen.getByText('No flows found.')).toBeInTheDocument();
  });

  it('renders a list of flows with singular and plural node counts', () => {
    const data: FlowListResult = {
      type: 'flow_list',
      flows: [
        { id: '1', name: 'Alpha', node_count: 1 }, // singular -> "1 node"
        { id: '2', name: 'Beta', node_count: 3 }, // plural -> "3 nodes"
        { id: '3', name: 'Gamma' }, // undefined -> 0 -> "0 nodes"
      ],
      message: '3 flows',
    };
    render(<FlowListCard data={data} />);

    expect(screen.getByText('Alpha')).toBeInTheDocument();
    expect(screen.getByText('Beta')).toBeInTheDocument();
    expect(screen.getByText('Gamma')).toBeInTheDocument();

    expect(screen.getByText('1 node')).toBeInTheDocument();
    expect(screen.getByText('3 nodes')).toBeInTheDocument();
    expect(screen.getByText('0 nodes')).toBeInTheDocument();
  });

  it('calls onCommand with the run command when the play button is clicked', () => {
    const onCommand = vi.fn();
    const data: FlowListResult = {
      type: 'flow_list',
      flows: [{ id: '1', name: 'Alpha', node_count: 2 }],
      message: '1 flow',
    };
    render(<FlowListCard data={data} onCommand={onCommand} />);

    fireEvent.click(screen.getByTitle('Run Alpha'));
    expect(onCommand).toHaveBeenCalledTimes(1);
    expect(onCommand).toHaveBeenCalledWith('/run flow Alpha');
  });

  it('does not throw when the play button is clicked without an onCommand handler', () => {
    const data: FlowListResult = {
      type: 'flow_list',
      flows: [{ id: '1', name: 'Alpha', node_count: 2 }],
      message: '1 flow',
    };
    render(<FlowListCard data={data} />);

    expect(() => fireEvent.click(screen.getByTitle('Run Alpha'))).not.toThrow();
  });
});
