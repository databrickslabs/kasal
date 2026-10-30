/**
 * The dialog is mounted once for the whole workspace, so each opening must ask
 * for the variables of the crew or flow being run now, not the first one.
 */
import React from 'react';
import { fireEvent, render, screen, within } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import type { Node } from 'reactflow';
import { InputVariablesDialog } from './InputVariablesDialog';

const task = (id: string, description: string): Node => ({
  id, type: 'taskNode', position: { x: 0, y: 0 }, data: { description },
});

/** The value field on the row whose name field reads `name`. */
const valueOf = (name: string) => {
  const row = screen.getByDisplayValue(name).closest('.MuiBox-root') as HTMLElement;
  return within(row).getAllByRole('textbox')[1];
};
const execute = () => fireEvent.click(screen.getByText('Execute with Variables'));

describe('InputVariablesDialog reopened for another run', () => {
  it('shows crew B\'s variables after it was used for crew A', () => {
    const onConfirm = vi.fn();
    const crewA = [task('a', 'Research {topic}')];
    const crewB = [task('b', 'Profile {company} for {year}')];
    const { rerender } = render(<InputVariablesDialog open onClose={vi.fn()} onConfirm={onConfirm} nodes={crewA} />);
    fireEvent.change(valueOf('topic'), { target: { value: 'AI' } });
    execute();
    expect(onConfirm).toHaveBeenLastCalledWith({ topic: 'AI' });

    rerender(<InputVariablesDialog open={false} onClose={vi.fn()} onConfirm={onConfirm} nodes={crewA} />);
    rerender(<InputVariablesDialog open onClose={vi.fn()} onConfirm={onConfirm} nodes={crewB} />);

    expect(screen.getByDisplayValue('company')).toBeInTheDocument();
    expect(screen.getByDisplayValue('year')).toBeInTheDocument();
    expect(screen.queryByDisplayValue('topic')).not.toBeInTheDocument();
    // B's variables are required, so B cannot start with A's inputs.
    onConfirm.mockClear();
    execute();
    expect(onConfirm).not.toHaveBeenCalled();
  });

  it('adds a placeholder introduced since the last run and keeps the earlier value', () => {
    const before = [task('t', 'Research {topic}')];
    const after = [task('t', 'Research {topic} in {region}')];
    const { rerender } = render(<InputVariablesDialog open onClose={vi.fn()} onConfirm={vi.fn()} nodes={before} />);
    fireEvent.change(valueOf('topic'), { target: { value: 'AI' } });

    rerender(<InputVariablesDialog open={false} onClose={vi.fn()} onConfirm={vi.fn()} nodes={before} />);
    rerender(<InputVariablesDialog open onClose={vi.fn()} onConfirm={vi.fn()} nodes={after} />);

    expect(screen.getByDisplayValue('region')).toBeInTheDocument();
    expect(valueOf('topic')).toHaveValue('AI');
  });
});
