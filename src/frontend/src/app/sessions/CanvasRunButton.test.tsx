import { describe, it, expect, vi, afterEach } from 'vitest';
import { render, screen, fireEvent, act } from '@testing-library/react';
import CanvasRunButton from './CanvasRunButton';
import { useBuilderExecutionControls } from '../../store/builderExecutionControls';

const edge = { id: 'link', source: 'one', target: 'two' };
afterEach(() => useBuilderExecutionControls.setState({ crew: null, flow: null }));

describe('canvas execution control', () => {
  it.each(['crew', 'flow'] as const)('runs and stops the active %s', mode => {
    const run = vi.fn();
    const stop = vi.fn().mockResolvedValue(undefined);
    render(<CanvasRunButton mode={mode} hasNodes edges={[]} onRun={run} />);
    fireEvent.click(screen.getByRole('button', { name: mode === 'flow' ? 'Run Flow' : 'Run Crew' }));
    expect(run).toHaveBeenCalledOnce();
    act(() => useBuilderExecutionControls.setState({ [mode]: { jobId: 'active-job', stopping: false, stop } }));
    fireEvent.click(screen.getByRole('button', { name: mode === 'flow' ? 'Stop Flow' : 'Stop Crew' }));
    expect(stop).toHaveBeenCalledOnce();
    act(() => useBuilderExecutionControls.setState({ [mode]: { jobId: 'active-job', stopping: true, stop } }));
    expect(screen.getByRole('button', { name: 'Stopping execution' })).toBeDisabled();
    act(() => useBuilderExecutionControls.setState({ [mode]: null }));
    expect(screen.getByRole('button', { name: mode === 'flow' ? 'Run Flow' : 'Run Crew' })).toBeEnabled();
  });

  it('requires nodes and complete flow connections before running', () => {
    const run = vi.fn();
    const { rerender } = render(<CanvasRunButton mode="flow" hasNodes={false} edges={[]} onRun={run} />);
    expect(screen.getByRole('button')).toBeDisabled();
    rerender(<CanvasRunButton mode="flow" hasNodes edges={[edge]} onRun={run} />);
    expect(screen.getByRole('button', { name: 'Configure flow connections to run' })).toBeDisabled();
    rerender(<CanvasRunButton mode="flow" hasNodes edges={[{ ...edge, data: { listenToTaskIds: ['source-task'], targetTaskIds: ['target-task'] } }]} onRun={run} />);
    expect(screen.getByRole('button', { name: 'Run Flow' })).toBeEnabled();
    expect(run).not.toHaveBeenCalled();
  });

  it('can stop even if the canvas is emptied and does not stop another mode', () => {
    const stop = vi.fn().mockResolvedValue(undefined);
    useBuilderExecutionControls.setState({ crew: { jobId: 'crew-job', stopping: false, stop } });
    const { rerender } = render(<CanvasRunButton mode="crew" hasNodes={false} edges={[]} onRun={vi.fn()} />);
    expect(screen.getByRole('button', { name: 'Stop Crew' })).toBeEnabled();
    rerender(<CanvasRunButton mode="flow" hasNodes edges={[]} onRun={vi.fn()} />);
    expect(screen.getByRole('button', { name: 'Run Flow' })).toBeEnabled();
    expect(stop).not.toHaveBeenCalled();
  });
});
