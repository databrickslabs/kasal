import { beforeEach, describe, expect, it, vi } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { UCMVResultWithAutoSave } from './UCMVResultWithAutoSave';
import type { UCMVResult } from './UCMVResultViewer';
import { runService } from '../../../api/execution/ExecutionHistoryService';

vi.mock('../../../api/execution/ExecutionHistoryService', () => ({
  runService: { updateExecutionResult: vi.fn() },
}));
vi.mock('./UCMVResultViewer', () => ({
  default: ({ result, editable, onSave }: {
    result: UCMVResult;
    editable: boolean;
    onSave?: (result: UCMVResult) => Promise<void>;
  }) => <button disabled={!editable} onClick={() => onSave?.(result)}>Save edit</button>,
}));

const result = { yaml: {}, sql: {}, stats: {} } as UCMVResult;

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(runService.updateExecutionResult).mockResolvedValue({
    success: true, job_id: 'job-1', updated_at: '',
  });
});

describe('metric-view result persistence', () => {
  it('saves once across rerenders and remounts, then saves the next job', async () => {
    const savedJobRef = { current: null as string | null };
    const onSave = vi.fn();
    const props = { result, jobId: 'job-1', savedJobRef, onSave };
    const view = render(<UCMVResultWithAutoSave {...props} />);
    await waitFor(() => expect(runService.updateExecutionResult).toHaveBeenCalledTimes(1));
    view.rerender(<UCMVResultWithAutoSave {...props} result={{ ...result }} />);
    view.unmount();
    const reopened = render(<UCMVResultWithAutoSave {...props} />);
    expect(runService.updateExecutionResult).toHaveBeenCalledTimes(1);
    reopened.rerender(<UCMVResultWithAutoSave {...props} jobId="job-2" />);
    await waitFor(() => expect(runService.updateExecutionResult).toHaveBeenCalledTimes(2));
    expect(runService.updateExecutionResult).toHaveBeenLastCalledWith('job-2', result);
    fireEvent.click(screen.getByRole('button', { name: 'Save edit' }));
    expect(onSave).toHaveBeenCalledWith(result);
  });

  it('does not persist or enable edits without a saved execution', () => {
    render(<UCMVResultWithAutoSave result={result} jobId="" savedJobRef={{ current: null }} onSave={vi.fn()} />);
    expect(runService.updateExecutionResult).not.toHaveBeenCalled();
    expect(screen.getByRole('button', { name: 'Save edit' })).toBeDisabled();
  });

  it('handles a failed auto-save without retrying on every render', async () => {
    vi.mocked(runService.updateExecutionResult).mockRejectedValue(new Error('offline'));
    const savedJobRef = { current: null as string | null };
    const view = render(<UCMVResultWithAutoSave result={result} jobId="job-1" savedJobRef={savedJobRef} onSave={vi.fn()} />);
    await waitFor(() => expect(runService.updateExecutionResult).toHaveBeenCalledTimes(1));
    view.rerender(<UCMVResultWithAutoSave result={{ ...result }} jobId="job-1" savedJobRef={savedJobRef} onSave={vi.fn()} />);
    expect(runService.updateExecutionResult).toHaveBeenCalledTimes(1);
  });
});
