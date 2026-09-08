import { beforeEach, describe, expect, it, vi } from 'vitest';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { apiClient } from '../../../../shared/api/client';
import { useMLflowStore } from '../../../../store/mlflow';
import MLflowRunAction from './MLflowRunAction';

vi.mock('../../../../shared/api/client', () => ({ apiClient: { get: vi.fn() } }));

describe('MLflow run action', () => {
  beforeEach(() => { vi.restoreAllMocks(); vi.mocked(apiClient.get).mockReset(); useMLflowStore.setState({ enabled: true }); });

  it('opens the selected run trace in a safely detached tab', async () => {
    const tab = { opener: {}, closed: false, location: { replace: vi.fn() }, close: vi.fn() };
    vi.spyOn(window, 'open').mockReturnValue(tab as unknown as Window);
    vi.mocked(apiClient.get).mockResolvedValue({ data: { url: 'https://example.com/ml/experiments/42/traces?selectedEvaluationId=run-a', experiment_id: '42' } });
    render(<MLflowRunAction executionId="run-a" />);
    fireEvent.click(screen.getByRole('button', { name: 'MLflow' }));
    expect(tab.opener).toBeNull();
    expect(apiClient.get).toHaveBeenCalledWith('/mlflow/trace-link', { params: { job_id: 'run-a' } });
    await waitFor(() => expect(tab.location.replace).toHaveBeenCalledWith(expect.stringContaining('selectedEvaluationId=run-a')));
  });

  it('offers a normal link when popups are blocked', async () => {
    vi.spyOn(window, 'open').mockReturnValue(null);
    vi.mocked(apiClient.get).mockResolvedValue({ data: { url: 'https://example.com/traces', experiment_id: '42' } });
    render(<MLflowRunAction executionId="run-a" />);
    fireEvent.click(screen.getByRole('button', { name: 'MLflow' }));
    expect(await screen.findByRole('link', { name: 'Open MLflow trace' })).toHaveAttribute('rel', 'noopener noreferrer');
  });

  it('does not redirect to the all-experiments page when no destination was resolved', async () => {
    const close = vi.fn(); const replace = vi.fn();
    vi.spyOn(window, 'open').mockReturnValue({ close, location: { replace } } as unknown as Window);
    vi.mocked(apiClient.get).mockResolvedValue({ data: { url: 'https://example.com/ml/experiments', experiment_id: '' } });
    render(<MLflowRunAction executionId="run-a" />);
    fireEvent.click(screen.getByRole('button', { name: 'MLflow' }));
    expect(await screen.findByRole('alert')).toHaveTextContent('unavailable');
    expect(close).toHaveBeenCalled(); expect(replace).not.toHaveBeenCalled();
  });

  it('hides when tracing is disabled and respects the running state', () => {
    const view = render(<MLflowRunAction executionId="run-a" disabled />);
    expect(screen.getByRole('button')).toBeDisabled();
    useMLflowStore.setState({ enabled: false });
    view.rerender(<MLflowRunAction executionId="run-a" />);
    expect(screen.queryByRole('button')).not.toBeInTheDocument();
    expect(apiClient.get).not.toHaveBeenCalled();
  });
});
