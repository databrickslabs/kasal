import { beforeEach, describe, expect, it, vi } from 'vitest';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import MLflowConfiguration from './MLflowConfiguration';
import { apiClient } from '../../../shared/api/client';
import { useMLflowStore } from '../../../store/mlflow';

vi.mock('../../../shared/api/client', () => ({ apiClient: { get: vi.fn(), patch: vi.fn() } }));

const settings = {
  enabled: false,
  evaluation_enabled: false,
  experiment_name: 'kasal-traces',
  backend: { kind: 'local', reachable: true },
};

beforeEach(() => {
  vi.clearAllMocks();
  useMLflowStore.setState({ enabled: null });
  vi.mocked(apiClient.get).mockResolvedValue({ data: settings });
});

describe('MLflow settings ownership', () => {
  it('enables tracing through MLflow settings and publishes the returned flag', async () => {
    vi.mocked(apiClient.patch).mockResolvedValue({ data: { ...settings, enabled: true } });
    render(<MLflowConfiguration />);
    fireEvent.click(await screen.findByRole('checkbox', { name: 'Tracing disabled' }));
    await waitFor(() => expect(apiClient.patch).toHaveBeenCalledWith('/mlflow/settings', { enabled: true }));
    expect(await screen.findByRole('checkbox', { name: 'Tracing enabled' })).toBeChecked();
    expect(useMLflowStore.getState().enabled).toBe(true);
  });

  it('persists evaluation separately when tracing is enabled', async () => {
    vi.mocked(apiClient.get).mockResolvedValue({ data: { ...settings, enabled: true } });
    vi.mocked(apiClient.patch).mockResolvedValue({ data: { ...settings, enabled: true, evaluation_enabled: true } });
    render(<MLflowConfiguration />);
    fireEvent.click(await screen.findByRole('checkbox', { name: 'Run LLM-judge evaluation on finished runs' }));
    await waitFor(() => expect(apiClient.patch).toHaveBeenCalledWith('/mlflow/settings', { evaluation_enabled: true }));
    await waitFor(() => expect(screen.getByRole('checkbox', { name: 'Run LLM-judge evaluation on finished runs' })).toBeChecked());
  });

  it.each([
    [404, false], [500, false], [404, true], [500, true],
  ])('shows a save error for status %s (evaluation: %s)', async (status, evaluation) => {
    vi.mocked(apiClient.get).mockResolvedValue({ data: { ...settings, enabled: evaluation } });
    vi.mocked(apiClient.patch).mockRejectedValue({ response: { status } });
    render(<MLflowConfiguration />);
    const name = evaluation ? 'Run LLM-judge evaluation on finished runs' : 'Tracing disabled';
    fireEvent.click(await screen.findByRole('checkbox', { name }));
    expect(await screen.findByText('Could not save MLflow settings.')).toBeInTheDocument();
    expect(screen.getByRole('checkbox', { name })).not.toBeChecked();
  });
});

describe('Open MLflow destination', () => {
  const hosted = {
    ...settings,
    enabled: true,
    installation_managed: true,
    backend: {
      kind: 'databricks', available: true, uri: 'https://example.com/',
      experiment: '/Shared/personal-traces-uc', url: 'https://example.com/ml/experiments',
    },
  };

  it('resolves the teamspace experiment instead of opening all experiments', async () => {
    const replace = vi.fn();
    const tab = { opener: {}, closed: false, location: { replace }, close: vi.fn() };
    const open = vi.spyOn(window, 'open').mockReturnValue(tab as unknown as Window);
    vi.mocked(apiClient.get).mockResolvedValueOnce({ data: hosted })
      .mockResolvedValueOnce({ data: { experiment_id: '1234' } });
    render(<MLflowConfiguration />);
    fireEvent.click(await screen.findByRole('button', { name: 'Open MLflow' }));
    await waitFor(() => expect(replace).toHaveBeenCalledWith('https://example.com/ml/experiments/1234/traces'));
    expect(apiClient.get).toHaveBeenLastCalledWith('/mlflow/experiment-info');
    expect(tab.opener).toBeNull();
    open.mockRestore();
  });

  it('offers the resolved link when the browser blocks the new tab', async () => {
    const open = vi.spyOn(window, 'open').mockReturnValue(null);
    vi.mocked(apiClient.get).mockResolvedValueOnce({ data: hosted })
      .mockResolvedValueOnce({ data: { experiment_id: '5678' } });
    render(<MLflowConfiguration />);
    fireEvent.click(await screen.findByRole('button', { name: 'Open MLflow' }));
    expect(await screen.findByRole('link', { name: 'Open tracing experiment' }))
      .toHaveAttribute('href', 'https://example.com/ml/experiments/5678/traces');
    open.mockRestore();
  });

  it.each([null, new Error('denied')])('does not fall back to all experiments on a lookup failure (%s)', async (failure) => {
    const close = vi.fn();
    const replace = vi.fn();
    const open = vi.spyOn(window, 'open').mockReturnValue({ close, location: { replace } } as unknown as Window);
    vi.mocked(apiClient.get).mockResolvedValueOnce({ data: hosted });
    if (failure) vi.mocked(apiClient.get).mockRejectedValueOnce(failure);
    else vi.mocked(apiClient.get).mockResolvedValueOnce({ data: {} });
    render(<MLflowConfiguration />);
    fireEvent.click(await screen.findByRole('button', { name: 'Open MLflow' }));
    expect(await screen.findByText(/Could not open the tracing experiment/)).toBeInTheDocument();
    expect(close).toHaveBeenCalled();
    expect(replace).not.toHaveBeenCalled();
    open.mockRestore();
  });
});
