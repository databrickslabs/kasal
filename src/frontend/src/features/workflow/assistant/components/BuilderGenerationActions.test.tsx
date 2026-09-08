import { beforeEach, expect, it, vi } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import { apiClient } from '../../../../shared/api/client';
import { useMLflowStore } from '../../../../store/mlflow';
import BuilderGenerationActions from './BuilderGenerationActions';

vi.mock('../../../../shared/api/client', () => ({ apiClient: { get: vi.fn() } }));
beforeEach(() => { vi.mocked(apiClient.get).mockReset(); useMLflowStore.setState({ enabled: true }); });

it('shows the link after a completed builder plan', async () => {
  vi.mocked(apiClient.get).mockResolvedValue({ data: { status: 'COMPLETED', result: { builder_result: { nodes: [] } } } });
  render(<BuilderGenerationActions jobId="plan" />);
  expect(await screen.findByRole('button', { name: 'MLflow' })).toBeVisible();
});

it.each(['RUNNING', 'FAILED', 'COMPLETED'])('does not add duplicate workload actions for %s runs', async status => {
  vi.mocked(apiClient.get).mockResolvedValue({ data: { status, result: { output: 'answer' } } });
  render(<BuilderGenerationActions jobId="workload" />);
  await waitFor(() => expect(apiClient.get).toHaveBeenCalled());
  expect(screen.queryByRole('button', { name: 'MLflow' })).not.toBeInTheDocument();
});
