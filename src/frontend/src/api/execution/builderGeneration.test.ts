import { afterEach, expect, it, vi } from 'vitest';
import { apiClient } from '../../shared/api/client';
import { generateWithTrace } from './builderGeneration';
vi.mock('../../shared/api/client', () => ({ apiClient: { get: vi.fn(), post: vi.fn() } }));
afterEach(() => { vi.clearAllMocks(); vi.useRealTimers(); });

it('publishes the trace identity while generation is still running and returns its answer', async () => {
  vi.useFakeTimers();
  vi.mocked(apiClient.post).mockResolvedValue({ data: { generation_id: 'generation' } });
  vi.mocked(apiClient.get).mockResolvedValueOnce({ data: { status: 'RUNNING' } }).mockResolvedValueOnce({ data: { status: 'COMPLETED', result: { builder_result: { name: 'Plan' } } } });
  const started = vi.fn();
  const pending = generateWithTrace('flow', { prompt: 'Connect crews' }, started);
  await vi.advanceTimersByTimeAsync(0);
  expect(started).toHaveBeenCalledWith('generation');
  expect(apiClient.get).toHaveBeenCalledTimes(1);
  await vi.advanceTimersByTimeAsync(1000);
  await expect(pending).resolves.toEqual({ name: 'Plan' });
});

it('surfaces a failed generation without returning a fake plan', async () => {
  vi.mocked(apiClient.post).mockResolvedValue({ data: { generation_id: 'failed' } });
  vi.mocked(apiClient.get).mockResolvedValue({ data: { status: 'FAILED', error: 'Invalid routing' } });
  await expect(generateWithTrace('crew', {}, vi.fn())).rejects.toThrow('Invalid routing');
});

it('stops result polling when the caller aborts', async () => {
  vi.useFakeTimers();
  vi.mocked(apiClient.post).mockResolvedValue({ data: { generation_id: 'cancel' } });
  vi.mocked(apiClient.get).mockResolvedValue({ data: { status: 'RUNNING' } });
  const controller = new AbortController();
  const pending = generateWithTrace('flow', {}, vi.fn(), controller.signal);
  const assertion = expect(pending).rejects.toThrow();
  await vi.advanceTimersByTimeAsync(0);
  controller.abort();
  await assertion;
  await vi.advanceTimersByTimeAsync(3000);
  expect(apiClient.get).toHaveBeenCalledTimes(1);
});
