import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { apiClient } from '../../shared/api/client';
import { DeckService } from './DeckService';

vi.mock('../../shared/api/client', () => ({ apiClient: { post: vi.fn(), get: vi.fn() } }));
vi.mock('../../features/chat/store/executionStore', () => ({
  useExecutionStore: { getState: () => ({ selectedMcpServers: ['browser'], selectedAgentBricksEndpoints: ['researcher'], selectedSkills: ['research'] }) },
}));

beforeEach(() => {
  vi.useFakeTimers();
  vi.mocked(apiClient.post).mockResolvedValue({ data: { job_id: 'run-1' } });
});
afterEach(() => { vi.useRealTimers(); vi.clearAllMocks(); });

describe('slide agent execution', () => {
  it('forwards capabilities and exposes the run before completion', async () => {
    const started = vi.fn();
    vi.mocked(apiClient.get)
      .mockResolvedValueOnce({ data: { status: 'RUNNING' } })
      .mockResolvedValueOnce({ data: { status: 'COMPLETED', result: { section: '<section class="slide">New</section>' } } });
    const result = DeckService.refineSlide({ mode: 'refine', instruction: 'Search online', slide: 'old' }, started);
    await vi.advanceTimersByTimeAsync(0);
    expect(started).toHaveBeenCalledWith('run-1');
    expect(apiClient.post).toHaveBeenCalledWith('/decks/slides/refine/start', expect.objectContaining({
      mcp_servers: ['browser'], agentbricks_endpoints: ['researcher'], skills: ['research'],
    }), expect.anything());
    await vi.advanceTimersByTimeAsync(2000);
    expect((await result).section).toContain('New');
  });

  it('retries interrupted reads without launching another agent', async () => {
    vi.mocked(apiClient.get).mockRejectedValueOnce(new Error('network'))
      .mockResolvedValueOnce({ data: { status: 'FAILED', error: 'Browser unavailable' } });
    const result = DeckService.refineSlide({ mode: 'add', instruction: 'Research' });
    await vi.advanceTimersByTimeAsync(2000);
    expect(await result).toEqual({ section: null, job_id: 'run-1', error: 'Browser unavailable' });
    expect(apiClient.post).toHaveBeenCalledTimes(1);
  });
});
