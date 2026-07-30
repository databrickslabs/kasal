import { describe, expect, it, vi, beforeEach } from 'vitest';
import ExecutionCheckpointService from './ExecutionCheckpointService';
import { apiClient } from '../../config/api/ApiConfig';

vi.mock('../../config/api/ApiConfig', () => ({
  apiClient: {
    get: vi.fn(),
    post: vi.fn(),
    delete: vi.fn(),
  },
}));

const mockedClient = apiClient as unknown as {
  get: ReturnType<typeof vi.fn>;
  post: ReturnType<typeof vi.fn>;
  delete: ReturnType<typeof vi.fn>;
};

describe('ExecutionCheckpointService', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('getCheckpoint', () => {
    it('reads the checkpoint off the execution, not off a flow', async () => {
      mockedClient.get.mockResolvedValue({ data: { job_id: 'job-1', kind: 'crew' } });

      const result = await ExecutionCheckpointService.getCheckpoint('job-1');

      expect(mockedClient.get).toHaveBeenCalledWith('/executions/job-1/checkpoints');
      expect(result).toEqual({ job_id: 'job-1', kind: 'crew' });
    });

    it('treats a 404 as "no checkpoint" rather than an error', async () => {
      mockedClient.get.mockRejectedValue({ response: { status: 404 } });

      // A run with nothing to resume is an ordinary answer, not a failure.
      await expect(ExecutionCheckpointService.getCheckpoint('job-1')).resolves.toBeNull();
    });

    it('still throws on a real failure', async () => {
      mockedClient.get.mockRejectedValue({ response: { status: 500 } });

      await expect(ExecutionCheckpointService.getCheckpoint('job-1')).rejects.toBeTruthy();
    });
  });

  describe('getUnit', () => {
    it('encodes the unit key', async () => {
      mockedClient.get.mockResolvedValue({ data: { key: 'a/b' } });

      await ExecutionCheckpointService.getUnit('job-1', 'a/b');

      expect(mockedClient.get).toHaveBeenCalledWith(
        '/executions/job-1/checkpoints/a%2Fb',
      );
    });
  });

  describe('resume', () => {
    it('sends no boundary when resuming from the crash point', async () => {
      mockedClient.post.mockResolvedValue({ data: { execution_id: 'new-job' } });

      const result = await ExecutionCheckpointService.resume('job-1');

      expect(mockedClient.post).toHaveBeenCalledWith('/executions/job-1/resume', {});
      // The resumed run is a NEW execution.
      expect(result.execution_id).toBe('new-job');
    });

    it('sends from_unit when rewinding further back', async () => {
      mockedClient.post.mockResolvedValue({ data: { execution_id: 'new-job' } });

      await ExecutionCheckpointService.resume('job-1', '2');

      expect(mockedClient.post).toHaveBeenCalledWith('/executions/job-1/resume', {
        from_unit: '2',
      });
    });
  });

  describe('expire', () => {
    it('deletes against the execution', async () => {
      mockedClient.delete.mockResolvedValue({ data: {} });

      await ExecutionCheckpointService.expire('job-1');

      expect(mockedClient.delete).toHaveBeenCalledWith('/executions/job-1/checkpoints');
    });
  });
});
