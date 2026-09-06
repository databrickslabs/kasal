import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
  createExecution,
  listExecutions,
  getExecutionStatus,
  getExecution,
  stopExecution,
  getJobTraces,
} from './executions';
import { getClient } from './client';
import type { Execution, ExecutionConfig } from '../types/execution';

vi.mock('./client', () => ({
  getClient: vi.fn(),
}));

const mockedGetClient = vi.mocked(getClient);

describe('ChatMode executions api', () => {
  let post: ReturnType<typeof vi.fn>;
  let get: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    post = vi.fn();
    get = vi.fn();
    mockedGetClient.mockReturnValue({ post, get } as unknown as ReturnType<typeof getClient>);
  });

  describe('createExecution', () => {
    it('POSTs to /executions with the config body and returns data', async () => {
      const config = { inputs: { foo: 'bar' } } as unknown as ExecutionConfig;
      const execution = { id: 'exec-1' } as unknown as Execution;
      post.mockResolvedValue({ data: execution });

      const result = await createExecution(config);

      expect(post).toHaveBeenCalledWith('/executions', config);
      expect(result).toBe(execution);
    });
  });

  describe('listExecutions', () => {
    it('GETs /executions with the default limit of 20', async () => {
      const executions = [{ id: 'a' }] as unknown as Execution[];
      get.mockResolvedValue({ data: executions });

      const result = await listExecutions();

      expect(get).toHaveBeenCalledWith('/executions', { params: { limit: 20 } });
      expect(result).toBe(executions);
    });

    it('GETs /executions with a custom limit', async () => {
      const executions = [{ id: 'b' }, { id: 'c' }] as unknown as Execution[];
      get.mockResolvedValue({ data: executions });

      const result = await listExecutions(5);

      expect(get).toHaveBeenCalledWith('/executions', { params: { limit: 5 } });
      expect(result).toBe(executions);
    });
  });

  describe('getExecutionStatus', () => {
    it('GETs /executions/:id/status and returns data', async () => {
      const execution = { id: 'exec-9', status: 'running' } as unknown as Execution;
      get.mockResolvedValue({ data: execution });

      const result = await getExecutionStatus('exec-9');

      expect(get).toHaveBeenCalledWith('/executions/exec-9/status');
      expect(result).toBe(execution);
    });
  });

  describe('getExecution', () => {
    it('GETs /executions/:id (full record incl. result) and returns data', async () => {
      const execution = { id: 'exec-5', result: { messages: [] } } as unknown as Execution;
      get.mockResolvedValue({ data: execution });

      const result = await getExecution('exec-5');

      expect(get).toHaveBeenCalledWith('/executions/exec-5');
      expect(result).toBe(execution);
    });
  });

  describe('stopExecution', () => {
    it('POSTs to /executions/:id/stop with the graceful stop payload', async () => {
      post.mockResolvedValue({ data: undefined });

      const result = await stopExecution('exec-7');

      expect(post).toHaveBeenCalledWith('/executions/exec-7/stop', {
        stop_type: 'graceful',
        reason: 'Stopped by user',
        preserve_partial_results: true,
      });
      expect(result).toBeUndefined();
    });
  });

  describe('getJobTraces', () => {
    const trace = (id: number) => ({ id, event_type: 'tool_usage' });

    it('fetches a single short page with an explicit limit', async () => {
      get.mockResolvedValue({ data: { traces: [trace(1), trace(2)] } });

      const result = await getJobTraces('job-1');

      expect(get).toHaveBeenCalledTimes(1);
      expect(get).toHaveBeenCalledWith('/traces/job/job-1', {
        params: { limit: 500, offset: 0 },
      });
      expect(result).toHaveLength(2);
    });

    it('pages through long runs until a short page (regression: backend default limit=100 truncated restored activity)', async () => {
      const fullPage = Array.from({ length: 500 }, (_, i) => trace(i));
      const shortPage = [trace(500), trace(501)];
      get
        .mockResolvedValueOnce({ data: { traces: fullPage } })
        .mockResolvedValueOnce({ data: { traces: shortPage } });

      const result = await getJobTraces('job-long');

      expect(get).toHaveBeenCalledTimes(2);
      expect(get).toHaveBeenNthCalledWith(1, '/traces/job/job-long', {
        params: { limit: 500, offset: 0 },
      });
      expect(get).toHaveBeenNthCalledWith(2, '/traces/job/job-long', {
        params: { limit: 500, offset: 500 },
      });
      expect(result).toHaveLength(502);
    });

    it('stops at the hard cap even if the server keeps returning full pages', async () => {
      const fullPage = Array.from({ length: 500 }, (_, i) => trace(i));
      get.mockResolvedValue({ data: { traces: fullPage } });

      const result = await getJobTraces('job-runaway');

      expect(get).toHaveBeenCalledTimes(30); // 15000 / 500
      expect(result).toHaveLength(15000);
    });
  });
});
