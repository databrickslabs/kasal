import { describe, it, expect, vi, beforeEach } from 'vitest';

const setSessionRunningJob = vi.fn();
const getSessionRunningJob = vi.fn();
const clearSessionRunningJob = vi.fn();

vi.mock('../db/sessionDb', () => ({
  setSessionRunningJob: (...a: unknown[]) => setSessionRunningJob(...a),
  getSessionRunningJob: (...a: unknown[]) => getSessionRunningJob(...a),
  clearSessionRunningJob: (...a: unknown[]) => clearSessionRunningJob(...a),
}));

import {
  persistActiveExecution,
  readActiveExecution,
  clearActiveExecution,
} from './activeExecutionMarker';

const flush = () => new Promise((r) => setTimeout(r, 0));

beforeEach(() => {
  vi.clearAllMocks();
});

describe('activeExecutionMarker', () => {
  it('persistActiveExecution writes the marker (fire-and-forget)', () => {
    setSessionRunningJob.mockResolvedValue(undefined);
    persistActiveExecution('s1', 'job1');
    expect(setSessionRunningJob).toHaveBeenCalledWith('s1', 'job1');
  });

  it('persistActiveExecution swallows write errors', async () => {
    setSessionRunningJob.mockRejectedValue(new Error('idb down'));
    expect(() => persistActiveExecution('s1', 'job1')).not.toThrow();
    await flush(); // let the .catch run
  });

  it('readActiveExecution returns the stored job id', async () => {
    getSessionRunningJob.mockResolvedValue('job9');
    expect(await readActiveExecution('s1')).toBe('job9');
  });

  it('readActiveExecution returns null on error', async () => {
    getSessionRunningJob.mockRejectedValue(new Error('boom'));
    expect(await readActiveExecution('s1')).toBeNull();
  });

  it('clearActiveExecution clears the marker', () => {
    clearSessionRunningJob.mockResolvedValue(undefined);
    clearActiveExecution('s1');
    expect(clearSessionRunningJob).toHaveBeenCalledWith('s1');
  });

  it('clearActiveExecution swallows errors', async () => {
    clearSessionRunningJob.mockRejectedValue(new Error('boom'));
    expect(() => clearActiveExecution('s1')).not.toThrow();
    await flush();
  });
});
