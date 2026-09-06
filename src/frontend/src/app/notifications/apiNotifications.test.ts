import { beforeEach, expect, it, vi } from 'vitest';
import toast from 'react-hot-toast';
import { reportApiFailure } from '../../shared/api/errors';
import './apiNotifications';

vi.mock('react-hot-toast', () => ({ default: { error: vi.fn() } }));

beforeEach(() => vi.clearAllMocks());

it('uses one stable toast identity for repeated database outages', () => {
  reportApiFailure({ status: 503, url: '/runs' });
  reportApiFailure({ status: 503, url: '/agents' });
  expect(toast.error).toHaveBeenCalledTimes(2);
  for (const args of vi.mocked(toast.error).mock.calls) {
    expect(args).toEqual([
      'Database connection issue — please try again shortly.',
      { id: 'lakebase-503', duration: 5000 },
    ]);
  }
});

it('leaves other HTTP errors to the calling feature', () => {
  reportApiFailure({ status: 404 });
  reportApiFailure({ status: 403 });
  expect(toast.error).not.toHaveBeenCalled();
});
