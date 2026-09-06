import { describe, expect, it } from 'vitest';
import { pendingActionsBelongTo } from './pendingActions';

describe('pendingActionsBelongTo', () => {
  it('posts for the run it was armed for', () => {
    expect(pendingActionsBelongTo({ jobId: 'B', ownerSession: 's1' }, 'B', 's1')).toBe(true);
  });

  it('does NOT post when a different job completes (the re-sent-question case)', () => {
    // Armed for the new run B; the previous run A of the same session finishes late.
    expect(pendingActionsBelongTo({ jobId: 'B', ownerSession: 's1' }, 'A', 's1')).toBe(false);
  });

  it('does NOT post for a completion owned by another session', () => {
    expect(pendingActionsBelongTo({ ownerSession: 's1' }, 'A', 's2')).toBe(false);
  });

  it('falls back to the session when the job id is not known yet', () => {
    expect(pendingActionsBelongTo({ ownerSession: 's1' }, 'A', 's1')).toBe(true);
    expect(pendingActionsBelongTo({}, 'A', null)).toBe(true);
  });
});
