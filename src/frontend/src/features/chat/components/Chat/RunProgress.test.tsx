import { describe, expect, it, vi } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
import RunProgress from './RunProgress';
import { useRunTimeline } from '../../hooks/useRunTimeline';
vi.mock('../../hooks/useRunTimeline', () => ({ useRunTimeline: vi.fn(() => ({ processed: null, loading: false })) }));
vi.mock('../Preview/RunTraceTimeline', () => ({ default: () => null }));

describe('restoring run activity', () => {
  it('keeps a previously expanded live trace open after returning to a completed run', () => {
    const view = render(<RunProgress inline jobId="return-to-live-plan" running generating={false} />);
    expect(screen.getByRole('button', { name: 'Collapse run activity' })).toBeVisible();
    view.unmount();
    render(<RunProgress inline jobId="return-to-live-plan" running={false} generating={false} />);
    expect(screen.getByRole('button', { name: 'Collapse run activity' })).toBeVisible();
    expect(useRunTimeline).toHaveBeenLastCalledWith('return-to-live-plan', false, true);
  });
  it('preserves a user-collapsed historical trace without opening unrelated runs', () => {
    const view = render(<RunProgress inline jobId="collapsed-plan" running generating={false} />);
    fireEvent.click(screen.getByRole('button', { name: 'Collapse run activity' }));
    view.unmount();
    const returned = render(<RunProgress inline jobId="collapsed-plan" running={false} generating={false} />);
    expect(screen.getByRole('button', { name: 'Expand run activity' })).toBeVisible();
    returned.unmount();
    render(<RunProgress inline jobId="unrelated-plan" running={false} generating={false} />);
    expect(screen.getByRole('button', { name: 'Expand run activity' })).toBeVisible();
  });
});
