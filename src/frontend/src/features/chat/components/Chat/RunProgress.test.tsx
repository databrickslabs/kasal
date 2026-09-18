import { describe, expect, it, vi } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
import RunProgress from './RunProgress';
import { useRunTimeline } from '../../hooks/useRunTimeline';
vi.mock('../../hooks/useRunTimeline', () => ({ useRunTimeline: vi.fn(() => ({ processed: null, loading: false })) }));
vi.mock('../Preview/RunTraceTimeline', () => ({ default: () => null }));

describe('restoring run activity', () => {
  it('keeps click-only activity collapsed through run updates until clicked', () => {
    const view = render(<RunProgress inline autoExpand={false} running generating />);
    expect(useRunTimeline).toHaveBeenLastCalledWith(undefined, true, false);
    view.rerender(<RunProgress inline autoExpand={false} jobId="studio-edit" running generating />);
    expect(screen.getByRole('button', { name: 'Expand run activity' })).toBeVisible();
    expect(useRunTimeline).toHaveBeenLastCalledWith('studio-edit', true, false);
    fireEvent.click(screen.getByRole('button', { name: 'Expand run activity' }));
    expect(useRunTimeline).toHaveBeenLastCalledWith('studio-edit', true, true);
    fireEvent.click(screen.getByRole('button', { name: 'Collapse run activity' }));
    view.rerender(<RunProgress inline autoExpand={false} jobId="studio-edit" running={false} generating={false} />);
    expect(useRunTimeline).toHaveBeenLastCalledWith('studio-edit', false, false);
  });
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
