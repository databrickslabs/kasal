/**
 * The rail's Schedules section: lists the workspace's schedules with cadence,
 * pause/resume and delete — and stays out of the way entirely while empty.
 */
import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';

const toggleSchedule = vi.fn(async () => ({}));
const deleteSchedule = vi.fn(async () => undefined);
const listSchedules = vi.fn(async () => [] as unknown[]);
vi.mock('../../../api/execution/ScheduleService', () => ({
  ScheduleService: {
    toggleSchedule: (...a: unknown[]) => toggleSchedule(...a),
    deleteSchedule: (...a: unknown[]) => deleteSchedule(...a),
    listSchedules: (...a: unknown[]) => listSchedules(...a),
  },
}));

import ScheduleLibrary, { cadenceLabel } from './ScheduleLibrary';
import { useAppStore } from '../store/appStore';
import { usePermissionStore } from '../../../store/permissions';

const sched = (over: Record<string, unknown> = {}) => ({
  id: 1,
  name: 'Lebanon briefing',
  cron_expression: '0 9 * * *',
  execution_type: 'crew',
  is_active: true,
  created_at: '',
  updated_at: '',
  ...over,
});

beforeEach(() => {
  toggleSchedule.mockClear();
  deleteSchedule.mockClear();
  listSchedules.mockClear();
  useAppStore.setState({ schedules: [], schedulesOpen: true });
});

describe('cadenceLabel', () => {
  it('phrases the composed cadences and falls back to the raw cron', () => {
    expect(cadenceLabel('0 * * * *')).toBe('Hourly');
    expect(cadenceLabel('30 * * * *')).toBe('Hourly');
    expect(cadenceLabel('0 9 * * *')).toBe('Daily 09:00');
    expect(cadenceLabel('15 17 * * *')).toBe('Daily 17:15');
    expect(cadenceLabel('0 9 * * 1-5')).toBe('Weekdays 09:00');
    expect(cadenceLabel('0 9 * * 1')).toBe('Mon 09:00');
    expect(cadenceLabel('30 8 * * 0')).toBe('Sun 08:30');
    expect(cadenceLabel('0 9 1 * *')).toBe('Monthly 09:00');
    expect(cadenceLabel('*/7 3 2 1 *')).toBe('*/7 3 2 1 *');
  });
});

describe('ScheduleLibrary', () => {
  it('renders nothing for a chat-only user, even with schedules', () => {
    // Operators cannot open the builders where a run's results live —
    // schedules they can never check on must not exist for them.
    usePermissionStore.setState({ allowAgentBuilder: false, allowFlowBuilder: false });
    useAppStore.setState({ schedules: [sched()] as never });
    const { container } = render(<ScheduleLibrary />);
    expect(container.firstChild).toBeNull();
    expect(listSchedules).not.toHaveBeenCalled();
    usePermissionStore.setState({ allowAgentBuilder: true, allowFlowBuilder: true });
  });

  it('renders nothing while there are no schedules', () => {
    const { container } = render(<ScheduleLibrary />);
    expect(container.firstChild).toBeNull();
  });

  it('lists schedules with their cadence', () => {
    useAppStore.setState({ schedules: [sched()] as never });
    render(<ScheduleLibrary />);
    expect(screen.getByText('Schedules')).toBeInTheDocument();
    expect(screen.getByText('Lebanon briefing')).toBeInTheDocument();
    expect(screen.getByText('Daily 09:00')).toBeInTheDocument();
  });

  it('a paused schedule reads Paused and offers Resume', () => {
    useAppStore.setState({ schedules: [sched({ is_active: false })] as never });
    render(<ScheduleLibrary />);
    expect(screen.getByText('Paused')).toBeInTheDocument();
    expect(screen.getByLabelText('Resume Lebanon briefing')).toBeInTheDocument();
  });

  it('pause and delete call the service, then refresh the list', async () => {
    // The refresh after each action re-reads the list; keep returning the row
    // or the buttons vanish from under the test's second click.
    listSchedules.mockResolvedValue([sched()] as never);
    useAppStore.setState({ schedules: [sched()] as never });
    render(<ScheduleLibrary />);
    fireEvent.click(screen.getByLabelText('Pause Lebanon briefing'));
    await waitFor(() => expect(toggleSchedule).toHaveBeenCalledWith(1));
    fireEvent.click(screen.getByLabelText('Delete Lebanon briefing'));
    await waitFor(() => expect(deleteSchedule).toHaveBeenCalledWith(1));
    // Mount + after toggle + after delete.
    expect(listSchedules.mock.calls.length).toBeGreaterThanOrEqual(3);
  });
});
