/**
 * Activity → Schedules: each schedule names the clock it runs on, and editing a
 * flow schedule keeps it a flow (its edit used to be sent as a crew).
 */
import React from 'react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import type { Schedule } from '../../../../api/execution/ScheduleService';

const listSchedules = vi.fn<() => Promise<Schedule[]>>();
const updateSchedule = vi.fn(async () => ({}));
vi.mock('../../../../api/execution/ScheduleService', () => ({
  ScheduleService: {
    listSchedules: () => listSchedules(),
    updateSchedule: (...a: unknown[]) => updateSchedule(...(a as [])),
    deleteSchedule: vi.fn(),
    toggleSchedule: vi.fn(),
  },
}));
vi.mock('react-hot-toast', () => ({ toast: { success: vi.fn(), error: vi.fn() } }));

import ScheduleDialog from './ScheduleDialog';

const FLOW_ID = '0d3f8e1a-7c2b-4d5e-8f9a-1b2c3d4e5f60';

const schedule = (extra: Partial<Schedule>): Schedule => ({
  id: 1,
  name: 'Morning flow',
  cron_expression: '0 9 * * *',
  execution_type: 'flow',
  flow_id: FLOW_ID,
  agents_yaml: {},
  tasks_yaml: {},
  inputs: {},
  is_active: true,
  next_run_at: '2026-10-18T07:00:00',
  created_at: '2026-10-17T10:00:00',
  updated_at: '2026-10-17T10:00:00',
  model: 'databricks-llama',
  ...extra,
});

const renderDialog = () =>
  render(<ScheduleDialog open embedded onClose={vi.fn()} nodes={[]} edges={[]} selectedModel="" />);

let restoreZone: () => void = () => undefined;

beforeEach(() => {
  listSchedules.mockReset();
  updateSchedule.mockClear();
  const real = Intl.DateTimeFormat.prototype.resolvedOptions;
  const spy = vi
    .spyOn(Intl.DateTimeFormat.prototype, 'resolvedOptions')
    .mockImplementation(function (this: Intl.DateTimeFormat) {
      return { ...real.call(this), timeZone: 'America/New_York' };
    });
  restoreZone = () => spy.mockRestore();
});

afterEach(() => restoreZone());

describe('ScheduleDialog', () => {
  it("names each schedule's clock: its zone, or the server's for an old one", async () => {
    listSchedules.mockResolvedValue([
      schedule({ id: 1, name: 'Berlin report', timezone: 'Europe/Berlin' }),
      schedule({ id: 2, name: 'Old report', timezone: null }),
    ]);
    renderDialog();
    expect(await screen.findByText('(Europe/Berlin)')).toBeInTheDocument();
    expect(screen.getByText('(server time (UTC))')).toBeInTheDocument();
  });

  it('editing a flow schedule sends it as a flow, on its own clock', async () => {
    listSchedules.mockResolvedValue([schedule({ timezone: 'Europe/Berlin' })]);
    renderDialog();
    fireEvent.click(await screen.findByRole('button', { name: 'Edit Schedule' }));
    expect(screen.getByText('Times are in Europe/Berlin')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Update Schedule' }));

    await waitFor(() => expect(updateSchedule).toHaveBeenCalledTimes(1));
    const [id, payload] = updateSchedule.mock.calls[0] as unknown as [number, Record<string, unknown>];
    expect(id).toBe(1);
    expect(payload).toMatchObject({
      execution_type: 'flow',
      flow_id: FLOW_ID,
      timezone: 'Europe/Berlin',
    });
    expect(payload).not.toHaveProperty('agents_yaml');
  });

  const legacyCrew = () =>
    schedule({
      execution_type: 'crew',
      flow_id: undefined,
      agents_yaml: { a: { role: 'R' } } as Schedule['agents_yaml'],
      tasks_yaml: { t: { description: 'D' } } as Schedule['tasks_yaml'],
      timezone: null,
    });

  const lastPayload = () =>
    (updateSchedule.mock.calls.at(-1) as unknown as [number, Record<string, unknown>])[1];

  it('renaming an old schedule leaves it on the server clock', async () => {
    listSchedules.mockResolvedValue([legacyCrew()]);
    renderDialog();
    fireEvent.click(await screen.findByRole('button', { name: 'Edit Schedule' }));
    expect(screen.getByText('Times are in server time (UTC)')).toBeInTheDocument();
    fireEvent.change(screen.getByLabelText('Schedule Name'), { target: { value: 'Renamed' } });
    fireEvent.click(screen.getByRole('button', { name: 'Update Schedule' }));

    await waitFor(() => expect(updateSchedule).toHaveBeenCalledTimes(1));
    expect(lastPayload()).toMatchObject({ name: 'Renamed', execution_type: 'crew' });
    expect(lastPayload()).not.toHaveProperty('timezone');
    expect(lastPayload().agents_yaml).toEqual({ a: { role: 'R' } });
  });

  it("changing an old schedule's time puts it on the editor's clock", async () => {
    listSchedules.mockResolvedValue([legacyCrew()]);
    renderDialog();
    fireEvent.click(await screen.findByRole('button', { name: 'Edit Schedule' }));
    fireEvent.click(screen.getByRole('button', { name: 'Advanced (Cron)' }));
    fireEvent.change(screen.getByLabelText('Cron Expression'), { target: { value: '0 10 * * *' } });
    expect(screen.getByText('Times are in America/New_York')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Update Schedule' }));

    await waitFor(() => expect(updateSchedule).toHaveBeenCalledTimes(1));
    expect(lastPayload()).toMatchObject({
      cron_expression: '0 10 * * *',
      timezone: 'America/New_York',
    });
  });
});
