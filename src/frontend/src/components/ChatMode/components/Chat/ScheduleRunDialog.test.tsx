/**
 * The schedule dialog speaks plain language: how often + when, a summary
 * sentence, and cron only behind the Advanced disclosure.
 */
import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';

const createScheduleFromExecution = vi.fn(async () => ({ name: 'S' }));
vi.mock('../../../../api/execution/ScheduleService', () => ({
  ScheduleService: {
    createScheduleFromExecution: (...a: unknown[]) => createScheduleFromExecution(...a),
  },
}));

import ScheduleRunDialog, { composeCron, describeChoice } from './ScheduleRunDialog';

describe('composeCron / describeChoice', () => {
  it('composes the five choices', () => {
    expect(composeCron('hourly', '09:00', 1, 1)).toBe('0 * * * *');
    expect(composeCron('daily', '17:30', 1, 1)).toBe('30 17 * * *');
    expect(composeCron('weekdays', '09:00', 1, 1)).toBe('0 9 * * 1-5');
    expect(composeCron('weekly', '08:15', 3, 1)).toBe('15 8 * * 3');
    expect(composeCron('monthly', '09:00', 1, 15)).toBe('0 9 15 * *');
  });

  it('says what the choice means in words', () => {
    expect(describeChoice('hourly', '09:00', 1, 1)).toBe('Runs every hour, on the hour');
    expect(describeChoice('daily', '17:30', 1, 1)).toBe('Runs every day at 17:30');
    expect(describeChoice('weekdays', '09:00', 1, 1)).toBe('Runs Monday to Friday at 09:00');
    expect(describeChoice('weekly', '09:00', 3, 1)).toBe('Runs every Wednesday at 09:00');
    expect(describeChoice('monthly', '09:00', 1, 22)).toBe(
      'Runs on the 22nd of every month at 09:00',
    );
  });
});

describe('ScheduleRunDialog', () => {
  const renderDialog = () =>
    render(
      <ScheduleRunDialog
        executionId="job-1"
        defaultName="My run schedule"
        onClose={vi.fn()}
        onCreated={vi.fn()}
      />,
    );

  beforeEach(() => createScheduleFromExecution.mockClear());

  it('shows no cron by default — words and pickers only', () => {
    renderDialog();
    expect(screen.getByText('Runs every day at 09:00')).toBeInTheDocument();
    expect(screen.queryByLabelText('Cron expression')).toBeNull();
    expect(screen.getByText('Advanced: edit as a cron expression')).toBeInTheDocument();
  });

  it('weekly reveals a day picker and updates the summary', () => {
    renderDialog();
    fireEvent.click(screen.getByText('Every week'));
    fireEvent.click(screen.getByText('Wed'));
    expect(screen.getByText('Runs every Wednesday at 09:00')).toBeInTheDocument();
  });

  it('creates with the composed cron', async () => {
    renderDialog();
    fireEvent.click(screen.getByText('Weekdays'));
    fireEvent.click(screen.getByText('Create schedule'));
    await waitFor(() =>
      expect(createScheduleFromExecution).toHaveBeenCalledWith(
        expect.objectContaining({ cron_expression: '0 9 * * 1-5', execution_id: 'job-1' }),
      ),
    );
  });

  it('the Advanced disclosure reveals the cron, and editing it takes over', () => {
    renderDialog();
    fireEvent.click(screen.getByText('Advanced: edit as a cron expression'));
    const cronInput = screen.getByLabelText('Cron expression') as HTMLInputElement;
    expect(cronInput.value).toBe('0 9 * * *');
    fireEvent.change(cronInput, { target: { value: '*/5 * * * *' } });
    expect(screen.getByText('Runs on the cron line */5 * * * *')).toBeInTheDocument();
  });
});
