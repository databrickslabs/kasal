import React, { useState } from 'react';
import { ScheduleService } from '../../../../api/execution/ScheduleService';

/**
 * Chat-native "Run this on a schedule" dialog for one finished run.
 *
 * The run's stored configuration is the template (POST /schedules/from-execution
 * with the message's execution id), so what gets scheduled is exactly what just
 * ran — same agents, tasks, tools and model.
 *
 * Built for people who have never seen a cron line: pick how often, pick when,
 * read the plain-English summary. The cron expression is composed from those
 * choices and stays behind a small "Advanced" disclosure; editing it there
 * switches to custom mode (the pickers step aside rather than lie).
 *
 * Rendered inside #kasal-chat-root (NOT portaled) so the chat CSS tokens apply;
 * `position: fixed` escapes the chat layout's overflow clipping — the same
 * pattern as ChatMcpDialog.
 */
export interface ScheduleRunDialogProps {
  executionId: string;
  defaultName: string;
  onClose: () => void;
  onCreated: (name: string) => void;
}

export type Frequency = 'hourly' | 'daily' | 'weekdays' | 'weekly' | 'monthly';

const FREQUENCIES: { value: Frequency; label: string }[] = [
  { value: 'hourly', label: 'Every hour' },
  { value: 'daily', label: 'Every day' },
  { value: 'weekdays', label: 'Weekdays' },
  { value: 'weekly', label: 'Every week' },
  { value: 'monthly', label: 'Every month' },
];

/** Cron day-of-week numbering: 0 = Sunday. Shown Monday-first. */
const WEEKDAYS: { value: number; label: string }[] = [
  { value: 1, label: 'Mon' },
  { value: 2, label: 'Tue' },
  { value: 3, label: 'Wed' },
  { value: 4, label: 'Thu' },
  { value: 5, label: 'Fri' },
  { value: 6, label: 'Sat' },
  { value: 0, label: 'Sun' },
];

const WEEKDAY_NAMES = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];

function parseTime(time: string): { h: number; m: number } {
  const [h, m] = time.split(':').map((n) => parseInt(n, 10));
  return { h: Number.isFinite(h) ? h : 9, m: Number.isFinite(m) ? m : 0 };
}

/** The cron line for one plain-language choice. Exported for tests. */
export function composeCron(
  freq: Frequency,
  time: string,
  weekday: number,
  monthday: number,
): string {
  const { h, m } = parseTime(time);
  switch (freq) {
    case 'hourly':
      return `${m} * * * *`;
    case 'daily':
      return `${m} ${h} * * *`;
    case 'weekdays':
      return `${m} ${h} * * 1-5`;
    case 'weekly':
      return `${m} ${h} * * ${weekday}`;
    case 'monthly':
      return `${m} ${h} ${monthday} * *`;
  }
}

function ordinal(n: number): string {
  const s = ['th', 'st', 'nd', 'rd'][n % 100 > 10 && n % 100 < 14 ? 0 : Math.min(n % 10, 4)] ?? 'th';
  return `${n}${s}`;
}

/** "Runs every weekday at 09:00" — what the choice means, in words. */
export function describeChoice(
  freq: Frequency,
  time: string,
  weekday: number,
  monthday: number,
): string {
  const { h, m } = parseTime(time);
  const at = `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`;
  switch (freq) {
    case 'hourly':
      return m === 0 ? 'Runs every hour, on the hour' : `Runs every hour at ${m} past`;
    case 'daily':
      return `Runs every day at ${at}`;
    case 'weekdays':
      return `Runs Monday to Friday at ${at}`;
    case 'weekly':
      return `Runs every ${WEEKDAY_NAMES[weekday]} at ${at}`;
    case 'monthly':
      return `Runs on the ${ordinal(monthday)} of every month at ${at}`;
  }
}

const chipStyle = (active: boolean): React.CSSProperties => ({
  padding: '6px 10px',
  color: active ? 'var(--accent)' : 'var(--text-secondary)',
  backgroundColor: active ? 'var(--bg-active-chip)' : 'var(--bg-secondary)',
  border: '1px solid var(--border-color)',
});

const ScheduleRunDialog: React.FC<ScheduleRunDialogProps> = ({
  executionId,
  defaultName,
  onClose,
  onCreated,
}) => {
  const [name, setName] = useState(defaultName);
  const [freq, setFreq] = useState<Frequency>('daily');
  const [time, setTime] = useState('09:00');
  const [weekday, setWeekday] = useState(1);
  const [monthday, setMonthday] = useState(1);
  const [advanced, setAdvanced] = useState(false);
  /** Non-null once the cron was hand-edited — the pickers step aside. */
  const [customCron, setCustomCron] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const cron = customCron ?? composeCron(freq, time, weekday, monthday);
  const summary = customCron === null ? describeChoice(freq, time, weekday, monthday) : `Runs on the cron line ${customCron}`;

  const create = async () => {
    setSaving(true);
    setError(null);
    try {
      const saved = await ScheduleService.createScheduleFromExecution({
        name: name.trim(),
        cron_expression: cron.trim(),
        execution_id: executionId,
      });
      onCreated(saved.name);
    } catch {
      setError('Could not create the schedule');
      setSaving(false);
    }
  };

  const canCreate = Boolean(name.trim() && cron.trim()) && !saving;
  const inputStyle: React.CSSProperties = {
    backgroundColor: 'var(--bg-input)',
    color: 'var(--text-primary)',
    border: '1px solid var(--border-color)',
  };
  const sectionLabel: React.CSSProperties = { color: 'var(--text-muted)' };
  const showTime = customCron === null && freq !== 'hourly';
  const showWeekday = customCron === null && freq === 'weekly';
  const showMonthday = customCron === null && freq === 'monthly';

  return (
    <div
      className="fixed inset-0 z-[60] flex items-center justify-center p-4 animate-fade-in"
      style={{ backgroundColor: 'rgba(0,0,0,0.4)' }}
      onMouseDown={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div
        role="dialog"
        aria-label="Run this on a schedule"
        className="w-full max-w-md rounded-2xl flex flex-col overflow-hidden"
        style={{
          backgroundColor: 'var(--bg-primary)',
          border: '1px solid var(--border-color)',
          boxShadow: 'var(--shadow-popover)',
        }}
      >
        <div className="flex items-center gap-3 px-5 py-4" style={{ borderBottom: '1px solid var(--border-color)' }}>
          <span
            className="flex items-center justify-center w-9 h-9 rounded-lg flex-shrink-0"
            style={{ backgroundColor: 'var(--bg-active-chip)', color: 'var(--text-secondary)' }}
          >
            <svg className="w-[18px] h-[18px]" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
              <circle cx="12" cy="12" r="8.5" />
              <path strokeLinecap="round" strokeLinejoin="round" d="M12 7.5V12l3 2" />
            </svg>
          </span>
          <div className="flex-1 min-w-0">
            <div className="text-base font-semibold" style={{ color: 'var(--text-primary)' }}>
              Run this on a schedule
            </div>
            <div className="text-xs" style={{ color: 'var(--text-muted)' }}>
              Re-runs exactly what just ran — same agents, tasks and model
            </div>
          </div>
          <button
            type="button"
            onClick={onClose}
            aria-label="Close"
            className="w-8 h-8 rounded-lg flex items-center justify-center transition-colors hover:bg-[var(--bg-rail-hover)]"
            style={{ color: 'var(--text-muted)' }}
          >
            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        <div className="px-5 py-4 space-y-3">
          <label className="block">
            <span className="text-[11px] font-semibold uppercase tracking-[0.08em]" style={sectionLabel}>
              Name
            </span>
            <input
              value={name}
              onChange={(e) => setName(e.target.value)}
              className="mt-1 w-full px-3 py-2 rounded-lg text-sm outline-none"
              style={inputStyle}
            />
          </label>

          <div>
            <span className="text-[11px] font-semibold uppercase tracking-[0.08em]" style={sectionLabel}>
              How often
            </span>
            <div className="mt-1 flex flex-wrap gap-1.5">
              {FREQUENCIES.map((f) => (
                <button
                  key={f.value}
                  type="button"
                  onClick={() => {
                    setFreq(f.value);
                    setCustomCron(null);
                  }}
                  className="rounded-lg text-xs transition-colors"
                  style={chipStyle(customCron === null && freq === f.value)}
                >
                  {f.label}
                </button>
              ))}
            </div>
          </div>

          {showWeekday && (
            <div>
              <span className="text-[11px] font-semibold uppercase tracking-[0.08em]" style={sectionLabel}>
                On which day
              </span>
              <div className="mt-1 flex flex-wrap gap-1.5">
                {WEEKDAYS.map((d) => (
                  <button
                    key={d.value}
                    type="button"
                    onClick={() => setWeekday(d.value)}
                    className="rounded-lg text-xs transition-colors"
                    style={chipStyle(weekday === d.value)}
                  >
                    {d.label}
                  </button>
                ))}
              </div>
            </div>
          )}

          {showMonthday && (
            <label className="block">
              <span className="text-[11px] font-semibold uppercase tracking-[0.08em]" style={sectionLabel}>
                On which day of the month
              </span>
              <input
                type="number"
                min={1}
                max={28}
                value={monthday}
                onChange={(e) => {
                  const v = parseInt(e.target.value, 10);
                  if (Number.isFinite(v)) setMonthday(Math.min(28, Math.max(1, v)));
                }}
                className="mt-1 w-24 px-3 py-2 rounded-lg text-sm outline-none"
                style={inputStyle}
              />
              <span className="ml-2 text-[11px]" style={sectionLabel}>
                1–28, so it exists in every month
              </span>
            </label>
          )}

          {showTime && (
            <label className="block">
              <span className="text-[11px] font-semibold uppercase tracking-[0.08em]" style={sectionLabel}>
                At what time
              </span>
              <input
                type="time"
                value={time}
                onChange={(e) => e.target.value && setTime(e.target.value)}
                className="mt-1 w-32 px-3 py-2 rounded-lg text-sm outline-none"
                style={inputStyle}
                aria-label="Time of day"
              />
            </label>
          )}

          {/* What the choice means, in words — the one line everyone reads. */}
          <div
            className="rounded-lg text-[13px]"
            style={{
              padding: '10px 12px',
              backgroundColor: 'var(--bg-secondary)',
              color: 'var(--text-primary)',
              border: '1px solid var(--border-color)',
            }}
          >
            {summary}
          </div>

          {!advanced ? (
            <button
              type="button"
              onClick={() => setAdvanced(true)}
              className="text-[11px] underline-offset-2 hover:underline"
              style={sectionLabel}
            >
              Advanced: edit as a cron expression
            </button>
          ) : (
            <label className="block">
              <span className="text-[11px] font-semibold uppercase tracking-[0.08em]" style={sectionLabel}>
                Cron expression
              </span>
              <input
                value={cron}
                onChange={(e) => setCustomCron(e.target.value)}
                spellCheck={false}
                className="mt-1 w-full px-3 py-2 rounded-lg text-sm outline-none font-mono"
                style={inputStyle}
                aria-label="Cron expression"
              />
              <span className="mt-1 block text-[11px]" style={sectionLabel}>
                minute hour day-of-month month day-of-week
              </span>
            </label>
          )}

          {error && (
            <div className="text-[12px]" style={{ color: '#ef4444' }}>{error}</div>
          )}
        </div>

        <div className="flex items-center justify-end gap-2 px-5 py-3" style={{ borderTop: '1px solid var(--border-color)' }}>
          <button
            type="button"
            onClick={onClose}
            className="rounded-lg text-sm transition-colors hover:bg-[var(--bg-rail-hover)]"
            style={{ padding: '8px 14px', color: 'var(--text-secondary)' }}
          >
            Cancel
          </button>
          <button
            type="button"
            onClick={() => void create()}
            disabled={!canCreate}
            className="rounded-lg text-sm font-medium transition-opacity disabled:opacity-50"
            style={{ padding: '8px 14px', color: 'white', backgroundColor: 'var(--accent)' }}
          >
            {saving ? 'Creating…' : 'Create schedule'}
          </button>
        </div>
      </div>
    </div>
  );
};

export default ScheduleRunDialog;
