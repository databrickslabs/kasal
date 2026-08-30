import React, { useEffect } from 'react';
import { ScheduleService } from '../../../api/execution/ScheduleService';
import { useAppStore } from '../store/appStore';
import { usePermissionStore } from '../../../store/permissions';

/**
 * The collapsible "Schedules" section in the chat rail: every schedule in the
 * workspace, each with its cadence, a pause/resume toggle and a delete. New
 * entries arrive from the clock action on a run's actions bar ("run this on a
 * schedule"); this section is where they live afterwards.
 *
 * Styled exactly like CatalogLibrary above it — a flat header row, no card
 * chrome — and renders nothing at all while there are no schedules: an empty
 * section would only advertise a feature the user has not used yet.
 */

/** A human phrase for the cadences the dialog composes; raw cron otherwise. */
export function cadenceLabel(cron: string): string {
  const c = cron.trim();
  const DAY = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
  const at = (m: string, h: string) => `${h.padStart(2, '0')}:${m.padStart(2, '0')}`;
  const hourly = /^(\d{1,2}) \* \* \* \*$/.exec(c);
  if (hourly) return 'Hourly';
  const daily = /^(\d{1,2}) (\d{1,2}) \* \* \*$/.exec(c);
  if (daily) return `Daily ${at(daily[1], daily[2])}`;
  const weekdays = /^(\d{1,2}) (\d{1,2}) \* \* 1-5$/.exec(c);
  if (weekdays) return `Weekdays ${at(weekdays[1], weekdays[2])}`;
  const weekly = /^(\d{1,2}) (\d{1,2}) \* \* ([0-6])$/.exec(c);
  if (weekly) return `${DAY[Number(weekly[3])]} ${at(weekly[1], weekly[2])}`;
  const monthly = /^(\d{1,2}) (\d{1,2}) (\d{1,2}) \* \*$/.exec(c);
  if (monthly) return `Monthly ${at(monthly[1], monthly[2])}`;
  return c;
}

const Chevron: React.FC<{ open: boolean }> = ({ open }) => (
  <svg
    className="w-3 h-3 flex-shrink-0 transition-transform"
    style={{ transform: open ? 'rotate(180deg)' : 'none' }}
    fill="none"
    viewBox="0 0 24 24"
    stroke="currentColor"
    strokeWidth={2.5}
  >
    <path strokeLinecap="round" strokeLinejoin="round" d="M19.5 8.25l-7.5 7.5-7.5-7.5" />
  </svg>
);

const ClockIcon: React.FC<{ dim?: boolean }> = ({ dim }) => (
  <svg
    className="w-3.5 h-3.5 flex-shrink-0"
    style={{ opacity: dim ? 0.45 : 0.8 }}
    fill="none"
    viewBox="0 0 24 24"
    stroke="currentColor"
    strokeWidth={1.8}
  >
    <circle cx="12" cy="12" r="8.5" />
    <path strokeLinecap="round" strokeLinejoin="round" d="M12 7.5V12l3 2" />
  </svg>
);

/** Stable fallback so a store stub without the slice cannot crash the rail. */
const NO_SCHEDULES: never[] = [];

const ScheduleLibrary: React.FC = () => {
  const schedules = useAppStore((s) => s.schedules) ?? NO_SCHEDULES;
  const open = useAppStore((s) => s.schedulesOpen);
  const setOpen = useAppStore((s) => s.setSchedulesOpen);
  const loadSchedules = useAppStore((s) => s.loadSchedules);
  // Chat-only users (operators) cannot open the builders, where a scheduled
  // run's results and history live — offering schedules they can never check
  // on is a dead end, so the section does not exist for them. Same gate as
  // the "Open in Agent Builder" actions.
  const allowAgentBuilder = usePermissionStore((s) => s.allowAgentBuilder);
  const allowFlowBuilder = usePermissionStore((s) => s.allowFlowBuilder);
  const canUseBuilders = allowAgentBuilder || allowFlowBuilder;

  useEffect(() => {
    if (canUseBuilders) void loadSchedules?.();
  }, [loadSchedules, canUseBuilders]);

  if (!canUseBuilders || schedules.length === 0) return null;

  const toggle = async (id: number) => {
    try {
      await ScheduleService.toggleSchedule(id);
    } finally {
      void loadSchedules();
    }
  };
  const remove = async (id: number) => {
    try {
      await ScheduleService.deleteSchedule(id);
    } finally {
      void loadSchedules();
    }
  };

  return (
    <div className="pt-1">
      <button
        onClick={() => setOpen(!open)}
        className="w-full flex items-center gap-1.5 !px-3 !py-1.5 text-left transition-colors hover:bg-[var(--bg-rail-hover)]"
        style={{ color: 'var(--text-muted)' }}
        aria-expanded={open}
      >
        <span className="text-[11px] font-semibold uppercase tracking-[0.08em] flex-1">
          Schedules
        </span>
        <span className="text-[10px] tabular-nums">{schedules.length}</span>
        <Chevron open={open} />
      </button>
      {open && (
        <div className="px-2 pt-1 max-h-52 overflow-y-auto">
          {schedules.map((item) => (
            <div
              key={item.id}
              className="group w-full flex items-center gap-2.5 !px-3 !py-1.5 my-0.5 rounded-lg transition-colors hover:bg-[var(--bg-rail-hover)]"
              style={{ color: 'var(--text-secondary)' }}
            >
              <ClockIcon dim={!item.is_active} />
              <span
                className="truncate flex-1 text-[13px]"
                style={{
                  color: item.is_active ? 'var(--text-primary)' : 'var(--text-muted)',
                }}
                title={`${item.name} — ${item.cron_expression}`}
              >
                {item.name}
              </span>
              <span className="text-[10px] flex-shrink-0" style={{ color: 'var(--text-muted)' }}>
                {item.is_active ? cadenceLabel(item.cron_expression) : 'Paused'}
              </span>
              <button
                type="button"
                onClick={() => void toggle(item.id)}
                aria-label={item.is_active ? `Pause ${item.name}` : `Resume ${item.name}`}
                title={item.is_active ? 'Pause' : 'Resume'}
                className="w-5 h-5 rounded flex items-center justify-center flex-shrink-0 transition-colors hover:bg-[var(--bg-active-chip)]"
                style={{ color: 'var(--text-secondary)' }}
              >
                {item.is_active ? (
                  <svg className="w-3 h-3" fill="currentColor" viewBox="0 0 24 24">
                    <rect x="7" y="5" width="3.5" height="14" rx="1" />
                    <rect x="13.5" y="5" width="3.5" height="14" rx="1" />
                  </svg>
                ) : (
                  <svg className="w-3 h-3" fill="currentColor" viewBox="0 0 24 24">
                    <path d="M8 5.5v13a1 1 0 001.54.84l9.6-6.5a1 1 0 000-1.68l-9.6-6.5A1 1 0 008 5.5z" />
                  </svg>
                )}
              </button>
              <button
                type="button"
                onClick={() => void remove(item.id)}
                aria-label={`Delete ${item.name}`}
                title="Delete schedule"
                className="w-5 h-5 rounded items-center justify-center flex-shrink-0 transition-colors hover:bg-[var(--bg-active-chip)] hidden group-hover:flex"
                style={{ color: 'var(--text-muted)' }}
              >
                <svg className="w-3 h-3" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            </div>
          ))}
        </div>
      )}
    </div>
  );
};

export default ScheduleLibrary;
