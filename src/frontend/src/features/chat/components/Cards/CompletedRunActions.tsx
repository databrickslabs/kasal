import React, { useState } from 'react';
import { Modal } from '@mui/material';
import { usePermissionStore } from '../../../../store/permissions';
import { useThemeStore } from '../../../../store/theme';
import { useExecutionStore } from '../../store/executionStore';
import { useAppStore } from '../../store/appStore';
import MLflowRunAction from './MLflowRunAction';
import ScheduleRunDialog from '../Chat/ScheduleRunDialog';

interface CompletedRunActionsProps {
  executionId?: string;
  defaultName: string;
  usedWorkspaceMemory?: boolean;
  disabled?: boolean;
  onOpenSchedule?: (executionId: string, defaultName: string, onCreated: (name: string) => void) => void;
  onOpenMemory?: (executionId: string) => void;
}

const ICON_BTN = 'flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg text-xs font-medium transition-all hover:opacity-80 disabled:opacity-50 disabled:cursor-not-allowed';

/** The same run-scoped actions in Chat and the builder transcript. */
const CompletedRunActions: React.FC<CompletedRunActionsProps> = ({ executionId, defaultName, usedWorkspaceMemory, disabled, onOpenMemory, onOpenSchedule }) => {
  const canSchedule = usePermissionStore((s) => s.allowAgentBuilder || s.allowFlowBuilder);
  const dark = useThemeStore((s) => s.isDarkMode);
  const [scheduleOpen, setScheduleOpen] = useState(false);
  const [scheduledName, setScheduledName] = useState<string>();
  const canShowGraph = Boolean(usedWorkspaceMemory && executionId);
  const scheduleCreated = (name: string) => {
    setScheduledName(name);
    setScheduleOpen(false);
    void useAppStore.getState().loadSchedules();
  };
  return <>
        {/* Schedule — re-run THIS run on a cadence. The run's stored config is
            the template (POST /schedules/from-execution), so it works the same
            for a generated crew and an answer-mode turn. */}
        {executionId && canSchedule && (
          <button
            type="button"
            onClick={() => onOpenSchedule ? onOpenSchedule(executionId, defaultName, scheduleCreated) : setScheduleOpen(true)}
            disabled={disabled}
            title={scheduledName ? `Scheduled — ${scheduledName}` : 'Run this on a schedule'}
            className={ICON_BTN}
            style={{
              color: scheduledName ? 'var(--text-primary)' : 'var(--text-secondary)',
              backgroundColor: 'transparent',
              border: 'none',
            }}
          >
            <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <circle cx="12" cy="12" r="8.5" />
              <path strokeLinecap="round" strokeLinejoin="round" d="M12 7.5V12l3 2" />
            </svg>
            {scheduledName ? 'Scheduled' : 'Schedule'}
          </button>
        )}

        {/* Memory graph — concept graph of what this run wrote to memory */}
        {canShowGraph && (
          <button
            type="button"
            onClick={() =>
              onOpenMemory ? onOpenMemory(executionId as string) : useExecutionStore.getState().openPreviewPane({
                type: 'memory',
                data: executionId as string,
                title: 'Run memory',
              })
            }
            aria-label="View memory graph"
            className={ICON_BTN}
            style={{
              color: 'var(--text-secondary)',
              backgroundColor: 'transparent',
              border: 'none',
            }}
          >
            <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <circle cx="6" cy="6" r="2.5" />
              <circle cx="18" cy="7" r="2.5" />
              <circle cx="12" cy="17" r="2.5" />
              <path strokeLinecap="round" d="M7.8 7.4l2.6 7.4M16.6 8.7l-3 6.4M8.3 6.4l7.2.4" />
            </svg>
            Memory graph
          </button>
        )}


    {executionId && <MLflowRunAction key={executionId} executionId={executionId} disabled={disabled} />}

    {scheduleOpen && executionId && canSchedule && (
      <Modal open onClose={() => setScheduleOpen(false)} hideBackdrop sx={{ zIndex: 1500 }}>
      <div tabIndex={-1} className="kasal-chat-root" data-theme={dark ? 'dark' : 'light'}>
        <ScheduleRunDialog
          executionId={executionId}
          defaultName={defaultName}
          onClose={() => setScheduleOpen(false)}
          onCreated={scheduleCreated}
        />
      </div>
      </Modal>
    )}
  </>;
};

export default CompletedRunActions;
