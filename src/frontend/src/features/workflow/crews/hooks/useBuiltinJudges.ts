import { useCallback, useEffect, useMemo, useState } from 'react';
import {
  BuiltinJudge,
  PromptOptimizationService,
} from '../../../../api/config/PromptOptimizationService';

export interface BuiltinJudgesState {
  /** Built-ins the installed MLflow provides, label judges included. */
  judges: BuiltinJudge[];
  /** The selection a run sends: judges waiting on labels are left out. */
  selected: string[];
  toggle: (id: string) => void;
  /** False for a judge that needs labels the run does not have yet. */
  isEnabled: (judge: BuiltinJudge) => boolean;
}

/** Whether a judge can run with the label fields present. */
export const hasLabelsFor = (judge: BuiltinJudge, present: string[]): boolean =>
  !judge.needs_labels || (judge.label_fields || []).some((field) => present.includes(field));

/**
 * The MLflow built-in judges a crew run can select, and this run's selection.
 * Nothing is selected by default: each one is an extra judge call per new
 * deliverable. Judges that need labels are offered, but only count while one
 * of their label fields is present (`present`, from useCrewLabels).
 */
export const useBuiltinJudges = (open: boolean, present: string[] = []): BuiltinJudgesState => {
  const [judges, setJudges] = useState<BuiltinJudge[]>([]);
  const [picked, setPicked] = useState<string[]>([]);

  useEffect(() => {
    if (!open) return;
    let cancelled = false;
    PromptOptimizationService.listBuiltinJudges()
      .then((all) => {
        if (!cancelled) setJudges(all.filter((j) => j.available));
      })
      .catch(() => {
        // Optional extras: without the catalog the run uses Kasal's judges only.
        if (!cancelled) setJudges([]);
      });
    return () => {
      cancelled = true;
    };
  }, [open]);

  const toggle = useCallback((id: string) => {
    setPicked((prev) => (prev.includes(id) ? prev.filter((x) => x !== id) : [...prev, id]));
  }, []);

  const isEnabled = useCallback((judge: BuiltinJudge) => hasLabelsFor(judge, present), [present]);

  const selected = useMemo(
    () => picked.filter((id) => {
      const judge = judges.find((j) => j.id === id);
      return judge !== undefined && isEnabled(judge);
    }),
    [picked, judges, isEnabled],
  );

  return { judges, selected, toggle, isEnabled };
};
