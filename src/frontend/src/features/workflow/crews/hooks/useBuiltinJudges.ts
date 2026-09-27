import { useCallback, useEffect, useState } from 'react';
import {
  BuiltinJudge,
  PromptOptimizationService,
} from '../../../../api/config/PromptOptimizationService';

export interface BuiltinJudgesState {
  /** Selectable built-ins: available and not waiting on labels. */
  judges: BuiltinJudge[];
  selected: string[];
  toggle: (id: string) => void;
}

/**
 * The MLflow built-in judges a crew run can select, and this run's selection.
 * Nothing is selected by default: each one is an extra judge call per new
 * deliverable. Judges that need labels are left out until runs collect them.
 */
export const useBuiltinJudges = (open: boolean): BuiltinJudgesState => {
  const [judges, setJudges] = useState<BuiltinJudge[]>([]);
  const [selected, setSelected] = useState<string[]>([]);

  useEffect(() => {
    if (!open) return;
    let cancelled = false;
    PromptOptimizationService.listBuiltinJudges()
      .then((all) => {
        if (!cancelled) setJudges(all.filter((j) => j.available && !j.needs_labels));
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
    setSelected((prev) => (prev.includes(id) ? prev.filter((x) => x !== id) : [...prev, id]));
  }, []);

  return { judges, selected, toggle };
};
