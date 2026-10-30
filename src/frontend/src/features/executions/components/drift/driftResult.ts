/**
 * Types, detection guard and pure helpers for the UCMV Drift Monitor report
 * (backend: services/tools/metric_view_utils/drift/orchestrator.py).
 */

export type DriftStatus =
  | 'unchanged'
  | 'unchanged_llm'
  | 'changed_in_pbi'
  | 'possibly_changed'
  | 'legacy_unverified'
  | 'new_in_pbi'
  | 'unassigned'
  | 'removed_from_pbi'
  | 'baseline_only';

export interface DriftMeasure {
  status: DriftStatus;
  original_name: string;
  view?: string;
  measure_name?: string;
  baseline_expr?: string;
  current_dax?: string;
  baseline_fingerprint?: string;
  current_fingerprint?: string;
  allocations?: string[];
  note?: string;
  proposed_expr?: string;
  translation?: 'llm' | 'deterministic';
  applied?: boolean;
  llm_reason?: string;
}

export interface DriftView {
  full_name: string;
  catalog: string;
  schema: string;
  name: string;
  error?: string | null;
  source?: string | null;
  baseline_yaml?: string;
  proposed_yaml?: string;
  changes?: { kind: string; name?: string }[];
  invariant_problems?: string[];
  measures?: DriftMeasure[];
  counts?: { measures_added: number; measures_updated: number; joins_added: number; untranslated: number };
}

export interface DriftSummary {
  views_checked?: number;
  views_unreadable?: number;
  views_with_proposals?: number;
  unchanged?: number;
  changed_in_pbi?: number;
  possibly_changed?: number;
  legacy_unverified?: number;
  new_in_pbi?: number;
  unassigned?: number;
  removed_from_pbi?: number;
  baseline_only?: number;
  measures_added?: number;
  measures_updated?: number;
  joins_added?: number;
  untranslated?: number;
  pbi_measures?: number;
}

export interface DriftResult {
  drift_monitor: true;
  error?: string;
  views: DriftView[];
  unassigned?: DriftMeasure[];
  summary: DriftSummary;
  semantic_check?: Record<string, unknown>;
  yaml?: Record<string, string>;
  deploy_ddl?: Record<string, string>;
  warnings?: string[];
  pbi?: { workspace_id?: string; dataset_id?: string; report_id?: string; measures_extracted?: number };
  settings?: { apply_suspected_changes?: boolean };
}

export function isDriftResult(value: unknown): value is DriftResult {
  if (typeof value !== 'object' || value === null) return false;
  const o = value as Record<string, unknown>;
  return o.drift_monitor === true && Array.isArray(o.views);
}

type ChipColor = 'default' | 'success' | 'warning' | 'error' | 'info' | 'primary' | 'secondary';

/** How a row reads to a reviewer: what happened in PBI vs what Kasal did about it. */
export function describeMeasure(m: DriftMeasure): { label: string; color: ChipColor; title: string } {
  switch (m.status) {
    case 'changed_in_pbi':
      return m.applied
        ? { label: 'PBI changed → updated', color: 'warning', title: 'DAX changed in Power BI (fingerprint mismatch); expr re-translated in the proposal' }
        : { label: 'PBI changed', color: 'warning', title: 'DAX changed in Power BI; not patched — see note' };
    case 'possibly_changed':
      return m.applied
        ? { label: 'Suspected change → updated', color: 'warning', title: 'LLM judged the DAX changed; patched because apply_suspected_changes is on' }
        : { label: 'Suspected change', color: 'secondary', title: 'LLM judged the DAX may have changed; suggested SQL shown, verified measure kept' };
    case 'new_in_pbi':
      return m.applied
        ? { label: m.translation === 'llm' ? 'New → LLM added' : 'New → added', color: 'success', title: 'New in Power BI; translated and appended to the proposal' }
        : { label: 'New in PBI', color: 'info', title: 'New in Power BI; not added — see note' };
    case 'removed_from_pbi':
      return { label: 'Removed in PBI', color: 'error', title: 'No longer in Power BI; kept in the metric view (flag only)' };
    case 'unassigned':
      return { label: 'Unassigned', color: 'default', title: 'New in Power BI but its fact table is not among the monitored views' };
    case 'legacy_unverified':
      return { label: 'Unverified', color: 'default', title: 'Deployed before DAX fingerprinting and not LLM-checked' };
    case 'unchanged_llm':
      return { label: 'Unchanged (LLM)', color: 'default', title: 'No fingerprint; LLM judged the DAX unchanged' };
    case 'baseline_only':
      return { label: 'Not tracked', color: 'default', title: 'In the view, but not from a Power BI measure' };
    default:
      return { label: 'Unchanged', color: 'default', title: 'Fingerprint of today’s DAX matches the deployed measure' };
  }
}

const ATTENTION: DriftStatus[] = ['changed_in_pbi', 'possibly_changed', 'new_in_pbi', 'removed_from_pbi'];

export function needsAttention(m: DriftMeasure): boolean {
  return ATTENTION.includes(m.status);
}

/** Rows a reviewer acts on first: applied changes, then flags, then the rest. */
export function sortMeasures(ms: DriftMeasure[]): DriftMeasure[] {
  const rank = (m: DriftMeasure) => (m.applied ? 0 : needsAttention(m) ? 1 : 2);
  return [...ms].sort((a, b) => rank(a) - rank(b) || a.original_name.localeCompare(b.original_name));
}

export type DiffLine = { text: string; added: boolean };

/**
 * Mark the proposed-YAML lines that are not in the baseline (an LCS walk, so a
 * replaced measure shows only its new lines). Falls back to a greedy walk for very
 * large views to keep the table cheap.
 */
export function diffYamlLines(baseline: string, proposed: string): DiffLine[] {
  const a = (baseline || '').split('\n');
  const b = (proposed || '').split('\n');
  if (a.length * b.length > 4_000_000) {
    let i = 0;
    return b.map((line) => {
      if (i < a.length && a[i] === line) {
        i += 1;
        return { text: line, added: false };
      }
      return { text: line, added: true };
    });
  }
  const dp: Uint32Array[] = Array.from({ length: a.length + 1 }, () => new Uint32Array(b.length + 1));
  for (let i = a.length - 1; i >= 0; i -= 1) {
    for (let j = b.length - 1; j >= 0; j -= 1) {
      dp[i][j] = a[i] === b[j] ? dp[i + 1][j + 1] + 1 : Math.max(dp[i + 1][j], dp[i][j + 1]);
    }
  }
  const out: DiffLine[] = [];
  let i = 0;
  let j = 0;
  while (j < b.length) {
    if (i < a.length && a[i] === b[j]) {
      out.push({ text: b[j], added: false });
      i += 1;
      j += 1;
    } else if (i < a.length && dp[i + 1][j] >= dp[i][j + 1]) {
      i += 1;
    } else {
      out.push({ text: b[j], added: true });
      j += 1;
    }
  }
  return out;
}

export function downloadText(filename: string, text: string): void {
  const blob = new Blob([text], { type: 'text/yaml' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}
