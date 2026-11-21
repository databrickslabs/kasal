/**
 * Type contracts for the UC Metric View result viewer (UCMVResultViewer).
 * Shared by the viewer, its presentational parts and the tests.
 */
import type { UntranslatableItem, ReviewAnnotation } from './NonTranspiledPanel';

/* ------------------------------------------------------------------ */
/*  Types                                                              */
/* ------------------------------------------------------------------ */

export interface MetricViewDef {
  version?: string;
  source?: string;
  filter?: string;
  dimensions?: Array<{ name: string; expr?: string; comment?: string }>;
  measures?: Array<{ name: string; expr?: string; comment?: string; format?: string }>;
  joins?: Array<{ name: string; source?: string; on?: string; using?: string[] }>;
}

export interface UCMVStats {
  total_measures_translated?: number;
  total_untranslatable?: number;
  total_artifact_measures?: number;
  coverage_pct?: number;
  [key: string]: unknown;
}

/** Counts of one view (or of all views, under "total") in a verify-numbers run. */
export interface UCMVReviewHeadline {
  in_scope: number;
  in_view: number;
  compared: number;
  matching: number;
  matching_pct: number | null;
}

/** Periods a metric view has and Power BI returns nothing for: left out of the comparison. */
export interface UCMVPeriodsLeftOut {
  periods: number;
  first: string;
  last: string;
}

export interface UCMVReviewCounts {
  in_scope: number;
  translated: number;
  match: number;
  below_bar: number;
  not_compared: number;
  lost_on_deploy: number;
  not_translated: number;
  column_sums: number;
  column_sums_match: number;
  display: number;
}

/** Where a view stands against the review bar. */
export interface UCMVReviewViewStatus {
  status: string;
  pct_aligned?: number | null;
  measures?: Record<string, number>;
  periods_not_in_power_bi?: UCMVPeriodsLeftOut;
}

export interface UCMVResult {
  yaml: Record<string, string>;
  sql: Record<string, string>;
  /** Full raw Power Query M per view (keyed like `sql`) — what the source SQL was derived from. */
  source_mquery?: Record<string, string>;
  /** Per-view labels for M transformation steps that may NOT be reflected in the derived Source SQL. */
  source_transform_todos?: Record<string, string[]>;
  /** Loud reason when LLM translation was enabled but produced nothing (provider/throttle/timeout vs silent 0 views). */
  llm_translation_warning?: string | null;
  stats: Record<string, UCMVStats> | UCMVStats;
  migration_report?: string;
  /** Verify numbers: per-view counts of in-scope measures (keyed by view, plus "total"). */
  review_overview?: Record<string, UCMVReviewCounts>;
  /** Verify numbers: the run in five numbers (in scope, in a view, compared, matching, share). */
  review_headline?: UCMVReviewHeadline;
  /** Verify numbers: per-view status against the bar. */
  review_summary?: Record<string, UCMVReviewViewStatus>;
  /** Verify numbers: the bar (share of matching cells, in percent) a measure has to reach. */
  review_bar_pct?: number;
  /** Verify numbers: one row per measure with status and reason (statistics only). */
  review_csv?: string;
  /** Raw source measures with their original DAX expressions (echoed from the generator). */
  measures_with_dax?: unknown[];
  /** Raw source M-Query entries (echoed from the generator). */
  mquery_raw?: unknown[];
  /** Per-table tabular extract (M-Query source + associated measures/DAX). Always
   *  present; the primary usable artifact when no views could be generated. */
  fallback_extract?: FallbackExtractRow[];
  /** Number of UC metric views actually generated (0 → show the fallback table). */
  views_generated?: number;
  /** Flat list of measures that were NOT transpiled (original DAX + reason +
   *  proposed approach), for the "Not transpiled" review panel. */
  untranslatable_items?: UntranslatableItem[];
  /** Persisted reviewer triage annotations, keyed by untranslatableKey(item). */
  untranslatable_review?: Record<string, ReviewAnnotation>;
  /** Deterministic PBI<->UCMV reconciliation mapping draft, one YAML-text
   *  entry per view (keyed by view name), from pbi_ucmv_mapping.py. Feeds the
   *  downstream KPI-reconciliation pipeline once reviewed. */
  pbi_ucmv_mapping?: Record<string, string>;
  /** The report's reachability-based scope table (Pipeline Config Generator draft,
   *  completed with what this run translated): one row per measure, table, column,
   *  calculation item, function and RLS filter, with a keep/snapshot/fold/genie/drop
   *  decision. `csv` is the review artifact; `kasal_gaps` counts the rows to look at first. */
  report_scope?: {
    csv?: string;
    summary?: Record<string, unknown>;
    kasal_gaps?: { kept_but_not_translated: number; translated_but_dropped: number };
  } | null;
  /** Views backed by a LIVE connection to a semantic model (M-Query
   *  AnalysisServices.Database) — the transpiler can't resolve these, so the UI
   *  flags which semantic model/table to parse. {view_name: {server, database, table}}. */
  live_connections?: Record<string, { server: string; database: string; table: string }>;
  /** Source layer DDL: per-table CREATE VIEW for the base source layer UCMVs read from. */
  source_layer_ddl?: Record<string, {
    ddl: string;
    todo_steps: string[];
    error: string | null;
  }>;
  /** Tables sourced from non-SQL files (Excel, SharePoint, Web, typed) — need a snapshot
   *  ingestion pipeline before the metric views can read them. */
  pbi_only_ingestion_tasks?: Array<{
    table: string;
    uc_target: string;
    kind: string;
    source_description: string;
    recommended_approach: string;
    snapshot_loader_stub: string;
  }>;
  /** Validation loop outcome. */
  pbi_validation?: {
    status: 'skipped' | 'ran' | 'error';
    reason?: string;
    candidate_views?: string[];
    views?: Record<string, string>;
  };
  /** Set only when reconciliation was requested but did not run (missing
   *  reference_years/warehouse, unresolved creds, or an error). Names the reason
   *  so a single-pass is never silent. */
  reconciliation_skipped?: string | null;
}

export interface FallbackExtractRow {
  table_name: string;
  mquery: string;
  measures: Array<{ measure_name: string; dax_expression: string }>;
  measure_count: number;
  has_mquery: boolean;
}

export interface UCMVResultViewerProps {
  result: UCMVResult;
  /** Enable editing of individual metric view YAMLs */
  editable?: boolean;
  /** Called whenever the user edits YAML. Returns the full updated result. */
  onResultChange?: (updated: UCMVResult) => void;
  /** Called when user clicks Save — persists current edits to the database. */
  onSave?: (updated: UCMVResult) => Promise<void>;
}
