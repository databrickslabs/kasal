/** A run budget per answer mode: {mode: {field: value}}. */
export type BudgetTable = Record<string, Record<string, number>>;

/** Configuration → Engines system settings, with defaults filled in. */
export interface EngineSettings {
  /** The Jev decisions API URL; null when not set (Jev then stays off). */
  jev_api_base: string | null;
  /** How the decision model is reached: TypeSafe, OpenRouter or Databricks. */
  decision_connection?: 'jev' | 'openrouter' | 'databricks';
  /** OpenRouter's API URL (its public API when not set). */
  openrouter_api_base?: string | null;
  /** The model the Databricks connection asks (the effective value, default filled in). */
  databricks_decision_model?: string | null;
  /** Seconds for one agent call when the agent sets none; 0 turns it off. */
  agent_max_execution_time: number;
  agent_max_execution_time_default: number;
  /** Effective budgets for the modes a run applies, and the built-in ones. */
  budgets: BudgetTable;
  budget_defaults: BudgetTable;
  /** Server-wide scalar settings (memory sweep, knowledge limits). */
  advanced: Record<string, boolean | number | string>;
  advanced_specs: Record<string, EngineSettingSpec>;
}

export interface EngineSettingSpec {
  default: boolean | number | string;
  minimum: number | null;
  maximum: number | null;
}

/** Partial update: an omitted field is left alone, null resets it. */
export interface EngineSettingsPatch {
  jev_api_base?: string | null;
  decision_connection?: 'jev' | 'openrouter' | 'databricks' | null;
  openrouter_api_base?: string | null;
  databricks_decision_model?: string | null;
  agent_max_execution_time?: number | null;
  budgets?: Record<string, Record<string, number | null>>;
  advanced?: Record<string, boolean | number | string | null>;
}
