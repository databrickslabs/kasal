/** A run budget per answer mode: {mode: {field: value}}. */
export type BudgetTable = Record<string, Record<string, number>>;

/** Configuration → Engines system settings, with defaults filled in. */
export interface EngineSettings {
  /** The Jev decisions API URL; null when not set (Jev then stays off). */
  jev_api_base: string | null;
  /** Seconds for one agent call when the agent sets none; 0 turns it off. */
  agent_max_execution_time: number;
  agent_max_execution_time_default: number;
  /** Effective budgets for the modes a run applies, and the built-in ones. */
  budgets: BudgetTable;
  budget_defaults: BudgetTable;
}

/** Partial update: an omitted field is left alone, null resets it. */
export interface EngineSettingsPatch {
  jev_api_base?: string | null;
  agent_max_execution_time?: number | null;
  budgets?: Record<string, Record<string, number | null>>;
}
