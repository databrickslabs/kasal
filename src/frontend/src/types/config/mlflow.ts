export interface MLflowBackend {
  kind: 'databricks' | 'local' | 'none';
  // Whether this backend is usable right now (workspace configured / local URI set).
  available?: boolean;
  uri?: string | null;
  reachable?: boolean | null;
  experiment?: string | null;
  url?: string | null;
}

export interface MLflowSettings {
  installation_managed?: boolean;
  resource_error?: string | null;
  enabled: boolean;
  evaluation_enabled: boolean;
  experiment_name?: string | null;
  // Judge model for evaluation and the default judge for prompt optimization.
  // null = not set (inside Databricks Apps the installed model is used).
  evaluation_judge_model?: string | null;
  // Advanced, with the built-in defaults filled in by the backend.
  evaluation_max_rows?: number;
  optimization_judge_samples?: number;
  // The backend a run WILL use (derived, not chosen).
  backend: MLflowBackend;
  // Every backend the environment offers, so Databricks / Local / None can be
  // shown side by side. The one whose kind === backend.kind is active.
  available?: MLflowBackend[];
}


/** A partial update. For the Advanced fields, null resets to the default. */
export type MLflowSettingsPatch = Partial<
  Pick<MLflowSettings, 'enabled' | 'evaluation_enabled' | 'experiment_name'>
> & {
  evaluation_judge_model?: string;
  evaluation_max_rows?: number | null;
  optimization_judge_samples?: number | null;
};
