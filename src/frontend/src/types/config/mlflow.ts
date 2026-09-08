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
  // The backend a run WILL use (derived, not chosen).
  backend: MLflowBackend;
  // Every backend the environment offers, so Databricks / Local / None can be
  // shown side by side. The one whose kind === backend.kind is active.
  available?: MLflowBackend[];
}

