/**
 * Config shape for the UC Metric View Generator tool (Tool 86).
 * Re-exported from UCMetricViewGeneratorConfigSelector for existing importers.
 */
// Authentication method type
export type PowerBIAuthMethod = 'service_principal' | 'service_account' | 'user_oauth';

export interface UCMetricViewGeneratorConfig {
  // Input mode
  mode?: 'api' | 'json';
  // PBI API config (API mode)
  workspace_id?: string;
  dataset_id?: string;
  auth_method?: PowerBIAuthMethod;
  tenant_id?: string;
  client_id?: string;
  client_secret?: string;
  username?: string;
  password?: string;
  oauth_client_id?: string;
  access_token?: string;
  pbi_api_base_url?: string;
  // JSON mode (paste JSONs directly)
  measures_json?: string;
  mquery_json?: string;
  relationships_json?: string;
  scan_data_json?: string;
  config_json?: string;
  // Target
  catalog?: string;
  schema_name?: string;
  // Behavior
  inner_dim_joins?: boolean;
  unflatten_tables?: boolean;
  use_llm_fallback?: boolean;
  llm_model?: string;
  llm_workspace_url?: string;
  llm_token?: string;
  // Reconciliation & quality
  warehouse_id?: string;
  reference_years?: string;
  enable_reconciliation?: boolean;
  max_reconciliation_cycles?: number;
  reconciliation_target_pct?: number;
  prune_non_visual_measures?: boolean;
  naming_config?: string;
  domain_context?: string;
  // Index signature for compatibility
  [key: string]: string | boolean | number | undefined;
}
