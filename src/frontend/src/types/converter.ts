/**
 * TypeScript types for Converter System
 * Matches backend Pydantic schemas
 */

// ===== Enum Types =====

export type ConversionStatus = 'pending' | 'running' | 'success' | 'failed';

export type JobStatus = 'pending' | 'running' | 'completed' | 'failed' | 'cancelled';

export type ConversionFormat = 'powerbi' | 'yaml' | 'dax' | 'sql' | 'uc_metrics' | 'tableau' | 'excel';

// Separate types for inbound (source) and outbound (target) formats
export type InboundFormat = 'powerbi' | 'yaml' | 'tableau' | 'excel';
export type OutboundFormat = 'dax' | 'sql' | 'uc_metrics';

export type SQLDialect = 'databricks' | 'standard';

// ===== Conversion History Types =====

export interface ConversionHistory {
  id: number;
  execution_id?: string;
  source_format: string;
  target_format: string;
  input_data?: Record<string, any>;
  output_data?: Record<string, any>;
  input_summary?: string;
  output_summary?: string;
  configuration?: Record<string, any>;
  status: ConversionStatus;
  measure_count?: number;
  job_id?: string;
  error_message?: string;
  warnings?: string[];
  execution_time_ms?: number;
  converter_version?: string;
  group_id?: string;
  created_by_email?: string;
  created_at: string;
  updated_at: string;
}

export interface ConversionHistoryCreate {
  execution_id?: string;
  source_format: string;
  target_format: string;
  input_data?: Record<string, any>;
  output_data?: Record<string, any>;
  input_summary?: string;
  output_summary?: string;
  configuration?: Record<string, any>;
  status?: ConversionStatus;
  measure_count?: number;
  error_message?: string;
  warnings?: string[];
  execution_time_ms?: number;
  converter_version?: string;
  extra_metadata?: Record<string, any>;
}

export interface ConversionHistoryUpdate {
  status?: ConversionStatus;
  output_data?: Record<string, any>;
  output_summary?: string;
  error_message?: string;
  warnings?: string[];
  measure_count?: number;
  execution_time_ms?: number;
}

export interface ConversionHistoryFilter {
  source_format?: string;
  target_format?: string;
  status?: ConversionStatus;
  execution_id?: string;
  limit?: number;
  offset?: number;
}

export interface ConversionHistoryListResponse {
  history: ConversionHistory[];
  count: number;
  limit: number;
  offset: number;
}

export interface ConversionStatistics {
  total_conversions: number;
  successful: number;
  failed: number;
  success_rate: number;
  average_execution_time_ms: number;
  popular_conversions: Array<{
    source: string;
    target: string;
    count: number;
  }>;
  period_days: number;
}

// ===== Conversion Job Types =====

export interface ConversionJob {
  id: string;
  tool_id?: number;
  name?: string;
  description?: string;
  source_format: string;
  target_format: string;
  configuration: Record<string, any>;
  status: JobStatus;
  progress?: number;
  result?: Record<string, any>;
  error_message?: string;
  execution_id?: string;
  history_id?: number;
  group_id?: string;
  created_by_email?: string;
  created_at: string;
  updated_at: string;
  started_at?: string;
  completed_at?: string;
}

export interface ConversionJobCreate {
  tool_id?: number;
  name?: string;
  description?: string;
  source_format: string;
  target_format: string;
  configuration: Record<string, any>;
  execution_id?: string;
  extra_metadata?: Record<string, any>;
}

export interface ConversionJobUpdate {
  name?: string;
  description?: string;
  status?: JobStatus;
  progress?: number;
  result?: Record<string, any>;
  error_message?: string;
}

export interface ConversionJobStatusUpdate {
  status: JobStatus;
  progress?: number;
  error_message?: string;
}

export interface ConversionJobListResponse {
  jobs: ConversionJob[];
  count: number;
}

// ===== Saved Configuration Types =====

export interface SavedConverterConfiguration {
  id: number;
  name: string;
  description?: string;
  source_format: string;
  target_format: string;
  configuration: Record<string, any>;
  is_public: boolean;
  is_template: boolean;
  tags?: string[];
  use_count: number;
  last_used_at?: string;
  extra_metadata?: Record<string, any>;
  group_id?: string;
  created_by_email: string;
  created_at: string;
  updated_at: string;
}

export interface SavedConfigurationCreate {
  name: string;
  description?: string;
  source_format: string;
  target_format: string;
  configuration: Record<string, any>;
  is_public?: boolean;
  is_template?: boolean;
  tags?: string[];
  extra_metadata?: Record<string, any>;
}

export interface SavedConfigurationUpdate {
  name?: string;
  description?: string;
  configuration?: Record<string, any>;
  is_public?: boolean;
  tags?: string[];
  extra_metadata?: Record<string, any>;
}

export interface SavedConfigurationFilter {
  source_format?: string;
  target_format?: string;
  is_public?: boolean;
  is_template?: boolean;
  search?: string;
  limit?: number;
}

export interface SavedConfigurationListResponse {
  configurations: SavedConverterConfiguration[];
  count: number;
}

// ===== Tool Configuration Types =====

export interface MeasureConversionConfig {
  // Inbound Selection
  inbound_connector: InboundFormat;

  // Power BI Config
  powerbi_semantic_model_id?: string;
  powerbi_group_id?: string;
  powerbi_tenant_id?: string;
  powerbi_client_id?: string;
  powerbi_client_secret?: string;
  powerbi_include_hidden?: boolean;
  powerbi_filter_pattern?: string;

  // YAML Config
  yaml_content?: string;
  yaml_file_path?: string;

  // Outbound Selection
  outbound_format: OutboundFormat;

  // SQL Config
  sql_dialect?: SQLDialect;
  sql_include_comments?: boolean;
  sql_process_structures?: boolean;

  // UC Metrics Config
  uc_catalog?: string;
  uc_schema?: string;
  uc_process_structures?: boolean;

  // DAX Config
  dax_process_structures?: boolean;

  // General
  definition_name?: string;
  result_as_answer?: boolean;
}

// ===== UI State Types =====

export interface ConverterFormState {
  config: MeasureConversionConfig;
  isLoading: boolean;
  error?: string;
  result?: any;
}

export interface ConverterDashboardFilters {
  historyFilters: ConversionHistoryFilter;
  jobStatus?: JobStatus;
  configFilters: SavedConfigurationFilter;
}
