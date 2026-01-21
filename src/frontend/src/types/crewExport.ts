/**
 * Types for crew export and deployment functionality
 */

export enum ExportFormat {
  PYTHON_PROJECT = 'python_project',
  DATABRICKS_NOTEBOOK = 'databricks_notebook'
}

export enum DeploymentTarget {
  DATABRICKS_MODEL_SERVING = 'databricks_model_serving'
}

export enum DeploymentStatus {
  PENDING = 'pending',
  READY = 'ready',
  UPDATING = 'updating',
  UPDATE_FAILED = 'update_failed',
  NOT_READY = 'not_ready'
}

export interface ExportOptions {
  include_custom_tools?: boolean;
  include_comments?: boolean;
  include_tests?: boolean;
  model_override?: string;
  // Databricks notebook options
  include_tracing?: boolean;
  include_evaluation?: boolean;
  include_deployment?: boolean;
}

export interface CrewExportRequest {
  export_format: ExportFormat;
  options?: ExportOptions;
}

export interface FileInfo {
  path: string;
  content: string;
}

export interface CrewExportResponse {
  crew_id: string;
  crew_name: string;
  export_format: string;
  files?: FileInfo[];
  notebook_content?: string;
  metadata: {
    agents_count: number;
    tasks_count: number;
    tools_count: number;
    cells_count?: number;
    sanitized_name: string;
  };
  generated_at: string;
  size_bytes: number;
  download_url?: string;
}

export interface ModelServingConfig {
  model_name: string;
  endpoint_name?: string;
  workload_size?: 'Small' | 'Medium' | 'Large';
  scale_to_zero_enabled?: boolean;
  min_instances?: number;
  max_instances?: number;
  unity_catalog_model?: boolean;
  catalog_name?: string;
  schema_name?: string;
  environment_vars?: Record<string, string>;
  tags?: Record<string, string>;
}

export interface DeploymentRequest {
  config: ModelServingConfig;
}

export interface DeploymentResponse {
  crew_id: string;
  crew_name: string;
  deployment_target: string;
  model_name: string;
  model_version: string;
  model_uri: string;
  endpoint_name: string;
  endpoint_url: string;
  endpoint_status: DeploymentStatus;
  deployed_at: string;
  metadata: {
    agents_count: number;
    tasks_count: number;
    workload_size?: string;
    scale_to_zero?: boolean;
  };
  usage_example: string;
}

export interface DeploymentStatusResponse {
  endpoint_name: string;
  state: string;
  config_update: string | null;
  pending_config: boolean;
  ready_replicas: number;
  target_replicas: number;
  creator: string;
  creation_timestamp: number;
  last_updated_timestamp: number;
}
