/**
 * Memory backend configuration types.
 *
 * CrewAI's unified Memory class replaces the legacy short/long/entity split
 * with a single scoped memory store, so these types no longer carry per-tier
 * enable flags or per-tier index/table names.
 */

export enum MemoryBackendType {
  DEFAULT = 'default', // Kasal unified Memory (local SQLite)
  // Legacy value kept so saved configurations still parse. Memory runs on
  // Lakebase or the local store; a databricks config falls back to local.
  DATABRICKS = 'databricks',
  LAKEBASE = 'lakebase', // Lakebase pgvector
}

/**
 * Memory Tuning knobs for a teamspace's memory. Every field here reaches the
 * layer that uses it — the scoring ones the storage backend, the rest the
 * engine's `Memory` — so a value set in the panel is never silently dropped.
 */
export interface MemoryTuningConfig {
  // Composite score weights (semantic + keyword + recency + importance —
  // should roughly sum to 1.0).
  semantic_weight?: number;
  keyword_weight?: number;
  recency_weight?: number;
  importance_weight?: number;
  recency_half_life_days?: number;
  /** Minimum semantic similarity for a memory to be recalled at all (default 0.35). */
  relevance_threshold?: number;
  /**
   * Blended-score floor below which a recall returns nothing. Unset = the
   * embedder's own default (0.75 Databricks, 0.62 local Ollama fallback).
   */
  recall_min_score?: number;

  // Consolidation.
  consolidation_threshold?: number;
  consolidation_limit?: number;
  default_importance?: number;

  // Recall depth control.
  confidence_threshold_high?: number;
  confidence_threshold_low?: number;
  complex_query_threshold?: number;
  exploration_budget?: number;
  query_analysis_threshold?: number;

  // LLM override for memory analysis.
  memory_llm_model?: string;
}

export interface DatabricksMemoryConfig {
  // Memory endpoint (Direct Access for dynamic record-level writes).
  endpoint_name: string;

  // Memory index — one index for every MemoryRecord.
  memory_index: string;

  // Document search endpoint + index (unrelated to memory).
  document_endpoint_name?: string;
  document_index?: string;

  // Database configuration
  catalog?: string;
  schema?: string;

  // Authentication (optional - can use environment variables)
  workspace_url?: string;
  auth_type?: 'default' | 'pat' | 'service_principal';

  // For PAT authentication
  personal_access_token?: string;

  // For Service Principal authentication
  service_principal_client_id?: string;
  service_principal_client_secret?: string;

  // Vector configuration
  embedding_dimension?: number;
}

export interface LakebaseMemoryConfig {
  instance_name?: string;
  embedding_dimension?: number;
  // Memory table — one table for every MemoryRecord.
  memory_table?: string;
  tables_initialized?: boolean;
}

export interface MemoryBackendConfig {
  backend_type: MemoryBackendType;

  // Backend-specific configuration
  databricks_config?: DatabricksMemoryConfig;
  lakebase_config?: LakebaseMemoryConfig;

  // Recall-scoring parameters. The wire field name is fixed by the API.
  cognitive_config?: MemoryTuningConfig;

  // Database persistence fields
  is_default?: boolean;
  is_active?: boolean;

  // Escape hatch for experimental backend-specific options
  custom_config?: Record<string, unknown>;
}

// Default configurations for easy setup
export const DEFAULT_MEMORY_BACKEND_CONFIG: MemoryBackendConfig = {
  backend_type: MemoryBackendType.DEFAULT,
};

export const DEFAULT_DATABRICKS_CONFIG: DatabricksMemoryConfig = {
  endpoint_name: '',
  memory_index: '',
  embedding_dimension: 1024,
  auth_type: 'default',
};

export const DEFAULT_LAKEBASE_CONFIG: LakebaseMemoryConfig = {
  embedding_dimension: 1024,
  memory_table: 'crew_memory',
  tables_initialized: false,
};

/**
 * The EFFECTIVE defaults — what the backend applies when a knob is unset —
 * so an untouched slider shows the value that is actually in force
 * (local_storage_backend.LocalMemoryStorage and engine/memory.Memory).
 */
export const MEMORY_TUNING_DEFAULTS: Required<
  Pick<
    MemoryTuningConfig,
    | 'semantic_weight'
    | 'keyword_weight'
    | 'recency_weight'
    | 'importance_weight'
    | 'recency_half_life_days'
    | 'relevance_threshold'
    | 'recall_min_score'
    | 'consolidation_threshold'
    | 'consolidation_limit'
    | 'default_importance'
    | 'confidence_threshold_high'
    | 'confidence_threshold_low'
    | 'complex_query_threshold'
    | 'exploration_budget'
    | 'query_analysis_threshold'
  >
> = {
  semantic_weight: 0.6,
  keyword_weight: 0.15,
  recency_weight: 0.15,
  importance_weight: 0.1,
  recency_half_life_days: 30,
  relevance_threshold: 0.35,
  recall_min_score: 0.75,
  consolidation_threshold: 0.85,
  consolidation_limit: 5,
  default_importance: 0.5,
  confidence_threshold_high: 0.8,
  confidence_threshold_low: 0.5,
  complex_query_threshold: 0.7,
  exploration_budget: 1,
  query_analysis_threshold: 200,
};

// Validation helpers
export const isValidMemoryBackendConfig = (config: unknown): config is MemoryBackendConfig => {
  if (!config || typeof config !== 'object' || config === null) return false;

  const configObj = config as Record<string, unknown>;

  if (!Object.values(MemoryBackendType).includes(configObj.backend_type as MemoryBackendType)) {
    return false;
  }

  if (configObj.backend_type === MemoryBackendType.DATABRICKS) {
    const databricksConfig = configObj.databricks_config as DatabricksMemoryConfig | undefined;
    if (!databricksConfig) return false;
    if (!databricksConfig.endpoint_name || !databricksConfig.memory_index) {
      return false;
    }
  }

  if (configObj.backend_type === MemoryBackendType.LAKEBASE) {
    const lakebaseConfig = configObj.lakebase_config as LakebaseMemoryConfig | undefined;
    if (!lakebaseConfig) return false;
    if (!lakebaseConfig.memory_table) return false;
  }

  return true;
};

/**
 * Knowledge sources (RAG) require a Lakebase pgvector memory backend, where
 * knowledge-file embeddings are stored. The default (local SQLite) backend
 * cannot store/retrieve document embeddings.
 */
export const isKnowledgeCapableMemoryConfig = (config: unknown): boolean => {
  if (!isValidMemoryBackendConfig(config)) return false;
  const c = config as MemoryBackendConfig;
  return c.backend_type === MemoryBackendType.LAKEBASE && Boolean(c.lakebase_config?.memory_table);
};

// Helper to get display name for backend type
export const getBackendDisplayName = (type: MemoryBackendType): string => {
  const displayNames: Record<MemoryBackendType, string> = {
    [MemoryBackendType.DEFAULT]: 'Local (Kasal unified Memory / SQLite)',
    [MemoryBackendType.DATABRICKS]: 'Legacy (falls back to local)',
    [MemoryBackendType.LAKEBASE]: 'Lakebase (pgvector)',
  };
  return displayNames[type] || type;
};

// Helper to get backend description
export const getBackendDescription = (type: MemoryBackendType): string => {
  const descriptions: Record<MemoryBackendType, string> = {
    [MemoryBackendType.DEFAULT]:
      'Kasal memory stored in a local SQLite database. No external infrastructure required.',
    [MemoryBackendType.DATABRICKS]:
      'Legacy configuration. Memory falls back to the local store; configure Lakebase for shared, persistent memory.',
    [MemoryBackendType.LAKEBASE]:
      'Kasal memory backed by your configured Lakebase PostgreSQL instance with pgvector. Zero additional infrastructure required.',
  };
  return descriptions[type] || '';
};

// Additional types for Databricks setup UI components
export interface EndpointInfo {
  name: string;
  type?: string;
  status?: string;
  error?: string;
  state?: string;
  ready?: boolean;
  can_delete_indexes?: boolean;
}

export interface IndexInfo {
  name: string;
  status?: string;
  index_type?: string;
}

export interface SavedConfigInfo {
  backend_id?: string;
  workspace_url?: string;
  catalog?: string;
  schema?: string;
  endpoints?: {
    memory?: EndpointInfo;
    document?: EndpointInfo;
  };
  indexes?: {
    unified?: IndexInfo;
    document?: IndexInfo;
  };
}

export interface SetupResult {
  success: boolean;
  message: string;
  endpoints?: {
    memory?: EndpointInfo;
    document?: EndpointInfo;
  };
  indexes?: {
    unified?: IndexInfo;
    document?: IndexInfo;
  };
  config?: {
    endpoint_name?: string;
    document_endpoint_name?: string;
    memory_index?: string;
    document_index?: string;
    workspace_url?: string;
    embedding_dimension?: number;
    catalog?: string;
    schema?: string;
  };
  catalog?: string;
  schema?: string;
  backend_id?: string;
  error?: string;
  warning?: string;
  info?: string;
}

export interface IndexInfoState {
  doc_count: number;
  loading: boolean;
  error?: string;
  status?: string;
  ready?: boolean;
  index_type?: string;
}

export interface ManualConfig {
  workspace_url: string;
  endpoint_name: string;
  document_endpoint_name: string;
  memory_index: string;
  document_index: string;
  embedding_model: string;
}
