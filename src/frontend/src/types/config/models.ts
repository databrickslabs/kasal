export interface ModelConfig {
  name: string;
  temperature?: number;
  provider?: string;
  extended_thinking?: boolean;
  context_window?: number;
  max_output_tokens?: number;
  enabled?: boolean;
  /**
   * Whether this model accepts a native reasoning-effort budget. Derived
   * server-side from the same allow-list the engine uses
   * (backend src/utils/model_config.py), so the UI cannot offer a Reasoning
   * Effort setting that the engine will silently discard.
   */
  supports_reasoning_effort?: boolean;
}

export interface Models {
  [key: string]: ModelConfig;
} 