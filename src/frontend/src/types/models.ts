export interface ModelConfig {
  name: string;
  temperature?: number;
  provider?: string;
  extended_thinking?: boolean;
  context_window?: number;
  max_output_tokens?: number;
  enabled?: boolean;
}

export interface Models {
  [key: string]: ModelConfig;
} 