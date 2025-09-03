export interface ApiKey {
  id: number;
  name: string;
  value: string;
  description?: string;
}

export interface ApiKeyCreate {
  name: string;
  value: string;
  description?: string;
}

export interface ApiKeyUpdate {
  value: string;
  description?: string;
}

export interface DatabricksSecret {
  id: number;
  name: string;
  value: string;
  description?: string;
  scope: string;
  source: string;
}

export interface DatabricksSecretCreate {
  name: string;
  value: string;
  description?: string;
}

export interface DatabricksSecretUpdate {
  value: string;
  description?: string;
}

export interface DatabricksTokenRequest {
  workspace_url: string;
  token: string;
}

export interface APIKeysContextType {
  apiKeys: ApiKey[];
  loading: boolean;
  error: string | null;
  updateApiKeys: (apiKeys: ApiKey[]) => void;
} 