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

export interface APIKeysContextType {
  apiKeys: ApiKey[];
  loading: boolean;
  error: string | null;
  updateApiKeys: (apiKeys: ApiKey[]) => void;
} 