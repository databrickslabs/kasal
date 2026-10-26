export interface Schema {
  id: number;
  name: string;
  description: string;
  schema_type: string;
  schema_definition: Record<string, unknown>;
  schema_json?: Record<string, unknown>;
  field_descriptions?: Record<string, string | unknown>;
  keywords?: string[];
  tools?: string[];
  example_data?: Record<string, unknown>;
  created_at: string;
  updated_at: string;
}

export interface SchemaCreate {
  name: string;
  description: string;
  schema_type: string;
  schema_definition: Record<string, unknown>;
  schema_json?: Record<string, unknown>;
  field_descriptions?: Record<string, string | unknown>;
  keywords?: string[];
  tools?: string[];
  example_data?: Record<string, unknown>;
}

export interface SchemaListResponse {
  schemas: Schema[];
  count: number;
} 