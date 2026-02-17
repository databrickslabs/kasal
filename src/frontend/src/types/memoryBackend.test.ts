import { describe, it, expect } from 'vitest';
import {
  MemoryBackendType,
  DEFAULT_MEMORY_BACKEND_CONFIG,
  DEFAULT_DATABRICKS_CONFIG,
  isValidMemoryBackendConfig,
  getBackendDisplayName,
  getBackendDescription,
} from './memoryBackend';

describe('MemoryBackendType enum', () => {
  it('has DEFAULT value set to "default"', () => {
    expect(MemoryBackendType.DEFAULT).toBe('default');
  });

  it('has DATABRICKS value set to "databricks"', () => {
    expect(MemoryBackendType.DATABRICKS).toBe('databricks');
  });

  it('contains exactly two members', () => {
    const values = Object.values(MemoryBackendType);
    expect(values).toHaveLength(2);
    expect(values).toContain('default');
    expect(values).toContain('databricks');
  });
});

describe('DEFAULT_MEMORY_BACKEND_CONFIG', () => {
  it('has backend_type set to default', () => {
    expect(DEFAULT_MEMORY_BACKEND_CONFIG.backend_type).toBe(MemoryBackendType.DEFAULT);
    expect(DEFAULT_MEMORY_BACKEND_CONFIG.backend_type).toBe('default');
  });

  it('enables short-term memory by default', () => {
    expect(DEFAULT_MEMORY_BACKEND_CONFIG.enable_short_term).toBe(true);
  });

  it('enables long-term memory by default', () => {
    expect(DEFAULT_MEMORY_BACKEND_CONFIG.enable_long_term).toBe(true);
  });

  it('enables entity memory by default', () => {
    expect(DEFAULT_MEMORY_BACKEND_CONFIG.enable_entity).toBe(true);
  });

  it('disables relationship retrieval by default', () => {
    expect(DEFAULT_MEMORY_BACKEND_CONFIG.enable_relationship_retrieval).toBe(false);
  });

  it('does not include a databricks_config', () => {
    expect(DEFAULT_MEMORY_BACKEND_CONFIG.databricks_config).toBeUndefined();
  });
});

describe('DEFAULT_DATABRICKS_CONFIG', () => {
  it('has embedding_dimension set to 1024', () => {
    expect(DEFAULT_DATABRICKS_CONFIG.embedding_dimension).toBe(1024);
  });

  it('has auth_type set to "default"', () => {
    expect(DEFAULT_DATABRICKS_CONFIG.auth_type).toBe('default');
  });

  it('has empty endpoint_name', () => {
    expect(DEFAULT_DATABRICKS_CONFIG.endpoint_name).toBe('');
  });

  it('has empty short_term_index', () => {
    expect(DEFAULT_DATABRICKS_CONFIG.short_term_index).toBe('');
  });
});

describe('isValidMemoryBackendConfig', () => {
  describe('rejects invalid inputs', () => {
    it('returns false for null', () => {
      expect(isValidMemoryBackendConfig(null)).toBe(false);
    });

    it('returns false for undefined', () => {
      expect(isValidMemoryBackendConfig(undefined)).toBe(false);
    });

    it('returns false for a number', () => {
      expect(isValidMemoryBackendConfig(42)).toBe(false);
    });

    it('returns false for a string', () => {
      expect(isValidMemoryBackendConfig('default')).toBe(false);
    });

    it('returns false for a boolean', () => {
      expect(isValidMemoryBackendConfig(true)).toBe(false);
    });

    it('returns false for an empty object', () => {
      expect(isValidMemoryBackendConfig({})).toBe(false);
    });
  });

  describe('rejects invalid backend_type', () => {
    it('returns false when backend_type is an unknown string', () => {
      expect(isValidMemoryBackendConfig({ backend_type: 'unknown' })).toBe(false);
    });

    it('returns false when backend_type is missing', () => {
      expect(isValidMemoryBackendConfig({ enable_short_term: true })).toBe(false);
    });

    it('returns false when backend_type is null', () => {
      expect(isValidMemoryBackendConfig({ backend_type: null })).toBe(false);
    });
  });

  describe('validates default backend config', () => {
    it('returns true for a minimal valid default config', () => {
      expect(
        isValidMemoryBackendConfig({ backend_type: 'default' })
      ).toBe(true);
    });

    it('returns true for the full DEFAULT_MEMORY_BACKEND_CONFIG constant', () => {
      expect(isValidMemoryBackendConfig(DEFAULT_MEMORY_BACKEND_CONFIG)).toBe(true);
    });

    it('returns true for default config with optional fields', () => {
      expect(
        isValidMemoryBackendConfig({
          backend_type: 'default',
          enable_short_term: true,
          enable_long_term: false,
          enable_entity: true,
          enable_relationship_retrieval: false,
        })
      ).toBe(true);
    });
  });

  describe('validates databricks backend config', () => {
    it('returns false when databricks config is missing entirely', () => {
      expect(
        isValidMemoryBackendConfig({ backend_type: 'databricks' })
      ).toBe(false);
    });

    it('returns false when endpoint_name is empty', () => {
      expect(
        isValidMemoryBackendConfig({
          backend_type: 'databricks',
          databricks_config: {
            endpoint_name: '',
            short_term_index: 'my_index',
          },
        })
      ).toBe(false);
    });

    it('returns false when short_term_index is empty', () => {
      expect(
        isValidMemoryBackendConfig({
          backend_type: 'databricks',
          databricks_config: {
            endpoint_name: 'my_endpoint',
            short_term_index: '',
          },
        })
      ).toBe(false);
    });

    it('returns false when both endpoint_name and short_term_index are empty', () => {
      expect(
        isValidMemoryBackendConfig({
          backend_type: 'databricks',
          databricks_config: {
            endpoint_name: '',
            short_term_index: '',
          },
        })
      ).toBe(false);
    });

    it('returns true for a complete valid databricks config', () => {
      expect(
        isValidMemoryBackendConfig({
          backend_type: 'databricks',
          databricks_config: {
            endpoint_name: 'my_endpoint',
            short_term_index: 'my_index',
            embedding_dimension: 1024,
            auth_type: 'default',
          },
        })
      ).toBe(true);
    });

    it('returns true for databricks config with all optional fields', () => {
      expect(
        isValidMemoryBackendConfig({
          backend_type: 'databricks',
          databricks_config: {
            endpoint_name: 'my_endpoint',
            document_endpoint_name: 'doc_endpoint',
            short_term_index: 'st_index',
            long_term_index: 'lt_index',
            entity_index: 'ent_index',
            document_index: 'doc_index',
            catalog: 'my_catalog',
            schema: 'my_schema',
            workspace_url: 'https://example.com',
            auth_type: 'pat',
            personal_access_token: 'token123',
            embedding_dimension: 1024,
          },
          enable_short_term: true,
          enable_long_term: true,
        })
      ).toBe(true);
    });
  });
});

describe('getBackendDisplayName', () => {
  it('returns correct display name for DEFAULT type', () => {
    expect(getBackendDisplayName(MemoryBackendType.DEFAULT)).toBe(
      'Default (ChromaDB + SQLite)'
    );
  });

  it('returns correct display name for DATABRICKS type', () => {
    expect(getBackendDisplayName(MemoryBackendType.DATABRICKS)).toBe(
      'Databricks Vector Search'
    );
  });

  it('falls back to the raw type string for an unknown type', () => {
    const unknownType = 'unknown_backend' as MemoryBackendType;
    expect(getBackendDisplayName(unknownType)).toBe('unknown_backend');
  });
});

describe('getBackendDescription', () => {
  it('returns a non-empty description for DEFAULT type', () => {
    const description = getBackendDescription(MemoryBackendType.DEFAULT);
    expect(description).toBeTruthy();
    expect(description.length).toBeGreaterThan(0);
  });

  it('returns a non-empty description for DATABRICKS type', () => {
    const description = getBackendDescription(MemoryBackendType.DATABRICKS);
    expect(description).toBeTruthy();
    expect(description.length).toBeGreaterThan(0);
  });

  it('DEFAULT description mentions ChromaDB', () => {
    const description = getBackendDescription(MemoryBackendType.DEFAULT);
    expect(description).toContain('ChromaDB');
  });

  it('DATABRICKS description mentions Vector Search', () => {
    const description = getBackendDescription(MemoryBackendType.DATABRICKS);
    expect(description).toContain('Vector Search');
  });

  it('falls back to empty string for an unknown type', () => {
    const unknownType = 'unknown_backend' as MemoryBackendType;
    expect(getBackendDescription(unknownType)).toBe('');
  });
});
