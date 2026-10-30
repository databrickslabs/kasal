import { describe, expect, it, vi } from 'vitest';

vi.mock('../api/databricks/DatabricksService', () => ({ DatabricksService: {} }));

import { providerToKeyName } from './apiKeyUtils';

describe('providerToKeyName', () => {
  it('maps the openrouter provider to OPENROUTER_API_KEY', () => {
    expect(providerToKeyName.openrouter).toBe('OPENROUTER_API_KEY');
  });
});
