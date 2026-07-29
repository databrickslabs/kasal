/**
 * Search over the API Keys list.
 *
 * The page shows ~20 rows once the model and local placeholders are added, and
 * finding one meant scanning by eye. The one thing a naive filter gets wrong is
 * the naming: keys are SCREAMING_SNAKE_CASE, so the query a person actually
 * types — "openai key" — is not a substring of "OPENAI_API_KEY".
 */

import { describe, expect, it } from 'vitest';

import { matchesSearch } from './searchApiKeys';
import type { ApiKey } from '../../../api';

const key = (name: string, description = ''): ApiKey => ({
  id: 1,
  name,
  value: 'Set',
  description,
  created_at: '',
  updated_at: '',
});

describe('matchesSearch', () => {
  it('keeps every key when the query is empty', () => {
    expect(matchesSearch(key('KIMI_API_KEY'), '')).toBe(true);
    expect(matchesSearch(key('KIMI_API_KEY'), '   ')).toBe(true);
  });

  it('matches a name case-insensitively', () => {
    expect(matchesSearch(key('KIMI_API_KEY'), 'kimi')).toBe(true);
    expect(matchesSearch(key('KIMI_API_KEY'), 'KIMI')).toBe(true);
  });

  it('matches across the underscores', () => {
    // The regression this guards: a literal substring test fails here, because
    // "openai key" appears nowhere in "OPENAI_API_KEY".
    expect(matchesSearch(key('OPENAI_API_KEY'), 'openai key')).toBe(true);
    expect(matchesSearch(key('POWERBI_CLIENT_SECRET'), 'powerbi secret')).toBe(true);
  });

  it('requires every term, so terms narrow rather than widen', () => {
    expect(matchesSearch(key('OPENAI_API_KEY'), 'openai anthropic')).toBe(false);
  });

  it('ignores term order', () => {
    expect(matchesSearch(key('POWERBI_CLIENT_SECRET'), 'secret client')).toBe(true);
  });

  it('searches the description too', () => {
    const perplexity = key('PERPLEXITY_API_KEY', 'API Key for perplexity search');
    expect(matchesSearch(perplexity, 'search')).toBe(true);
  });

  it('tolerates a missing description', () => {
    const noDescription = { ...key('EXA_API_KEY'), description: undefined as unknown as string };
    expect(matchesSearch(noDescription, 'exa')).toBe(true);
    expect(matchesSearch(noDescription, 'nothing')).toBe(false);
  });

  it('excludes a key that matches nothing', () => {
    expect(matchesSearch(key('GEMINI_API_KEY'), 'databricks')).toBe(false);
  });

  it('treats a hyphen or dot in the query like the underscore it stands for', () => {
    expect(matchesSearch(key('DEEPSEEK_API_KEY'), 'deepseek-api')).toBe(true);
    expect(matchesSearch(key('DEEPSEEK_API_KEY'), 'deepseek.api.key')).toBe(true);
  });

  it('narrows a realistic list to one row', () => {
    const keys = [
      'OPENAI_API_KEY',
      'DATABRICKS_API_KEY',
      'ANTHROPIC_API_KEY',
      'KIMI_API_KEY',
      'POWERBI_USERNAME',
      'POWERBI_PASSWORD',
      'POWERBI_CLIENT_SECRET',
    ].map(name => key(name));

    expect(keys.filter(k => matchesSearch(k, 'kimi')).map(k => k.name)).toEqual([
      'KIMI_API_KEY',
    ]);
    expect(keys.filter(k => matchesSearch(k, 'powerbi')).map(k => k.name)).toEqual([
      'POWERBI_USERNAME',
      'POWERBI_PASSWORD',
      'POWERBI_CLIENT_SECRET',
    ]);
  });
});
