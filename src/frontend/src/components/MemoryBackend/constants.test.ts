import { describe, it, expect } from 'vitest';
import { EMBEDDING_MODELS, INDEX_DESCRIPTIONS } from './constants';

describe('EMBEDDING_MODELS', () => {
  it('should contain exactly 3 embedding models', () => {
    expect(EMBEDDING_MODELS).toHaveLength(3);
  });

  it('should have required fields on every model', () => {
    const requiredFields = ['name', 'value', 'dimension', 'description'];
    EMBEDDING_MODELS.forEach((model) => {
      requiredFields.forEach((field) => {
        expect(model).toHaveProperty(field);
      });
    });
  });

  describe('first model - Databricks GTE Large (default)', () => {
    it('should have value databricks-gte-large-en', () => {
      expect(EMBEDDING_MODELS[0].value).toBe('databricks-gte-large-en');
    });

    it('should have name Databricks GTE Large (English)', () => {
      expect(EMBEDDING_MODELS[0].name).toBe('Databricks GTE Large (English)');
    });

    it('should have dimension 1024', () => {
      expect(EMBEDDING_MODELS[0].dimension).toBe(1024);
    });

    it('should have a non-empty description', () => {
      expect(EMBEDDING_MODELS[0].description).toBeTruthy();
      expect(typeof EMBEDDING_MODELS[0].description).toBe('string');
    });
  });

  describe('second model - Databricks BGE Large', () => {
    it('should have value databricks-bge-large-en', () => {
      expect(EMBEDDING_MODELS[1].value).toBe('databricks-bge-large-en');
    });

    it('should have name Databricks BGE Large (English)', () => {
      expect(EMBEDDING_MODELS[1].name).toBe('Databricks BGE Large (English)');
    });

    it('should have dimension 1024', () => {
      expect(EMBEDDING_MODELS[1].dimension).toBe(1024);
    });

    it('should have a non-empty description', () => {
      expect(EMBEDDING_MODELS[1].description).toBeTruthy();
      expect(typeof EMBEDDING_MODELS[1].description).toBe('string');
    });
  });

  describe('third model - OpenAI text-embedding-3-large', () => {
    it('should have value text-embedding-3-large', () => {
      expect(EMBEDDING_MODELS[2].value).toBe('text-embedding-3-large');
    });

    it('should have name OpenAI text-embedding-3-large', () => {
      expect(EMBEDDING_MODELS[2].name).toBe('OpenAI text-embedding-3-large');
    });

    it('should have dimension 3072', () => {
      expect(EMBEDDING_MODELS[2].dimension).toBe(3072);
    });

    it('should have a non-empty description', () => {
      expect(EMBEDDING_MODELS[2].description).toBeTruthy();
      expect(typeof EMBEDDING_MODELS[2].description).toBe('string');
    });
  });

  it('should not contain the old text-embedding-3-small model', () => {
    const values = EMBEDDING_MODELS.map((m) => m.value);
    expect(values).not.toContain('text-embedding-3-small');
  });

  it('should have numeric dimensions on all models', () => {
    EMBEDDING_MODELS.forEach((model) => {
      expect(typeof model.dimension).toBe('number');
      expect(model.dimension).toBeGreaterThan(0);
    });
  });
});

describe('INDEX_DESCRIPTIONS', () => {
  const expectedKeys = ['short_term', 'long_term', 'entity', 'document'] as const;

  it('should have all 4 index type keys', () => {
    expectedKeys.forEach((key) => {
      expect(INDEX_DESCRIPTIONS).toHaveProperty(key);
    });
  });

  it('should have exactly 4 keys', () => {
    expect(Object.keys(INDEX_DESCRIPTIONS)).toHaveLength(4);
  });

  expectedKeys.forEach((key) => {
    describe(`${key} index description`, () => {
      it('should have a brief field', () => {
        expect(INDEX_DESCRIPTIONS[key]).toHaveProperty('brief');
      });

      it('should have a detailed field', () => {
        expect(INDEX_DESCRIPTIONS[key]).toHaveProperty('detailed');
      });

      it('should have a non-empty brief string', () => {
        const { brief } = INDEX_DESCRIPTIONS[key];
        expect(typeof brief).toBe('string');
        expect(brief.length).toBeGreaterThan(0);
      });

      it('should have a non-empty detailed string', () => {
        const { detailed } = INDEX_DESCRIPTIONS[key];
        expect(typeof detailed).toBe('string');
        expect(detailed.length).toBeGreaterThan(0);
      });

      it('should have detailed text longer than brief text', () => {
        const { brief, detailed } = INDEX_DESCRIPTIONS[key];
        expect(detailed.length).toBeGreaterThan(brief.length);
      });
    });
  });
});
