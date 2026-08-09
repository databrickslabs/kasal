import { describe, it, expect } from 'vitest';
import { schemaToRoutableFields, singularize } from './schemaFields';
import { MAX_NESTING } from './schemaModel';

describe('schemaToRoutableFields', () => {
  it('lists a flat schema exactly as before', () => {
    const fields = schemaToRoutableFields({
      type: 'object',
      properties: { category: { type: 'string' } },
    });

    expect(fields).toEqual([
      { path: 'category', label: 'category', type: 'string', isList: false },
    ]);
  });

  it('reaches a value nested inside an object', () => {
    const fields = schemaToRoutableFields({
      type: 'object',
      properties: {
        classification: {
          type: 'object',
          properties: {
            category: { type: 'string' },
            confidence: { type: 'number' },
          },
        },
      },
    });

    expect(fields).toEqual([
      {
        path: 'classification.category',
        label: 'classification → category',
        type: 'string',
        isList: false,
      },
      {
        path: 'classification.confidence',
        label: 'classification → confidence',
        type: 'number',
        isList: false,
      },
    ]);
  });

  it('reaches a value across a list of objects, and says so', () => {
    const fields = schemaToRoutableFields({
      type: 'object',
      properties: {
        articles: {
          type: 'array',
          items: {
            type: 'object',
            properties: {
              category: { type: 'string' },
              score: { type: 'number' },
            },
          },
        },
      },
    });

    expect(fields).toEqual([
      {
        path: 'articles[].category',
        label: 'Any article → category',
        type: 'string',
        isList: true,
      },
      {
        path: 'articles[].score',
        label: 'Any article → score',
        type: 'number',
        isList: true,
      },
    ]);
  });

  it('handles a list of plain values', () => {
    const fields = schemaToRoutableFields({
      type: 'object',
      properties: { tags: { type: 'array', items: { type: 'string' } } },
    });

    expect(fields).toEqual([
      { path: 'tags[]', label: 'Any tag', type: 'string', isList: true },
    ]);
  });

  it('returns nothing comparable for an object with no scalar leaves', () => {
    expect(
      schemaToRoutableFields({
        type: 'object',
        properties: { blob: { type: 'object' } },
      })
    ).toEqual([]);
  });

  it('reaches a value across a list inside a list', () => {
    const fields = schemaToRoutableFields({
      type: 'object',
      properties: {
        orders: {
          type: 'array',
          items: {
            type: 'object',
            properties: {
              lines: {
                type: 'array',
                items: { type: 'object', properties: { sku: { type: 'string' } } },
              },
            },
          },
        },
      },
    });

    expect(fields).toEqual([
      {
        path: 'orders[].lines[].sku',
        label: 'Any order → Any line → sku',
        type: 'string',
        isList: true,
      },
    ]);
  });

  it('reaches a value inside an object inside a list', () => {
    const fields = schemaToRoutableFields({
      type: 'object',
      properties: {
        articles: {
          type: 'array',
          items: {
            type: 'object',
            properties: {
              meta: { type: 'object', properties: { category: { type: 'string' } } },
            },
          },
        },
      },
    });

    expect(fields[0].path).toBe('articles[].meta.category');
    expect(fields[0].label).toBe('Any article → meta → category');
    expect(fields[0].isList).toBe(true);
  });

  it('stops descending eventually so the dropdown stays readable', () => {
    let node: Record<string, unknown> = { type: 'string' };
    for (let i = 0; i < 10; i += 1) {
      node = { type: 'object', properties: { [`n${i}`]: node } };
    }

    const fields = schemaToRoutableFields(node);

    expect(fields.every((f) => f.path.split('.').length <= MAX_NESTING + 2)).toBe(true);
  });

  it('humanizes underscored names', () => {
    const fields = schemaToRoutableFields({
      type: 'object',
      properties: { news_items: { type: 'array', items: { type: 'object', properties: { top_category: { type: 'string' } } } } },
    });

    expect(fields[0].label).toBe('Any news item → top category');
    expect(fields[0].path).toBe('news_items[].top_category');
  });

  it.each([
    [undefined],
    [null],
    ['not an object'],
    [{}],
    [{ type: 'object' }],
  ])('returns [] for unusable input %s', (input) => {
    expect(schemaToRoutableFields(input)).toEqual([]);
  });
});

describe('singularize', () => {
  it.each([
    ['articles', 'article'],
    ['categories', 'category'],
    ['status', 'status'],
    ['analysis', 'analysis'],
    ['item', 'item'],
  ])('%s → %s', (input, expected) => {
    expect(singularize(input)).toBe(expected);
  });
});
