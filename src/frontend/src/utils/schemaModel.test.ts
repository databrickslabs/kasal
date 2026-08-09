import { describe, it, expect } from 'vitest';
import { fieldsToSchema, schemaToFields } from './schemaModel';

const roundTrip = (definition: unknown) => fieldsToSchema(schemaToFields(definition));

describe('schemaToFields', () => {
  it('reads a flat schema', () => {
    expect(
      schemaToFields({
        type: 'object',
        properties: { category: { type: 'string' } },
        required: ['category'],
      })
    ).toEqual([
      {
        name: 'category',
        kind: 'text',
        required: true,
        source: { type: 'string' },
      },
    ]);
  });

  it('reads a list of items with their own fields', () => {
    const fields = schemaToFields({
      type: 'object',
      properties: {
        articles: {
          type: 'array',
          items: {
            type: 'object',
            properties: { title: { type: 'string' }, score: { type: 'number' } },
            required: ['title'],
          },
        },
      },
    });

    expect(fields[0].kind).toBe('list');
    expect(fields[0].children?.map((c) => [c.name, c.kind, c.required])).toEqual([
      ['title', 'text', true],
      ['score', 'number', false],
    ]);
  });

  it('reads a group', () => {
    const fields = schemaToFields({
      type: 'object',
      properties: {
        classification: {
          type: 'object',
          properties: { category: { type: 'string' } },
        },
      },
    });

    expect(fields[0].kind).toBe('group');
    expect(fields[0].children?.[0].name).toBe('category');
  });

  it('accepts a JSON string, as stored', () => {
    expect(
      schemaToFields('{"type":"object","properties":{"a":{"type":"string"}}}')
    ).toHaveLength(1);
  });

  it.each([[undefined], [null], ['not json'], [{}], [42]])(
    'returns [] for unusable input %s',
    (input) => {
      expect(schemaToFields(input)).toEqual([]);
    }
  );
});

describe('fieldsToSchema', () => {
  it('writes a list of items', () => {
    const fields = schemaToFields({
      type: 'object',
      properties: {
        articles: {
          type: 'array',
          items: { type: 'object', properties: { category: { type: 'string' } } },
        },
      },
    });

    expect(fieldsToSchema(fields)).toEqual({
      type: 'object',
      properties: {
        articles: {
          type: 'array',
          items: { type: 'object', properties: { category: { type: 'string' } } },
        },
      },
    });
  });

  it('drops unnamed rows', () => {
    expect(
      fieldsToSchema([
        { name: 'a', kind: 'text', required: false },
        { name: '   ', kind: 'text', required: false },
      ])
    ).toEqual({ type: 'object', properties: { a: { type: 'string' } } });
  });

  it('omits required when nothing is required', () => {
    expect(fieldsToSchema([{ name: 'a', kind: 'text', required: false }])).toEqual({
      type: 'object',
      properties: { a: { type: 'string' } },
    });
  });

  it('defaults a list with no per-item fields to a list of text', () => {
    expect(fieldsToSchema([{ name: 'tags', kind: 'list', required: false }])).toEqual({
      type: 'object',
      properties: { tags: { type: 'array', items: { type: 'string' } } },
    });
  });
});

describe('editing never destroys what it cannot show', () => {
  it('round-trips a nested schema unchanged', () => {
    const original = {
      type: 'object',
      properties: {
        category: { type: 'string' },
        classification: {
          type: 'object',
          properties: { category: { type: 'string' }, confidence: { type: 'number' } },
          required: ['category'],
        },
        articles: {
          type: 'array',
          items: {
            type: 'object',
            properties: { title: { type: 'string' }, score: { type: 'integer' } },
          },
        },
      },
      required: ['category'],
    };

    expect(roundTrip(original)).toEqual(original);
  });

  it('preserves keywords the editor does not model', () => {
    const original = {
      type: 'object',
      properties: {
        category: {
          type: 'string',
          description: 'the topic',
          enum: ['politics', 'sports'],
        },
      },
    };

    expect(roundTrip(original)).toEqual(original);
  });

  it('preserves a node it cannot model at all', () => {
    const original = {
      type: 'object',
      properties: { weird: { $ref: '#/definitions/Thing' } },
    };

    expect(schemaToFields(original)[0].kind).toBe('advanced');
    expect(roundTrip(original)).toEqual(original);
  });

  it('round-trips a list inside a list', () => {
    const original = {
      type: 'object',
      properties: {
        orders: {
          type: 'array',
          items: {
            type: 'object',
            properties: {
              lines: {
                type: 'array',
                items: {
                  type: 'object',
                  properties: { sku: { type: 'string' } },
                  required: ['sku'],
                },
              },
            },
          },
        },
      },
    };

    expect(roundTrip(original)).toEqual(original);
  });

  it('keeps integer as integer rather than widening it', () => {
    const original = { type: 'object', properties: { n: { type: 'integer' } } };
    expect(roundTrip(original)).toEqual(original);
  });

  it('preserves a list of plain values', () => {
    const original = {
      type: 'object',
      properties: { tags: { type: 'array', items: { type: 'number' } } },
    };
    expect(roundTrip(original)).toEqual(original);
  });
});
