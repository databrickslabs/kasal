import { describe, expect, it } from 'vitest';

import { resolveMissingVariables } from './useChatExecutionActions';

const nodes = [
  { type: 'agentNode', data: { role: 'Analyst for {region}' } },
  { type: 'taskNode', data: { description: 'Review {quarter}, format {format}' } },
];

describe('what the user still has to be asked for', () => {
  it('asks for every placeholder when there is no publication schema', () => {
    // The unrouted paths — /run, click-to-run — and every publication that
    // predates the schema editor. All-required is the honest answer there.
    const missing = resolveMissingVariables(nodes);
    expect(missing.map((v) => v.name).sort()).toEqual([
      'format',
      'quarter',
      'region',
    ]);
  });

  it('trusts the publication schema over the placeholders', () => {
    // `format` is a cosmetic placeholder the publisher marked optional. Asking
    // for it is exactly the interrogation the schema exists to prevent.
    const missing = resolveMissingVariables(nodes, {
      inputSchema: {
        type: 'object',
        properties: {
          region: { type: 'string' },
          quarter: { type: 'string' },
          format: { type: 'string' },
        },
        required: ['region', 'quarter'],
      },
    });
    expect(missing.map((v) => v.name)).toEqual(['region', 'quarter']);
  });

  it('does not ask again for what the router already bound', () => {
    const missing = resolveMissingVariables(nodes, {
      extractedInputs: { region: 'DACH' },
      inputSchema: {
        type: 'object',
        properties: { region: { type: 'string' }, quarter: { type: 'string' } },
        required: ['region', 'quarter'],
      },
    });
    expect(missing.map((v) => v.name)).toEqual(['quarter']);
  });

  it('asks nothing when the prompt supplied everything', () => {
    expect(
      resolveMissingVariables(nodes, {
        extractedInputs: { region: 'DACH', quarter: 'Q3' },
        inputSchema: {
          type: 'object',
          properties: { region: { type: 'string' }, quarter: { type: 'string' } },
          required: ['region', 'quarter'],
        },
      }),
    ).toEqual([]);
  });

  it('asks nothing when the publisher declared nothing required', () => {
    // An EMPTY required array is a decision. It must not fall back to the
    // detector, or unticking everything would change nothing.
    expect(
      resolveMissingVariables(nodes, {
        inputSchema: {
          type: 'object',
          properties: { region: { type: 'string' } },
          required: [],
        },
      }),
    ).toEqual([]);
  });

  it('falls back to the detector when required is ABSENT, not empty', () => {
    const missing = resolveMissingVariables(nodes, {
      inputSchema: {
        type: 'object',
        properties: { region: { type: 'string' } },
      },
    });
    expect(missing.map((v) => v.name).sort()).toEqual([
      'format',
      'quarter',
      'region',
    ]);
  });

  it('does not treat an empty extracted value as unbound', () => {
    // The router only ever emits values the user actually said, but an empty
    // string is a said thing — asking again would be asking twice.
    const missing = resolveMissingVariables(nodes, {
      extractedInputs: { region: '' },
      inputSchema: {
        type: 'object',
        properties: { region: { type: 'string' } },
        required: ['region'],
      },
    });
    expect(missing).toEqual([]);
  });
});
