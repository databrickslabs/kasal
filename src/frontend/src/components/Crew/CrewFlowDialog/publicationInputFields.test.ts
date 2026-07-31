import { describe, expect, it } from 'vitest';

import {
  buildInputSchema,
  deriveCrewInputFields,
  deriveFlowInputFields,
  fieldsFromSchema,
} from './publicationInputFields';

const crewNodes = [
  { type: 'agentNode', data: { role: 'Analyst for {region}', goal: 'Report' } },
  { type: 'taskNode', data: { description: 'Review {quarter} in {region}' } },
];

describe('deriving what a capability declares', () => {
  it('finds every placeholder across a crew, once', () => {
    const fields = deriveCrewInputFields(crewNodes);
    expect(fields.map((f) => f.name).sort()).toEqual(['quarter', 'region']);
  });

  it('starts everything required, because the syntax cannot say otherwise', () => {
    // The publisher unticks what is optional. Nothing downstream can infer it —
    // that is the whole reason the editor exists.
    expect(deriveCrewInputFields(crewNodes).every((f) => f.required)).toBe(true);
  });

  it('walks a flow deeply, since its inputs are not in agent backstories', () => {
    const fields = deriveFlowInputFields([
      { type: 'crewNode', data: { config: { nested: ['run for {region}'] } } },
    ]);
    expect(fields.map((f) => f.name)).toEqual(['region']);
  });
});

describe('fields to JSON Schema', () => {
  it('emits only what the publisher ticked as required', () => {
    const schema = buildInputSchema([
      { name: 'region', required: true, description: 'The market' },
      { name: 'format', required: false, description: '' },
    ]);
    expect(schema?.required).toEqual(['region']);
    expect(Object.keys(schema!.properties)).toEqual(['region', 'format']);
    expect(schema!.properties.region.description).toBe('The market');
  });

  it('emits an EMPTY required array rather than omitting it', () => {
    // Absent means "nobody has said" and makes the consumer fall back to
    // treating every placeholder as required. A publisher who unticked
    // everything HAS said something, and it must survive.
    const schema = buildInputSchema([
      { name: 'format', required: false, description: '' },
    ]);
    expect(schema?.required).toEqual([]);
  });

  it('is null when there is nothing to declare', () => {
    expect(buildInputSchema([])).toBeNull();
  });
});

describe('reading an existing publication back', () => {
  it('round-trips required and optional', () => {
    const fields = fieldsFromSchema({
      type: 'object',
      properties: { region: { type: 'string' }, format: { type: 'string' } },
      required: ['region'],
    });
    expect(fields).toEqual([
      { name: 'region', required: true, description: '' },
      { name: 'format', required: false, description: '' },
    ]);
  });

  it('treats an absent required array as everything-required', () => {
    const fields = fieldsFromSchema({
      type: 'object',
      properties: { region: { type: 'string' } },
    });
    expect(fields?.[0].required).toBe(true);
  });

  it('keeps an empty required array meaning nothing is required', () => {
    const fields = fieldsFromSchema({
      type: 'object',
      properties: { region: { type: 'string' } },
      required: [],
    });
    expect(fields?.[0].required).toBe(false);
  });

  it('returns null with no schema, so the caller derives instead', () => {
    // Every publication predating the editor is in this state, so this is the
    // live path for the whole back catalogue — not a defensive nicety.
    expect(fieldsFromSchema(null)).toBeNull();
    expect(fieldsFromSchema(undefined)).toBeNull();
  });
});
