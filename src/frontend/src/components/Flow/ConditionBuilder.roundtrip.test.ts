import { describe, it, expect } from 'vitest';
import { Condition, conditionsToPython, pythonToConditions } from './ConditionBuilder';

/**
 * Everything the builder can emit must parse back. A form that does not
 * round-trip returns zero rows on reopen, which disables Save and leaves the
 * whole edge uneditable.
 */
describe('condition round-trip', () => {
  const cases: Array<[string, Condition[]]> = [
    ['equals', [{ field: 'category', operator: '=', value: 'politics' }]],
    ['not equals', [{ field: 'category', operator: '!=', value: 'politics' }]],
    ['greater than', [{ field: 'score', operator: '>', value: '8' }]],
    ['contains', [{ field: 'category', operator: 'contains', value: 'politics' }]],
    ['starts with', [{ field: 'category', operator: 'starts_with', value: 'pol' }]],
    ['ends with', [{ field: 'category', operator: 'ends_with', value: 'ics' }]],
    ['nested path', [{ field: 'classification.category', operator: '=', value: 'politics' }]],
    ['list projection', [{ field: 'articles[].category', operator: '=', value: 'politics' }]],
    ['boolean', [{ field: 'has_results', operator: '=', value: 'true' }]],
  ];

  it.each(cases)('%s survives a round-trip', (_name, conditions) => {
    const python = conditionsToPython(conditions);
    const parsed = pythonToConditions(python);

    expect(parsed).toHaveLength(conditions.length);
    expect(parsed[0].field).toBe(conditions[0].field);
    expect(parsed[0].operator).toBe(conditions[0].operator);
    expect(parsed[0].value).toBe(conditions[0].value);
  });

  it('keeps each connector attached to the condition it precedes', () => {
    const conditions: Condition[] = [
      { field: 'a', operator: '=', value: '1' },
      { field: 'b', operator: '=', value: '2', connector: 'AND' },
      { field: 'c', operator: '=', value: '3', connector: 'OR' },
    ];

    const parsed = pythonToConditions(conditionsToPython(conditions));

    expect(parsed.map((c) => c.connector)).toEqual([undefined, 'AND', 'OR']);
    expect(parsed.map((c) => c.field)).toEqual(['a', 'b', 'c']);
  });

  it('emits a path verbatim so the backend can resolve it', () => {
    expect(
      conditionsToPython([{ field: 'articles[].category', operator: '=', value: 'politics' }])
    ).toBe('state.get("articles[].category", "") == "politics"');
  });

  it('is unchanged for a flat field, so saved flows keep their condition', () => {
    expect(
      conditionsToPython([{ field: 'category', operator: '=', value: 'politics' }])
    ).toBe('state.get("category", "") == "politics"');
  });

  it('returns nothing for an expression it cannot represent', () => {
    expect(pythonToConditions('len(state.keys()) > 0')).toEqual([]);
  });
});
