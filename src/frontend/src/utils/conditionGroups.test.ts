import { describe, it, expect } from 'vitest';
import {
  ConditionGroup,
  RESULT_SUBJECT,
  defaultSubject,
  emptyGroup,
  fieldsForSubject,
  groupsToPython,
  pythonToGroups,
  splitListPath,
  subjectsFor,
} from './conditionGroups';
import { RoutableField } from './schemaFields';

const FIELDS: RoutableField[] = [
  { path: 'status', label: 'status', type: 'string', isList: false },
  { path: 'rows_failed', label: 'rows failed', type: 'number', isList: false },
  {
    path: 'articles[].category',
    label: 'Any article → category',
    type: 'string',
    isList: true,
  },
  { path: 'articles[].score', label: 'Any article → score', type: 'number', isList: true },
  { path: 'tags[]', label: 'Any tag', type: 'string', isList: true },
  {
    path: 'orders[].lines[].sku',
    label: 'Any order → Any line → sku',
    type: 'string',
    isList: true,
  },
];

describe('subjects', () => {
  it('always offers the result, plus one per list of items', () => {
    expect(subjectsFor(FIELDS)).toEqual([
      { subject: RESULT_SUBJECT, label: 'The result' },
      { subject: 'articles', label: 'Any article' },
    ]);
  });

  it('offers no subject for a list of plain values or a deeper list', () => {
    // `tags[]` has no per-item fields and `orders[].lines[].sku` is two lists
    // deep — neither can be searched one item at a time, so both stay on the
    // result as ordinary any-element projections.
    const subjects = subjectsFor(FIELDS).map((s) => s.subject);

    expect(subjects).not.toContain('tags');
    expect(subjects).not.toContain('orders');
    expect(fieldsForSubject(FIELDS, RESULT_SUBJECT).map((f) => f.value)).toContain(
      'tags[]'
    );
    expect(fieldsForSubject(FIELDS, RESULT_SUBJECT).map((f) => f.value)).toContain(
      'orders[].lines[].sku'
    );
  });

  it('drops "The result" when nothing can be said about it', () => {
    // A schema that is only a list of items has no top-level value to compare,
    // so offering the result gave an empty field dropdown and no clue why.
    const listOnly: RoutableField[] = [
      {
        path: 'classification[].category',
        label: 'Any classification → category',
        type: 'string',
        isList: true,
      },
    ];

    expect(subjectsFor(listOnly)).toEqual([
      { subject: 'classification', label: 'Any classification' },
    ]);
    expect(defaultSubject(listOnly)).toBe('classification');
  });

  it('starts a new group on the result whenever the result is offered', () => {
    expect(defaultSubject(FIELDS)).toBe(RESULT_SUBJECT);
    expect(defaultSubject([])).toBe(RESULT_SUBJECT);
  });

  it('shows an item subject its own leaves, without the "Any article" prefix', () => {
    expect(fieldsForSubject(FIELDS, 'articles')).toEqual([
      { value: 'category', label: 'category', type: 'string', isList: false },
      { value: 'score', label: 'score', type: 'number', isList: false },
    ]);
  });
});

describe('the result as subject', () => {
  it('emits what it always emitted for one term', () => {
    const groups: ConditionGroup[] = [
      { subject: RESULT_SUBJECT, terms: [{ field: 'status', operator: '=', value: 'failed' }] },
    ];

    expect(groupsToPython(groups)).toBe('state.get("status", "") == "failed"');
  });

  it('parenthesises several terms so the group survives a round-trip', () => {
    const groups: ConditionGroup[] = [
      {
        subject: RESULT_SUBJECT,
        terms: [
          { field: 'status', operator: '=', value: 'failed' },
          { field: 'rows_failed', operator: '>', value: '0' },
        ],
      },
    ];

    expect(groupsToPython(groups)).toBe(
      '(state.get("status", "") == "failed" and state.get("rows_failed", "") > 0)'
    );
    expect(pythonToGroups(groupsToPython(groups))).toEqual(groups);
  });
});

describe('one item as subject', () => {
  it('emits where(), so every term is asserted of the SAME item', () => {
    const groups: ConditionGroup[] = [
      {
        subject: 'articles',
        terms: [
          { field: 'category', operator: '=', value: 'politics' },
          { field: 'score', operator: '>', value: '5' },
        ],
      },
    ];

    expect(groupsToPython(groups)).toBe(
      'where("articles", category="politics", score__gt=5)'
    );
  });

  it('keeps two groups independent, which is the cross-item reading', () => {
    // "the batch contains both politics and sports" — genuinely two articles,
    // and the only way to say it.
    const groups: ConditionGroup[] = [
      { subject: 'articles', terms: [{ field: 'category', operator: '=', value: 'politics' }] },
      {
        subject: 'articles',
        terms: [{ field: 'category', operator: '=', value: 'sports' }],
        connector: 'AND',
      },
    ];

    expect(groupsToPython(groups)).toBe(
      'where("articles", category="politics") and where("articles", category="sports")'
    );
    expect(pythonToGroups(groupsToPython(groups))).toEqual(groups);
  });

  it('maps every operator to a suffix and back', () => {
    const operators: ConditionGroup['terms'][number]['operator'][] = [
      '=', '!=', '>', '>=', '<', '<=', 'contains', 'starts_with', 'ends_with',
    ];

    for (const operator of operators) {
      const groups: ConditionGroup[] = [
        { subject: 'articles', terms: [{ field: 'score', operator, value: '5' }] },
      ];
      expect(pythonToGroups(groupsToPython(groups))[0].terms[0].operator, operator).toBe(
        operator
      );
    }
  });
});

describe('round-trips', () => {
  const cases: Array<[string, ConditionGroup[]]> = [
    ['a single scalar', [{ subject: '', terms: [{ field: 'status', operator: '=', value: 'failed' }] }]],
    ['contains', [{ subject: '', terms: [{ field: 'status', operator: 'contains', value: 'fail' }] }]],
    ['starts with', [{ subject: '', terms: [{ field: 'status', operator: 'starts_with', value: 'f' }] }]],
    ['a boolean', [{ subject: '', terms: [{ field: 'ok', operator: '=', value: 'true' }] }]],
    ['a projection on the result', [{ subject: '', terms: [{ field: 'articles[].category', operator: '=', value: 'politics' }] }]],
    ['a compound item group', [{ subject: 'articles', terms: [{ field: 'category', operator: '=', value: 'politics' }, { field: 'score', operator: '>', value: '5' }] }]],
  ];

  it.each(cases)('%s', (_name, groups) => {
    expect(pythonToGroups(groupsToPython(groups))).toEqual(groups);
  });

  it('keeps each connector with the group it precedes', () => {
    const groups: ConditionGroup[] = [
      { subject: '', terms: [{ field: 'a', operator: '=', value: '1' }] },
      { subject: '', terms: [{ field: 'b', operator: '=', value: '2' }], connector: 'AND' },
      { subject: 'articles', terms: [{ field: 'category', operator: '=', value: 'x' }], connector: 'OR' },
    ];

    expect(pythonToGroups(groupsToPython(groups)).map((g) => g.connector)).toEqual([
      undefined,
      'AND',
      'OR',
    ]);
  });

  it('is not confused by "and" inside a quoted value', () => {
    const groups: ConditionGroup[] = [
      { subject: '', terms: [{ field: 'title', operator: '=', value: 'salt and pepper' }] },
    ];

    expect(pythonToGroups(groupsToPython(groups))).toEqual(groups);
  });

  it('drops unnamed terms rather than emitting a broken condition', () => {
    expect(
      groupsToPython([{ subject: '', terms: [{ field: '', operator: '=', value: 'x' }] }])
    ).toBe('');
  });

  it('returns nothing for an expression it cannot represent', () => {
    expect(pythonToGroups('len(state.keys()) > 0')).toEqual([]);
  });
});

describe('splitListPath', () => {
  it.each([
    ['articles[].category', { list: 'articles', leaf: 'category' }],
    ['orders[].lines[].sku', null],
    ['articles[].meta.category', null],
    ['category', null],
    ['tags[]', null],
  ])('%s', (path, expected) => {
    expect(splitListPath(path)).toEqual(expected);
  });
});

describe('emptyGroup', () => {
  it('starts on the result with one blank term', () => {
    expect(emptyGroup()).toEqual({
      subject: RESULT_SUBJECT,
      terms: [{ field: '', operator: '=', value: '' }],
    });
  });
});
