/**
 * The shape of a router condition, and how it converts to and from Python.
 *
 * A condition is a list of GROUPS. Each group names a SUBJECT — the whole
 * result, or one item of a list — and everything inside that group is asserted
 * about that ONE subject.
 *
 * That structure exists to remove an ambiguity users cannot be expected to see.
 * With flat rows, each row independently said "Any article -> category", so
 * whether two rows meant one article or two was invisible:
 *
 *   Any article -> category is Sports  AND  Any article -> score > 5
 *
 * is satisfied by a Sports article and a DIFFERENT high-scoring one, because
 * each path is projected across the list on its own. Both readings are
 * legitimate — "a Sports article scoring over 5" and "the batch contains both
 * Sports and Politics" — so neither can be assumed. Choosing the subject before
 * describing it makes the difference structural: one group is one item, two
 * groups are independent.
 */

import { RoutableField } from './schemaFields';

export type ConditionOperator =
  | '>'
  | '<'
  | '='
  | '!='
  | '>='
  | '<='
  | 'contains'
  | 'starts_with'
  | 'ends_with';

export interface ConditionTerm {
  /** A path when the subject is the result; a leaf name when it is a list item. */
  field: string;
  operator: ConditionOperator;
  value: string;
}

export interface ConditionGroup {
  /** '' for the whole result, otherwise the list path, e.g. `articles`. */
  subject: string;
  terms: ConditionTerm[];
  /** Joins this group to the previous one. Absent on the first. */
  connector?: 'AND' | 'OR';
}

/** The subject meaning "the crew's result as a whole". */
export const RESULT_SUBJECT = '';

/** Operator -> the suffix `where(...)` uses. `=` needs none. */
const TERM_SUFFIX: Record<ConditionOperator, string> = {
  '=': '',
  '!=': '__ne',
  '>': '__gt',
  '>=': '__gte',
  '<': '__lt',
  '<=': '__lte',
  contains: '__contains',
  starts_with: '__startswith',
  ends_with: '__endswith',
};

const SUFFIX_OPERATOR: Record<string, ConditionOperator> = Object.fromEntries(
  Object.entries(TERM_SUFFIX).map(([op, suffix]) => [suffix, op as ConditionOperator])
);

/**
 * Split `articles[].category` into the list to search and the field on one item.
 *
 * Only a single `[]` followed by a plain leaf qualifies. `orders[].lines[].sku`
 * would hand `where` a list of lists, and `articles[].meta.category` needs a
 * nested lookup per item — so neither becomes a subject, and both stay
 * available on the result as ordinary any-element projections.
 */
export function splitListPath(path: string): { list: string; leaf: string } | null {
  const match = /^([^[\]]+)\[\]\.([^.[\]]+)$/.exec(path);
  return match ? { list: match[1], leaf: match[2] } : null;
}

/**
 * The subjects a condition can be built about, given the schema's fields.
 *
 * `The result` is always present. A list contributes a subject only if its
 * items have fields — which is exactly when asking about ONE of them is
 * meaningful.
 */
export function subjectsFor(
  fields: RoutableField[]
): Array<{ subject: string; label: string }> {
  const lists: string[] = [];
  fields.forEach((field) => {
    const split = splitListPath(field.path);
    if (split && !lists.includes(split.list)) lists.push(split.list);
  });

  const subjects = lists.map((list) => ({
    subject: list,
    label: `Any ${itemNoun(list, fields)}`,
  }));

  // Offer "The result" only when something can actually be said about it. A
  // schema that is nothing but a list of items — `{classification: [...]}` —
  // has no top-level value to compare, so offering it led to an empty field
  // dropdown and no way to tell why.
  if (fieldsForSubject(fields, RESULT_SUBJECT).length > 0) {
    subjects.unshift({ subject: RESULT_SUBJECT, label: 'The result' });
  }
  return subjects;
}

/** The subject a new group should start on: the first one actually offered. */
export function defaultSubject(fields: RoutableField[]): string {
  return subjectsFor(fields)[0]?.subject ?? RESULT_SUBJECT;
}

/** The fields a term may use, given its group's subject. */
export function fieldsForSubject(
  fields: RoutableField[],
  subject: string
): Array<{ value: string; label: string; type: string; isList: boolean }> {
  if (subject === RESULT_SUBJECT) {
    return fields
      .filter((field) => !splitListPath(field.path))
      .map((field) => ({
        value: field.path,
        label: field.label,
        type: field.type,
        isList: field.isList,
      }));
  }
  return fields
    .filter((field) => splitListPath(field.path)?.list === subject)
    .map((field) => ({
      value: splitListPath(field.path)!.leaf,
      // Inside an item the "Any article ->" prefix is noise: the group already
      // says what the subject is.
      label: field.label.replace(/^Any\s+[^→]+→\s*/, ''),
      type: field.type,
      isList: false,
    }));
}

/** "article" from "Any article -> category", for labelling a subject. */
function itemNoun(list: string, fields: RoutableField[]): string {
  const sample = fields.find((f) => splitListPath(f.path)?.list === list);
  const match = /Any ([^→]+?)\s*→/.exec(sample?.label ?? '');
  return match ? match[1].trim() : list;
}

/** A Python literal for a value the user typed. */
function literal(raw: string): string {
  const lowered = raw.trim().toLowerCase();
  if (lowered === 'true') return 'True';
  if (lowered === 'false') return 'False';
  return isNaN(Number(raw)) ? `"${raw}"` : raw;
}

function unquote(raw: string): string {
  const trimmed = raw.trim();
  if (trimmed === 'True' || trimmed === 'False') return trimmed.toLowerCase();
  return trimmed.replace(/^["']|["']$/g, '');
}

function termToPython(term: ConditionTerm): string {
  const field = `state.get("${term.field}", "")`;
  const value = literal(term.value);
  switch (term.operator) {
    case 'contains':
      return `${value} in ${field}`;
    case 'starts_with':
      return `${field}.startswith(${value})`;
    case 'ends_with':
      return `${field}.endswith(${value})`;
    case '=':
      return `${field} == ${value}`;
    default:
      return `${field} ${term.operator} ${value}`;
  }
}

function groupToPython(group: ConditionGroup): string {
  const named = group.terms.filter((term) => term.field);
  if (named.length === 0) return '';

  if (group.subject !== RESULT_SUBJECT) {
    const terms = named
      .map((term) => `${term.field}${TERM_SUFFIX[term.operator]}=${literal(term.value)}`)
      .join(', ');
    return `where("${group.subject}", ${terms})`;
  }

  const parts = named.map(termToPython);
  // Parenthesised when it has several terms, so the top-level split can find
  // the group boundaries again without re-implementing precedence.
  return parts.length > 1 ? `(${parts.join(' and ')})` : parts[0];
}

/** The Python a router evaluates. */
export function groupsToPython(groups: ConditionGroup[]): string {
  return groups
    .map((group) => ({ group, python: groupToPython(group) }))
    .filter(({ python }) => python)
    .map(({ group, python }, index) =>
      index > 0 ? `${(group.connector ?? 'AND').toLowerCase()} ${python}` : python
    )
    .join(' ');
}

/** Split on top-level `and`/`or`, ignoring anything inside quotes or brackets. */
function splitTopLevel(expression: string): Array<{ connector?: 'AND' | 'OR'; part: string }> {
  const out: Array<{ connector?: 'AND' | 'OR'; part: string }> = [];
  let depth = 0;
  let quote: string | null = null;
  let start = 0;
  let pending: 'AND' | 'OR' | undefined;

  for (let i = 0; i < expression.length; i += 1) {
    const char = expression[i];
    if (quote) {
      if (char === quote && expression[i - 1] !== '\\') quote = null;
      continue;
    }
    if (char === '"' || char === "'") {
      quote = char;
      continue;
    }
    if (char === '(' || char === '[') depth += 1;
    else if (char === ')' || char === ']') depth -= 1;
    else if (depth === 0) {
      const rest = expression.slice(i);
      const match = /^\s+(and|or)\s+/i.exec(rest);
      if (match) {
        out.push({ connector: pending, part: expression.slice(start, i) });
        pending = match[1].toUpperCase() as 'AND' | 'OR';
        i += match[0].length - 1;
        start = i + 1;
      }
    }
  }
  out.push({ connector: pending, part: expression.slice(start) });
  return out.filter(({ part }) => part.trim());
}

function parseTerm(text: string): ConditionTerm | null {
  const part = text.trim();

  const comparison = /^state\.get\("([^"]+)",\s*[^)]*\)\s*(==|!=|>=|<=|>|<)\s*(.+)$/.exec(part);
  if (comparison) {
    const [, field, operator, value] = comparison;
    return {
      field,
      operator: (operator === '==' ? '=' : operator) as ConditionOperator,
      value: unquote(value),
    };
  }

  const contains = /^(.+?)\s+in\s+state\.get\("([^"]+)",\s*[^)]*\)$/.exec(part);
  if (contains) {
    return { field: contains[2], operator: 'contains', value: unquote(contains[1]) };
  }

  const method = /^state\.get\("([^"]+)",\s*[^)]*\)\.(startswith|endswith)\((.+)\)$/.exec(part);
  if (method) {
    return {
      field: method[1],
      operator: method[2] === 'startswith' ? 'starts_with' : 'ends_with',
      value: unquote(method[3]),
    };
  }
  return null;
}

/** Read a stored condition back into groups. */
export function pythonToGroups(expression: string): ConditionGroup[] {
  if (!expression.trim()) return [];

  try {
    const groups: ConditionGroup[] = [];

    for (const { connector, part } of splitTopLevel(expression)) {
      const text = part.trim().replace(/^\((.*)\)$/s, '$1');

      const grouped = /^where\(\s*"([^"]+)"\s*,\s*(.+)\)$/.exec(text);
      if (grouped) {
        const terms = grouped[2]
          .split(/\s*,\s*/)
          .map((term) => /^([A-Za-z_][A-Za-z0-9_]*?)(__[a-z]+)?=(.+)$/.exec(term))
          .filter((match): match is RegExpExecArray => Boolean(match))
          .map((match) => ({
            field: match[1],
            operator: SUFFIX_OPERATOR[match[2] ?? ''] ?? '=',
            value: unquote(match[3]),
          }));
        if (terms.length) groups.push({ subject: grouped[1], terms, connector });
        continue;
      }

      const terms = splitTopLevel(text)
        .map(({ part: term }) => parseTerm(term))
        .filter((term): term is ConditionTerm => Boolean(term));
      if (terms.length) groups.push({ subject: RESULT_SUBJECT, terms, connector });
    }

    return groups;
  } catch {
    return [];
  }
}

/** A blank group, for the "add" buttons. */
export function emptyGroup(subject: string = RESULT_SUBJECT): ConditionGroup {
  return { subject, terms: [{ field: '', operator: '=', value: '' }] };
}
