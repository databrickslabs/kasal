/**
 * Turn an output schema into the values a router condition can be built on.
 *
 * The router's field dropdown used to list only the schema's TOP-LEVEL scalar
 * properties. Anything typed `object` or `array` was filtered out, so a schema
 * describing what a model actually returns — a classification nested in an
 * object, or a list of classified items — produced an EMPTY dropdown and a
 * permanently disabled Save. The condition could not be authored at all.
 *
 * This projects the editable field model onto one entry per addressable value:
 *
 *   { path: "category",                label: "category" }
 *   { path: "classification.category", label: "classification → category" }
 *   { path: "articles[].category",     label: "Any article → category", isList: true }
 *
 * `path` is what gets interpolated into `state.get("<path>", "")` — the backend
 * resolves dots and `[]`. `label` is the only thing the user sees; nobody should
 * have to read `articles[].category` to configure a route.
 *
 * Deliberately built on `schemaToFields` rather than walking the raw JSON again.
 * A second walker meant a second idea of what a schema node is, and the two
 * could disagree about a shape — the dropdown offering a field the editor
 * cannot show, or the reverse. One parser, two projections.
 */

import {
  FieldKind,
  MAX_NESTING,
  SchemaFieldModel,
  schemaToFields,
} from './schemaModel';

export interface RoutableField {
  /** Interpolated into the emitted condition. Never shown to the user. */
  path: string;
  /** Shown in the dropdown. Never parsed. */
  label: string;
  /** JSON-schema scalar type, used to tailor the value input. */
  type: string;
  /** Reached through a list, so comparisons mean "any item matches". */
  isList: boolean;
}

/** The scalar kinds, and the JSON-schema type each stands for. */
const SCALAR_TYPE: Partial<Record<FieldKind, string>> = {
  text: 'string',
  number: 'number',
  yesno: 'boolean',
};

const SCALAR_ITEM_TYPES = ['string', 'number', 'integer', 'boolean'];

/**
 * "articles" → "article", so a list field reads "Any article → category".
 * Best-effort and cosmetic — the path is unaffected if this guesses wrong.
 */
export function singularize(name: string): string {
  if (/ies$/i.test(name)) return name.replace(/ies$/i, 'y');
  if (/(ss|us|is)$/i.test(name)) return name;
  if (/s$/i.test(name)) return name.slice(0, -1);
  return name;
}

function humanize(name: string): string {
  return name.replace(/[_-]+/g, ' ').trim();
}

function collect(
  fields: SchemaFieldModel[],
  pathPrefix: string,
  labelPrefix: string,
  isList: boolean,
  depth: number,
  out: RoutableField[]
): void {
  if (depth > MAX_NESTING) return;

  fields.forEach((field) => {
    const name = field.name.trim();
    if (!name) return;

    const path = `${pathPrefix}${name}`;
    const label = `${labelPrefix}${humanize(name)}`;

    const scalar = SCALAR_TYPE[field.kind];
    if (scalar) {
      // Keep `integer` when that is what the schema declared: the editor shows
      // both as "Number", but the value input reads this.
      const declared = (field.source?.type as string) || scalar;
      out.push({ path, label, type: declared, isList });
      return;
    }

    if (field.kind === 'group') {
      collect(field.children ?? [], `${path}.`, `${label} → `, isList, depth + 1, out);
      return;
    }

    if (field.kind === 'list') {
      const children = field.children ?? [];
      const anyLabel = `${labelPrefix}Any ${humanize(singularize(name))}`;

      // A list of things that have their own fields — "Any article → category".
      if (children.length > 0) {
        collect(children, `${path}[].`, `${anyLabel} → `, true, depth + 1, out);
        return;
      }

      // A list of plain values — "Any tag".
      const itemType = (field.source?.items as { type?: string } | undefined)?.type;
      if (itemType && SCALAR_ITEM_TYPES.includes(itemType)) {
        out.push({ path: `${path}[]`, label: anyLabel, type: itemType, isList: true });
      }
    }

    // `advanced` is a node the editor cannot model, so it cannot be compared
    // either. Offering it would put a value in the dropdown that no operator
    // can meaningfully test.
  });
}

/**
 * Every value in `schemaDefinition` a router condition can compare against.
 *
 * Returns an empty array for a schema with nothing comparable in it — the
 * caller still needs to tell the user that, but it now means "genuinely no
 * scalar anywhere" rather than "the scalar is one level down".
 */
export function schemaToRoutableFields(schemaDefinition: unknown): RoutableField[] {
  const out: RoutableField[] = [];
  collect(schemaToFields(schemaDefinition), '', '', false, 0, out);
  return out;
}

/** Look up a field by the path stored in a saved condition. */
export function findFieldByPath(
  fields: RoutableField[],
  path: string
): RoutableField | undefined {
  return fields.find((f) => f.path === path);
}
