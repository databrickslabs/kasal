/**
 * The field model behind both schema editors.
 *
 * Two problems this replaces:
 *
 * 1. Both editors could only express a FLAT list of name + type. Picking
 *    `array` or `object` produced `{"type": "array"}` with no `items` — you
 *    could say "this is a list" but not "a list of what", so the shape a model
 *    actually returns was inexpressible.
 * 2. Parsing was LOSSY. The old `parseSchemaToFields` read `prop.type` and
 *    discarded everything else, so opening a nested schema in the edit dialog
 *    and pressing Save silently flattened it.
 *
 * Every field therefore keeps its original JSON-schema node in `source`.
 * Serialization spreads that node and overrides only the keywords the editor
 * actually owns, so `description`, `enum`, `format` and anything else survive
 * untouched. A node the editor cannot model at all becomes kind `advanced` and
 * is written back verbatim.
 *
 * Vocabulary is deliberately not JSON Schema's: users see Text, Number, Yes/No,
 * List of items, Group — never "array", "object" or "properties".
 */

export type FieldKind = 'text' | 'number' | 'yesno' | 'list' | 'group' | 'advanced';

export interface SchemaFieldModel {
  name: string;
  kind: FieldKind;
  required: boolean;
  /** For `list` (what one item holds) and `group` (what the group holds). */
  children?: SchemaFieldModel[];
  /** The original node, so unmodelled keywords round-trip. */
  source?: Record<string, unknown>;
}

export const KIND_OPTIONS: Array<{ kind: FieldKind; label: string }> = [
  { kind: 'text', label: 'Text' },
  { kind: 'number', label: 'Number' },
  { kind: 'yesno', label: 'Yes/No' },
  { kind: 'list', label: 'List of items' },
  { kind: 'group', label: 'Group' },
];

/**
 * How deep a field may nest below the top level.
 *
 * Not a resolver limit — the backend walks 32 path segments and compares "any,
 * at any depth", so `orders[].lines[].sku` resolves fine. This is a usability
 * limit: each level multiplies the router's field list, and a model's chance of
 * producing the shape correctly falls off fast. Three covers the real cases
 * (a report of sections of items, an order of line items) without either.
 */
export const MAX_NESTING = 3;

type Node = Record<string, unknown>;

const asNode = (value: unknown): Node | undefined =>
  value && typeof value === 'object' && !Array.isArray(value)
    ? (value as Node)
    : undefined;

const namesOf = (node: Node | undefined): string[] =>
  Array.isArray(node?.required) ? (node!.required as string[]) : [];

function nodeToField(name: string, node: unknown, required: boolean): SchemaFieldModel {
  const source = asNode(node);
  const type = source?.type;
  const base = { name, required, source };

  if (type === 'string') return { ...base, kind: 'text' };
  if (type === 'number' || type === 'integer') return { ...base, kind: 'number' };
  if (type === 'boolean') return { ...base, kind: 'yesno' };

  if (type === 'object') {
    return { ...base, kind: 'group', children: readProperties(source) };
  }

  if (type === 'array') {
    const items = asNode(source?.items);
    // A list of things that have their own fields.
    if (items?.properties) {
      return { ...base, kind: 'list', children: readProperties(items) };
    }
    // A list of plain values — no per-item fields to show.
    if (typeof items?.type === 'string') {
      return { ...base, kind: 'list', children: [] };
    }
    return { ...base, kind: 'list', children: [] };
  }

  // No recognisable type: an $ref, a oneOf, a bare {}. Keep it verbatim rather
  // than guessing, which is what silently rewrote schemas before.
  return { ...base, kind: 'advanced' };
}

function readProperties(node: Node | undefined): SchemaFieldModel[] {
  const properties = asNode(node?.properties);
  if (!properties) return [];
  const required = namesOf(node);
  return Object.entries(properties).map(([name, child]) =>
    nodeToField(name, child, required.includes(name))
  );
}

/** Read a stored schema into editable fields, losing nothing. */
export function schemaToFields(definition: unknown): SchemaFieldModel[] {
  const parsed =
    typeof definition === 'string'
      ? (() => {
          try {
            return JSON.parse(definition);
          } catch {
            return undefined;
          }
        })()
      : definition;
  return readProperties(asNode(parsed));
}

function fieldToNode(field: SchemaFieldModel): unknown {
  // Start from whatever was there, then override only what the editor owns.
  const base: Node = { ...(field.source ?? {}) };
  if (field.kind !== 'advanced') {
    delete base.type;
    delete base.properties;
    delete base.items;
    delete base.required;
  }

  switch (field.kind) {
    case 'text':
      return { ...base, type: 'string' };
    case 'number':
      // Keep `integer` if that is what it already was — the editor shows both
      // as "Number" and should not silently widen the declared type.
      return { ...base, type: field.source?.type === 'integer' ? 'integer' : 'number' };
    case 'yesno':
      return { ...base, type: 'boolean' };
    case 'group':
      return { ...base, type: 'object', ...writeProperties(field.children ?? []) };
    case 'list': {
      const children = field.children ?? [];
      if (children.length > 0) {
        return {
          ...base,
          type: 'array',
          items: { type: 'object', ...writeProperties(children) },
        };
      }
      // No per-item fields: preserve the declared item type if there was one,
      // otherwise a list of text is the sane default.
      const existingItems = asNode(field.source?.items);
      return {
        ...base,
        type: 'array',
        items: existingItems ?? { type: 'string' },
      };
    }
    case 'advanced':
    default:
      return base;
  }
}

function writeProperties(fields: SchemaFieldModel[]): Node {
  const named = fields.filter((f) => f.name.trim());
  const properties: Node = {};
  named.forEach((field) => {
    properties[field.name.trim()] = fieldToNode(field);
  });
  const required = named.filter((f) => f.required).map((f) => f.name.trim());
  return required.length > 0 ? { properties, required } : { properties };
}

/** Write editable fields back to a JSON-schema definition. */
export function fieldsToSchema(fields: SchemaFieldModel[]): Record<string, unknown> {
  return { type: 'object', ...writeProperties(fields) };
}

/** Whether a kind holds sub-fields the editor should offer to expand. */
export function holdsChildren(kind: FieldKind): boolean {
  return kind === 'list' || kind === 'group';
}

/** A blank field, for the "add" buttons.
 *
 * Required by default: a router cannot compare a value the model felt free to
 * omit, and someone who names a field means the step to produce it. The
 * flow-builder dialog used to force this invisibly on every field; the checkbox
 * now shows it and can be turned off.
 */
export function emptyField(): SchemaFieldModel {
  return { name: '', kind: 'text', required: true };
}

