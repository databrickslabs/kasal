/**
 * The declared input fields of a crew or flow, and the JSON Schema they become.
 *
 * Pure, and separate from the editor component that renders them: this is the
 * translation between "what the placeholders say" and "what the publisher
 * decided", and both ends of it are worth testing without a DOM.
 */
import { PublicationInputSchema } from '../../../types/workflow/publication';
import { detectVariablesFromNodes } from '../../../utils/variableDetector';

/** One declared input, as the publisher is editing it. */
export interface PublicationInputField {
  name: string;
  required: boolean;
  description: string;
}

/**
 * The fields a crew declares, seeded from its `{placeholder}` variables.
 *
 * Returns them all as `required` — that is what the placeholder syntax can tell
 * us, and the point of the editor is that a human corrects it.
 */
export function deriveCrewInputFields(nodes: unknown[]): PublicationInputField[] {
  return detectVariablesFromNodes(nodes).map((v) => ({
    name: v.name,
    required: true,
    description: '',
  }));
}

/**
 * The fields a flow declares.
 *
 * Deliberately weaker than the crew case: a flow's inputs are read by router
 * conditions and state operations rather than written as placeholders in an
 * agent's backstory, so a scan of node text finds only some of them. This walks
 * every node's data for `{placeholders}` and expects the publisher to add the
 * rest by hand.
 */
export function deriveFlowInputFields(nodes: unknown[]): PublicationInputField[] {
  const found = new Set<string>();
  const pattern = /\{([a-zA-Z_][a-zA-Z0-9_-]*)\}/g;

  const walk = (value: unknown, depth: number): void => {
    if (depth > 6) return;
    if (typeof value === 'string') {
      let match: RegExpExecArray | null;
      pattern.lastIndex = 0;
      while ((match = pattern.exec(value)) !== null) found.add(match[1]);
      return;
    }
    if (Array.isArray(value)) {
      value.forEach((item) => walk(item, depth + 1));
      return;
    }
    if (value && typeof value === 'object') {
      Object.values(value as Record<string, unknown>).forEach((item) =>
        walk(item, depth + 1),
      );
    }
  };

  walk(nodes, 0);
  return Array.from(found).map((name) => ({ name, required: true, description: '' }));
}

/** Read an existing publication's schema back into editable fields. */
export function fieldsFromSchema(
  schema: PublicationInputSchema | null | undefined,
): PublicationInputField[] | null {
  if (!schema?.properties) return null;
  const required = new Set(schema.required ?? []);
  return Object.entries(schema.properties).map(([name, spec]) => ({
    name,
    // An ABSENT `required` array means nobody has said — treat everything as
    // required, matching what the consumer falls back to. An EMPTY one means
    // the publisher said nothing is required, and must survive a round trip.
    required: schema.required === undefined ? true : required.has(name),
    description: spec?.description ?? '',
  }));
}

/**
 * Fields → JSON Schema, or null when there are no fields.
 *
 * `required` is always emitted, even empty: the consumer distinguishes an absent
 * array ("nobody has said") from an empty one ("nothing is required"), and a
 * publisher who unticked everything has said something.
 */
export function buildInputSchema(
  fields: PublicationInputField[],
): PublicationInputSchema | null {
  const named = fields.filter((f) => f.name.trim().length > 0);
  if (named.length === 0) return null;

  const properties: PublicationInputSchema['properties'] = {};
  for (const field of named) {
    properties[field.name.trim()] = {
      type: 'string',
      ...(field.description.trim() ? { description: field.description.trim() } : {}),
    };
  }
  return {
    type: 'object',
    properties,
    required: named.filter((f) => f.required).map((f) => f.name.trim()),
  };
}

