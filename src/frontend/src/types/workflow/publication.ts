/**
 * Publishing a crew or flow so something other than the canvas can reach it.
 *
 * One record, N protocols — deliberately not a `publishedToMcp` flag beside a
 * `publishedToA2a` one. Every surface reads the same description and input
 * schema, and two copies drift until one is quietly wrong while each surface
 * still looks correct on its own.
 */

/**
 * The surfaces a capability can be published to.
 *
 * `mcp` and `a2a` are external — they make it callable from outside the
 * workspace. `chat` is INTERNAL: it only lets a ChatMode prompt in "Use
 * existing" mode route to it, and exposes nothing outside. Named for
 * publication rather than externality for exactly that reason.
 */
export type PublicationProtocol = 'mcp' | 'a2a' | 'chat';

/** Crews and flows are equal citizens externally; only the engine path differs. */
export type PublishableEntity = 'crew' | 'flow';

/** What the publish form sends. */
export interface PublicationRequest {
  /**
   * The MCP tool name / A2A skill id. Lowercase, digits and underscores — it is
   * a wire identifier external clients pin, not a display name.
   */
  external_name: string;
  /**
   * The ONLY thing a calling agent matches on, in either protocol. A vague one
   * means the capability is never selected.
   */
  description: string;
  protocols: PublicationProtocol[];
  /**
   * JSON Schema for declared inputs, including which are `required`.
   *
   * Authored here and nowhere else: the `{placeholder}` syntax carries no
   * optionality, so nothing downstream can tell a required `quarter` from a
   * cosmetic `format`. Without it the consumer treats every placeholder as
   * required and interrogates the user for each one.
   */
  input_schema?: PublicationInputSchema | null;
}

/**
 * The `input_schema` shape this app writes and reads.
 *
 * A deliberate subset of JSON Schema — object, flat string properties, a
 * `required` array. Wide enough for what a crew or flow actually takes as
 * input, narrow enough that both ends can rely on it.
 *
 * An ABSENT `required` is not an empty one: absent means nobody has said
 * (fall back to treating every detected variable as required), empty means the
 * publisher said nothing is required.
 */
export interface PublicationInputSchema {
  type: 'object';
  properties: Record<string, { type: 'string'; description?: string }>;
  required?: string[];
}

/** What the API returns for a publication. */
export interface PublicationResponse extends PublicationRequest {
  id: number;
  entity_type: PublishableEntity;
  entity_id: string;
  group_id: string;
  created_by_email?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
}
