/**
 * Publishing a crew or flow to callers OUTSIDE this Kasal workspace.
 *
 * One record, N protocols — deliberately not a `publishedToMcp` flag beside a
 * `publishedToA2a` one. Both surfaces read the same description and input
 * schema, and two copies drift until one is quietly wrong while each surface
 * still looks correct on its own.
 */

/** The external surfaces a capability can be exposed over. */
export type ExternalProtocol = 'mcp' | 'a2a';

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
  protocols: ExternalProtocol[];
  /** JSON Schema for declared inputs. Optional, but a tool without one is blunt. */
  input_schema?: Record<string, unknown> | null;
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
