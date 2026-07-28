import { apiClient as API } from '../../config/api/ApiConfig';
import {
  PublicationRequest,
  PublicationResponse,
  PublishableEntity,
} from '../../types/workflow/publication';

/**
 * Publishing crews and flows to external agents (MCP / A2A).
 *
 * One service for both, because the backend is one table: only the URL segment
 * differs. A separate CrewPublicationService and FlowPublicationService would be
 * two places to fix the day the payload changes.
 */
export class PublicationService {
  private static basePath(entity: PublishableEntity, id: string): string {
    return `/${entity === 'flow' ? 'flows' : 'crews'}/${id}/publish`;
  }

  /**
   * The publication for a crew or flow, or null when it is not published.
   *
   * Not-published is the normal case, not an error — nothing is exposed until
   * someone chooses to expose it — so a 404 returns null rather than throwing
   * and forcing every caller to catch.
   */
  static async get(
    entity: PublishableEntity,
    id: string,
  ): Promise<PublicationResponse | null> {
    try {
      const response = await API.get<PublicationResponse>(this.basePath(entity, id));
      return response.data;
    } catch (error: unknown) {
      const status = (error as { response?: { status?: number } })?.response?.status;
      if (status === 404) return null;
      throw error;
    }
  }

  /** Publish, or update an existing publication. Idempotent by entity. */
  static async publish(
    entity: PublishableEntity,
    id: string,
    publication: PublicationRequest,
  ): Promise<PublicationResponse> {
    const response = await API.post<PublicationResponse>(
      this.basePath(entity, id),
      publication,
    );
    return response.data;
  }

  /**
   * The ids of every published crew or flow the caller can see.
   *
   * One request for the whole catalogue rather than one per card — a list of
   * fifty crews should not become fifty round trips just to draw an icon.
   * Reads the MCP capability listing, which is the same group-scoped query the
   * A2A card renders from, so the catalogue cannot disagree with what is
   * actually exposed.
   */
  static async listPublishedIds(entity: PublishableEntity): Promise<string[]> {
    const response = await API.post<{
      content?: { crews?: { entity_type?: string; entity_id?: string }[] };
    }>('/mcp/v1/tools/call', { name: 'list_crews', arguments: {} });

    const published = response.data?.content?.crews ?? [];
    return published
      .filter((c) => (c.entity_type ?? 'crew') === entity)
      .map((c) => String(c.entity_id ?? ''))
      .filter(Boolean);
  }

  /** Withdraw from every external surface. */
  static async unpublish(entity: PublishableEntity, id: string): Promise<void> {
    await API.delete(this.basePath(entity, id));
  }
}

export default PublicationService;
