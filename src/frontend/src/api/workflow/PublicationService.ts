import { apiClient as API } from '../../config/api/ApiConfig';
import {
  PublicationRequest,
  PublicationResponse,
  PublishableEntity,
} from '../../types/workflow/publication';

/**
 * Publishing crews and flows — to external agents (MCP / A2A) and to chat.
 *
 * One service for both entity kinds, because the backend is one table: only the
 * URL segment differs. A separate CrewPublicationService and
 * FlowPublicationService would be two places to fix the day the payload changes.
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
   * The ids of every published crew or flow in this workspace.
   *
   * Reads the workspace's own /publications endpoint, NOT the MCP tool surface.
   * An MCP tool result is shaped for a calling agent — a name and a description,
   * which is what it selects on — and carries no entity ids, so filtering it for
   * ids silently produced an empty set and every card looked unpublished.
   *
   * One request for the whole catalogue rather than one per card.
   */
  static async listPublishedIds(entity: PublishableEntity): Promise<string[]> {
    const response = await API.get<PublicationResponse[]>('/publications', {
      params: { entity_type: entity },
    });
    return (response.data ?? []).map((p) => String(p.entity_id)).filter(Boolean);
  }

  /**
   * The entity ids of everything published to chat — what "Use existing" can
   * route to, and what the rail's Catalog section lists.
   *
   * Ids only. The router does its own group-scoped read server-side, so the
   * client never needs the descriptions; asking for them would put every
   * capability's prose on the wire to draw a list of names.
   */
  static async listChatPublished(): Promise<string[]> {
    const response = await API.get<PublicationResponse[]>('/publications', {
      params: { protocol: 'chat' },
    });
    return (response.data ?? []).map((p) => String(p.entity_id)).filter(Boolean);
  }

  /** Withdraw from every surface, internal and external. */
  static async unpublish(entity: PublishableEntity, id: string): Promise<void> {
    await API.delete(this.basePath(entity, id));
  }
}

export default PublicationService;
