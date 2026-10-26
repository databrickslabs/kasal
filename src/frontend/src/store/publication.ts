import { create } from 'zustand';

import { PublicationService } from '../api/workflow/PublicationService';
import { PublishableEntity } from '../types/workflow/publication';

interface PublicationState {
  /** Ids of crews exposed to external agents. */
  publishedCrewIds: Set<string>;
  /** Ids of flows exposed to external agents. */
  publishedFlowIds: Set<string>;
  loaded: boolean;

  /** Refresh both sets from the server. Best-effort; never throws. */
  refresh: () => Promise<void>;
  /** Record a change made in the publish dialog, without a round trip. */
  setPublished: (entity: PublishableEntity, id: string, published: boolean) => void;
  isPublished: (entity: PublishableEntity, id: string) => boolean;
}

/**
 * Which crews and flows are exposed outside this workspace.
 *
 * A store rather than component state because MORE THAN ONE view needs it:
 * the crew/flow catalogue in CrewFlowDialog and the standalone FlowDialog both
 * draw a Published marker, and a crew published in one must not still look
 * unpublished in the other. Two copies of this state is how that happens.
 *
 * Not persisted. Publication lives on the server and is group-scoped; a cached
 * copy surviving a reload — or a workspace switch — would show one tenant's
 * publication state over another's.
 */
export const usePublicationStore = create<PublicationState>((set, get) => ({
  publishedCrewIds: new Set(),
  publishedFlowIds: new Set(),
  loaded: false,

  refresh: async () => {
    try {
      const [crewIds, flowIds] = await Promise.all([
        PublicationService.listPublishedIds('crew'),
        PublicationService.listPublishedIds('flow'),
      ]);
      set({
        publishedCrewIds: new Set(crewIds),
        publishedFlowIds: new Set(flowIds),
        loaded: true,
      });
    } catch {
      // Best-effort: the catalogue must open even when the external-publication
      // surface is unavailable. An unmarked card is a far better failure than a
      // catalogue that will not render.
      set({ loaded: true });
    }
  },

  setPublished: (entity, id, published) => {
    const key = entity === 'flow' ? 'publishedFlowIds' : 'publishedCrewIds';
    const next = new Set(get()[key]);
    if (published) next.add(String(id));
    else next.delete(String(id));
    set({ [key]: next } as Pick<PublicationState, typeof key>);
  },

  isPublished: (entity, id) => {
    const ids = entity === 'flow' ? get().publishedFlowIds : get().publishedCrewIds;
    return ids.has(String(id));
  },
}));
