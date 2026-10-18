import type { BuilderCanvas } from '../../../../app/sessions/builderCanvasStore';
import { CrewService } from '../../../../api/workflow/CrewService';

type SaveTab = Pick<BuilderCanvas, 'savedCrewId' | 'savedCrewName' | 'name'>;

export type CrewSaveTarget =
  | { kind: 'create' }
  | { kind: 'update'; crewId: string }
  | { kind: 'refuse'; message: string; crewId: string };

/**
 * Where "Save crew" under `name` may go for this canvas.
 *
 * A canvas bound to a catalog crew (opened in place from the catalog, a flow or
 * chat, or saved before) holds that crew's OWN agent and task rows. Saving it
 * as a new crew would point a second crew at the same rows, so a later task or
 * agent edit would silently change both, published ones included. So: the
 * bound crew's name updates that crew, another name is refused, and "Open as
 * copy" is how to start a new crew from it.
 */
export function crewSaveTarget(tab: SaveTab | null | undefined, name: string): CrewSaveTarget {
  if (!tab?.savedCrewId) return { kind: 'create' };
  const boundName = tab.savedCrewName || tab.name;
  if (name.trim() === boundName.trim()) return { kind: 'update', crewId: tab.savedCrewId };
  return {
    kind: 'refuse',
    crewId: tab.savedCrewId,
    message: `"${boundName}" is open from the catalog. Use Update catalog to save it, or Open as copy to start a new crew.`,
  };
}

/**
 * `crewSaveTarget`, except that a bound crew which has since been deleted no
 * longer holds the canvas: with no crew left to share the rows, a new name
 * creates a new crew. Only a definite 404 counts as deleted; when the check
 * itself fails, the refusal stands.
 */
export async function resolveCrewSaveTarget(
  tab: SaveTab | null | undefined,
  name: string,
): Promise<CrewSaveTarget> {
  const target = crewSaveTarget(tab, name);
  if (target.kind !== 'refuse') return target;
  try {
    await CrewService.getCrew(target.crewId);
    return target;
  } catch (error) {
    const status = (error as { response?: { status?: number } })?.response?.status;
    return status === 404 ? { kind: 'create' } : target;
  }
}
