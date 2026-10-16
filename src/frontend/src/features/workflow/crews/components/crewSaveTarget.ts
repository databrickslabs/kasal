import type { BuilderCanvas } from '../../../../app/sessions/builderCanvasStore';

export type CrewSaveTarget =
  | { kind: 'create' }
  | { kind: 'update'; crewId: string }
  | { kind: 'refuse'; message: string };

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
export function crewSaveTarget(
  tab: Pick<BuilderCanvas, 'savedCrewId' | 'savedCrewName' | 'name'> | null | undefined,
  name: string,
): CrewSaveTarget {
  if (!tab?.savedCrewId) return { kind: 'create' };
  const boundName = tab.savedCrewName || tab.name;
  if (name.trim() === boundName.trim()) return { kind: 'update', crewId: tab.savedCrewId };
  return {
    kind: 'refuse',
    message: `"${boundName}" is open from the catalog. Use Update catalog to save it, or Open as copy to start a new crew.`,
  };
}
